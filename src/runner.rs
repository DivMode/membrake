//! The loop that connects the pieces: sample, ask the engine, carry out its actions, record.
//! Also `status`, which shows what the loop would be looking at.
//!
//! One rule orders everything here: logging never stops braking. A signal is sent first and
//! described afterwards, every log write ignores errors, the recorder is optional, and a
//! panic thaws whatever is frozen before the process exits.

use crate::act::{self, ActError, FrozenState, Recovered, State};
use crate::engine::{Action, Engine, Policy};
use crate::jobs::{self, Job, Rules};
use crate::recorder::{Recorder, describe};
use crate::signals::{self, KERNEL_LIMIT, Signals};
use std::io::Write;
use std::panic::{AssertUnwindSafe, catch_unwind};
use std::path::{Path, PathBuf};

const INTERVAL: f64 = 1.0;
/// Say a persistent problem at most this often (seconds).
const WARN_EVERY: f64 = 60.0;
/// If the signals cannot be read for this long (seconds) while jobs are frozen, resume them
/// all: never leave work frozen without a working brake.
const FAILSAFE_AFTER: f64 = 60.0;
/// A SIGSTOP takes a moment to show in the process table; do not judge a fresh freeze.
const STOP_GRACE: f64 = 2.0;

pub struct RunOptions {
    pub policy: Policy,
    pub rules: Rules,
    /// Grace between SIGTERM and SIGKILL for a non-emergency kill.
    pub term_seconds: f64,
    pub state_dir: PathBuf,
    pub log_dir: PathBuf,
    pub on_event: Option<String>,
    /// Log every decision and send no signals.
    pub dry_run: bool,
}

/// Best-effort stderr line. Not `eprintln!`, which panics when stderr is closed.
fn stderr_line(message: &str) {
    let _ = writeln!(std::io::stderr(), "membrake: {message}");
}

/// Runs `body`. If it panics, resumes everything recorded in `state_dir` before reporting
/// the panic: a bug must not leave anyone's work frozen.
pub fn guarded(
    state_dir: Option<&Path>,
    body: impl FnOnce() -> Result<(), String>,
) -> Result<(), String> {
    if let Ok(result) = catch_unwind(AssertUnwindSafe(body)) {
        return result;
    }
    let outcome = match state_dir.map(act::recover_dir) {
        None => "nothing was frozen (dry run)".to_string(),
        Some(Ok(report)) => {
            let resumed = report
                .iter()
                .filter(|(_, o)| *o == Recovered::Resumed)
                .count();
            format!("resumed {resumed} frozen job(s)")
        }
        Some(Err(e)) => format!("could not resume the frozen jobs: {e}"),
    };
    Err(format!("membrake panicked; {outcome}"))
}

struct Runner {
    opts: RunOptions,
    engine: Engine,
    rec: Recorder,
    /// None in a dry run: nothing is frozen, so nothing is recorded.
    state: Option<State>,
    last_stderr: f64,
}

pub fn run(opts: RunOptions) -> Result<(), String> {
    // First, before any file is opened: the fallback re-executes the process.
    let protection = act::protect_self();
    act::install_signal_handlers();
    let _lock = if opts.dry_run {
        None
    } else {
        Some(act::lock_instance(&opts.state_dir)?)
    };
    let state_dir = (!opts.dry_run).then(|| opts.state_dir.clone());
    guarded(state_dir.as_deref(), || run_loop(opts, &protection))
}

/// The kind and text of an event for each recovered record.
fn thaw_lines(report: Vec<(act::Record, Recovered)>, what: &str) -> Vec<(&'static str, String)> {
    report
        .into_iter()
        .map(|(r, outcome)| match outcome {
            Recovered::Resumed => (
                "resume",
                format!("{what}: group {}: {}", r.pgid, r.command),
            ),
            Recovered::Skipped => (
                "warn",
                format!(
                    "{what}: group {} is gone or its pgid now belongs to a newer group; not signalled",
                    r.pgid
                ),
            ),
            Recovered::Failed(e) => (
                "warn",
                format!("{what}: cannot resume group {}: {e}", r.pgid),
            ),
        })
        .collect()
}

fn run_loop(opts: RunOptions, protection: &str) -> Result<(), String> {
    // State first, and thaw whatever a previous run left frozen, before anything that could
    // fail for an unrelated reason such as a full log disk.
    let mut startup: Vec<(&'static str, String)> = Vec::new();
    let state = if opts.dry_run {
        None
    } else {
        let mut state = State::open(&opts.state_dir).map_err(|e| {
            format!(
                "cannot open the state directory {}: {e}",
                opts.state_dir.display()
            )
        })?;
        if !state.unparsed().is_empty() {
            startup.push((
                "warn",
                format!(
                    "{} line(s) of the state file in {} cannot be read; they are kept, not deleted",
                    state.unparsed().len(),
                    opts.state_dir.display()
                ),
            ));
        }
        match act::recover(&mut state) {
            Ok(report) => startup.extend(thaw_lines(report, "recovered at start")),
            Err(e) => startup.push((
                "warn",
                format!("recovery could not update the state file: {e}"),
            )),
        }
        Some(state)
    };

    // The recorder is optional: without it we keep braking, and each write retries.
    let mut rec = Recorder::new(&opts.log_dir);
    if let Err(e) = rec.ensure_open() {
        stderr_line(&format!(
            "cannot open the log directory {}: {e}; braking without a recorder and retrying on each write",
            opts.log_dir.display()
        ));
    }
    let engine = Engine::new(opts.policy.clone());
    let mut r = Runner {
        opts,
        engine,
        rec,
        state,
        last_stderr: f64::NEG_INFINITY,
    };
    let wall = signals::wall();
    for (kind, text) in &startup {
        r.rec.event(wall, kind, text);
    }
    let start = r.start_line(protection);
    r.rec.event(wall, "start", &start);
    r.rec.sync_events();

    let mut next_tick = signals::uptime();
    let mut unreadable_since: Option<f64> = None;
    let mut failsafe_done = false;
    let mut last_read_warning = f64::NEG_INFINITY;
    while !act::shutdown_requested() {
        match signals::read() {
            Ok(s) => {
                unreadable_since = None;
                failsafe_done = false;
                r.tick(&s);
            }
            Err(e) => {
                let now = signals::uptime();
                let since = *unreadable_since.get_or_insert(now);
                if now - last_read_warning >= WARN_EVERY {
                    last_read_warning = now;
                    r.rec.event(
                        signals::wall(),
                        "warn",
                        &format!("cannot read signals: {e}"),
                    );
                    r.rec.sync_events();
                }
                if now - since >= FAILSAFE_AFTER && !failsafe_done {
                    failsafe_done = true;
                    r.thaw("resumed because the signals were unreadable for 60 s");
                }
            }
        }
        next_tick = (next_tick + INTERVAL).max(signals::uptime());
        act::sleep_until(next_tick);
    }
    r.thaw("resumed on shutdown");
    r.rec.event(signals::wall(), "stop", "membrake stopped");
    r.rec.sync_events();
    Ok(())
}

impl Runner {
    fn start_line(&self, protection: &str) -> String {
        let p = &self.opts.policy;
        format!(
            "membrake {} pid {}{}: freezes the newest job of at least {:.1}GB at {:.0}% compressor occupancy, \
             kills at {:.0}%; {protection}",
            env!("CARGO_PKG_VERSION"),
            std::process::id(),
            if self.opts.dry_run {
                " (dry run, no signals are sent)"
            } else {
                ""
            },
            p.min_job_bytes as f64 / (1u64 << 30) as f64,
            p.freeze_occupancy * 100.0,
            p.kill_occupancy_now * 100.0,
        )
    }

    /// Say something on stderr, rarely: it is for problems with the logs themselves.
    fn warn_stderr(&mut self, now: f64, message: &str) {
        if now - self.last_stderr >= WARN_EVERY {
            self.last_stderr = now;
            stderr_line(message);
        }
    }

    fn tick(&mut self, s: &Signals) {
        let mut shown: Option<Vec<Job>> = None;
        let rules = &self.opts.rules;
        let min = self.opts.policy.min_job_bytes;
        let step = self.engine.step(s, || {
            let jobs = jobs::scan(rules, min);
            shown = Some(jobs.clone());
            jobs
        });
        // Signals first; the sample and the events describe them afterwards.
        for action in &step.actions {
            self.perform(action, s);
        }
        self.check_frozen(s);
        if let Err(e) = self.rec.sample(s, &step, shown.as_deref()) {
            self.warn_stderr(s.time, &format!("cannot record the sample: {e}"));
        }
        self.rec.sync_events();
    }

    fn perform(&mut self, action: &Action, s: &Signals) {
        let (kind, text) = describe(action);
        let (job, immediate) = match action {
            Action::Stall { .. } | Action::Warn { .. } => {
                self.rec.event(s.wall, kind, &text);
                return;
            }
            Action::Freeze { job, .. } | Action::Resume { job } => (job, false),
            Action::Kill { job, immediate, .. } => (job, *immediate),
        };
        let Some(state) = self.state.as_mut() else {
            self.rec
                .event(s.wall, kind, &format!("dry run, would {kind}: {text}"));
            return;
        };
        // The signal goes out before anything is logged. Each arm yields whether the job is
        // in the state we wanted (a kill can time out waiting for the group to exit).
        let outcome: Result<bool, ActError> = match action {
            Action::Freeze { .. } => act::freeze(state, job, s.wall).map(|()| true),
            Action::Resume { .. } => act::resume(state, job).map(|()| true),
            _ => {
                let frozen_at = state.record(job.pgid).map(|r| r.frozen_at);
                let result = act::kill(job, immediate, self.opts.term_seconds, frozen_at);
                let _ = state.remove(job.pgid);
                // The wait was ours, not a stall, and the fill rate before it is stale.
                self.engine.resync();
                result
            }
        };
        match outcome {
            Ok(done) => {
                self.rec.event(s.wall, kind, &text);
                if !done {
                    self.rec.event(
                        signals::wall(),
                        "warn",
                        &format!("group {} still has processes after SIGKILL", job.pgid),
                    );
                }
                if let Some(command) = &self.opts.on_event
                    && let Err(e) = act::spawn_hook(command, kind, job)
                {
                    self.rec.event(
                        s.wall,
                        "warn",
                        &format!("cannot run the --on-event command: {e}"),
                    );
                }
            }
            Err(e) => {
                self.engine.forget(job.pgid);
                let why = match e {
                    ActError::Gone => "it has already exited".to_string(),
                    ActError::Io(e) => e.to_string(),
                };
                self.rec.event(
                    s.wall,
                    "warn",
                    &format!("cannot {kind} group {}: {why}", job.pgid),
                );
            }
        }
    }

    /// Forgets frozen jobs that are no longer frozen: someone continued them by hand, or they
    /// exited. Looks at the frozen groups only, so it costs nothing when there are none.
    fn check_frozen(&mut self, s: &Signals) {
        let Some(state) = self.state.as_mut() else {
            return;
        };
        let changed: Vec<(i32, FrozenState)> = self
            .engine
            .frozen()
            .iter()
            .filter(|f| s.time - f.since >= STOP_GRACE)
            .filter_map(|f| {
                let rec = state.record(f.job.pgid)?;
                match act::frozen_state(rec) {
                    FrozenState::Stopped => None,
                    other => Some((f.job.pgid, other)),
                }
            })
            .collect();
        for (pgid, why) in changed {
            self.engine.forget(pgid);
            let command = state
                .record(pgid)
                .map(|r| r.command.clone())
                .unwrap_or_default();
            let _ = state.remove(pgid);
            let (kind, what) = match why {
                FrozenState::ResumedOutside => ("resume", "resumed outside membrake"),
                _ => ("warn", "frozen job exited while frozen"),
            };
            self.rec
                .event(s.wall, kind, &format!("{what}: group {pgid}: {command}"));
        }
    }

    /// Resumes every job the state file records, reading the file afresh: it, not memory, is
    /// the truth after a crash or an outside edit.
    fn thaw(&mut self, what: &str) {
        let Some(state) = self.state.as_mut() else {
            return;
        };
        let result = state.reload().and_then(|()| act::recover(state));
        let wall = signals::wall();
        match result {
            Ok(report) => {
                for (kind, text) in thaw_lines(report, what) {
                    self.rec.event(wall, kind, &text);
                }
            }
            Err(e) => self.rec.event(
                wall,
                "warn",
                &format!("{what}: cannot read or update the state file: {e}"),
            ),
        }
        self.engine.forget_all();
        self.rec.sync_events();
    }
}

// ---------------------------------------------------------------------------------------
// status

fn age(seconds: f64) -> String {
    let s = seconds.max(0.0) as u64;
    match s {
        0..=119 => format!("{s}s"),
        120..=7199 => format!("{}m", s / 60),
        _ => format!("{}h{:02}m", s / 3600, s % 3600 / 60),
    }
}

fn gb(bytes: u64) -> String {
    format!("{:.1}GB", bytes as f64 / (1u64 << 30) as f64)
}

/// Prints the live signals, the jobs frozen according to the state file, and the next
/// candidates in the order membrake would freeze them. Needs no privileges; it lists what
/// the calling user can see.
pub fn status(policy: &Policy, rules: &Rules, state_dir: &Path) -> Result<(), String> {
    let s = signals::read().map_err(|e| format!("cannot read the signals: {e}"))?;
    let tight = s.pressure > signals::Pressure::Normal || s.level <= policy.tight_level;
    println!(
        "compressor  {:.0}% full (segments {:.0}%, pages {:.0}%); the kernel gives up at {:.0}%",
        s.occupancy() * 100.0,
        s.segments * 100.0,
        s.pages * 100.0,
        KERNEL_LIMIT * 100.0
    );
    println!(
        "swap        {} used, {} headroom, {} RAM",
        gb(s.swap_used),
        gb(s.headroom),
        gb(s.ram)
    );
    println!(
        "memory      level {}%, pressure {:?}: {}",
        s.level,
        s.pressure,
        if tight {
            "tight, so the arms are live".to_string()
        } else {
            format!(
                "not tight, so the arms wait (they count at level <= {}% or raised pressure)",
                policy.tight_level
            )
        }
    );

    println!();
    match act::read_state(state_dir) {
        Ok((records, unparsed)) => {
            if records.is_empty() {
                println!("frozen      none");
            } else {
                println!("frozen      {} job(s), oldest freeze first:", records.len());
                for r in records {
                    println!(
                        "  group {:<7} {:>7}  frozen {} ago  {}",
                        r.pgid,
                        gb(r.bytes),
                        age(s.wall - r.frozen_at),
                        r.command
                    );
                }
            }
            if !unparsed.is_empty() {
                println!(
                    "            {} unreadable line(s) in the state file are kept as they are",
                    unparsed.len()
                );
            }
        }
        Err(e) => println!(
            "frozen      unknown: cannot read {}: {e}",
            state_dir.display()
        ),
    }

    let mut jobs = jobs::scan(rules, policy.min_job_bytes);
    // Freeze order: preferred first, then newest; protected users' jobs are never frozen.
    jobs.sort_by_key(|j| {
        (
            j.protected,
            std::cmp::Reverse(j.prefer),
            std::cmp::Reverse(j.start_us),
        )
    });
    println!();
    if jobs.is_empty() {
        println!(
            "candidates  none of at least {} are visible to this user",
            gb(policy.min_job_bytes)
        );
    } else {
        let shown = jobs.len().min(5);
        println!(
            "candidates  {shown} of {} in freeze order (newest first):",
            jobs.len()
        );
        println!(
            "  {:<10} {:>8} {:>7}  {:<7} {:<8} command",
            "tier", "size", "age", "group", "uid"
        );
        for j in jobs.iter().take(5) {
            let tier = if j.protected {
                "protected"
            } else if j.prefer {
                "prefer"
            } else {
                "normal"
            };
            println!(
                "  {:<10} {:>8} {:>7}  {:<7} {:<8} {}",
                tier,
                gb(j.bytes),
                age(s.wall - j.start_us as f64 / 1e6),
                j.pgid,
                j.uid,
                j.command
            );
        }
    }
    // SAFETY: getuid has no preconditions.
    let uid = unsafe { libc::getuid() };
    if uid != 0 {
        println!(
            "\nrunning as uid {uid}: other users' processes are not readable, so only this user's jobs are listed"
        );
    }
    Ok(())
}

pub fn own_pgid() -> i32 {
    // SAFETY: getpgrp has no preconditions.
    unsafe { libc::getpgrp() }
}

#[cfg(test)]
mod tests {
    use super::*;
    use crate::signals::Pressure;
    use crate::testutil::{Sleeper, scratch};
    use std::fs;

    fn signals_at(time: f64) -> Signals {
        Signals {
            time,
            wall: signals::wall(),
            segments: 0.65,
            pages: 0.0,
            swap_used: 0,
            headroom: 500 << 30,
            ram: 96 << 30,
            level: 35,
            pressure: Pressure::Normal,
        }
    }

    /// A runner whose engine has, through the real freeze path, frozen `job` at engine time
    /// 101 (two tight samples).
    fn runner_with_frozen(dir: &Path, job: &Job) -> Runner {
        let opts = RunOptions {
            policy: Policy::default(),
            rules: Rules::new(&[], &[], vec![], vec![], 0).unwrap(),
            term_seconds: 1.0,
            state_dir: dir.join("state"),
            log_dir: dir.join("log"),
            on_event: None,
            dry_run: false,
        };
        let mut r = Runner {
            engine: Engine::new(Policy::default()),
            rec: Recorder::new(&opts.log_dir),
            state: Some(State::open(&opts.state_dir).unwrap()),
            last_stderr: f64::NEG_INFINITY,
            opts,
        };
        for time in [100.0, 101.0] {
            let s = signals_at(time);
            let step = r.engine.step(&s, || vec![job.clone()]);
            for action in &step.actions {
                r.perform(action, &s);
            }
        }
        assert_eq!(r.engine.frozen().len(), 1, "the engine froze the job");
        r
    }

    fn events(dir: &Path) -> String {
        fs::read_to_string(dir.join("log/events.log")).unwrap_or_default()
    }

    /// Someone continues a frozen job by hand (`fg`, `kill -CONT`): membrake must stop
    /// treating it as frozen, or it would later "resume" it again or kill it as overdue.
    #[test]
    fn a_frozen_job_continued_by_hand_is_dropped() {
        let dir = scratch("outside");
        let sleeper = Sleeper::spawn();
        let job = sleeper.job(1);
        let mut r = runner_with_frozen(&dir, &job);
        assert!(sleeper.settles(true));

        r.check_frozen(&signals_at(110.0));
        assert_eq!(r.engine.frozen().len(), 1, "still stopped: still frozen");

        // SAFETY: signalling the group this test spawned.
        unsafe { libc::killpg(job.pgid, libc::SIGCONT) };
        assert!(sleeper.settles(false));
        r.check_frozen(&signals_at(111.0));
        assert!(r.engine.frozen().is_empty());
        assert!(r.state.as_ref().unwrap().records().is_empty());
        assert!(
            events(&dir).contains("resumed outside membrake"),
            "{}",
            events(&dir)
        );
        let _ = fs::remove_dir_all(&dir);
    }

    #[test]
    fn a_frozen_job_that_exited_is_dropped() {
        let dir = scratch("exited");
        let mut sleeper = Sleeper::spawn();
        let job = sleeper.job(1);
        let mut r = runner_with_frozen(&dir, &job);
        sleeper.die();
        r.check_frozen(&signals_at(110.0));
        assert!(r.engine.frozen().is_empty());
        assert!(r.state.as_ref().unwrap().records().is_empty());
        assert!(
            events(&dir).contains("exited while frozen"),
            "{}",
            events(&dir)
        );
        let _ = fs::remove_dir_all(&dir);
    }

    /// A kill is carried out, then logged; the record of the frozen job goes with it.
    #[test]
    fn a_kill_is_carried_out_and_the_frozen_record_dropped() {
        let dir = scratch("kill");
        let mut sleeper = Sleeper::spawn();
        let job = sleeper.job(1);
        let mut r = runner_with_frozen(&dir, &job);
        let s = signals_at(120.0);
        r.perform(
            &Action::Kill {
                job: job.clone(),
                immediate: true,
                reason: "test".into(),
            },
            &s,
        );
        assert!(sleeper.0.try_wait().unwrap().is_some(), "the group is dead");
        sleeper.die();
        assert!(
            r.state.as_ref().unwrap().records().is_empty(),
            "record dropped"
        );
        assert!(events(&dir).contains(" kill "), "{}", events(&dir));
        let _ = fs::remove_dir_all(&dir);
    }
}
