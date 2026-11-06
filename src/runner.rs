//! The loop that connects the pieces: sample, ask the engine, carry out its actions, record.
//! Also `status`, which shows what the loop would be looking at.
//!
//! One rule orders everything here: logging never stops braking. Every signal a tick sends is
//! sent first, the events that describe them are queued afterwards for a logger thread that
//! never makes this one wait, the recorder is optional, and a panic thaws whatever is frozen
//! before the process exits.
//!
//! Another orders the state file: a job is dropped from it only once it is confirmed gone or
//! continued. A resume that fails, or a group that cannot be inspected, leaves the record, and
//! every sample tries again.
//!
//! The queue (`broker`) is served from the same tick, after the engine has decided, so what it
//! admits is always judged by the state the brake has just left the machine in.

use crate::act::{self, ActError, FrozenState, Killed, Recovered, State};
use crate::broker::{self, Broker};
use crate::engine::{Action, Engine, Policy};
use crate::jobs::{self, Inspector, Job, Rules};
use crate::recorder::{Recorder, describe};
use crate::signals::{self, KERNEL_LIMIT, Signals};
use crate::source::{SignalSource, Unreadable};
use std::io::Write;
use std::panic::{AssertUnwindSafe, catch_unwind};
use std::path::{Path, PathBuf};
use std::time::Duration;

/// Say a persistent problem at most this often (seconds).
const WARN_EVERY: f64 = 60.0;
/// If the signals cannot be read for this long (seconds) while jobs are frozen, resume them
/// all: never leave work frozen without a working brake.
const FAILSAFE_AFTER: f64 = 60.0;
/// A SIGSTOP takes a moment to show in the process table; do not judge a fresh freeze.
const STOP_GRACE: f64 = 2.0;
/// How long, in total, a record membrake cannot resolve may hold the queue; see `Holds`.
const UNRESOLVED_HOLD: f64 = 300.0;

/// A record that cannot be resolved: membrake cannot tell whether its group is still the
/// recorded one, or cannot continue it, or has not got all of it on disk.
struct Unresolved {
    pgid: i32,
    /// Seconds it was unresolved, over all the times it was.
    total: f64,
    /// When it was last seen unresolved, if that was on the latest sample. The time since
    /// then counts, up to the sample on which it was not unresolved, which ends the span:
    /// every interval that began unresolved is in the total.
    last_seen: Option<f64>,
    /// It has used up `UNRESOLVED_HOLD`, and that was said.
    spent: bool,
}

/// What holds the admission queue back: frozen work, so that it resumes before new work
/// starts.
///
/// A frozen job the engine tracks and whose last check read it stopped holds the queue until
/// it is resumed or killed, however long the engine takes to get to it (one job per settle
/// period, and only after memory has been calm for a while). That is no one's mistake, so it
/// has no timeout; a window computed from the number of jobs left, which shrinks as they are
/// resumed, released the queue before the last of twenty came back.
///
/// A record membrake cannot resolve is different: a group it can no longer inspect, a
/// SIGCONT that keeps failing, a record not completely on disk. It is kept and tried again on
/// every sample, but it holds the queue for `UNRESOLVED_HOLD` seconds in total, over all the
/// times it was unresolved. How long memory has been calm, or tight again, does not come into
/// it: a sample of pressure every few minutes must not renew a hold that has failed.
struct Holds {
    unresolved: Vec<Unresolved>,
}

impl Holds {
    /// Whether the queue is held at `now`. `healthy`: a frozen job is tracked and as far as
    /// anyone knows stopped. `stuck`: the records that are unresolved on this sample.
    /// `exists` says which records there still are; `spent` is called, once ever for each,
    /// with the record whose hold has run out.
    fn update(
        &mut self,
        now: f64,
        healthy: bool,
        stuck: impl Iterator<Item = i32>,
        exists: impl Fn(i32) -> bool,
        mut spent: impl FnMut(i32),
    ) -> bool {
        let mut held = healthy;
        for pgid in stuck {
            let at = self
                .unresolved
                .iter()
                .position(|u| u.pgid == pgid)
                .unwrap_or_else(|| {
                    self.unresolved.push(Unresolved {
                        pgid,
                        total: 0.0,
                        last_seen: None,
                        spent: false,
                    });
                    self.unresolved.len() - 1
                });
            let u = &mut self.unresolved[at];
            if let Some(since) = u.last_seen {
                u.total += (now - since).max(0.0);
            }
            u.last_seen = Some(now);
            if u.total < UNRESOLVED_HOLD {
                held = true;
            } else if !std::mem::replace(&mut u.spent, true) {
                spent(pgid);
            }
        }
        self.unresolved.retain_mut(|u| {
            // Unresolved when last seen, and not on this sample: that interval was unresolved
            // too, and is the last of the span.
            if u.last_seen != Some(now)
                && let Some(since) = u.last_seen.take()
            {
                u.total += (now - since).max(0.0);
            }
            exists(u.pgid)
        });
        held
    }
}

/// Consecutive checks a frozen job must look continued before membrake believes it. A
/// member blocked in the kernel (a page fault served from swap, while memory is tight)
/// only reaches the stopped state when the wait ends, which can take seconds; on
/// 2026-11-05 a 22-process job read "resumed outside" 3 s after its freeze, with memory
/// tight, and no one is known to have continued it.
const OUTSIDE_CHECKS: u32 = 3;
/// How long shutdown waits for the logger thread to write the last events.
const SHUTDOWN_FLUSH: Duration = Duration::from_secs(2);

/// Where the daemon keeps its files, and whether it may signal anything. These alone are
/// needed to recover frozen jobs, so they are parsed before anything else on the command line
/// is checked.
pub struct Location {
    pub state_dir: PathBuf,
    pub log_dir: PathBuf,
    /// Log every decision and send no signals.
    pub dry_run: bool,
    /// Where `membrake exec` asks for its turn. None: no queue is served.
    pub socket: Option<PathBuf>,
}

pub struct RunOptions {
    pub policy: Policy,
    pub rules: Rules,
    /// Grace between SIGTERM and SIGKILL for a non-emergency kill.
    pub term_seconds: f64,
    pub on_event: Option<String>,
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
        Some(recovery) => {
            let resumed = recovery
                .report
                .iter()
                .filter(|(_, o)| *o == Recovered::Resumed)
                .count();
            match recovery.problem {
                None => format!("resumed {resumed} frozen job(s)"),
                Some(e) => format!(
                    "resumed {resumed} frozen job(s); the state file could not be read or updated: {e}"
                ),
            }
        }
    };
    Err(format!("membrake panicked; {outcome}"))
}

/// What a tick tells the logger once its signals are sent.
struct Event {
    wall: f64,
    kind: &'static str,
    text: String,
    /// Run the `--on-event` command for this job, too.
    hook: Option<Job>,
}

impl Event {
    fn note(wall: f64, kind: &'static str, text: String) -> Event {
        Event {
            wall,
            kind,
            text,
            hook: None,
        }
    }
}

pub struct Runner {
    opts: RunOptions,
    engine: Engine,
    rec: Recorder,
    /// None in a dry run: nothing is frozen, so nothing is recorded.
    state: Option<State>,
    last_unresolved: f64,
    /// Frozen groups seen not fully stopped, and on how many consecutive checks.
    outside: Vec<(i32, u32)>,
    /// Frozen groups whose last check could not tell what became of them.
    unknown: Vec<i32>,
    /// What keeps the admission queue waiting, and whether it does as of the last sample.
    holds: Holds,
    held: bool,
    /// Buffers for looking at the frozen groups, allocated once.
    inspector: Inspector,
    /// Events of the tick in progress, written after all of its signals are sent.
    events: Vec<Event>,
    /// The admission queue, if one is served.
    queue: Option<Broker>,
}

/// Runs the loop on the readings of `source` (the kernel, except in tests).
///
/// Only the `location` is known when this starts. The instance lock is taken and the jobs a
/// previous run left frozen are thawed first; `configure` then checks the rest of the command
/// line (a policy that does not parse, an account that no longer exists) and yields the
/// options and the source. If it fails, the error is the result, after the recovery.
pub fn run<S: SignalSource>(
    location: Location,
    configure: impl FnOnce() -> Result<(RunOptions, S), String>,
) -> Result<(), String> {
    // First, before any file is opened: the fallback re-executes the process.
    let protection = act::protect_self();
    act::install_signal_handlers();
    let _lock = if location.dry_run {
        None
    } else {
        Some(act::lock_instance(&location.state_dir)?)
    };
    let state_dir = (!location.dry_run).then(|| location.state_dir.clone());
    guarded(state_dir.as_deref(), || {
        run_loop(&location, &protection, configure)
    })
}

/// The kind and text of an event for each recovered record, and whether it is a failure.
fn thaw_lines(
    report: Vec<(act::Record, Recovered)>,
    what: &str,
) -> Vec<(&'static str, String, bool)> {
    report
        .into_iter()
        .map(|(r, outcome)| match outcome {
            Recovered::Resumed => (
                "resume",
                format!("{what}: group {}: {}", r.pgid, r.command),
                false,
            ),
            Recovered::Skipped => (
                "warn",
                format!(
                    "{what}: group {} is gone or its pgid now belongs to a newer group; not signalled",
                    r.pgid
                ),
                false,
            ),
            Recovered::Failed(e) => (
                "warn",
                format!(
                    "{what}: cannot resume group {}: {e}; it stays recorded and is tried again",
                    r.pgid
                ),
                true,
            ),
        })
        .collect()
}

fn run_loop<S: SignalSource>(
    location: &Location,
    protection: &str,
    configure: impl FnOnce() -> Result<(RunOptions, S), String>,
) -> Result<(), String> {
    // State first, and thaw whatever a previous run left frozen, before anything that could
    // fail for an unrelated reason: a full log disk, a flag that no longer parses, an account
    // that has since been deleted.
    let mut startup: Vec<(&'static str, String)> = Vec::new();
    let state = if location.dry_run {
        None
    } else {
        let mut state = State::open(&location.state_dir).map_err(|e| {
            format!(
                "cannot open the state directory {}: {e}",
                location.state_dir.display()
            )
        })?;
        if !state.unparsed().is_empty() {
            startup.push((
                "warn",
                format!(
                    "{} line(s) of the state file in {} cannot be read; they are kept, not deleted",
                    state.unparsed().len(),
                    location.state_dir.display()
                ),
            ));
        }
        match act::recover(&mut state) {
            Ok(report) => startup.extend(
                thaw_lines(report, "recovered at start")
                    .into_iter()
                    .map(|(kind, text, _)| (kind, text)),
            ),
            Err(e) => startup.push((
                "warn",
                format!("recovery could not update the state file: {e}"),
            )),
        }
        Some(state)
    };

    // The recorder is optional: without it we keep braking, and each write retries.
    let mut rec = Recorder::new(&location.log_dir);
    if let Err(e) = rec.ensure_open() {
        stderr_line(&format!(
            "cannot open the log directory {}: {e}; braking without a recorder and retrying on each write",
            location.log_dir.display()
        ));
    }
    let wall = signals::wall();
    for (kind, text) in &startup {
        rec.event(wall, kind, text);
    }
    let (opts, mut source) = match configure() {
        Ok(configured) => configured,
        Err(e) => {
            rec.event(signals::wall(), "warn", &format!("not starting: {e}"));
            rec.sync_events();
            rec.flush(SHUTDOWN_FLUSH);
            return Err(e);
        }
    };
    let mut r = Runner::new(opts, state, rec);
    let start = r.start_line(protection);
    r.rec.event(wall, "start", &start);
    if let Some(socket) = &location.socket {
        // Without a queue the brake is unchanged and every `membrake exec` runs at once.
        match Broker::start(broker::Options {
            socket: socket.clone(),
            state_dir: location.state_dir.clone(),
            dry_run: location.dry_run,
        }) {
            Ok(queue) => r.serve_queue(queue),
            Err(e) => r.rec.event(
                signals::wall(),
                "warn",
                &format!("not serving the queue (clients run at once): {e}"),
            ),
        }
    }
    r.rec.sync_events();

    let mut unreadable_since: Option<f64> = None;
    let mut failsafe_fired = false;
    let mut last_read_warning = f64::NEG_INFINITY;
    let mut broken_source = None;
    while !act::shutdown_requested() {
        match source.next_reading() {
            Ok(Some(Ok(s))) => {
                unreadable_since = None;
                failsafe_fired = false;
                r.tick(&s);
            }
            Ok(Some(Err(Unreadable { time: now, error }))) => {
                let since = *unreadable_since.get_or_insert(now);
                if now - last_read_warning >= WARN_EVERY {
                    last_read_warning = now;
                    r.rec.event(
                        signals::wall(),
                        "warn",
                        &format!("cannot read signals: {error}"),
                    );
                    r.rec.sync_events();
                }
                // Once, and then on every unreadable sample until nothing is left recorded:
                // a job whose SIGCONT failed must not stay frozen behind a blind brake.
                if now - since >= FAILSAFE_AFTER && (!failsafe_fired || r.has_records()) {
                    failsafe_fired = true;
                    r.thaw("resumed because the signals were unreadable for 60 s", now);
                }
            }
            Ok(None) => break,
            Err(e) => {
                broken_source = Some(e);
                break;
            }
        }
    }
    r.thaw("resumed on shutdown", signals::uptime());
    r.rec.event(signals::wall(), "stop", "membrake stopped");
    r.rec.sync_events();
    r.rec.flush(SHUTDOWN_FLUSH);
    broken_source.map_or(Ok(()), Err)
}

impl Runner {
    pub fn new(opts: RunOptions, state: Option<State>, rec: Recorder) -> Runner {
        let engine = Engine::new(opts.policy.clone());
        Runner {
            opts,
            engine,
            rec,
            state,
            last_unresolved: f64::NEG_INFINITY,
            outside: Vec::new(),
            unknown: Vec::new(),
            holds: Holds {
                unresolved: Vec::new(),
            },
            held: false,
            inspector: Inspector::new(),
            events: Vec::new(),
            queue: None,
        }
    }

    /// Serves the admission queue from this runner's tick.
    pub fn serve_queue(&mut self, queue: Broker) {
        self.queue = Some(queue);
    }

    fn start_line(&self, protection: &str) -> String {
        let p = &self.opts.policy;
        format!(
            "membrake {} pid {}{}: freezes the newest job of at least {:.1}GB at {:.0}% compressor occupancy, \
             kills at {:.0}%; {protection}",
            env!("CARGO_PKG_VERSION"),
            std::process::id(),
            if self.state.is_none() {
                " (dry run, no signals are sent)"
            } else {
                ""
            },
            p.min_job_bytes as f64 / (1u64 << 30) as f64,
            p.freeze_occupancy * 100.0,
            p.kill_occupancy_now * 100.0,
        )
    }

    /// Whether any frozen job is still recorded.
    pub fn has_records(&self) -> bool {
        self.state.as_ref().is_some_and(|s| !s.records().is_empty())
    }

    /// Whether frozen work held the admission queue back on the last sample.
    pub fn queue_held(&self) -> bool {
        self.held
    }

    /// The admission queue, if one is served.
    pub fn queue(&self) -> Option<&Broker> {
        self.queue.as_ref()
    }

    /// Decides whether frozen work holds the queue (see `Holds`) and says when a record
    /// has used up its hold.
    fn update_hold(&mut self, s: &Signals) {
        let (records, pending) = match &self.state {
            Some(state) => (state.records(), state.pending()),
            None => (&[][..], &[][..]),
        };
        let engine = &self.engine;
        let tracked = |pgid: i32| engine.frozen().iter().any(|f| f.job.pgid == pgid);
        self.unknown.retain(|&pgid| tracked(pgid));
        let unknown = &self.unknown;
        let unresolved = |pgid: i32| unknown.contains(&pgid) || pending.contains(&pgid);
        let healthy = engine.frozen().iter().any(|f| !unresolved(f.job.pgid));
        // A record the engine is not tracking is one a resume or a kill could not finish.
        let stuck = records
            .iter()
            .map(|r| r.pgid)
            .filter(|&pgid| !tracked(pgid) || unresolved(pgid));
        let events = &mut self.events;
        self.held = self.holds.update(
            s.time,
            healthy,
            stuck,
            |pgid| records.iter().any(|r| r.pgid == pgid),
            |pgid| {
                let text = format!(
                    "group {pgid} has been unresolved for {UNRESOLVED_HOLD} s; it stays recorded and is tried again on every sample, but no longer holds the queue"
                );
                events.push(Event::note(s.wall, "warn", text));
            },
        );
    }

    /// Takes up again the stopping of groups that are not completely recorded and stopped:
    /// the members they have now are written down, and then stopped.
    fn complete_records(&mut self, s: &Signals) {
        let Some(state) = self.state.as_mut() else {
            return;
        };
        let rules = &self.opts.rules;
        for pgid in state.finish_pending(&mut self.inspector, &mut |pgid| jobs::spared(rules, pgid))
        {
            let text = format!("group {pgid}: every member is recorded and stopped now (a retry)");
            self.events.push(Event::note(s.wall, "warn", text));
        }
    }

    /// One sample: ask the engine, send every signal that leads to, and only then describe
    /// them. Allocation-free when there is nothing to do, frozen jobs included.
    pub fn tick(&mut self, s: &Signals) {
        let mut shown: Option<Vec<Job>> = None;
        // When the process table was read: the bound of the identity of every group this
        // tick signals. Taken once, here, never worked out at the moment a signal goes out.
        let mut scanned = s.wall;
        let rules = &self.opts.rules;
        let min = self.opts.policy.min_job_bytes;
        let step = self.engine.step(s, || {
            scanned = signals::wall();
            let jobs = jobs::scan(rules, min);
            shown = Some(jobs.clone());
            jobs
        });
        for action in &step.actions {
            self.perform(action, s, scanned);
        }
        self.complete_records(s);
        self.check_frozen(s);
        self.retry_unresolved(s);
        self.update_hold(s);
        if let Some(queue) = self.queue.as_mut() {
            for (kind, text) in queue.tick(s, step.tight, self.held) {
                self.events.push(Event::note(s.wall, kind, text));
            }
        }
        // All signals of this tick are sent. Now say so.
        self.log_events();
        self.rec.sample(s, &step, shown.as_deref());
        self.rec.sync_events();
    }

    /// Queues the tick's events for the logger and runs the `--on-event` command for those
    /// that have a job. Neither waits for the log.
    fn log_events(&mut self) {
        let mut events = std::mem::take(&mut self.events);
        for e in events.drain(..) {
            self.rec.event(e.wall, e.kind, &e.text);
            if let (Some(command), Some(job)) = (&self.opts.on_event, &e.hook)
                && let Err(err) = act::spawn_hook(command, e.kind, job)
            {
                self.rec.event(
                    e.wall,
                    "warn",
                    &format!("cannot run the --on-event command: {err}"),
                );
            }
        }
        self.events = events;
    }

    /// Carries out one action: signals only, no logging. What there is to say goes into
    /// `self.events`, to be logged when the whole tick's signals are out. `scanned` is the
    /// wall-clock time the process table was read for this sample.
    fn perform(&mut self, action: &Action, s: &Signals, scanned: f64) {
        let (job, immediate) = match action {
            Action::Stall { .. } | Action::Warn { .. } => {
                let (kind, text) = describe(action);
                self.events.push(Event::note(s.wall, kind, text));
                return;
            }
            Action::Freeze { job, .. } | Action::Resume { job } => (job, false),
            Action::Kill { job, immediate, .. } => (job, *immediate),
        };
        let Some(state) = self.state.as_mut() else {
            let (kind, text) = describe(action);
            let text = format!("dry run, would {kind}: {text}");
            self.events.push(Event::note(s.wall, kind, text));
            return;
        };
        // Judged again on the group itself, as it is now, after the record is written and
        // before anything is sent.
        let rules = &self.opts.rules;
        let mut allowed = || jobs::spared(rules, job.pgid);
        // Each arm yields how the job came out of it (a kill can time out waiting for the
        // group to exit, or be refused because the group is no longer eligible).
        let outcome: Result<Done, ActError> = match action {
            Action::Freeze { .. } => {
                act::freeze(state, job, scanned, &mut allowed).map(|()| Done::Complete)
            }
            Action::Resume { .. } => act::resume(state, job.pgid).map(|()| Done::Complete),
            _ => {
                let result = act::kill(
                    state,
                    job,
                    scanned,
                    immediate,
                    self.opts.term_seconds,
                    &mut allowed,
                );
                // The wait was ours, not a stall, and the fill rate before it is stale.
                self.engine.resync();
                result.map(|killed| match killed {
                    Killed::Exited => Done::Complete,
                    Killed::Survivors => Done::Survivors,
                    Killed::Spared(why) => Done::Spared(why),
                })
            }
        };
        let still_recorded = state.record(job.pgid).is_some();
        let unrecorded = state.pending().contains(&job.pgid);
        let (kind, text) = describe(action);
        let pgid = job.pgid;
        match outcome {
            Ok(done) => {
                if let Done::Spared(why) = done {
                    self.engine.forget(pgid);
                    let text = format!("not killing group {pgid}: {why}; it is left running");
                    self.events.push(Event::note(s.wall, "warn", text));
                    return;
                }
                self.events.push(Event {
                    wall: s.wall,
                    kind,
                    text,
                    hook: Some(job.clone()),
                });
                if unrecorded {
                    let text = format!(
                        "group {pgid} is stopped in part: a member could not be recorded on disk, so it was not stopped; it is tried again on every sample"
                    );
                    self.events.push(Event::note(signals::wall(), "warn", text));
                }
                if matches!(done, Done::Survivors) {
                    let text = format!(
                        "group {pgid} still has processes after SIGKILL; it stays recorded and is continued until it is gone"
                    );
                    self.events.push(Event::note(signals::wall(), "warn", text));
                }
            }
            Err(e) => {
                self.engine.forget(pgid);
                let why = match e {
                    ActError::Gone => "it has already exited".to_string(),
                    e => e.to_string(),
                };
                let retry = if still_recorded {
                    "; it stays recorded and is continued as soon as that works"
                } else {
                    ""
                };
                let text = format!("cannot {kind} group {pgid}: {why}{retry}");
                self.events.push(Event::note(s.wall, "warn", text));
            }
        }
    }

    /// Forgets frozen jobs that are no longer frozen: someone continued them by hand, or they
    /// exited. Looks at the frozen groups only, through buffers allocated once, so it costs
    /// nothing when there are none and allocates nothing when there are.
    ///
    /// A job must look continued on `OUTSIDE_CHECKS` consecutive checks, and is then sent
    /// SIGCONT before it is forgotten: a no-op if it really was continued, and if a slow
    /// member had only not yet stopped, the job is resumed rather than left stopped with no
    /// one to resume it. A group that cannot be inspected is neither: it is checked again
    /// next time.
    fn check_frozen(&mut self, s: &Signals) {
        let Some(state) = self.state.as_mut() else {
            return;
        };
        let mut changed: Vec<(i32, FrozenState)> = Vec::new();
        for f in self.engine.frozen() {
            if s.time - f.since < STOP_GRACE {
                continue;
            }
            let pgid = f.job.pgid;
            let Some(rec) = state.record(pgid) else {
                continue;
            };
            let found = act::frozen_state(rec, &mut self.inspector);
            if matches!(found, FrozenState::Unknown) {
                if !self.unknown.contains(&pgid) {
                    self.unknown.push(pgid);
                }
            } else {
                self.unknown.retain(|&g| g != pgid);
            }
            match found {
                FrozenState::Stopped => self.outside.retain(|&(g, _)| g != pgid),
                FrozenState::ResumedOutside => {
                    let seen = match self.outside.iter_mut().find(|(g, _)| *g == pgid) {
                        Some((_, n)) => {
                            *n += 1;
                            *n
                        }
                        None => {
                            self.outside.push((pgid, 1));
                            1
                        }
                    };
                    if seen >= OUTSIDE_CHECKS {
                        changed.push((pgid, FrozenState::ResumedOutside));
                    }
                }
                FrozenState::Gone => changed.push((pgid, FrozenState::Gone)),
                FrozenState::Unknown => {}
            }
        }
        for (pgid, why) in changed {
            self.engine.forget(pgid);
            self.outside.retain(|&(g, _)| g != pgid);
            let command = state
                .record(pgid)
                .map(|r| r.command.clone())
                .unwrap_or_default();
            let (kind, what) = match why {
                FrozenState::ResumedOutside => {
                    // Sends SIGCONT and drops the record once it is sent. If it cannot be
                    // sent the record stays, and `retry_unresolved` tries again.
                    let _ = act::resume(state, pgid);
                    (
                        "resume",
                        "resumed outside membrake (sent SIGCONT to be sure)",
                    )
                }
                _ => {
                    let _ = state.remove(pgid);
                    ("warn", "frozen job exited while frozen")
                }
            };
            let text = format!("{what}: group {pgid}: {command}");
            self.events.push(Event::note(s.wall, kind, text));
        }
    }

    /// Tries again to continue every recorded group the engine is not tracking: a resume or a
    /// kill that could not be completed or confirmed. Each sample, until it is confirmed gone
    /// or the SIGCONT is sent.
    fn retry_unresolved(&mut self, s: &Signals) {
        let Some(state) = self.state.as_mut() else {
            return;
        };
        let engine = &self.engine;
        let pending: Vec<i32> = state
            .records()
            .iter()
            .map(|r| r.pgid)
            .filter(|&pgid| !engine.frozen().iter().any(|f| f.job.pgid == pgid))
            .collect();
        for pgid in pending {
            match act::resume(state, pgid) {
                Ok(()) => {
                    let text = format!("resumed group {pgid} (a retry)");
                    self.events.push(Event::note(s.wall, "resume", text));
                }
                Err(ActError::Gone) => {
                    let text = format!("group {pgid} is gone; its record is dropped");
                    self.events.push(Event::note(s.wall, "warn", text));
                }
                Err(e) if s.time - self.last_unresolved >= WARN_EVERY => {
                    self.last_unresolved = s.time;
                    let text =
                        format!("cannot resume group {pgid}: {e}; trying again on every sample");
                    self.events.push(Event::note(s.wall, "warn", text));
                }
                Err(_) => {}
            }
        }
    }

    /// Resumes every job the state file records, reading the file afresh: it, not memory, is
    /// the truth after a crash or an outside edit. If it cannot be read the records in memory
    /// are used, and any job that still cannot be continued stays recorded and tracked, to be
    /// tried again. `now` is engine time.
    fn thaw(&mut self, what: &str, now: f64) {
        let Some(state) = self.state.as_mut() else {
            return;
        };
        let reloaded = state.reload();
        let result = act::recover(state);
        let wall = signals::wall();
        if let Err(e) = reloaded {
            self.rec.event(
                wall,
                "warn",
                &format!("{what}: cannot read the state file ({e}); using the records in memory"),
            );
        }
        match result {
            Ok(report) => {
                for (kind, text, failed) in thaw_lines(report, what) {
                    // A failure that repeats on every sample is said once a minute.
                    if failed {
                        if now - self.last_unresolved < WARN_EVERY {
                            continue;
                        }
                        self.last_unresolved = now;
                    }
                    self.rec.event(wall, kind, &text);
                }
            }
            Err(e) => self.rec.event(
                wall,
                "warn",
                &format!("{what}: cannot update the state file: {e}"),
            ),
        }
        self.engine
            .retain_frozen(|pgid| state.record(pgid).is_some());
        self.rec.sync_events();
    }
}

/// How an action that did not fail ended.
enum Done {
    Complete,
    /// A kill left processes behind.
    Survivors,
    /// A kill was refused: the group is no longer eligible.
    Spared(String),
}

// ---------------------------------------------------------------------------------------
// status

pub(crate) fn age(seconds: f64) -> String {
    let s = seconds.max(0.0) as u64;
    match s {
        0..=119 => format!("{s}s"),
        120..=7199 => format!("{}m", s / 60),
        _ => format!("{}h{:02}m", s / 3600, s % 3600 / 60),
    }
}

pub(crate) fn gb(bytes: u64) -> String {
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
    print_queue(state_dir, s.wall);
    // SAFETY: getuid has no preconditions.
    let uid = unsafe { libc::getuid() };
    if uid != 0 {
        println!(
            "\nrunning as uid {uid}: other users' processes are not readable, so only this user's jobs are listed"
        );
    }
    Ok(())
}

/// Whether process `pid` exists (a process of another user does).
fn alive(pid: i32) -> bool {
    // SAFETY: signal 0 only checks that the process exists and may be signalled.
    let sent = unsafe { libc::kill(pid, 0) } == 0;
    sent || std::io::Error::last_os_error().raw_os_error() == Some(libc::EPERM)
}

/// The queue as the daemon last wrote it: the learned cap, the memory in use, who is waiting
/// and what is running.
fn print_queue(state_dir: &Path, wall: f64) {
    println!();
    let queue = match broker::read_queue(state_dir) {
        Ok(Some(queue)) => queue,
        Ok(None) => {
            println!(
                "queue       not served: no queue state in {}",
                state_dir.display()
            );
            return;
        }
        Err(e) => {
            println!(
                "queue       unknown: cannot read {}: {e}",
                state_dir.display()
            );
            return;
        }
    };
    if !alive(queue.daemon) {
        println!(
            "queue       not served: the daemon that wrote the queue state (pid {}) has exited",
            queue.daemon
        );
        return;
    }
    println!(
        "queue       cap {} of {} RAM, {} in use; {} running, {} waiting",
        gb(queue.cap),
        gb(queue.ram),
        gb(queue.used),
        queue.running.len(),
        queue.waiting.len()
    );
    for r in &queue.running {
        println!(
            "  running   group {:<7} session {:<7} peak {:>7}  for {}  {}",
            r.pgid,
            r.session,
            gb(r.peak),
            age(wall - r.since),
            r.label
        );
    }
    for w in &queue.waiting {
        println!(
            "  waiting   #{:<3} session {:<7} needs {:>7}  for {}  {}",
            w.position,
            w.session,
            gb(w.need),
            age(wall - w.since),
            w.label
        );
    }
}

pub fn own_pgid() -> i32 {
    // SAFETY: getpgrp has no preconditions.
    unsafe { libc::getpgrp() }
}

#[cfg(test)]
mod tests {
    use super::*;
    use crate::signals::Pressure;
    use crate::testutil::{Sleeper, fault, scratch};
    use std::fs;
    use std::os::unix::fs::PermissionsExt;

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

    /// Memory not tight, the compressor quiet.
    fn calm_at(time: f64) -> Signals {
        Signals {
            segments: 0.1,
            level: 80,
            ..signals_at(time)
        }
    }

    /// A job the engine can be handed; nothing of it exists.
    fn fake_job(pgid: i32) -> Job {
        Job {
            pgid,
            leader_pid: pgid,
            start_us: 1_000 + u64::try_from(pgid).unwrap(),
            uid: 501,
            bytes: 4 << 30,
            procs: 1,
            prefer: false,
            protected: false,
            stopped: false,
            command: format!("job-{pgid}"),
        }
    }

    /// Frozen work holds the queue until it is resumed, however long that takes. Twenty jobs
    /// frozen together come back one per settle period, after five calm samples: the last
    /// at about 195 s. The queue stays held at every sample until then, and not after. (A
    /// window worked out from the number of jobs left, as it was, ran out with six still
    /// frozen, at about 120 s.) This drives the engine's decisions and `update_hold` on
    /// jobs that do not exist, so it shows when the hold is on and off for the engine's
    /// frozen set; it does not freeze a process, nor show the broker refusing to admit while
    /// the hold is on (`queue_nothing_is_admitted_while_a_job_is_frozen` in `tests/daemon.rs`
    /// does that for one job).
    #[test]
    fn twenty_frozen_jobs_hold_the_queue_until_the_last_is_resumed() {
        let dir = scratch("hold-twenty");
        let opts = RunOptions {
            policy: Policy::default(),
            rules: Rules::new(&[], &[], vec![], vec![], 0).unwrap(),
            term_seconds: 1.0,
            on_event: None,
        };
        let state = State::open(&dir.join("state")).unwrap();
        let mut r = Runner::new(opts, Some(state), Recorder::new(&dir.join("log")));
        let jobs: Vec<Job> = (1..=20).map(|i| fake_job(900 + i)).collect();

        // The cliff in view: every job is frozen at once.
        for time in [0.0, 1.0] {
            let s = Signals {
                segments: 0.86,
                ..signals_at(time)
            };
            r.engine.step(&s, || jobs.clone());
            r.update_hold(&s);
        }
        assert_eq!(r.engine.frozen().len(), 20);
        assert!(r.queue_held());

        let mut last_resume = 0.0;
        for time in 2..400 {
            let s = calm_at(f64::from(time));
            let step = r.engine.step(&s, Vec::new);
            if step
                .actions
                .iter()
                .any(|a| matches!(a, Action::Resume { .. }))
            {
                last_resume = s.time;
            }
            r.update_hold(&s);
            assert_eq!(
                r.queue_held(),
                !r.engine.frozen().is_empty(),
                "at {time} s, with {} jobs still frozen",
                r.engine.frozen().len()
            );
        }
        assert!(last_resume > 190.0, "the last resume was at {last_resume}");
        assert!(r.engine.frozen().is_empty() && !r.queue_held());
        let _ = fs::remove_dir_all(&dir);
    }

    /// A record membrake cannot resolve (here: its group cannot be looked at, whatever it is
    /// asked) holds the queue for 300 s in total and no longer, and a sample of pressure
    /// every 110 s does not renew the hold: it stays recorded and retried, and the end of
    /// its hold is said once. (Measured from the last tight sample, as it was, a hold
    /// renewed every 110 s was never over.)
    #[test]
    fn a_record_that_cannot_be_resolved_stops_holding_the_queue_after_300_seconds_of_it() {
        let dir = scratch("hold-stuck");
        let sleeper = Sleeper::spawn();
        let job = sleeper.job(1);
        let mut r = runner_with_frozen(&dir, &job);
        assert!(sleeper.settles(true));
        fault::arm(&fault::LIST, 1_000_000);

        // One second apart from engine time 102. Tight, with the compressor quiet (nothing for
        // the brake to do), on every 110th sample.
        let mut first_free = None;
        for i in 0..450u32 {
            let time = 102.0 + f64::from(i);
            let s = if i % 110 == 109 {
                Signals {
                    segments: 0.1,
                    ..signals_at(time)
                }
            } else {
                calm_at(time)
            };
            r.tick(&s);
            match first_free {
                None if !r.queue_held() => first_free = Some(i),
                None => {}
                Some(_) => assert!(!r.queue_held(), "held again at sample {i}"),
            }
        }
        // The first sample it could not be looked at is 103 s (the freeze is 2 s old, and is
        // not looked at before); 300 s later its hold has run out.
        assert_eq!(
            first_free,
            Some(301),
            "300 s after the first sample it was unresolved"
        );
        assert!(r.has_records(), "and it is still recorded");
        let said = events(&mut r, &dir);
        assert_eq!(
            said.matches("no longer holds the queue").count(),
            1,
            "{said}"
        );

        // It is still tried on every sample: once it can be looked at, it is resumed.
        fault::arm(&fault::LIST, 0);
        r.tick(&calm_at(600.0));
        assert!(sleeper.settles(false) && !r.has_records());
        let _ = fs::remove_dir_all(&dir);
    }

    /// A record that is unresolved on every other sample is unresolved for as long as it is
    /// looked at, and its hold runs out after 300 s of that: each unresolved sample counts
    /// until the next one, whether that one is unresolved or not. (Counting only the time
    /// between two unresolved samples left the total at zero for ever, and the queue held for
    /// good.) Samples a second apart, unresolved on the even ones: the 300th second of
    /// unresolved time is the one that starts at sample 598, so from sample 600 on it holds
    /// nothing.
    #[test]
    fn an_unresolved_record_that_is_healthy_on_every_other_sample_still_uses_up_its_hold() {
        let mut holds = Holds {
            unresolved: Vec::new(),
        };
        let mut spent = Vec::new();
        let mut first_free = None;
        for second in 0..1000u32 {
            let stuck = (second % 2 == 0).then_some(7);
            let held = holds.update(
                f64::from(second),
                false,
                stuck.into_iter(),
                |_| true,
                |pgid| spent.push(pgid),
            );
            if stuck.is_some() && !held && first_free.is_none() {
                first_free = Some(second);
            }
            if stuck.is_some() && first_free.is_some() {
                assert!(!held, "held again at sample {second}");
            }
        }
        assert_eq!(first_free, Some(600));
        assert_eq!(spent, [7], "said once");
    }

    /// A runner whose engine has, through the real freeze path, frozen `job` at engine time
    /// 101 (two tight samples).
    fn runner_with_frozen(dir: &Path, job: &Job) -> Runner {
        let opts = RunOptions {
            policy: Policy::default(),
            // The current user counts as eligible even below uid 500: a build sandbox runs the
            // tests as a build user with such a uid, and the kill is judged by the rules.
            rules: Rules::new(&[], &[], vec![], vec![unsafe { libc::getuid() }], 0).unwrap(),
            term_seconds: 1.0,
            on_event: None,
        };
        let state = State::open(&dir.join("state")).unwrap();
        let mut r = Runner::new(opts, Some(state), Recorder::new(&dir.join("log")));
        for time in [100.0, 101.0] {
            let s = signals_at(time);
            let step = r.engine.step(&s, || vec![job.clone()]);
            for action in &step.actions {
                r.perform(action, &s, s.wall);
            }
        }
        assert_eq!(r.engine.frozen().len(), 1, "the engine froze the job");
        r
    }

    /// Everything the runner has said so far, once the logger thread has written it.
    fn events(r: &mut Runner, dir: &Path) -> String {
        r.log_events();
        assert!(r.rec.flush(Duration::from_secs(5)), "the logger answers");
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
        // One or two checks are not enough: a member may only be slow to stop.
        r.check_frozen(&signals_at(111.0));
        r.check_frozen(&signals_at(112.0));
        assert_eq!(r.engine.frozen().len(), 1, "not yet believed");
        r.check_frozen(&signals_at(113.0));
        assert!(r.engine.frozen().is_empty());
        assert!(r.state.as_ref().unwrap().records().is_empty());
        let said = events(&mut r, &dir);
        assert!(said.contains("resumed outside membrake"), "{said}");
        let _ = fs::remove_dir_all(&dir);
    }

    /// A job that looks continued and then reads stopped again (a member was only slow to
    /// stop) stays frozen: the count of continued-looking checks starts over.
    #[test]
    fn a_job_that_only_looked_continued_stays_frozen() {
        let dir = scratch("straggler");
        let sleeper = Sleeper::spawn();
        let job = sleeper.job(1);
        let mut r = runner_with_frozen(&dir, &job);
        assert!(sleeper.settles(true));
        for (i, time) in [110.0, 111.0, 112.0, 113.0].into_iter().enumerate() {
            // SAFETY: signalling the group this test spawned.
            unsafe { libc::killpg(job.pgid, libc::SIGCONT) };
            assert!(sleeper.settles(false));
            r.check_frozen(&signals_at(time));
            // Stopped again before the next check, as a slow member would be.
            // SAFETY: as above.
            unsafe { libc::killpg(job.pgid, libc::SIGSTOP) };
            assert!(sleeper.settles(true));
            r.check_frozen(&signals_at(time + 0.5));
            assert_eq!(r.engine.frozen().len(), 1, "round {i}: still frozen");
        }
        let said = events(&mut r, &dir);
        assert!(!said.contains("resumed outside"), "{said}");
        // SAFETY: as above; leave nothing stopped behind.
        unsafe { libc::killpg(job.pgid, libc::SIGCONT) };
        let _ = fs::remove_dir_all(&dir);
    }

    /// The safeguard against believing a straggler: a group that reads "resumed outside" is
    /// sent SIGCONT before it is forgotten. Here one member of two was continued by hand and
    /// the other is still stopped, so the group reads as continued on every check. Believing
    /// that and merely forgetting it would leave the second member stopped for good.
    #[test]
    fn a_group_continued_in_part_is_continued_whole_before_it_is_forgotten() {
        let dir = scratch("partly");
        let leader = Sleeper::spawn();
        let member = Sleeper::spawn_in(leader.pid(), false);
        let job = leader.job(2);
        let mut r = runner_with_frozen(&dir, &job);
        assert!(leader.settles(true) && member.settles(true));

        // SAFETY: signalling the one process this test spawned.
        unsafe { libc::kill(member.pid(), libc::SIGCONT) };
        assert!(member.settles(false));
        assert!(leader.stopped(), "the other member is still stopped");
        for time in [110.0, 111.0] {
            r.check_frozen(&signals_at(time));
            assert!(leader.stopped(), "not yet believed, so not yet signalled");
        }
        r.check_frozen(&signals_at(112.0));
        assert!(r.engine.frozen().is_empty());
        assert!(leader.settles(false), "the stopped member was continued");
        assert!(member.settles(false));
        assert!(r.state.as_ref().unwrap().records().is_empty());
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
        let said = events(&mut r, &dir);
        assert!(said.contains("exited while frozen"), "{said}");
        let _ = fs::remove_dir_all(&dir);
    }

    /// A group that cannot be inspected is not a group that exited: the job stays frozen and
    /// recorded.
    #[test]
    fn a_frozen_job_that_cannot_be_inspected_is_not_forgotten() {
        let dir = scratch("uninspectable");
        let sleeper = Sleeper::spawn();
        let job = sleeper.job(1);
        let mut r = runner_with_frozen(&dir, &job);
        assert!(sleeper.settles(true));
        for time in [110.0, 111.0, 112.0, 113.0] {
            fault::arm(&fault::LIST, 1);
            r.check_frozen(&signals_at(time));
        }
        assert_eq!(r.engine.frozen().len(), 1);
        assert_eq!(r.state.as_ref().unwrap().records().len(), 1);
        assert!(sleeper.stopped());
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
            s.wall,
        );
        assert!(sleeper.0.try_wait().unwrap().is_some(), "the group is dead");
        sleeper.die();
        assert!(
            r.state.as_ref().unwrap().records().is_empty(),
            "record dropped"
        );
        let said = events(&mut r, &dir);
        assert!(said.contains(" kill "), "{said}");
        let _ = fs::remove_dir_all(&dir);
    }

    /// Nothing is logged until the whole tick's signals are out: after the actions are
    /// performed the events are still waiting to be said.
    #[test]
    fn a_tick_sends_its_signals_before_it_says_anything() {
        let dir = scratch("order");
        let sleeper = Sleeper::spawn();
        let job = sleeper.job(1);
        let r = runner_with_frozen(&dir, &job);
        assert!(sleeper.settles(true), "the signal is out");
        assert_eq!(r.events.len(), 1, "and nothing has been said about it");
        assert!(!dir.join("log/events.log").exists());
        let _ = fs::remove_dir_all(&dir);
    }

    /// The fail-safe reads the state file afresh, but if it cannot be read the records in
    /// memory are all there is, and it resumes from them. (Forgetting the jobs without a
    /// signal would leave them frozen for good.)
    #[test]
    fn the_failsafe_resumes_from_memory_when_the_state_file_cannot_be_read() {
        let dir = scratch("failsafe-memory");
        let sleeper = Sleeper::spawn();
        let job = sleeper.job(1);
        let mut r = runner_with_frozen(&dir, &job);
        assert!(sleeper.settles(true));
        let file = dir.join("state/frozen.tsv");
        fs::set_permissions(&file, fs::Permissions::from_mode(0o000)).unwrap();
        assert!(fs::read_to_string(&file).is_err(), "unreadable");

        r.thaw("test", 200.0);
        assert!(sleeper.settles(false), "resumed from the records in memory");
        assert!(r.engine.frozen().is_empty());
        assert!(r.state.as_ref().unwrap().records().is_empty());
        let said = events(&mut r, &dir);
        assert!(said.contains("using the records in memory"), "{said}");
        let _ = fs::remove_dir_all(&dir);
    }

    /// A job that cannot be continued stays recorded and tracked, and every attempt after
    /// that tries it again until it works.
    #[test]
    fn the_failsafe_keeps_a_job_it_could_not_continue_and_tries_again() {
        let dir = scratch("failsafe-retry");
        let sleeper = Sleeper::spawn();
        let job = sleeper.job(1);
        let mut r = runner_with_frozen(&dir, &job);
        assert!(sleeper.settles(true));

        fault::arm(&fault::SIGNAL, 1);
        r.thaw("test", 200.0);
        assert!(sleeper.stopped());
        assert_eq!(r.engine.frozen().len(), 1, "still tracked");
        assert!(r.has_records(), "and recorded");

        r.thaw("test", 201.0);
        assert!(sleeper.settles(false), "the next attempt resumes it");
        assert!(r.engine.frozen().is_empty());
        assert!(!r.has_records());
        let _ = fs::remove_dir_all(&dir);
    }

    /// A resume the engine asked for that fails leaves the record, and the next samples
    /// resume the job.
    #[test]
    fn a_resume_that_failed_is_retried_on_the_next_sample() {
        let dir = scratch("resume-retry");
        let sleeper = Sleeper::spawn();
        let job = sleeper.job(1);
        let mut r = runner_with_frozen(&dir, &job);
        assert!(sleeper.settles(true));
        // Calm long enough, and past the settle period, for the engine to resume it.
        let mut resumed = None;
        for time in 112..120 {
            let s = calm_at(f64::from(time));
            let step = r.engine.step(&s, Vec::new);
            if let Some(action) = step
                .actions
                .iter()
                .find(|a| matches!(a, Action::Resume { .. }))
            {
                resumed = Some((action.clone(), s));
                break;
            }
        }
        let (action, s) = resumed.expect("the engine resumes the job");
        assert!(r.engine.frozen().is_empty(), "the engine let go of it");

        fault::arm(&fault::SIGNAL, 1);
        r.perform(&action, &s, s.wall);
        assert!(sleeper.stopped(), "the signal failed");
        assert!(r.has_records(), "so the record stays");
        let said = events(&mut r, &dir);
        assert!(said.contains("cannot resume group"), "{said}");

        r.tick(&calm_at(f64::from(121)));
        assert!(sleeper.settles(false), "the next sample resumes it");
        assert!(!r.has_records());
        let _ = fs::remove_dir_all(&dir);
    }
}
