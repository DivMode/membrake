//! The real binary, end to end, against real `sleep` process groups that the tests spawn.
//!
//! Two kinds of daemon run here, and neither can touch a process the tests did not spawn.
//!
//! - Most run with `--min-job-gb 1000000`, so they have no job they could ever freeze or
//!   kill: the only processes they signal are the recorded ones the tests put in their state
//!   directory.
//! - The simulated ones (`run --simulate`, the tests at the end) read scripted signals the
//!   test appends to a file, with `--min-job-gb 0` so that tiny groups qualify. They are
//!   confined by `--simulate-only` to groups whose command line is `sleep` plus a number
//!   made of this test process's id and a per-test counter, which only that test's groups
//!   carry, and, whatever the pattern, to groups whose leader is a child of this test process
//!   (the daemon takes that from the kernel). The tests also spawn a decoy group outside that
//!   filter, and another that matches it but was not started by this process, and check that
//!   both are left alone.

use membrake::act::{self, State};
use membrake::engine::Step;
use membrake::jobs::{self, Job};
use membrake::recorder;
use membrake::signals::{Pressure, Signals};
use std::fs;
use std::io::{Read, Write};
use std::os::fd::{AsRawFd, FromRawFd, OwnedFd};
use std::os::unix::ffi::OsStrExt;
use std::os::unix::fs::{OpenOptionsExt, PermissionsExt};
use std::os::unix::process::CommandExt;
use std::path::{Path, PathBuf};
use std::process::{Child, Command, Stdio};
use std::sync::atomic::{AtomicU32, Ordering};
use std::time::{Duration, Instant};

/// A `sleep` in its own process group, killed when dropped.
struct Sleeper(Child);

impl Sleeper {
    fn spawn() -> Sleeper {
        Sleeper::spawn_in("300", 0)
    }

    /// `sleep ARG`, leading a new group (`pgid` 0) or joining the group `pgid`.
    fn spawn_in(arg: &str, pgid: i32) -> Sleeper {
        Sleeper(
            Command::new("sleep")
                .arg(arg)
                .process_group(pgid)
                .spawn()
                .unwrap(),
        )
    }

    fn pid(&self) -> i32 {
        self.0.id() as i32
    }

    fn job(&self) -> Job {
        let p = jobs::bsd_info(self.pid()).expect("child is visible");
        Job {
            pgid: self.pid(),
            leader_pid: self.pid(),
            start_us: p.start_us,
            uid: p.uid,
            bytes: 1 << 30,
            procs: 1,
            prefer: false,
            protected: false,
            stopped: false,
            command: "sleep 300".into(),
        }
    }

    fn stopped(&self) -> bool {
        jobs::bsd_info(self.pid())
            .expect("child is visible")
            .stopped
    }

    /// Stop and continue take effect asynchronously: wait for the state.
    fn settles(&self, stopped: bool) -> bool {
        let deadline = Instant::now() + Duration::from_secs(5);
        while self.stopped() != stopped && Instant::now() < deadline {
            std::thread::sleep(Duration::from_millis(10));
        }
        self.stopped() == stopped
    }

    fn freeze_into(&self, state_dir: &Path) {
        let mut state = State::open(state_dir).unwrap();
        act::freeze(
            &mut state,
            &self.job(),
            membrake::signals::wall(),
            &mut || None,
        )
        .unwrap();
        assert!(self.settles(true), "the group is stopped");
    }
}

impl Drop for Sleeper {
    fn drop(&mut self) {
        let _ = self.0.kill();
        let _ = self.0.wait();
    }
}

struct Daemon(Child);

impl Daemon {
    fn spawn(args: &[&str], state: &Path, log: &Path) -> Daemon {
        Daemon(
            Command::new(env!("CARGO_BIN_EXE_membrake"))
                .arg("run")
                .args(["--min-job-gb", "1000000"])
                .arg("--state-dir")
                .arg(state)
                .arg("--log-dir")
                .arg(log)
                // Never the live daemon's socket.
                .arg("--socket")
                .arg(state.with_file_name("q.sock"))
                .args(args)
                .stdout(Stdio::null())
                .stderr(Stdio::null())
                .spawn()
                .unwrap(),
        )
    }

    fn wait_started(&self, log: &Path) {
        self.wait_logged(log, " start ");
    }

    /// Waits until the event log contains `text`.
    fn wait_logged(&self, log: &Path, text: &str) {
        let events = log.join("events.log");
        let deadline = Instant::now() + Duration::from_secs(60);
        while !fs::read_to_string(&events).is_ok_and(|t| t.contains(text)) {
            assert!(
                Instant::now() < deadline,
                "the daemon never logged {text:?}"
            );
            std::thread::sleep(Duration::from_millis(20));
        }
    }

    /// The command of a daemon driven by the scripted signals in `script` (`run --simulate`),
    /// confined to the groups whose command line matches `only`.
    fn simulate_command(script: &Path, only: &str, state: &Path, log: &Path) -> Command {
        let mut command = Command::new(env!("CARGO_BIN_EXE_membrake"));
        command
            .args(["run", "--min-job-gb", "0", "--simulate"])
            .arg(script)
            .args(["--simulate-only", only])
            .arg("--state-dir")
            .arg(state)
            .arg("--log-dir")
            .arg(log)
            .stdout(Stdio::null())
            .stderr(Stdio::null());
        command
    }

    fn simulate(script: &Path, only: &str, state: &Path, log: &Path) -> Daemon {
        Daemon(
            Self::simulate_command(script, only, state, log)
                .spawn()
                .unwrap(),
        )
    }

    /// Waits until the event log names `count` freezes, resumes and kills. The daemon hands
    /// its events to a logger thread, so they can trail the signals they describe.
    fn wait_actions(&self, log: &Path, count: usize) {
        let deadline = Instant::now() + Duration::from_secs(60);
        while actions(log).len() < count {
            assert!(
                Instant::now() < deadline,
                "the daemon never logged {count} actions: {}",
                events(log)
            );
            std::thread::sleep(Duration::from_millis(10));
        }
    }

    /// Waits until the daemon has recorded `rows` samples: it records one after it has
    /// dealt with a reading and sent whatever signals that led to, so every reading appended
    /// so far has been acted on.
    fn wait_samples(&self, log: &Path, rows: usize) {
        let samples = log.join("samples.tsv");
        let deadline = Instant::now() + Duration::from_secs(60);
        while fs::read_to_string(&samples).map_or(0, |t| t.lines().count().saturating_sub(1)) < rows
        {
            assert!(
                Instant::now() < deadline,
                "the daemon never recorded {rows} samples"
            );
            std::thread::sleep(Duration::from_millis(10));
        }
    }

    /// SIGTERM, as launchd does, and wait for the exit.
    fn terminate(mut self) -> std::process::ExitStatus {
        // SAFETY: signalling the child this test spawned.
        unsafe { libc::kill(self.0.id() as i32, libc::SIGTERM) };
        let deadline = Instant::now() + Duration::from_secs(60);
        loop {
            if let Some(status) = self.0.try_wait().unwrap() {
                return status;
            }
            assert!(Instant::now() < deadline, "the daemon ignored SIGTERM");
            std::thread::sleep(Duration::from_millis(20));
        }
    }
}

impl Drop for Daemon {
    fn drop(&mut self) {
        let _ = self.0.kill();
        let _ = self.0.wait();
    }
}

fn scratch(name: &str) -> PathBuf {
    let dir = std::env::temp_dir().join(format!(
        "membrake-test-{}-daemon-{name}",
        std::process::id()
    ));
    let _ = fs::remove_dir_all(&dir);
    fs::create_dir_all(&dir).unwrap();
    dir
}

fn records(state: &Path) -> usize {
    act::read_state(state).unwrap().0.len()
}

/// A dry run sends no signal at all, even to a recorded frozen group, and touches neither
/// the state nor the lock.
#[test]
fn dry_run_leaves_a_recorded_frozen_group_alone() {
    let dir = scratch("dry");
    let (state, log) = (dir.join("state"), dir.join("log"));
    let sleeper = Sleeper::spawn();
    sleeper.freeze_into(&state);

    let daemon = Daemon::spawn(&["--dry-run"], &state, &log);
    daemon.wait_started(&log);
    std::thread::sleep(Duration::from_millis(2500)); // a few samples
    assert!(daemon.terminate().success());

    assert!(sleeper.stopped(), "a dry run must not resume anything");
    assert_eq!(records(&state), 1, "a dry run must not touch the state");
    assert!(
        !state.join("membrake.lock").exists(),
        "a dry run takes no lock"
    );
    let events = fs::read_to_string(log.join("events.log")).unwrap();
    assert!(events.contains("dry run, no signals are sent"), "{events}");
    let _ = fs::remove_dir_all(&dir);
}

/// The daemon thaws a recorded group when it starts, and thaws whatever the state file
/// holds when it is told to stop.
#[test]
fn daemon_resumes_recorded_groups_at_start_and_on_sigterm() {
    let dir = scratch("live");
    let (state, log) = (dir.join("state"), dir.join("log"));
    let sleeper = Sleeper::spawn();
    sleeper.freeze_into(&state);

    let daemon = Daemon::spawn(&[], &state, &log);
    daemon.wait_started(&log);
    assert!(
        sleeper.settles(false),
        "start-up recovery continues the group"
    );
    assert_eq!(records(&state), 0);

    // Freeze it again while the daemon runs; only SIGTERM can bring it back now.
    sleeper.freeze_into(&state);
    assert_eq!(records(&state), 1);
    assert!(daemon.terminate().success(), "a clean exit");

    assert!(sleeper.settles(false), "shutdown continues the group");
    assert_eq!(records(&state), 0, "and forgets it");
    let events = fs::read_to_string(log.join("events.log")).unwrap();
    assert!(events.contains("resumed on shutdown"), "{events}");
    let _ = fs::remove_dir_all(&dir);
}

/// Logging must never stop braking: with a log directory that cannot exist, the daemon still
/// thaws at start and on SIGTERM, and exits cleanly.
#[test]
fn a_log_directory_that_cannot_exist_does_not_stop_the_daemon() {
    let dir = scratch("nolog");
    let state = dir.join("state");
    let log = dir.join("a-file").join("log"); // a path below a regular file
    fs::write(dir.join("a-file"), "not a directory").unwrap();
    let sleeper = Sleeper::spawn();
    sleeper.freeze_into(&state);

    let daemon = Daemon::spawn(&[], &state, &log);
    // With no event log to watch, the daemon has finished recovering when it has emptied the
    // state file; only then may this test write to it again (the daemon is its other writer).
    let deadline = Instant::now() + Duration::from_secs(20);
    while records(&state) != 0 {
        assert!(Instant::now() < deadline, "the daemon never recovered");
        std::thread::sleep(Duration::from_millis(20));
    }
    assert!(
        sleeper.settles(false),
        "start-up recovery ran without a recorder"
    );
    sleeper.freeze_into(&state);
    assert!(
        daemon.terminate().success(),
        "still running, and a clean exit"
    );
    assert!(sleeper.settles(false), "shutdown thawed without a recorder");
    assert_eq!(records(&state), 0);
    let _ = fs::remove_dir_all(&dir);
}

#[test]
fn a_second_daemon_exits_with_a_clear_error() {
    let dir = scratch("twice");
    let (state, log) = (dir.join("state"), dir.join("log"));
    let first = Daemon::spawn(&[], &state, &log);
    first.wait_started(&log);

    let second = Command::new(env!("CARGO_BIN_EXE_membrake"))
        .args(["run", "--min-job-gb", "1000000", "--state-dir"])
        .arg(&state)
        .arg("--log-dir")
        .arg(dir.join("log2"))
        .output()
        .unwrap();
    assert!(!second.status.success());
    let stderr = String::from_utf8_lossy(&second.stderr);
    assert!(stderr.contains("already running"), "{stderr}");
    assert!(first.terminate().success());
    let _ = fs::remove_dir_all(&dir);
}

/// A bug that panics the loop must not leave work frozen.
#[test]
fn a_panic_in_the_loop_resumes_the_frozen_jobs() {
    let dir = scratch("panic");
    let state = dir.join("state");
    let sleeper = Sleeper::spawn();
    sleeper.freeze_into(&state);

    let result = membrake::runner::guarded(Some(&state), || panic!("simulated bug"));
    let message = result.unwrap_err();
    assert!(
        message.contains("panicked") && message.contains("resumed 1"),
        "{message}"
    );
    assert!(sleeper.settles(false));
    assert_eq!(records(&state), 0);
    let _ = fs::remove_dir_all(&dir);
}

// ---------------------------------------------------------------------------------------
// Simulated signals (`run --simulate`)

/// A `sleep` argument no other process has. `sleep` takes a number, so the marker is the
/// test process id and a per-call counter as the digits after the point.
fn marker() -> String {
    static N: AtomicU32 = AtomicU32::new(0);
    format!(
        "3600.{:07}{:03}",
        std::process::id(),
        N.fetch_add(1, Ordering::SeqCst)
    )
}

/// The `--simulate-only` pattern that matches exactly the groups started with `marker`.
fn only(marker: &str) -> String {
    format!("^sleep {}$", marker.replace('.', "\\."))
}

/// One job: a process group of `sleep`s, the first of which leads it.
struct Group(Vec<Sleeper>);

impl Group {
    fn spawn(marker: &str, members: usize) -> Group {
        let leader = Sleeper::spawn_in(marker, 0);
        let pgid = leader.pid();
        let mut all = vec![leader];
        all.extend((1..members).map(|_| Sleeper::spawn_in(marker, pgid)));
        Group(all)
    }

    fn pgid(&self) -> i32 {
        self.0[0].pid()
    }

    fn start_us(&self) -> u64 {
        jobs::bsd_info(self.pgid())
            .expect("leader is visible")
            .start_us
    }

    fn footprint(&self) -> u64 {
        self.0.iter().map(|m| jobs::footprint(m.pid())).sum()
    }

    /// Every member reaches (or leaves) the stopped state.
    fn settles(&self, stopped: bool) -> bool {
        self.0.iter().all(|m| m.settles(stopped))
    }

    /// No member is stopped, right now.
    fn running(&self) -> bool {
        self.0.iter().all(|m| !m.stopped())
    }

    /// Every member has exited (and is reaped).
    fn exited(&mut self) -> bool {
        let deadline = Instant::now() + Duration::from_secs(5);
        loop {
            if self.0.iter_mut().all(|m| m.0.try_wait().unwrap().is_some()) {
                return true;
            }
            if Instant::now() >= deadline {
                return false;
            }
            std::thread::sleep(Duration::from_millis(10));
        }
    }

    /// No member has exited.
    fn alive(&mut self) -> bool {
        self.0.iter_mut().all(|m| m.0.try_wait().unwrap().is_none())
    }
}

/// The scripted signals of one test: rows appended to the file the daemon follows, on a clock
/// that advances one second per row. A stage is appended, the test waits for its effect on
/// the real processes, then appends the next.
struct Script {
    path: PathBuf,
    time: f64,
    wall: f64,
}

impl Script {
    fn new(dir: &Path) -> Script {
        let path = dir.join("script.tsv");
        fs::write(&path, recorder::HEADER).unwrap();
        Script {
            path,
            time: 5000.0,
            wall: membrake::signals::wall(),
        }
    }

    fn append(&mut self, text: &str) {
        let mut file = fs::OpenOptions::new()
            .append(true)
            .open(&self.path)
            .unwrap();
        file.write_all(text.as_bytes()).unwrap();
    }

    /// `count` readings of this compressor occupancy and memory level, one second apart.
    fn readings(&mut self, count: u32, occupancy: f64, level: i32) {
        let mut text = String::new();
        let mut line = String::new();
        for _ in 0..count {
            self.time += 1.0;
            self.wall += 1.0;
            let s = Signals {
                time: self.time,
                wall: self.wall,
                segments: occupancy,
                pages: 0.0,
                swap_used: 0,
                headroom: 500 << 30,
                ram: 96 << 30,
                level,
                pressure: Pressure::Normal,
            };
            // The derived columns are informational; the daemon reads the raw ones.
            let step = Step {
                occupancy,
                eta: f64::INFINITY,
                tight: level <= 40,
                actions: Vec::new(),
            };
            recorder::format_row(&mut line, &s, &step, None);
            text.push_str(&line);
        }
        self.append(&text);
    }

    /// Memory is tight and the compressor 65% full: past the freeze line.
    fn tight(&mut self, count: u32) {
        self.readings(count, 0.65, 35);
    }

    /// Memory is tight but the compressor nearly empty: the queue holds its clients, and the
    /// brake has nothing to look at. (It scans the process table on every reading past its
    /// freeze line, which on a busy machine takes seconds, and a daemon that is slow to get
    /// through its script is not what a test of the queue is about.)
    fn tight_and_quiet(&mut self, count: u32) {
        self.readings(count, 0.10, 35);
    }

    /// Tight and 88% full: past the urgent line (85%), short of the cliff (92%).
    fn urgent(&mut self, count: u32) {
        self.readings(count, 0.88, 35);
    }

    /// Tight and 95% full.
    fn cliff(&mut self, count: u32) {
        self.readings(count, 0.95, 35);
    }

    fn calm(&mut self, count: u32) {
        self.readings(count, 0.10, 80);
    }

    /// `count` ticks whose signals could not be read, one second apart.
    fn unreadable(&mut self, count: u32) {
        let mut text = String::new();
        for _ in 0..count {
            self.time += 1.0;
            self.wall += 1.0;
            text.push_str(&format!("{}\t{:.3}\n", recorder::UNREADABLE, self.time));
        }
        self.append(&text);
    }
}

/// The (kind, group) of every freeze, resume and kill in the events log, in order.
fn actions(log: &Path) -> Vec<(String, i32)> {
    fs::read_to_string(log.join("events.log"))
        .unwrap_or_default()
        .lines()
        .filter_map(|line| {
            let kind = line.split_whitespace().nth(1)?;
            if !["freeze", "resume", "kill"].contains(&kind) {
                return None;
            }
            let after = line.split(" group ").nth(1)?;
            let pgid = after
                .chars()
                .take_while(char::is_ascii_digit)
                .collect::<String>();
            Some((kind.to_string(), pgid.parse().ok()?))
        })
        .collect()
}

fn action(kind: &str, group: &Group) -> (String, i32) {
    (kind.to_string(), group.pgid())
}

fn events(log: &Path) -> String {
    fs::read_to_string(log.join("events.log")).unwrap_or_default()
}

/// The whole life of a job through the real daemon: the newest eligible group is frozen when
/// memory gets tight, resumed when it calms, and at the cliff the biggest eligible group is
/// killed. A decoy that is newer and bigger than all of them, but outside the filter, is
/// never touched. The events log tells the same story in order.
#[test]
fn simulated_pressure_freezes_the_newest_group_resumes_it_and_kills_the_biggest() {
    let dir = scratch("lifecycle");
    let (state, log) = (dir.join("state"), dir.join("log"));
    let mine = marker();
    let mut big = Group::spawn(&mine, 3); // oldest, and the biggest of the eligible groups
    let mut small = Group::spawn(&mine, 1); // the newest eligible group
    let mut decoy = Group::spawn(&marker(), 5); // newer and bigger, but not eligible
    assert!(big.start_us() < small.start_us() && small.start_us() < decoy.start_us());
    assert!(
        big.footprint() > small.footprint(),
        "the premise: big is bigger"
    );
    let mut script = Script::new(&dir);
    let daemon = Daemon::simulate(&script.path, &only(&mine), &state, &log);
    daemon.wait_started(&log);

    // Two tight readings are two strikes: the newest eligible group is frozen.
    script.tight(2);
    daemon.wait_samples(&log, 2);
    assert!(small.settles(true), "the newest eligible group is stopped");
    assert!(big.running() && decoy.running());

    // Calm for the settle period and the five calm samples resume needs.
    script.calm(10);
    daemon.wait_samples(&log, 12);
    assert!(small.settles(false), "it is resumed");

    // The cliff: the biggest eligible group is killed, at once.
    script.cliff(1);
    daemon.wait_samples(&log, 13);
    assert!(big.exited(), "the biggest eligible group is gone");
    assert!(small.alive() && small.running() && decoy.alive() && decoy.running());

    daemon.wait_actions(&log, 3);
    assert_eq!(
        actions(&log),
        [
            action("freeze", &small),
            action("resume", &small),
            action("kill", &big)
        ],
        "{}",
        events(&log)
    );
    assert!(daemon.terminate().success());
    let _ = fs::remove_dir_all(&dir);
}

/// With signals unreadable the brake cannot be trusted, so frozen work is resumed once they
/// have been unreadable for 60 s: not at 59 s, and a good reading starts the count over.
#[test]
fn simulated_unreadable_signals_resume_frozen_groups_after_60_seconds() {
    let dir = scratch("failsafe");
    let (state, log) = (dir.join("state"), dir.join("log"));
    let mine = marker();
    let group = Group::spawn(&mine, 1);
    let mut script = Script::new(&dir);
    let daemon = Daemon::simulate(&script.path, &only(&mine), &state, &log);
    daemon.wait_started(&log);

    script.tight(2);
    daemon.wait_samples(&log, 2);
    assert!(group.settles(true));

    // 60 unreadable ticks span 59 s. A calm reading after them proves they were all dealt
    // with (the daemon records no sample for an unreadable tick) without being enough, on
    // its own, to freeze or resume anything.
    let failsafe = "resumed because the signals were unreadable for 60 s";
    script.unreadable(60);
    script.calm(1);
    daemon.wait_samples(&log, 3);
    assert!(!events(&log).contains(failsafe), "{}", events(&log));
    assert!(!group.running(), "59 s unreadable: still frozen");
    assert_eq!(records(&state), 1);

    // The good reading started the count over: one unreadable tick now is 61 s after the
    // first one before it, so a count that was not reset would fire here.
    script.unreadable(1);
    script.calm(1);
    daemon.wait_samples(&log, 4);
    assert!(!events(&log).contains(failsafe), "{}", events(&log));
    assert!(!group.running(), "the count started over: still frozen");

    // 61 ticks span exactly 60 s, counted from the first unreadable tick since the good one.
    // The daemon saves its state before it logs, so the log line is the cue to look at it.
    script.unreadable(61);
    daemon.wait_logged(&log, failsafe);
    assert!(group.settles(false), "60 s unreadable: resumed");
    assert_eq!(records(&state), 0, "and forgotten");
    assert!(daemon.terminate().success());
    let _ = fs::remove_dir_all(&dir);
}

/// Past 85% the daemon freezes every running eligible group at once instead of killing one,
/// and kills only when it fires again with nothing left to freeze, after the settle period.
#[test]
fn simulated_urgent_pressure_freezes_everything_before_it_kills_anything() {
    let dir = scratch("urgent");
    let (state, log) = (dir.join("state"), dir.join("log"));
    let mine = marker();
    let mut groups = [
        Group::spawn(&mine, 1),
        Group::spawn(&mine, 1),
        Group::spawn(&mine, 1),
    ];
    let mut script = Script::new(&dir);
    let daemon = Daemon::simulate(&script.path, &only(&mine), &state, &log);
    daemon.wait_started(&log);

    script.urgent(2);
    daemon.wait_samples(&log, 2);
    assert!(
        groups.iter().all(|g| g.settles(true)),
        "all three are frozen"
    );
    daemon.wait_actions(&log, 3);
    let mut frozen: Vec<i32> = actions(&log).into_iter().map(|(_, pgid)| pgid).collect();
    frozen.sort();
    let mut all: Vec<i32> = groups.iter().map(Group::pgid).collect();
    all.sort();
    assert_eq!(frozen, all, "three freezes, one per group, and no kill");
    assert!(actions(&log).iter().all(|(kind, _)| kind == "freeze"));

    // Still urgent for the rest of the settle period: nothing is killed.
    script.urgent(9);
    daemon.wait_samples(&log, 11);
    assert!(groups.iter_mut().all(|g| g.alive()));
    assert!(groups.iter().all(|g| !g.running()));

    // The settle period is over and there is nothing left to freeze: one is killed.
    script.urgent(1);
    daemon.wait_samples(&log, 12);
    daemon.wait_actions(&log, 4);
    let killed: Vec<_> = actions(&log)
        .into_iter()
        .filter(|(kind, _)| kind == "kill")
        .collect();
    assert_eq!(killed.len(), 1, "{}", events(&log));
    // The kill is sent before it is logged, and a group takes a moment to die of it.
    let victim = groups
        .iter_mut()
        .find(|g| g.pgid() == killed[0].1)
        .expect("the log names one of the groups");
    assert!(victim.exited(), "the group the log names is gone");
    let mut gone = Vec::new();
    for g in &mut groups {
        if !g.alive() {
            gone.push(g.pgid());
        }
    }
    assert_eq!(
        gone,
        [killed[0].1],
        "exactly the group the log names is gone"
    );
    assert!(
        groups
            .iter()
            .filter(|g| g.pgid() != killed[0].1)
            .all(|g| !g.running()),
        "the others are still frozen"
    );
    assert!(daemon.terminate().success());
    let _ = fs::remove_dir_all(&dir);
}

/// Groups frozen together come back oldest first, one per settle period.
#[test]
fn simulated_calm_resumes_groups_frozen_together_oldest_first() {
    let dir = scratch("order");
    let (state, log) = (dir.join("state"), dir.join("log"));
    let mine = marker();
    let groups = [
        Group::spawn(&mine, 1),
        Group::spawn(&mine, 1),
        Group::spawn(&mine, 1),
    ];
    assert!(groups[0].start_us() < groups[1].start_us());
    assert!(groups[1].start_us() < groups[2].start_us());
    let mut script = Script::new(&dir);
    let daemon = Daemon::simulate(&script.path, &only(&mine), &state, &log);
    daemon.wait_started(&log);

    script.urgent(2);
    daemon.wait_samples(&log, 2);
    assert!(groups.iter().all(|g| g.settles(true)));

    // Resume #1 at the end of the first settle period, then one more every 10 s.
    script.calm(30);
    daemon.wait_samples(&log, 32);
    assert!(groups.iter().all(|g| g.settles(false)));
    daemon.wait_actions(&log, 6);
    let resumed: Vec<(String, i32)> = actions(&log)
        .into_iter()
        .filter(|(kind, _)| kind == "resume")
        .collect();
    let oldest_first: Vec<(String, i32)> = groups.iter().map(|g| action("resume", g)).collect();
    assert_eq!(resumed, oldest_first, "{}", events(&log));
    assert!(daemon.terminate().success());
    let _ = fs::remove_dir_all(&dir);
}

/// The test hook cannot be pointed at the live daemon's directories, cannot run without a
/// filter that confines it, and is not advertised.
#[test]
fn simulate_needs_explicit_directories_and_a_filter_and_is_hidden() {
    let run = |args: &[&str]| {
        let out = Command::new(env!("CARGO_BIN_EXE_membrake"))
            .arg("run")
            .args(args)
            .output()
            .unwrap();
        assert!(!out.status.success(), "{args:?}");
        String::from_utf8_lossy(&out.stderr).into_owned()
    };
    let base = ["--simulate", "/nonexistent/script.tsv"];
    let stderr = run(&[
        base[0],
        base[1],
        "--simulate-only",
        "x",
        "--log-dir",
        "/nonexistent/log",
    ]);
    assert!(stderr.contains("--state-dir"), "{stderr}");
    let stderr = run(&[
        base[0],
        base[1],
        "--simulate-only",
        "x",
        "--state-dir",
        "/nonexistent/s",
    ]);
    assert!(stderr.contains("--log-dir"), "{stderr}");
    let stderr = run(&[
        base[0],
        base[1],
        "--state-dir",
        "/nonexistent/s",
        "--log-dir",
        "/nonexistent/l",
    ]);
    assert!(stderr.contains("--simulate-only"), "{stderr}");

    let help = Command::new(env!("CARGO_BIN_EXE_membrake"))
        .args(["run", "--help"])
        .output()
        .unwrap();
    assert!(!String::from_utf8_lossy(&help.stdout).contains("simulate"));
}

// ---------------------------------------------------------------------------------------
// Faults the daemon must survive: a log that blocks, a state file that cannot be read, a
// command line that no longer parses, a group that stops being eligible.

/// A named pipe at `path`: whoever opens it for writing waits for a reader.
fn mkfifo(path: &Path) {
    let c = std::ffi::CString::new(path.as_os_str().as_bytes()).unwrap();
    // SAFETY: `c` is a NUL-terminated path.
    assert_eq!(unsafe { libc::mkfifo(c.as_ptr(), 0o600) }, 0, "mkfifo");
}

fn mode(path: &Path) -> u32 {
    fs::metadata(path).unwrap().permissions().mode() & 0o777
}

/// `text` as a POSIX regular expression that matches exactly itself.
fn escape(text: &str) -> String {
    text.chars()
        .flat_map(|c| {
            let special = ".\\+*?()[]{}^$|".contains(c);
            special.then_some('\\').into_iter().chain([c])
        })
        .collect()
}

/// A process group led by `/bin/sh` running a script, killed as a whole when dropped.
struct ShellGroup(Child);

impl ShellGroup {
    fn spawn(script: &Path, args: &[&str]) -> ShellGroup {
        ShellGroup(
            Command::new("/bin/sh")
                .arg(script)
                .args(args)
                .stdin(Stdio::null())
                .process_group(0)
                .spawn()
                .unwrap(),
        )
    }

    fn pgid(&self) -> i32 {
        self.0.id() as i32
    }

    fn alive(&mut self) -> bool {
        self.0.try_wait().unwrap().is_none()
    }

    fn stopped(&self) -> bool {
        jobs::bsd_info(self.pgid())
            .expect("the leader is visible")
            .stopped
    }

    /// The pids of the group's live members.
    fn members(&self) -> Vec<i32> {
        jobs::list_pids()
            .into_iter()
            .filter(|&pid| jobs::bsd_info(pid).is_some_and(|p| p.pgid == self.pgid() && !p.zombie))
            .collect()
    }

    /// Waits until the group has `count` members.
    fn wait_members(&self, count: usize) {
        let deadline = Instant::now() + Duration::from_secs(10);
        while self.members().len() != count {
            assert!(Instant::now() < deadline, "{:?}", self.members());
            std::thread::sleep(Duration::from_millis(10));
        }
    }

    fn footprint(&self) -> u64 {
        self.members().into_iter().map(jobs::footprint).sum()
    }

    /// The leader reaches (or leaves) the stopped state.
    fn settles(&self, stopped: bool) -> bool {
        let deadline = Instant::now() + Duration::from_secs(5);
        while self.stopped() != stopped && Instant::now() < deadline {
            std::thread::sleep(Duration::from_millis(10));
        }
        self.stopped() == stopped
    }
}

impl Drop for ShellGroup {
    fn drop(&mut self) {
        // SAFETY: signalling the group this test spawned.
        unsafe { libc::killpg(self.pgid(), libc::SIGKILL) };
        let _ = self.0.wait();
    }
}

/// Logging must never stop braking. The event log here is a named pipe nobody reads, which
/// blocks any writer that opens it. The daemon still samples, freezes at the first sign of
/// pressure and kills at the cliff, and still exits when told to.
#[test]
fn a_blocked_event_log_does_not_stop_the_brake() {
    let dir = scratch("blocked-log");
    let (state, log) = (dir.join("state"), dir.join("log"));
    fs::create_dir_all(&log).unwrap();
    mkfifo(&log.join("events.log"));
    let mine = marker();
    let mut first = Group::spawn(&mine, 1);
    let mut second = Group::spawn(&mine, 1);
    let mut script = Script::new(&dir);
    let daemon = Daemon::simulate(&script.path, &only(&mine), &state, &log);

    script.tight(2);
    daemon.wait_samples(&log, 2);
    assert!(
        second.settles(true),
        "the newest is frozen with the log blocked"
    );
    script.cliff(1);
    daemon.wait_samples(&log, 3);
    assert!(second.exited(), "and, frozen, it is killed first");
    assert!(first.alive() && first.running());
    assert!(
        daemon.terminate().success(),
        "and the daemon still shuts down"
    );
    let _ = fs::remove_dir_all(&dir);
}

/// Under launchd stdout is a file that newsyslog cannot rotate while launchd holds it open,
/// and the event log has every event already: events go to stdout only on a terminal.
#[test]
fn events_go_to_stdout_only_when_it_is_a_terminal() {
    let run = |stdout: Stdio| {
        let dir = scratch("stdout");
        let (state, log) = (dir.join("state"), dir.join("log"));
        let mut command = Command::new(env!("CARGO_BIN_EXE_membrake"));
        command
            .args(["run", "--min-job-gb", "1000000", "--state-dir"])
            .arg(&state)
            .arg("--log-dir")
            .arg(&log)
            .arg("--socket")
            .arg(dir.join("q.sock"))
            .stdout(stdout)
            .stderr(Stdio::null());
        let daemon = Daemon(command.spawn().unwrap());
        daemon.wait_started(&log);
        (daemon, dir)
    };

    // A pipe, as when something reads the daemon's output.
    let (mut daemon, dir) = run(Stdio::piped());
    let mut out = daemon.0.stdout.take().unwrap();
    assert!(daemon.terminate().success());
    let mut text = String::new();
    out.read_to_string(&mut text).unwrap();
    assert_eq!(text, "", "nothing is written to a pipe");
    let _ = fs::remove_dir_all(&dir);

    // A terminal: the start line appears.
    let (mut master, mut slave) = (0, 0);
    // SAFETY: openpty fills in two descriptors.
    let rc = unsafe {
        libc::openpty(
            &mut master,
            &mut slave,
            std::ptr::null_mut(),
            std::ptr::null_mut(),
            std::ptr::null_mut(),
        )
    };
    assert_eq!(rc, 0, "openpty");
    // SAFETY: both descriptors were just opened and are owned here.
    let (master, slave) = unsafe { (OwnedFd::from_raw_fd(master), OwnedFd::from_raw_fd(slave)) };
    let (daemon, dir) = run(Stdio::from(slave));
    let mut shown = Vec::new();
    let deadline = Instant::now() + Duration::from_secs(10);
    while !String::from_utf8_lossy(&shown).contains(" start ") {
        assert!(
            Instant::now() < deadline,
            "{}",
            String::from_utf8_lossy(&shown)
        );
        let mut chunk = [0u8; 4096];
        let mut ready = libc::pollfd {
            fd: master.as_raw_fd(),
            events: libc::POLLIN,
            revents: 0,
        };
        // SAFETY: one valid pollfd.
        if unsafe { libc::poll(&mut ready, 1, 100) } > 0 {
            // SAFETY: reading into a buffer of the length given.
            let n = unsafe { libc::read(ready.fd, chunk.as_mut_ptr().cast(), chunk.len()) };
            if n > 0 {
                shown.extend_from_slice(&chunk[..n as usize]);
            }
        }
    }
    assert!(daemon.terminate().success());
    let _ = fs::remove_dir_all(&dir);
}

/// Every SIGSTOP is on disk before it is sent, the one that starts a kill included. The state
/// file's write is held on a named pipe here: until it goes through, the group that is about
/// to be killed has not been touched.
#[test]
fn a_kill_records_its_stop_before_it_sends_it() {
    let dir = scratch("kill-record");
    let (state, log) = (dir.join("state"), dir.join("log"));
    let mine = marker();
    let mut big = Group::spawn(&mine, 3);
    let small = Group::spawn(&mine, 1);
    assert!(big.footprint() > small.footprint(), "the premise");
    let mut script = Script::new(&dir);
    let daemon = Daemon::simulate(&script.path, &only(&mine), &state, &log);
    daemon.wait_started(&log);
    let tmp = state.join("frozen.tsv.tmp");
    mkfifo(&tmp);

    script.cliff(1); // the biggest group is to be killed at once
    std::thread::sleep(Duration::from_millis(500));
    assert!(
        big.alive() && big.running(),
        "not yet stopped, not yet dead"
    );
    let mut written = String::new();
    fs::File::open(&tmp)
        .unwrap()
        .read_to_string(&mut written)
        .unwrap();
    assert!(
        written.starts_with(&format!("{}\t", big.pgid())),
        "the record names the group: {written:?}"
    );
    daemon.wait_samples(&log, 1);
    assert!(big.exited(), "and then it is killed");
    assert_eq!(records(&state), 0, "gone, so nothing is promised");
    assert!(daemon.terminate().success());
    let _ = fs::remove_dir_all(&dir);
}

/// The fail-safe resumes frozen jobs when the signals have been unreadable for a minute. If
/// the state file cannot be read just then, it resumes from the records it holds in memory:
/// forgetting the jobs without a signal would leave them frozen for good.
#[test]
fn the_failsafe_resumes_from_memory_when_the_state_file_cannot_be_read() {
    let dir = scratch("failsafe-unreadable");
    let (state, log) = (dir.join("state"), dir.join("log"));
    let mine = marker();
    let group = Group::spawn(&mine, 1);
    let mut script = Script::new(&dir);
    let daemon = Daemon::simulate(&script.path, &only(&mine), &state, &log);
    daemon.wait_started(&log);

    script.tight(2);
    daemon.wait_samples(&log, 2);
    assert!(group.settles(true));
    assert_eq!(records(&state), 1);
    let file = state.join("frozen.tsv");
    fs::set_permissions(&file, fs::Permissions::from_mode(0o000)).unwrap();
    assert!(
        fs::read_to_string(&file).is_err(),
        "the premise: unreadable"
    );

    script.unreadable(61);
    // The daemon takes a moment to get through the 61 readings; the fail-safe says so when it
    // acts (the same words in the old and the new log line).
    daemon.wait_logged(&log, "resumed because the signals were unreadable for 60 s");
    assert!(group.settles(false), "resumed all the same");
    daemon.wait_logged(&log, "using the records in memory");
    assert!(daemon.terminate().success());
    assert!(group.running());
    assert_eq!(records(&state), 0, "and forgotten, now that it is resolved");
    let _ = fs::remove_dir_all(&dir);
}

/// A frozen job that someone continues, and that then becomes something the rules forbid
/// touching (here its leader execs a different program, keeping pid and start time, so it no
/// longer matches the simulated filter), is not killed when the cliff comes, although the
/// engine still counts it as frozen. It is left running, and the next job is the victim.
#[test]
fn a_frozen_job_that_became_ineligible_is_not_killed() {
    let dir = scratch("ineligible");
    let (state, log) = (dir.join("state"), dir.join("log"));
    let mine = marker();
    let later = marker();
    let mut older = Group::spawn(&mine, 1);
    let go = dir.join("go");
    mkfifo(&go);
    let wait = dir.join("wait.sh");
    fs::write(&wait, "#!/bin/sh\nread line < \"$1\"\nexec sleep \"$2\"\n").unwrap();
    let mut newer = ShellGroup::spawn(&wait, &[go.to_str().unwrap(), &later]);
    // Only this test's own scripts: tests run side by side in one process, and a pattern
    // that named every `wait.sh` would be a licence to touch the others' groups.
    let only = format!(
        "^(sleep {}|/bin/sh {}/wait\\.sh .*)$",
        mine.replace('.', "\\."),
        escape(dir.to_str().unwrap())
    );
    let mut script = Script::new(&dir);
    let daemon = Daemon::simulate(&script.path, &only, &state, &log);
    daemon.wait_started(&log);

    // The newest eligible group is frozen.
    script.tight(2);
    daemon.wait_samples(&log, 2);
    assert!(newer.settles(true), "frozen");

    // Someone continues it, and it becomes another program: no longer eligible.
    // SAFETY: signalling the group this test spawned.
    unsafe { libc::killpg(newer.pgid(), libc::SIGCONT) };
    let deadline = Instant::now() + Duration::from_secs(10);
    loop {
        let opened = fs::OpenOptions::new()
            .write(true)
            .custom_flags(libc::O_NONBLOCK)
            .open(&go);
        if let Ok(mut file) = opened {
            file.write_all(b"go\n").unwrap();
            break;
        }
        assert!(Instant::now() < deadline, "the script never waited for go");
        std::thread::sleep(Duration::from_millis(10));
    }
    while jobs::command_line(newer.pgid()).is_none_or(|c| c != format!("sleep {later}")) {
        assert!(
            Instant::now() < deadline,
            "{:?}",
            jobs::command_line(newer.pgid())
        );
        std::thread::sleep(Duration::from_millis(10));
    }

    // The cliff: the engine still counts the group as frozen, and as the first to kill.
    script.cliff(1);
    daemon.wait_samples(&log, 3);
    assert!(newer.alive() && !newer.stopped(), "left running");
    daemon.wait_logged(&log, "not killing group");
    assert!(older.alive(), "and the other is not touched yet");
    assert_eq!(records(&state), 0, "nothing is left recorded");

    // The next cliff takes the job that is still eligible.
    script.cliff(1);
    daemon.wait_samples(&log, 4);
    assert!(older.exited());
    assert!(newer.alive());
    assert!(daemon.terminate().success());
    let _ = fs::remove_dir_all(&dir);
}

/// An agent hosted by an interpreter shows only in that member's own command line
/// (`/bin/sh .../codex`). It protects the group it runs in even though a plain shell leads
/// the group and nothing in the leader's command line names an agent.
#[test]
fn a_shell_led_group_hosting_an_agent_in_a_member_is_never_killed() {
    let dir = scratch("agent");
    let (state, log) = (dir.join("state"), dir.join("log"));
    let mine = marker();
    let hosted = marker();
    let mut ordinary = Group::spawn(&mine, 1);
    // The stand-in for the agent: a script called codex, run by a shell that the leader starts.
    let bin = dir.join("bin");
    fs::create_dir_all(&bin).unwrap();
    let agent = bin.join("codex");
    fs::write(&agent, format!("#!/bin/sh\nsleep {hosted}\n")).unwrap();
    let lead = dir.join("lead.sh");
    fs::write(
        &lead,
        format!("#!/bin/sh\n/bin/sh {} &\nwait\n", agent.display()),
    )
    .unwrap();
    let mut led = ShellGroup::spawn(&lead, &[&hosted]);
    led.wait_members(3); // the leader, the agent's shell and its sleep
    assert!(
        led.footprint() > ordinary.footprint(),
        "the premise: the shell-led group is the biggest, so it is the one to kill"
    );
    let only = format!(
        "^(sleep {}|/bin/sh {}/lead\\.sh {})$",
        escape(&mine),
        escape(dir.to_str().unwrap()),
        escape(&hosted)
    );
    let mut script = Script::new(&dir);
    let daemon = Daemon::simulate(&script.path, &only, &state, &log);
    daemon.wait_started(&log);

    script.cliff(1);
    daemon.wait_samples(&log, 1);
    assert!(
        ordinary.exited(),
        "the ordinary group is the one that is killed"
    );
    assert!(
        led.alive() && !led.stopped(),
        "the agent's group is untouched"
    );
    assert!(daemon.terminate().success());
    let _ = fs::remove_dir_all(&dir);
}

/// The state directory and files are readable by every user (so `membrake status` works for
/// a normal user) and the lock is not, whatever the umask, and also when an earlier version
/// left them as 0700 and 0600.
#[test]
fn state_permissions_are_set_explicitly_and_repaired() {
    let dir = scratch("modes");
    let (state, log) = (dir.join("state"), dir.join("log"));
    fs::create_dir_all(&state).unwrap();
    fs::set_permissions(&state, fs::Permissions::from_mode(0o700)).unwrap();
    for (name, mode) in [
        ("frozen.tsv", 0o600),
        ("frozen.tsv.tmp", 0o600),
        ("membrake.lock", 0o644),
    ] {
        let file = state.join(name);
        fs::write(&file, "").unwrap();
        fs::set_permissions(&file, fs::Permissions::from_mode(mode)).unwrap();
    }
    let mut command = Command::new(env!("CARGO_BIN_EXE_membrake"));
    command
        .args(["run", "--min-job-gb", "1000000", "--state-dir"])
        .arg(&state)
        .arg("--log-dir")
        .arg(&log)
        .arg("--socket")
        .arg(dir.join("q.sock"))
        .stdout(Stdio::null())
        .stderr(Stdio::null());
    // SAFETY: only async-signal-safe `umask` between fork and exec.
    unsafe {
        command.pre_exec(|| {
            libc::umask(0o077);
            Ok(())
        })
    };
    let daemon = Daemon(command.spawn().unwrap());
    daemon.wait_started(&log);
    assert_eq!(mode(&state), 0o755, "the directory");
    assert_eq!(mode(&state.join("frozen.tsv")), 0o644, "the state file");
    assert_eq!(mode(&state.join("membrake.lock")), 0o600, "the lock");
    // The queue's socket is open to every user and its status file readable by all, whatever
    // the umask. The status file is written by the first sample.
    assert_eq!(mode(&dir.join("q.sock")), 0o666, "the queue socket");
    let status = state.join("queue.tsv");
    let deadline = Instant::now() + Duration::from_secs(20);
    while !status.exists() {
        assert!(Instant::now() < deadline, "no queue status file");
        std::thread::sleep(Duration::from_millis(20));
    }
    assert_eq!(mode(&status), 0o644, "the queue status file");
    assert!(daemon.terminate().success());
    assert!(
        !dir.join("q.sock").exists(),
        "the socket goes with the daemon"
    );
    assert_eq!(
        mode(&state.join("frozen.tsv")),
        0o644,
        "and after a rewrite"
    );

    // A directory that does not exist yet is created with the same modes under a strict umask.
    let fresh = dir.join("fresh");
    let log = dir.join("log-of-the-second");
    let mut command = Command::new(env!("CARGO_BIN_EXE_membrake"));
    command
        .args(["run", "--min-job-gb", "1000000", "--state-dir"])
        .arg(&fresh)
        .arg("--log-dir")
        .arg(&log)
        .arg("--socket")
        .arg(dir.join("q.sock"))
        .stdout(Stdio::null())
        .stderr(Stdio::null());
    // SAFETY: as above.
    unsafe {
        command.pre_exec(|| {
            libc::umask(0o077);
            Ok(())
        })
    };
    let daemon = Daemon(command.spawn().unwrap());
    daemon.wait_started(&log);
    assert_eq!(mode(&fresh), 0o755);
    assert_eq!(mode(&fresh.join("frozen.tsv")), 0o644);
    assert!(daemon.terminate().success());
    let _ = fs::remove_dir_all(&dir);
}

/// A flag that no longer parses, or an account that has been deleted since the daemon was
/// set up, stops it from starting: but only after it has resumed the jobs a previous run left
/// frozen. launchd restarts it again and again; without the recovery, those jobs would stay
/// frozen through every restart.
#[test]
fn a_bad_command_line_fails_only_after_the_recovery() {
    for bad in [
        ["--protect-user", "no-such-user-for-membrake"],
        ["--settle-seconds", "nan"],
        ["--term-seconds", "nan"],
    ] {
        let dir = scratch("late-validation");
        let (state, log) = (dir.join("state"), dir.join("log"));
        let sleeper = Sleeper::spawn();
        sleeper.freeze_into(&state);

        let out = Command::new(env!("CARGO_BIN_EXE_membrake"))
            .args(["run", "--min-job-gb", "1000000", "--state-dir"])
            .arg(&state)
            .arg("--log-dir")
            .arg(&log)
            .args(bad)
            .output()
            .unwrap();
        assert!(!out.status.success(), "{bad:?}: a bad flag is an error");
        let stderr = String::from_utf8_lossy(&out.stderr);
        assert!(
            stderr.contains("no such user") || stderr.contains("non-negative"),
            "{bad:?}: {stderr}"
        );
        assert!(sleeper.settles(false), "{bad:?}: the job was resumed first");
        assert_eq!(records(&state), 0, "{bad:?}");
        let said = events(&log);
        assert!(said.contains("recovered at start"), "{bad:?}: {said}");
        let _ = fs::remove_dir_all(&dir);
    }
}

// ---------------------------------------------------------------------------------------
// The queue (`membrake exec`), against a real daemon on scripted readings

/// A socket path of its own, short enough for `sockaddr_un` (104 bytes) wherever the
/// temporary directory is. The file, if there is one, is removed when this is dropped.
struct TempSocket(PathBuf);

impl std::ops::Deref for TempSocket {
    type Target = Path;
    fn deref(&self) -> &Path {
        &self.0
    }
}

impl AsRef<Path> for TempSocket {
    fn as_ref(&self) -> &Path {
        &self.0
    }
}

impl Drop for TempSocket {
    fn drop(&mut self) {
        let _ = fs::remove_file(&self.0);
    }
}

fn queue_socket(name: &str) -> TempSocket {
    let path = std::env::temp_dir().join(format!("mbq-{}-{name}.sock", std::process::id()));
    assert!(path.as_os_str().len() < 100, "{}", path.display());
    let _ = fs::remove_file(&path);
    TempSocket(path)
}

/// Polls `done` for up to `seconds`, failing with `what` if it never holds.
fn wait_until(what: &str, seconds: u64, mut done: impl FnMut() -> bool) {
    let deadline = Instant::now() + Duration::from_secs(seconds);
    while !done() {
        assert!(Instant::now() < deadline, "timed out waiting for {what}");
        std::thread::sleep(Duration::from_millis(20));
    }
}

/// Waits until the daemon is listening on `socket`: the file appears when it is bound, a moment
/// before it listens, and a connection in between is refused.
fn wait_for_the_queue(socket: &Path) {
    wait_until("the queue socket", 60, || {
        std::os::unix::net::UnixStream::connect(socket).is_ok()
    });
}

/// A simulated daemon that serves the queue on `socket`. `only` decides which groups the
/// brake may touch; most queue tests give it a pattern nothing matches, so that no process
/// at all can be frozen or killed.
fn queue_daemon(script: &Script, dir: &Path, socket: &Path, only: &str) -> Daemon {
    queue_daemon_env(script, dir, socket, only, &[])
}

/// `queue_daemon` with environment variables for the daemon.
fn queue_daemon_env(
    script: &Script,
    dir: &Path,
    socket: &Path,
    only: &str,
    env: &[(&str, &str)],
) -> Daemon {
    let (state, log) = (dir.join("state"), dir.join("log"));
    let mut command = Daemon::simulate_command(&script.path, only, &state, &log);
    command
        .arg("--socket")
        .arg(socket)
        .envs(env.iter().copied());
    let daemon = Daemon(command.spawn().unwrap());
    daemon.wait_started(&log);
    wait_for_the_queue(socket);
    daemon
}

const NOTHING: &str = "^no group is called this$";

/// A `membrake exec` client with its stderr in a file. It is told to stop, and then killed,
/// when dropped.
struct Client {
    child: Child,
    stderr: PathBuf,
}

impl Client {
    fn spawn(dir: &Path, name: &str, socket: &Path, options: &[&str], command: &[&str]) -> Client {
        Client::spawn_with(dir, name, socket, options, command, false, &[])
    }

    /// `admitted`: the client runs as inside a command the queue already admitted. `env`:
    /// variables for the client.
    fn spawn_with(
        dir: &Path,
        name: &str,
        socket: &Path,
        options: &[&str],
        command: &[&str],
        admitted: bool,
        env: &[(&str, &str)],
    ) -> Client {
        let stderr = dir.join(format!("{name}.stderr"));
        let mut exec = Command::new(env!("CARGO_BIN_EXE_membrake"));
        exec.envs(env.iter().copied());
        // These tests may themselves be running inside an admitted command.
        exec.env_remove("MEMBRAKE_ADMITTED");
        if admitted {
            exec.env("MEMBRAKE_ADMITTED", "1");
        }
        let child = exec
            .arg("exec")
            .arg("--socket")
            .arg(socket)
            .args(options)
            .arg("--")
            .args(command)
            .stdout(Stdio::null())
            .stderr(fs::File::create(&stderr).unwrap())
            .spawn()
            .unwrap();
        Client { child, stderr }
    }

    fn said(&self) -> String {
        fs::read_to_string(&self.stderr).unwrap_or_default()
    }

    fn wait_said(&self, text: &str) {
        wait_until(&format!("{text:?} on stderr"), 20, || {
            self.said().contains(text)
        });
    }

    fn running(&mut self) -> bool {
        self.child.try_wait().unwrap().is_none()
    }

    fn pid(&self) -> i32 {
        self.child.id() as i32
    }

    /// The exit code, once it has ended, with a calm reading appended at every look: the
    /// daemon decides only on the readings it is given, and a client that has to wait for a
    /// turn waits for ever on a script that has run out.
    fn code_while_calm(&mut self, script: &mut Script) -> Option<i32> {
        let mut status = None;
        wait_until("the client to end", 30, || {
            script.calm(1);
            status = self.child.try_wait().unwrap();
            status.is_some()
        });
        status.and_then(|s| s.code())
    }

    /// The exit code, once it has ended.
    fn code(&mut self) -> Option<i32> {
        let mut status = None;
        wait_until("the client to end", 30, || {
            status = self.child.try_wait().unwrap();
            status.is_some()
        });
        status.and_then(|s| s.code())
    }
}

impl Drop for Client {
    fn drop(&mut self) {
        // Already ended and reaped: its pid may belong to another process by now.
        if self.child.try_wait().ok().flatten().is_some() {
            return;
        }
        // SIGTERM first: the client passes it to the group it started.
        // SAFETY: signalling the child this test spawned, which has not been reaped.
        unsafe { libc::kill(self.child.id() as i32, libc::SIGTERM) };
        let deadline = Instant::now() + Duration::from_secs(3);
        while self.child.try_wait().ok().flatten().is_none() && Instant::now() < deadline {
            std::thread::sleep(Duration::from_millis(10));
        }
        let _ = self.child.kill();
        let _ = self.child.wait();
    }
}

fn marker_file(dir: &Path, name: &str) -> String {
    dir.join(name).to_string_lossy().into_owned()
}

/// While memory is tight the client is told its place and waits, and its command does not
/// run; when memory is calm it runs. The command is `touch`, so that "it ran" is a fact on
/// disk and not the client's say-so.
#[test]
fn queue_a_client_waits_while_memory_is_tight_and_runs_when_it_is_calm() {
    let dir = scratch("qwait");
    let socket = queue_socket("wait");
    let mut script = Script::new(&dir);
    script.tight(3);
    let log = dir.join("log");
    let daemon = queue_daemon(&script, &dir, &socket, NOTHING);

    let ran = marker_file(&dir, "ran");
    let mut client = Client::spawn(&dir, "c", &socket, &["--label", "tests"], &["touch", &ran]);
    client.wait_said("membrake: queued (position 1): memory is tight");
    script.tight(8);
    daemon.wait_samples(&log, 11);
    assert!(client.running(), "held while memory is tight");
    assert!(!Path::new(&ran).exists(), "and its command has not run");

    script.calm(8);
    assert_eq!(client.code(), Some(0));
    assert!(Path::new(&ran).exists(), "it ran once memory was calm");
    assert_eq!(client.said().matches("queued (position 1)").count(), 1);
    assert!(daemon.terminate().success());
    let _ = fs::remove_dir_all(&dir);
}

/// The client exits with its command's exit code, with 128 plus the signal when the client is
/// signalled and passes the signal on, and with a shell's 127 when there is no such command.
#[test]
fn queue_the_exit_code_is_passed_through() {
    let dir = scratch("qcode");
    let socket = queue_socket("code");
    let mut script = Script::new(&dir);
    script.calm(10);
    let daemon = queue_daemon(&script, &dir, &socket, NOTHING);

    // Three clients one after the other, each using the latest calm memory reading.
    let mut three = Client::spawn(&dir, "three", &socket, &[], &["sh", "-c", "exit 3"]);
    assert_eq!(three.code_while_calm(&mut script), Some(3));

    let mut missing = Client::spawn(&dir, "missing", &socket, &[], &["membrake-no-such-command"]);
    assert_eq!(missing.code_while_calm(&mut script), Some(127));
    assert!(
        missing.said().contains("command not found"),
        "{}",
        missing.said()
    );

    // SIGTERM to the client reaches the command (here a `sleep` that `sh` became), which
    // dies of it: 128 + 15.
    let started = marker_file(&dir, "started");
    let mut slow = Client::spawn(
        &dir,
        "slow",
        &socket,
        &[],
        &["sh", "-c", &format!("touch {started}; exec sleep 300")],
    );
    wait_until("the command to start", 20, || {
        script.calm(1);
        Path::new(&started).exists()
    });
    // SAFETY: signalling the client this test spawned.
    unsafe { libc::kill(slow.pid(), libc::SIGTERM) };
    assert_eq!(slow.code(), Some(128 + 15));
    assert!(daemon.terminate().success());
    let _ = fs::remove_dir_all(&dir);
}

/// A client that is killed while it waits gives up its place: the one behind it moves up and
/// is the next to run, and the one that left never runs anything.
#[test]
fn queue_a_client_killed_while_waiting_releases_its_turn() {
    let dir = scratch("qkill");
    let socket = queue_socket("kill");
    let mut script = Script::new(&dir);
    script.tight(3);
    let daemon = queue_daemon(&script, &dir, &socket, NOTHING);

    let (first_ran, second_ran) = (marker_file(&dir, "first"), marker_file(&dir, "second"));
    let mut first = Client::spawn(&dir, "a", &socket, &[], &["touch", &first_ran]);
    first.wait_said("position 1");
    let mut second = Client::spawn(&dir, "b", &socket, &[], &["touch", &second_ran]);
    second.wait_said("position 2");

    first.child.kill().unwrap();
    first.child.wait().unwrap();
    second.wait_said("position 1");

    script.calm(8);
    assert_eq!(second.code(), Some(0));
    assert!(Path::new(&second_ran).exists());
    assert!(
        !Path::new(&first_ran).exists(),
        "the client that left never ran"
    );
    assert!(daemon.terminate().success());
    let _ = fs::remove_dir_all(&dir);
}

/// With no daemon the command runs at once, after one line saying why, and its exit code is
/// still passed on. The same when the "daemon" answers with an error, and when it hangs up.
#[test]
fn queue_a_client_fails_open_when_the_daemon_cannot_be_used() {
    let dir = scratch("qopen");

    let nobody = queue_socket("nobody");
    let mut client = Client::spawn(&dir, "none", &nobody, &[], &["sh", "-c", "exit 3"]);
    assert_eq!(client.code(), Some(3));
    let said = client.said();
    assert!(
        said.starts_with("membrake: queue unavailable (no daemon is listening on")
            && said.trim_end().ends_with("; running now"),
        "{said}"
    );

    // A socket nobody listens on any more is the same as none.
    let stale = queue_socket("stale");
    drop(std::os::unix::net::UnixListener::bind(&stale).unwrap());
    let mut client = Client::spawn(&dir, "stale", &stale, &[], &["sh", "-c", "exit 4"]);
    assert_eq!(client.code(), Some(4));
    assert!(
        client.said().contains("queue unavailable"),
        "{}",
        client.said()
    );

    // A daemon that errors, and one that hangs up on the request.
    let broken = queue_socket("broken");
    let listener = std::os::unix::net::UnixListener::bind(&broken).unwrap();
    let server = std::thread::spawn(move || {
        for reply in [Some("ERROR\tboom\n"), None] {
            let (mut stream, _) = listener.accept().unwrap();
            let mut line = [0u8; 64];
            let _ = stream.read(&mut line);
            if let Some(reply) = reply {
                stream.write_all(reply.as_bytes()).unwrap();
            }
        }
    });
    let mut client = Client::spawn(&dir, "error", &broken, &[], &["sh", "-c", "exit 5"]);
    assert_eq!(client.code(), Some(5));
    assert!(
        client.said().contains("queue unavailable (boom)"),
        "{}",
        client.said()
    );
    let mut client = Client::spawn(&dir, "hangup", &broken, &[], &["sh", "-c", "exit 6"]);
    assert_eq!(client.code(), Some(6));
    assert!(
        client
            .said()
            .contains("queue unavailable (the daemon closed the connection)"),
        "{}",
        client.said()
    );
    server.join().unwrap();
    let _ = fs::remove_dir_all(&dir);
}

/// The times of the keep-alive, shortened for the tests (see `membrake::wire::timing`): a
/// client gives up on a daemon that has said nothing for 4 s, and the daemon says where a
/// client stands at least every 0.67 s.
const SHORT: &[(&str, &str)] = &[("MEMBRAKE_TEST_SILENCE_MS", "4000")];
const SILENCE: Duration = Duration::from_millis(4000);

/// A daemon that answers once and then says nothing, with the connection open, is a daemon
/// that has stopped answering: the client runs its command instead of waiting for ever, and
/// says why. (A real daemon that stopped answering, because it hung or its socket thread
/// died, looks like this to a client.)
#[test]
fn queue_a_client_fails_open_when_the_daemon_goes_silent_after_its_first_answer() {
    let dir = scratch("qsilent");
    let socket = queue_socket("silent");
    let listener = std::os::unix::net::UnixListener::bind(&socket).unwrap();
    let (release, released) = std::sync::mpsc::channel::<()>();
    let (answered, answers) = std::sync::mpsc::channel::<Instant>();
    let server = std::thread::spawn(move || {
        let (mut stream, _) = listener.accept().unwrap();
        let mut line = [0u8; 256];
        let _ = stream.read(&mut line);
        stream.write_all(b"QUEUED\t1\t1073741824\n").unwrap();
        answered.send(Instant::now()).unwrap();
        // Silent, and open, until the test is over.
        let _ = released.recv();
    });

    let mut client = Client::spawn_with(
        &dir,
        "c",
        &socket,
        &[],
        &["sh", "-c", "exit 7"],
        false,
        SHORT,
    );
    assert_eq!(
        client.code(),
        Some(7),
        "it ran its command: {}",
        client.said()
    );
    // The limit runs from the first answer, so that is where the silence is measured from.
    let answered_at = answers.recv().unwrap();
    assert!(
        answered_at.elapsed() >= SILENCE,
        "not before the daemon had been silent for the whole limit: {:?}",
        answered_at.elapsed()
    );
    assert!(
        client
            .said()
            .contains("membrake: queue unavailable (the daemon stopped answering); running now"),
        "{}",
        client.said()
    );
    release.send(()).unwrap();
    server.join().unwrap();
    let _ = fs::remove_dir_all(&dir);
}

/// A client that waits for a turn through a tight spell longer than its silence limit is not
/// given up on: the daemon tells it where it stands every keep-alive though the place has not
/// changed, so a running daemon never lets it hear nothing. The hand-made client, which
/// asked first, only listens, and sees the same line repeated; the real one is still waiting
/// when its limit has passed, and runs its command when memory is calm. The daemon is healthy
/// all that time: its sampling loop keeps getting readings, every one tight, so it keeps
/// scheduling the queue and deciding that nobody may start. (The lines themselves come from
/// the socket thread; see the next test for a daemon whose sampling loop is not scheduling.)
#[test]
fn queue_a_waiting_client_is_kept_alive_through_a_tight_spell_longer_than_its_silence_limit() {
    let dir = scratch("qkeep");
    let socket = queue_socket("keep");
    let mut script = Script::new(&dir);
    script.tight_and_quiet(3);
    let daemon = queue_daemon_env(&script, &dir, &socket, NOTHING, SHORT);
    // Start with a known reason so every keep-alive can be compared byte for byte.
    daemon.wait_samples(&dir.join("log"), 3);

    // The hand-made client only listens. It asks first, and is told its place at once (the
    // socket thread answers, whatever the sampling thread is doing).
    let mut listener = std::os::unix::net::UnixStream::connect(&socket).unwrap();
    listener
        .set_read_timeout(Some(Duration::from_millis(100)))
        .unwrap();
    listener.write_all(b"REQUEST\tv1\tlistener\tsig\n").unwrap();
    // The real client asks second, and says so once it has waited long enough to say anything.
    let ran = marker_file(&dir, "ran");
    let mut client = Client::spawn_with(&dir, "c", &socket, &[], &["touch", &ran], false, SHORT);
    client.wait_said("position 2");

    let mut input = Vec::new();
    let mut lines: Vec<String> = Vec::new();
    let started = Instant::now();
    let mut fed = started;
    while started.elapsed() < SILENCE + SILENCE / 2 {
        // A reading for the sampling loop every eighth of the limit for a loop that has
        // stopped scheduling. (Readings past the brake's freeze line make it scan the process
        // table, which on a busy machine is slow enough to fall behind the script: in one run,
        // with such a reading every 0.1 s, the daemon had got through 62 of 64 rows 30 s after
        // the last was appended; in another, at a load average near 50, it had recorded 2 of
        // 6 rows 5 s in, and its loop was rightly called stalled.)
        if fed.elapsed() >= SILENCE / 8 {
            script.tight_and_quiet(1);
            fed = Instant::now();
        }
        let mut chunk = [0u8; 256];
        if let Ok(n) = listener.read(&mut chunk) {
            input.extend_from_slice(&chunk[..n]);
        }
        while let Some(line) = membrake::wire::take_line(&mut input) {
            lines.push(line);
        }
        assert!(client.running(), "given up on: {}", client.said());
    }
    assert!(lines.len() >= 6, "one at least every 0.67 s: {lines:?}");
    assert!(
        lines.iter().all(|l| l == &lines[0])
            && lines[0].starts_with("QUEUED\t1\t")
            && lines[0].ends_with("\ttight"),
        "the same place, said again: {lines:?}"
    );
    assert!(!Path::new(&ran).exists(), "and its command has not run");

    script.calm(8);
    assert_eq!(client.code(), Some(0));
    assert!(Path::new(&ran).exists());
    assert!(!client.said().contains("unavailable"), "{}", client.said());
    assert!(daemon.terminate().success());
    let _ = fs::remove_dir_all(&dir);
}

/// The lines that keep a client waiting come from the daemon's socket thread, which shows that
/// the daemon is up, not that its sampling loop is scheduling anything. Here the loop is
/// alive and never does: every reading is unreadable, so it never reaches the queue, and no
/// turn will ever be given. Past the silence limit the clients that wait are told so
/// (`ERROR`, which the real one takes as the queue being unavailable and runs its command)
/// instead of being told their place for ever, and so is one that asks while it lasts; once
/// the readings are good again the queue serves clients as before.
#[test]
fn queue_clients_are_sent_away_while_the_sampling_loop_is_not_scheduling() {
    let dir = scratch("qstalled");
    let socket = queue_socket("stalled");
    let mut script = Script::new(&dir);
    script.tight_and_quiet(3);
    let log = dir.join("log");
    let daemon = queue_daemon_env(&script, &dir, &socket, NOTHING, SHORT);
    daemon.wait_samples(&log, 3);

    // Two clients wait: one that only listens, and a real one.
    let mut listener = std::os::unix::net::UnixStream::connect(&socket).unwrap();
    listener
        .set_read_timeout(Some(Duration::from_millis(100)))
        .unwrap();
    listener.write_all(b"REQUEST\tv1\tlistener\tsig\n").unwrap();
    let ran = marker_file(&dir, "ran");
    let mut client = Client::spawn_with(&dir, "c", &socket, &[], &["touch", &ran], false, SHORT);
    client.wait_said("position 2");

    // From here on the readings are unreadable, one every eighth of the silence limit (the loop
    // is alive), until the listener has been told, which takes the silence limit.
    let (mut input, mut lines) = (Vec::new(), Vec::new());
    let started = Instant::now();
    let mut fed = started;
    script.unreadable(1);
    while !lines.iter().any(|l: &String| l.starts_with("ERROR")) {
        assert!(
            started.elapsed() < SILENCE * 4,
            "never sent away: {lines:?}, {}",
            client.said()
        );
        if fed.elapsed() >= SILENCE / 8 {
            script.unreadable(1);
            fed = Instant::now();
        }
        let mut chunk = [0u8; 256];
        if let Ok(n) = listener.read(&mut chunk) {
            input.extend_from_slice(&chunk[..n]);
        }
        while let Some(line) = membrake::wire::take_line(&mut input) {
            lines.push(line);
        }
    }
    assert_eq!(
        lines.last().map(String::as_str),
        Some("ERROR\tthe queue is not scheduling"),
        "{lines:?}"
    );
    assert_eq!(client.code(), Some(0), "{}", client.said());
    assert!(Path::new(&ran).exists(), "the real client ran its command");
    assert!(
        client
            .said()
            .contains("queue unavailable (the queue is not scheduling); running now"),
        "{}",
        client.said()
    );

    // A client that asks while the loop is still not scheduling is sent away at once.
    let mut late = std::os::unix::net::UnixStream::connect(&socket).unwrap();
    late.set_read_timeout(Some(Duration::from_secs(10)))
        .unwrap();
    late.write_all(b"REQUEST\tv1\tlate\tsig\n").unwrap();
    let mut said = [0u8; 256];
    let n = late.read(&mut said).unwrap();
    assert_eq!(
        String::from_utf8_lossy(&said[..n]),
        "ERROR\tthe queue is not scheduling\n"
    );

    // Good readings again: an ordinary client waits for its turn and runs.
    script.calm(3);
    daemon.wait_samples(&log, 6);
    let again = marker_file(&dir, "again");
    let mut served = Client::spawn_with(&dir, "s", &socket, &[], &["touch", &again], false, SHORT);
    assert_eq!(served.code_while_calm(&mut script), Some(0));
    assert!(Path::new(&again).exists());
    assert!(!served.said().contains("unavailable"), "{}", served.said());
    assert!(daemon.terminate().success());
    let _ = fs::remove_dir_all(&dir);
}

/// Inside an admitted command a `membrake exec` runs its command directly. The proof is a
/// daemon that would hold it for good (memory is tight) and never hears of it.
#[test]
fn queue_a_nested_call_runs_directly() {
    let dir = scratch("qnest");
    let socket = queue_socket("nest");
    let mut script = Script::new(&dir);
    script.tight(5);
    let log = dir.join("log");
    let daemon = queue_daemon(&script, &dir, &socket, NOTHING);

    let mut nested =
        Client::spawn_with(&dir, "n", &socket, &[], &["sh", "-c", "exit 3"], true, &[]);
    assert_eq!(nested.code(), Some(3));
    assert_eq!(nested.said(), "", "no queue, no line");
    script.tight(2);
    daemon.wait_samples(&log, 7);
    assert!(
        !events(&log).contains("asks for a turn"),
        "{}",
        events(&log)
    );
    assert!(daemon.terminate().success());
    let _ = fs::remove_dir_all(&dir);
}

/// `status` shows the learned cap and the memory in use, who waits (with place, session,
/// label and need) and what runs.
#[test]
fn queue_status_lists_the_waiting_and_the_admitted() {
    let dir = scratch("qstat");
    let socket = queue_socket("stat");
    let mut script = Script::new(&dir);
    script.calm(10);
    let (state, log) = (dir.join("state"), dir.join("log"));
    let daemon = queue_daemon(&script, &dir, &socket, NOTHING);

    let started = marker_file(&dir, "started");
    let mut running = Client::spawn(
        &dir,
        "run",
        &socket,
        &["--label", "alpha"],
        &["sh", "-c", &format!("touch {started}; exec sleep 300")],
    );
    wait_until("alpha to run", 20, || Path::new(&started).exists());
    // The daemon reads its script at its own pace: beta must ask only once it has read
    // that memory is tight, or a calm reading could still admit it.
    script.tight(3);
    daemon.wait_samples(&log, 13);
    let ran = marker_file(&dir, "beta-ran");
    let waiting = Client::spawn(
        &dir,
        "wait",
        &socket,
        &["--label", "beta"],
        &["touch", &ran],
    );
    waiting.wait_said("position 1");
    script.tight(3);
    daemon.wait_samples(&log, 16);
    // The status file is written by a thread of its own, behind the sample that changed it.
    wait_until(
        "the status file to show alpha running and beta waiting",
        20,
        || {
            membrake::broker::read_queue(&state)
                .ok()
                .flatten()
                .is_some_and(|q| q.running.len() == 1 && q.waiting.len() == 1)
        },
    );

    let out = Command::new(env!("CARGO_BIN_EXE_membrake"))
        .arg("status")
        .arg("--state-dir")
        .arg(&state)
        .output()
        .unwrap();
    let out = String::from_utf8_lossy(&out.stdout);
    let queue: Vec<&str> = out
        .lines()
        .skip_while(|l| !l.starts_with("queue"))
        .take_while(|l| !l.trim().is_empty())
        .collect();
    let queue = queue.join("\n");
    assert!(queue.starts_with("queue       cap "), "{out}");
    assert!(queue.contains("1 running, 1 waiting"), "{queue}");
    assert!(
        queue.contains("running   group") && queue.contains("alpha"),
        "{queue}"
    );
    assert!(
        queue
            .lines()
            .any(|l| l.contains("waiting   #1") && l.contains("needs") && l.contains("beta")),
        "{queue}"
    );
    assert!(!Path::new(&ran).exists());
    // The session is worked out by the daemon from the client's pid as the kernel reports it:
    // the same as working it out here from the pid of the client this test spawned.
    let agents = membrake::jobs::Regex::new(membrake::jobs::AGENT_CLI).unwrap();
    let session = membrake::jobs::session_of(&agents, waiting.pid());
    assert!(
        queue.contains(&format!("session {session} ")),
        "session {session}: {queue}"
    );

    // SAFETY: signalling the client this test spawned, which passes it to its command.
    unsafe { libc::kill(running.pid(), libc::SIGTERM) };
    assert_eq!(running.code(), Some(128 + 15));
    assert!(daemon.terminate().success());
    let _ = fs::remove_dir_all(&dir);
}

/// The command is a process group of its own at the caller's priority (no nice, no clamp: the
/// brake guards memory, not the CPU), and knows it was admitted, so what it starts does not
/// queue again.
#[test]
fn queue_the_command_runs_at_the_callers_priority_in_a_group_of_its_own() {
    let dir = scratch("qprio");
    let socket = queue_socket("prio");
    let mut script = Script::new(&dir);
    script.calm(10);
    let state = dir.join("state");
    let daemon = queue_daemon(&script, &dir, &socket, NOTHING);

    let env = marker_file(&dir, "env");
    let client = Client::spawn(
        &dir,
        "c",
        &socket,
        &[],
        &[
            "sh",
            "-c",
            &format!("echo $MEMBRAKE_ADMITTED > {env}; exec sleep 300"),
        ],
    );
    let mut group = 0;
    wait_until("the group to be known", 30, || {
        script.calm(1);
        let queue = membrake::broker::read_queue(&state).ok().flatten();
        group = queue
            .and_then(|q| q.running.first().map(|r| r.pgid))
            .unwrap_or(0);
        group > 1 && Path::new(&env).exists()
    });
    assert_ne!(group, client.pid(), "not the client's group");
    let leader = jobs::bsd_info(group).expect("the command is running");
    assert_eq!(leader.pgid, group, "it leads the group it runs in");
    // SAFETY: getpriority has no preconditions.
    let nice = unsafe { libc::getpriority(libc::PRIO_PROCESS, group as libc::id_t) };
    // SAFETY: as above.
    let caller = unsafe { libc::getpriority(libc::PRIO_PROCESS, 0) };
    assert_eq!(nice, caller, "the command keeps the caller's priority");
    assert_eq!(fs::read_to_string(&env).unwrap().trim(), "1");
    drop(client);
    assert!(daemon.terminate().success());
    let _ = fs::remove_dir_all(&dir);
}

/// What a command was seen to need is written down when it is done, under its signature.
#[test]
fn queue_the_learned_size_is_written_after_the_command_is_done() {
    let dir = scratch("qsize");
    let socket = queue_socket("size");
    let mut script = Script::new(&dir);
    script.calm(10);
    let state = dir.join("state");
    let daemon = queue_daemon(&script, &dir, &socket, NOTHING);
    assert!(!state.join("sizes.tsv").exists());

    let mut client = Client::spawn(&dir, "c", &socket, &[], &["sleep", "2"]);
    // A reading while it runs measures the group; keep the readings coming.
    wait_until("the group to be measured", 30, || {
        script.calm(1);
        membrake::broker::read_queue(&state)
            .ok()
            .flatten()
            .is_some_and(|q| q.running.iter().any(|r| r.pgid > 1 && r.peak > 0))
    });
    assert_eq!(client.code(), Some(0));
    wait_until("sizes.tsv", 20, || state.join("sizes.tsv").exists());
    let (sizes, skipped) =
        membrake::sizes::Sizes::parse(&fs::read_to_string(state.join("sizes.tsv")).unwrap());
    assert_eq!(skipped, 0);
    assert!(
        sizes.get("sleep 2").is_some_and(|peak| peak > 0),
        "{}",
        sizes.render()
    );
    assert_eq!(mode(&state.join("sizes.tsv")), 0o644);
    assert!(daemon.terminate().success());

    // A restarted daemon starts out knowing it.
    let again = queue_daemon(&script, &dir, &socket, NOTHING);
    script.calm(1);
    again.wait_logged(&dir.join("log"), "1 sizes learned");
    assert!(again.terminate().success());
    let _ = fs::remove_dir_all(&dir);
}

/// The queue and the brake are one ordered system: while the brake holds a job frozen nothing
/// is admitted, however calm memory looks, and the queue goes on as soon as it is resumed.
#[test]
fn queue_nothing_is_admitted_while_a_job_is_frozen() {
    let dir = scratch("qfrozen");
    let socket = queue_socket("frozen");
    let (state, log) = (dir.join("state"), dir.join("log"));
    let mine = marker();
    let group = Group::spawn(&mine, 1);
    let mut script = Script::new(&dir);
    let daemon = queue_daemon(&script, &dir, &socket, &only(&mine));

    script.tight(2);
    daemon.wait_samples(&log, 2);
    assert!(group.settles(true), "the brake froze the group");
    assert_eq!(records(&state), 1);

    let ran = marker_file(&dir, "ran");
    let mut client = Client::spawn(&dir, "c", &socket, &[], &["touch", &ran]);
    client.wait_said("queued (position 1)");
    // Calm, but inside the 10 s the brake waits before it resumes anything.
    script.calm(8);
    daemon.wait_samples(&log, 10);
    assert!(
        client.running() && !Path::new(&ran).exists(),
        "held while frozen"
    );
    assert!(!group.running(), "still frozen");

    script.calm(8);
    assert!(group.settles(false), "the brake resumed it");
    assert_eq!(client.code(), Some(0));
    assert!(Path::new(&ran).exists());
    assert!(daemon.terminate().success());
    let _ = fs::remove_dir_all(&dir);
}

/// A dry run never holds a client, and writes nothing into the state directory.
#[test]
fn queue_a_dry_run_never_holds_a_client() {
    let dir = scratch("qdry");
    let socket = queue_socket("dry");
    let (state, log) = (dir.join("state"), dir.join("log"));
    let daemon = Daemon(
        Command::new(env!("CARGO_BIN_EXE_membrake"))
            .args(["run", "--dry-run", "--min-job-gb", "1000000", "--state-dir"])
            .arg(&state)
            .arg("--log-dir")
            .arg(&log)
            .arg("--socket")
            .arg(&*socket)
            .stdout(Stdio::null())
            .stderr(Stdio::null())
            .spawn()
            .unwrap(),
    );
    daemon.wait_started(&log);
    wait_for_the_queue(&socket);

    let mut client = Client::spawn(&dir, "c", &socket, &[], &["sh", "-c", "exit 3"]);
    assert_eq!(client.code(), Some(3));
    assert!(!client.said().contains("unavailable"), "{}", client.said());
    assert!(daemon.terminate().success());
    assert!(
        !state.exists(),
        "a dry run leaves the state directory alone"
    );
    let _ = fs::remove_dir_all(&dir);
}

// ---------------------------------------------------------------------------------------
// A simulated daemon reaches only what the test spawned, and never at a stale moment.

/// A `sleep` that leads a group of its own and is not a child of this process: a helper that
/// has since exited started it, so launchd is its parent. Killed when dropped.
struct Orphan {
    pid: i32,
    arg: String,
}

impl Orphan {
    fn spawn(arg: &str) -> Orphan {
        use std::ffi::CString;
        let program = CString::new("sleep").unwrap();
        let operand = CString::new(arg).unwrap();
        let argv = [program.as_ptr(), operand.as_ptr(), std::ptr::null()];
        let mut ends = [0; 2];
        // SAFETY: `ends` has room for the two descriptors pipe fills in.
        assert_eq!(unsafe { libc::pipe(ends.as_mut_ptr()) }, 0, "pipe");
        // SAFETY: after each fork the child makes only async-signal-safe calls (fork, close,
        // setpgid, write, execvp, _exit) on things prepared before the first.
        let helper = unsafe { libc::fork() };
        assert!(helper >= 0, "fork");
        if helper == 0 {
            // SAFETY: as above.
            unsafe {
                match libc::fork() {
                    0 => {
                        libc::close(ends[0]);
                        libc::close(ends[1]);
                        libc::setpgid(0, 0);
                        libc::execvp(program.as_ptr(), argv.as_ptr());
                        libc::_exit(127)
                    }
                    started => {
                        libc::write(ends[1], (&raw const started).cast(), size_of::<i32>());
                        libc::_exit(0)
                    }
                }
            }
        }
        let mut pid = 0i32;
        // SAFETY: reading four bytes into an i32; the helper, and so the write end, is ours.
        let got = unsafe {
            libc::close(ends[1]);
            let got = libc::read(ends[0], (&raw mut pid).cast(), size_of::<i32>());
            libc::close(ends[0]);
            libc::waitpid(helper, std::ptr::null_mut(), 0);
            got
        };
        assert_eq!(
            got as usize,
            size_of::<i32>(),
            "the helper said who it started"
        );
        let orphan = Orphan {
            pid,
            arg: arg.to_string(),
        };
        let deadline = Instant::now() + Duration::from_secs(5);
        while !jobs::bsd_info(pid).is_some_and(|p| p.pgid == pid && p.ppid == 1)
            || jobs::command_line(pid) != Some(format!("sleep {arg}"))
        {
            assert!(Instant::now() < deadline, "the orphan never settled");
            std::thread::sleep(Duration::from_millis(10));
        }
        orphan
    }

    fn job(&self) -> Job {
        let p = jobs::bsd_info(self.pid).expect("the orphan is visible");
        Job {
            pgid: self.pid,
            leader_pid: self.pid,
            start_us: p.start_us,
            uid: p.uid,
            bytes: 1 << 30,
            procs: 1,
            prefer: false,
            protected: false,
            stopped: false,
            command: format!("sleep {}", self.arg),
        }
    }

    fn stopped(&self) -> bool {
        jobs::bsd_info(self.pid)
            .expect("the orphan is visible")
            .stopped
    }

    fn settles(&self, stopped: bool) -> bool {
        let deadline = Instant::now() + Duration::from_secs(5);
        while self.stopped() != stopped && Instant::now() < deadline {
            std::thread::sleep(Duration::from_millis(10));
        }
        self.stopped() == stopped
    }
}

impl Drop for Orphan {
    fn drop(&mut self) {
        // Only the process this test started: the pid is checked against its command line.
        if jobs::command_line(self.pid) == Some(format!("sleep {}", self.arg)) {
            // SAFETY: signalling the process the helper started for this test.
            unsafe { libc::kill(self.pid, libc::SIGKILL) };
        }
    }
}

/// `--simulate-only` is a regular expression, and a pattern that matches everything is
/// accepted. The confinement is not the pattern: a group is eligible only if its leader is a
/// child of the test that started the daemon. The decoy here matches the pattern, is newer
/// than the group that is meant, and is not a child of this process: it is never frozen, and
/// at the cliff never killed.
#[test]
fn a_simulated_daemon_never_touches_a_group_that_matches_but_the_test_did_not_spawn() {
    let dir = scratch("decoy");
    let (state, log) = (dir.join("state"), dir.join("log"));
    let mine = marker();
    let mut target = Group::spawn(&mine, 1);
    let decoy = Orphan::spawn(&mine); // newer, so it is the first choice if it is eligible
    assert!(target.start_us() < jobs::bsd_info(decoy.pid).unwrap().start_us);
    let mut script = Script::new(&dir);
    let daemon = Daemon::simulate(&script.path, &only(&mine), &state, &log);
    daemon.wait_started(&log);

    script.tight(2);
    daemon.wait_samples(&log, 2);
    assert!(
        target.settles(true),
        "the group this test spawned is frozen"
    );
    assert!(!decoy.stopped(), "and the look-alike is not");

    script.cliff(1);
    daemon.wait_samples(&log, 3);
    assert!(target.exited(), "it is the one killed");
    assert!(!decoy.stopped(), "the look-alike is still running");
    assert!(jobs::bsd_info(decoy.pid).is_some(), "and alive");
    assert!(daemon.terminate().success());
    let _ = fs::remove_dir_all(&dir);
}

/// The jobs a previous run froze are recovered when a simulated daemon starts, and that too
/// is confined: the group this test spawned is continued, and another that was recorded the
/// same way but was not started by this process is not signalled and stays recorded.
#[test]
fn simulated_recovery_continues_only_the_groups_the_test_spawned() {
    let dir = scratch("decoy-recovery");
    let (state, log) = (dir.join("state"), dir.join("log"));
    let mine = marker();
    let target = Group::spawn(&mine, 1);
    let decoy = Orphan::spawn(&mine);
    target.0[0].freeze_into(&state);
    {
        let mut recorded = State::open(&state).unwrap();
        act::freeze(
            &mut recorded,
            &decoy.job(),
            membrake::signals::wall(),
            &mut || None,
        )
        .unwrap();
    }
    assert!(decoy.settles(true));
    assert_eq!(records(&state), 2);

    let mut script = Script::new(&dir);
    let daemon = Daemon::simulate(&script.path, &only(&mine), &state, &log);
    daemon.wait_started(&log);
    assert!(
        target.settles(false),
        "the group this test spawned is continued"
    );
    // Several samples later (each one retries every record it still holds) it is the same.
    script.calm(5);
    daemon.wait_samples(&log, 5);
    assert!(decoy.stopped(), "the other is not signalled");
    assert_eq!(records(&state), 1, "and its record stays");
    assert!(daemon.terminate().success());
    assert!(decoy.stopped(), "not even at shutdown");
    assert_eq!(records(&state), 1);
    let _ = fs::remove_dir_all(&dir);
}

// ---------------------------------------------------------------------------------------
// The rules are asked again after the record is on disk, and before the signal.

/// Everything a writer put into the named pipe `path`, waiting at most 5 s for it to have put
/// anything: the pipe is opened without waiting, so a writer that never comes is an empty
/// string and not a test that never ends. (A writer that is blocked on the pipe comes the
/// moment it is opened for reading.)
fn read_fifo(path: &Path) -> String {
    let mut pipe = fs::OpenOptions::new()
        .read(true)
        .custom_flags(libc::O_NONBLOCK)
        .open(path)
        .unwrap();
    let (mut text, deadline) = (Vec::new(), Instant::now() + Duration::from_secs(5));
    let mut chunk = [0u8; 4096];
    while Instant::now() < deadline {
        match pipe.read(&mut chunk) {
            // No writer is there (yet), or it has written everything and closed.
            Ok(0) if !text.is_empty() => break,
            Ok(0) => {}
            Ok(n) => text.extend_from_slice(&chunk[..n]),
            Err(e) if e.kind() == std::io::ErrorKind::WouldBlock => {}
            Err(e) => panic!("{e}"),
        }
        std::thread::sleep(Duration::from_millis(10));
    }
    String::from_utf8_lossy(&text).into_owned()
}

/// A group in which a script waits on a named pipe and then becomes another program, which the
/// simulated filter does not name: eligible until it is told to go.
struct Changeling {
    group: ShellGroup,
    go: PathBuf,
    later: String,
}

impl Changeling {
    fn spawn(dir: &Path) -> (Changeling, String) {
        let later = marker();
        let go = dir.join("go");
        mkfifo(&go);
        let wait = dir.join("wait.sh");
        fs::write(&wait, "#!/bin/sh\nread line < \"$1\"\nexec sleep \"$2\"\n").unwrap();
        let group = ShellGroup::spawn(&wait, &[go.to_str().unwrap(), &later]);
        let only = format!("^/bin/sh {}/wait\\.sh .*$", escape(dir.to_str().unwrap()));
        (Changeling { group, go, later }, only)
    }

    /// It becomes `sleep LATER`, which no longer matches.
    fn change(&self) {
        let deadline = Instant::now() + Duration::from_secs(10);
        loop {
            let opened = fs::OpenOptions::new()
                .write(true)
                .custom_flags(libc::O_NONBLOCK)
                .open(&self.go);
            if let Ok(mut file) = opened {
                file.write_all(b"go\n").unwrap();
                break;
            }
            assert!(Instant::now() < deadline, "the script never waited for go");
            std::thread::sleep(Duration::from_millis(10));
        }
        while jobs::command_line(self.group.pgid()) != Some(format!("sleep {}", self.later)) {
            assert!(Instant::now() < deadline, "it never became another program");
            std::thread::sleep(Duration::from_millis(10));
        }
    }
}

/// Holds the write of the state file on a named pipe, runs `stage` to make the daemon act
/// on the changeling, changes it while the write waits, lets the write through, and returns
/// when the daemon has said `said`.
///
/// A daemon blocked in a write cannot be seen to be, so it is given time to get there: the
/// samples it records are how far it has read, and `recorded` is how many it records before
/// the row that makes it act. A daemon that is slower than that (this machine, loaded) has not
/// chosen the group by the time it changes: it then finds it ineligible and writes nothing,
/// which this notices (nothing is written, and the daemon has acted on nothing) and tries
/// again, up to three times, with a daemon of its own each time and twice the time to get
/// there. The daemon having acted, or the group having been touched, is a failure at once,
/// and so is no write three times.
fn change_while_the_record_is_written(
    name: &str,
    stage: impl Fn(&mut Script),
    recorded: usize,
    said: &str,
) {
    for attempt in 1..=3 {
        if record_is_written_while_the_group_changes(
            &format!("{name}-{attempt}"),
            &stage,
            recorded,
            said,
            Duration::from_millis(1500 << (attempt - 1)),
        ) {
            return;
        }
    }
    panic!(
        "the daemon never wrote the record in three attempts: it chose the group too late each time"
    );
}

/// One attempt of `change_while_the_record_is_written`, in which the daemon has `time_to_wait`
/// to be waiting for the pipe; false if it wrote nothing and did nothing, and the attempt
/// tested nothing.
fn record_is_written_while_the_group_changes(
    name: &str,
    stage: &dyn Fn(&mut Script),
    recorded: usize,
    said: &str,
    time_to_wait: Duration,
) -> bool {
    let dir = scratch(name);
    let (state, log) = (dir.join("state"), dir.join("log"));
    let (mut changeling, only) = Changeling::spawn(&dir);
    let mut script = Script::new(&dir);
    let daemon = Daemon::simulate(&script.path, &only, &state, &log);
    daemon.wait_started(&log);
    let tmp = state.join("frozen.tsv.tmp");
    mkfifo(&tmp);

    script.calm(1);
    daemon.wait_samples(&log, 1);
    stage(&mut script);
    daemon.wait_samples(&log, recorded);
    std::thread::sleep(time_to_wait);
    assert!(
        changeling.group.alive() && !changeling.group.stopped(),
        "the record is waiting to be written, and nothing has been done to the group"
    );
    changeling.change();
    let written = read_fifo(&tmp);
    if written.is_empty() {
        assert!(
            actions(&log).is_empty() && !changeling.group.stopped(),
            "the daemon acted without writing the record\n{}",
            events(&log)
        );
        let _ = fs::remove_dir_all(&dir);
        return false;
    }
    assert!(
        written.starts_with(&format!("{}\t", changeling.group.pgid())),
        "{written:?}: not the record of the group\n{}",
        events(&log)
    );

    daemon.wait_logged(&log, said);
    assert!(
        changeling.group.alive() && !changeling.group.stopped(),
        "left running: it is not what the rules allow any more"
    );
    assert_eq!(records(&state), 0, "and nothing is promised");
    assert!(daemon.terminate().success());
    let _ = fs::remove_dir_all(&dir);
    true
}

#[test]
fn a_group_that_becomes_ineligible_while_its_freeze_is_recorded_is_not_stopped() {
    change_while_the_record_is_written(
        "freeze-window",
        |script| script.tight(2),
        2, // the calm row, and the first tight one; the second is the freeze
        "no longer eligible",
    );
}

#[test]
fn a_group_that_becomes_ineligible_while_its_kill_is_recorded_is_neither_stopped_nor_killed() {
    change_while_the_record_is_written(
        "kill-window",
        |script| script.cliff(1),
        1, // the calm row; the cliff is the kill
        "not killing group",
    );
}

// ---------------------------------------------------------------------------------------
// Files that block: the brake still brakes, clients are still told to go.

/// A simulated daemon with the brake and the queue, in which `blocked` is a named pipe nobody
/// reads, so that whoever opens it for writing waits for ever. The brake freezes and resumes
/// a group, one client runs a command that is measured, and when it is done another is told
/// to go and runs: nothing waited for the file, and the daemon still stops when told to.
///
/// `sizes_blocked`: the file that blocks is the one the first command's size is written to,
/// which happens only if the command was measured. The status file is readable then, and
/// says whether it was.
fn nothing_waits_for_a_file_that_blocks(
    name: &str,
    sizes_blocked: bool,
    blocked: impl Fn(&Path, &Path) -> PathBuf,
) {
    let dir = scratch(name);
    let (state, log) = (dir.join("state"), dir.join("log"));
    fs::create_dir_all(&state).unwrap();
    fs::create_dir_all(&log).unwrap();
    mkfifo(&blocked(&state, &log));
    let socket = queue_socket(name);
    let mine = marker();
    let group = Group::spawn(&mine, 1);
    let mut script = Script::new(&dir);
    let daemon = queue_daemon(&script, &dir, &socket, &only(&mine));

    // The brake: tight memory freezes the group, calm memory resumes it.
    script.tight(2);
    daemon.wait_actions(&log, 1);
    assert!(group.settles(true), "frozen");
    script.calm(30);
    daemon.wait_actions(&log, 2);
    assert!(group.settles(false), "and resumed");

    // The queue: the first command runs until the test lets it end, which is once it has been
    // measured (when a command that was measured is done, its size is to be written); the
    // second is told to go after that.
    let release = marker_file(&dir, "release");
    let mut first = Client::spawn(
        &dir,
        "first",
        &socket,
        &["--label", "first"],
        &[
            "sh",
            "-c",
            "while [ ! -e \"$1\" ]; do sleep 0.05; done",
            "sh",
            &release,
        ],
    );
    if sizes_blocked {
        // The status file is readable here, and says whether the command was measured.
        wait_until("the first command to be measured", 60, || {
            script.calm(1);
            membrake::broker::read_queue(&state)
                .ok()
                .flatten()
                .is_some_and(|q| q.running.iter().any(|r| r.pgid > 1 && r.peak > 0))
        });
    }
    fs::write(&release, "").unwrap();
    wait_until("the first command to be done", 60, || {
        script.calm(1);
        first.child.try_wait().unwrap().is_some()
    });
    assert_eq!(first.code(), Some(0));
    // A client fails open when the daemon does not answer, and then runs anyway: that the
    // commands ran is not enough, they must have been let go by the queue.
    assert!(!first.said().contains("unavailable"), "{}", first.said());
    let ran = marker_file(&dir, "second");
    let mut second = Client::spawn(
        &dir,
        "second",
        &socket,
        &["--label", "second"],
        &["touch", &ran],
    );
    wait_until("the second command to run", 60, || {
        script.calm(1);
        Path::new(&ran).exists()
    });
    assert_eq!(second.code(), Some(0));
    assert!(!second.said().contains("unavailable"), "{}", second.said());
    assert!(daemon.terminate().success(), "and the daemon stops");
    let _ = fs::remove_dir_all(&dir);
}

#[test]
fn a_samples_file_that_blocks_does_not_stop_the_brake_or_the_queue() {
    nothing_waits_for_a_file_that_blocks("blocked-samples", false, |_, log| {
        log.join("samples.tsv")
    });
}

#[test]
fn a_queue_status_file_that_blocks_does_not_stop_the_brake_or_the_queue() {
    nothing_waits_for_a_file_that_blocks("blocked-status", false, |state, _| {
        state.join("queue.tsv.tmp")
    });
}

#[test]
fn a_sizes_file_that_blocks_does_not_stop_the_brake_or_the_queue() {
    nothing_waits_for_a_file_that_blocks("blocked-sizes", true, |state, _| {
        state.join("sizes.tsv.tmp")
    });
}

// ---------------------------------------------------------------------------------------
// What a command leaves running in its group (nix-config issue 270)
//
// The only process these tests signal is their own `membrake exec` client, before it has been
// reaped. What a command leaves behind is a `sleep 60`, which ends by itself; the tests only
// look at it, by the pid it wrote down once ready and the start time first seen there.

/// A process a test's command left behind: its pid, as it wrote it once ready, and its start
/// time, so that a pid that went to another process is never mistaken for it.
struct Left {
    pid: i32,
    start_us: u64,
}

impl Left {
    fn read(file: &str) -> Left {
        let mut pid = None;
        wait_until(&format!("a ready pid in {file}"), 20, || {
            pid = fs::read_to_string(file)
                .ok()
                .and_then(|text| text.trim().parse().ok());
            pid.is_some()
        });
        let pid = pid.unwrap();
        let start_us = jobs::bsd_info(pid)
            .expect("the leftover is there when it says it is ready")
            .start_us;
        Left { pid, start_us }
    }

    fn alive(&self) -> bool {
        jobs::bsd_info(self.pid).is_some_and(|p| p.start_us == self.start_us && !p.zombie)
    }
}

/// `sh -c` text that starts `sleep 60` in the background, in the command's group, and has it
/// write its own pid to `file` once `trap` (if any) is in place: the shell execs `sleep` only
/// after writing, and an ignored signal stays ignored across exec. 60 s outlasts the grace and
/// the longest the checks may wait (20 s for the pid, 30 s for the client), and nothing needs to
/// stop it afterwards.
fn leftover(trap: &str, file: &str) -> String {
    format!("sh -c '{trap} echo $$ > {file}; exec sleep 60' &")
}

/// `sh -c` text that waits until the test has written `go`: the command goes on, and so lets
/// the cleanup start, only once the test holds the leftover's identity (`Left::read`).
fn until(go: &str) -> String {
    format!("while [ ! -e {go} ]; do sleep 0.05; done;")
}

/// An interrupted command's leftovers are stopped: `SIGTERM` to the group, then `SIGKILL`
/// after the grace, here because the leftover ignores `SIGTERM`.
#[test]
fn exec_stops_what_an_interrupted_command_left_running() {
    let dir = scratch("reap-interrupted");
    let nobody = queue_socket("reap-interrupted");
    let file = marker_file(&dir, "left");
    let script = format!("{} sleep 60", leftover("trap \"\" TERM;", &file));
    let mut client = Client::spawn(&dir, "c", &nobody, &[], &["sh", "-c", &script]);
    let left = Left::read(&file);
    // SAFETY: the client this test started; it passes the signal to its command's group.
    unsafe { libc::kill(client.pid(), libc::SIGTERM) };
    assert_eq!(client.code(), Some(128 + libc::SIGTERM));
    assert!(
        !left.alive(),
        "the leftover outlived the client: {}",
        client.said()
    );
    assert!(
        client.said().contains(
            "stopped what the command left running in its process group (SIGTERM, then SIGKILL"
        ),
        "{}",
        client.said()
    );
    let _ = fs::remove_dir_all(&dir);
}

/// A command that dies of a signal nobody passed on (it signalled itself) counts as
/// interrupted too.
#[test]
fn exec_stops_what_a_command_that_died_of_a_signal_left_running() {
    let dir = scratch("reap-died");
    let nobody = queue_socket("reap-died");
    let (file, go) = (marker_file(&dir, "left"), marker_file(&dir, "go"));
    let script = format!("{} {} kill -TERM $$", leftover("", &file), until(&go));
    let mut client = Client::spawn(&dir, "c", &nobody, &[], &["sh", "-c", &script]);
    let left = Left::read(&file);
    fs::write(&go, "").unwrap();
    assert_eq!(client.code(), Some(128 + libc::SIGTERM));
    assert!(
        !left.alive(),
        "the leftover outlived the client: {}",
        client.said()
    );
    assert!(
        client
            .said()
            .contains("stopped what the command left running"),
        "{}",
        client.said()
    );
    let _ = fs::remove_dir_all(&dir);
}

/// With `--reap`, a command that ends normally has its leftovers stopped too.
#[test]
fn exec_with_reap_stops_what_a_finished_command_left_running() {
    let dir = scratch("reap-finished");
    let nobody = queue_socket("reap-finished");
    let (file, go) = (marker_file(&dir, "left"), marker_file(&dir, "go"));
    let script = format!("{} {} true", leftover("", &file), until(&go));
    let mut client = Client::spawn(&dir, "c", &nobody, &["--reap"], &["sh", "-c", &script]);
    let left = Left::read(&file);
    fs::write(&go, "").unwrap();
    assert_eq!(client.code(), Some(0));
    assert!(
        !left.alive(),
        "the leftover outlived the client: {}",
        client.said()
    );
    assert!(
        client
            .said()
            .contains("stopped what the command left running"),
        "{}",
        client.said()
    );
    let _ = fs::remove_dir_all(&dir);
}

/// Without `--reap` a command that ends normally keeps its leftovers (a server it means to
/// leave up), and one line says they are there.
#[test]
fn exec_without_reap_leaves_a_finished_commands_leftovers_and_says_so() {
    let dir = scratch("reap-not");
    let nobody = queue_socket("reap-not");
    let (file, go) = (marker_file(&dir, "left"), marker_file(&dir, "go"));
    let script = format!("{} {} true", leftover("", &file), until(&go));
    let mut client = Client::spawn(&dir, "c", &nobody, &[], &["sh", "-c", &script]);
    let left = Left::read(&file);
    fs::write(&go, "").unwrap();
    assert_eq!(client.code(), Some(0));
    assert!(left.alive(), "the leftover was stopped: {}", client.said());
    assert!(
        client
            .said()
            .contains("still has processes running; left alone"),
        "{}",
        client.said()
    );
    let _ = fs::remove_dir_all(&dir);
}

/// A command that leaves nothing behind ends at once, `--reap` or not, and says nothing more.
#[test]
fn exec_with_nothing_left_ends_at_once() {
    let dir = scratch("reap-nothing");
    let nobody = queue_socket("reap-nothing");
    for options in [&[][..], &["--reap"][..]] {
        let started = Instant::now();
        let mut client = Client::spawn(&dir, "c", &nobody, options, &["sh", "-c", "exit 7"]);
        assert_eq!(client.code(), Some(7));
        assert!(
            started.elapsed() < Duration::from_secs(2),
            "{options:?}: took {:?}",
            started.elapsed()
        );
        let said = client.said();
        assert!(
            !said.contains("process group") && !said.contains("left running"),
            "{options:?}: {said}"
        );
    }
    let _ = fs::remove_dir_all(&dir);
}

/// A process that left the command's group is not followed, even with `--reap`: stopping
/// leftovers is the group this client created, never a search.
#[test]
fn exec_does_not_follow_a_process_that_left_the_group() {
    let dir = scratch("reap-left-group");
    let nobody = queue_socket("reap-left-group");
    let (file, go) = (marker_file(&dir, "left"), marker_file(&dir, "go"));
    // The process leaves the group first and only then writes its pid. The system's perl by
    // its path: the Nix build that runs these tests has none on PATH.
    let script = format!(
        "/usr/bin/perl -e 'setpgrp(0, 0); open my $f, \">\", $ARGV[0]; print $f $$; close $f; \
         exec \"sleep\", \"60\"' {file} & {} true",
        until(&go)
    );
    let mut client = Client::spawn(&dir, "c", &nobody, &["--reap"], &["sh", "-c", &script]);
    let left = Left::read(&file);
    fs::write(&go, "").unwrap();
    assert_eq!(client.code(), Some(0));
    assert!(
        left.alive(),
        "a process outside the group was stopped: {}",
        client.said()
    );
    assert!(!client.said().contains("left running"), "{}", client.said());
    let _ = fs::remove_dir_all(&dir);
}
