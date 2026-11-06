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
//!   carry. The tests also spawn a decoy group outside that filter and check it is left alone.

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
        act::freeze(&mut state, &self.job(), membrake::signals::wall()).unwrap();
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
        let deadline = Instant::now() + Duration::from_secs(20);
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
        let deadline = Instant::now() + Duration::from_secs(20);
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
        let deadline = Instant::now() + Duration::from_secs(20);
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
        let deadline = Instant::now() + Duration::from_secs(20);
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
    let only = format!(
        "^(sleep {}|/bin/sh [^ ]*/wait\\.sh .*)$",
        mine.replace('.', "\\.")
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
    while !jobs::command_line(newer.pgid()).is_some_and(|c| c == format!("sleep {later}")) {
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
    assert!(daemon.terminate().success());
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
