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
use std::io::Write;
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

    /// A daemon driven by the scripted signals in `script` (`run --simulate`), confined to
    /// the groups whose command line matches `only`.
    fn simulate(script: &Path, only: &str, state: &Path, log: &Path) -> Daemon {
        Daemon(
            Command::new(env!("CARGO_BIN_EXE_membrake"))
                .args(["run", "--min-job-gb", "0", "--simulate"])
                .arg(script)
                .args(["--simulate-only", only])
                .arg("--state-dir")
                .arg(state)
                .arg("--log-dir")
                .arg(log)
                .stdout(Stdio::null())
                .stderr(Stdio::null())
                .spawn()
                .unwrap(),
        )
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
