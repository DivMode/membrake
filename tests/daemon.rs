//! The real binary, end to end, against real `sleep` process groups that the tests spawn.
//!
//! Every daemon here runs with `--min-job-gb 1000000`, so it has no job it could ever freeze
//! or kill: the only processes it signals are the recorded ones the tests put in its state
//! directory. Nothing that the tests did not spawn is touched.

use membrake::act::{self, State};
use membrake::jobs::{self, Job};
use std::fs;
use std::os::unix::process::CommandExt;
use std::path::{Path, PathBuf};
use std::process::{Child, Command, Stdio};
use std::time::{Duration, Instant};

/// A `sleep` in its own process group, killed when dropped.
struct Sleeper(Child);

impl Sleeper {
    fn spawn() -> Sleeper {
        Sleeper(
            Command::new("sleep")
                .arg("300")
                .process_group(0)
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
        let events = log.join("events.log");
        let deadline = Instant::now() + Duration::from_secs(20);
        while !fs::read_to_string(&events).is_ok_and(|t| t.contains(" start ")) {
            assert!(
                Instant::now() < deadline,
                "the daemon never logged its start"
            );
            std::thread::sleep(Duration::from_millis(20));
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
