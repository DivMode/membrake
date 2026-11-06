//! Helpers shared by the unit tests: real `sleep` process groups the tests spawn themselves.

use crate::jobs::{self, Job};
use std::os::unix::process::CommandExt;
use std::path::PathBuf;
use std::process::{Child, Command};
use std::sync::atomic::{AtomicU32, Ordering};
use std::thread;
use std::time::{Duration, Instant};

/// A `sleep` in a process group, killed when dropped.
pub struct Sleeper(pub Child);

impl Sleeper {
    /// Leads a new group of its own.
    pub fn spawn() -> Sleeper {
        Sleeper::spawn_in(0, false)
    }

    /// Joins process group `pgid` (0: leads a new one). `ignore_term` starts it with SIGTERM
    /// ignored, a disposition that survives exec.
    pub fn spawn_in(pgid: i32, ignore_term: bool) -> Sleeper {
        let mut cmd = Command::new("sleep");
        cmd.arg("300").process_group(pgid);
        if ignore_term {
            // SAFETY: only async-signal-safe `signal` between fork and exec.
            unsafe {
                cmd.pre_exec(|| {
                    libc::signal(libc::SIGTERM, libc::SIG_IGN);
                    Ok(())
                });
            }
        }
        Sleeper(cmd.spawn().unwrap())
    }

    pub fn pid(&self) -> i32 {
        self.0.id() as i32
    }

    /// The group it leads (or joined) as one job.
    pub fn job(&self, procs: u32) -> Job {
        let p = jobs::bsd_info(self.pid()).expect("child is visible");
        Job {
            pgid: p.pgid,
            leader_pid: p.pgid,
            start_us: jobs::bsd_info(p.pgid).expect("leader is visible").start_us,
            uid: p.uid,
            bytes: 1 << 30,
            procs,
            prefer: false,
            protected: false,
            stopped: false,
            command: "sleep 300".into(),
        }
    }

    pub fn stopped(&self) -> bool {
        jobs::bsd_info(self.pid())
            .expect("child is visible")
            .stopped
    }

    /// The stop and continue signals take effect asynchronously; wait for the state.
    pub fn settles(&self, stopped: bool) -> bool {
        let deadline = Instant::now() + Duration::from_secs(2);
        while self.stopped() != stopped && Instant::now() < deadline {
            thread::sleep(Duration::from_millis(10));
        }
        self.stopped() == stopped
    }

    /// SIGKILL it and reap it, so its pid is gone from the process table.
    pub fn die(&mut self) {
        let _ = self.0.kill();
        let _ = self.0.wait();
    }
}

impl Drop for Sleeper {
    fn drop(&mut self) {
        self.die();
    }
}

/// A fresh, empty-and-absent scratch path unique to this process and call.
pub fn scratch(name: &str) -> PathBuf {
    static N: AtomicU32 = AtomicU32::new(0);
    let dir = std::env::temp_dir().join(format!(
        "membrake-test-{}-{}-{name}",
        std::process::id(),
        N.fetch_add(1, Ordering::SeqCst)
    ));
    let _ = std::fs::remove_dir_all(&dir);
    dir
}
