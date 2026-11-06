//! The sampling loop must not allocate once it is warm: it runs once a second on a machine
//! that may be out of memory. Counts allocations made by the test's own thread.
//!
//! Three paths are measured: the calm one (read, decide, record), the same with a frozen
//! group on the books, which every sample then has to look at to see whether it is still
//! frozen, and the same with the queue served, with and without clients in it.

use membrake::act::State;
use membrake::broker::{self, Broker};
use membrake::engine::{Engine, Policy};
use membrake::jobs::{self, Rules};
use membrake::recorder::Recorder;
use membrake::runner::{RunOptions, Runner};
use membrake::signals::{self, Pressure};
use std::alloc::{GlobalAlloc, Layout, System};
use std::cell::Cell;
use std::os::unix::process::CommandExt;
use std::process::{Child, Command, Stdio};
use std::time::{Duration, Instant};

thread_local! {
    static ALLOCATIONS: Cell<usize> = const { Cell::new(0) };
}

struct Counting;

fn count() {
    ALLOCATIONS.with(|c| c.set(c.get() + 1));
}

// SAFETY: every method forwards to the system allocator unchanged.
unsafe impl GlobalAlloc for Counting {
    unsafe fn alloc(&self, layout: Layout) -> *mut u8 {
        count();
        unsafe { System.alloc(layout) }
    }
    unsafe fn alloc_zeroed(&self, layout: Layout) -> *mut u8 {
        count();
        unsafe { System.alloc_zeroed(layout) }
    }
    unsafe fn realloc(&self, ptr: *mut u8, layout: Layout, new_size: usize) -> *mut u8 {
        count();
        unsafe { System.realloc(ptr, layout, new_size) }
    }
    unsafe fn dealloc(&self, ptr: *mut u8, layout: Layout) {
        unsafe { System.dealloc(ptr, layout) }
    }
}

#[global_allocator]
static ALLOCATOR: Counting = Counting;

#[test]
fn steady_loop_does_not_allocate() {
    let dir = std::env::temp_dir().join(format!("membrake-test-{}-alloc", std::process::id()));
    let mut recorder = Recorder::open(&dir).unwrap();
    let mut engine = Engine::new(Policy::default());
    let mut iterate = || {
        let mut s = signals::read().expect("live signals");
        // Keep the engine on its calm path whatever the machine is doing right now; what is
        // measured is the read, the policy step and the recording.
        s.segments = 0.1;
        s.pages = 0.1;
        s.level = 80;
        s.pressure = Pressure::Normal;
        s.headroom = s.ram * 4;
        let step = engine.step(&s, || {
            panic!("a calm machine must not scan the process table")
        });
        recorder.sample(&s, &step, None);
    };
    for _ in 0..20 {
        iterate();
    }
    // The instrument must be able to see an allocation, or a zero below means nothing.
    let probe = ALLOCATIONS.with(Cell::get);
    drop(std::hint::black_box(Box::new(0u64)));
    assert!(
        ALLOCATIONS.with(Cell::get) > probe,
        "the counting allocator is not counting"
    );

    let before = ALLOCATIONS.with(Cell::get);
    for _ in 0..200 {
        iterate();
    }
    let allocated = ALLOCATIONS.with(Cell::get) - before;
    assert_eq!(
        allocated, 0,
        "the warm loop allocated {allocated} times in 200 samples"
    );
    let _ = std::fs::remove_dir_all(&dir);
}

/// A `sleep` in a group of its own, killed when dropped.
struct Sleeper(Child);

impl Drop for Sleeper {
    fn drop(&mut self) {
        let _ = self.0.kill();
        let _ = self.0.wait();
    }
}

/// With a group frozen and recorded, each sample looks at it (to see whether someone
/// continued it, or it exited), and that must not allocate either. The group is a real `sleep`
/// that the runner itself freezes; the readings keep memory tight, so it stays frozen.
#[test]
fn steady_loop_with_a_frozen_group_does_not_allocate() {
    let marker = format!("3600.{:07}", std::process::id());
    let sleeper = Sleeper(
        Command::new("sleep")
            .arg(&marker)
            .process_group(0)
            .spawn()
            .unwrap(),
    );
    let pid = sleeper.0.id() as i32;
    // Only the group this test spawned can be a victim: its leader is this process's child.
    let rules = Rules::new(&[], &[], vec![], vec![], unsafe { libc::getpgrp() })
        .unwrap()
        .only_matching(
            &format!("^sleep {}$", marker.replace('.', "\\.")),
            std::process::id() as i32,
        )
        .unwrap();
    let dir =
        std::env::temp_dir().join(format!("membrake-test-{}-alloc-frozen", std::process::id()));
    let _ = std::fs::remove_dir_all(&dir);
    let opts = RunOptions {
        policy: Policy {
            min_job_bytes: 0,
            ..Policy::default()
        },
        rules,
        term_seconds: 1.0,
        on_event: None,
    };
    let mut runner = Runner::new(
        opts,
        Some(State::open(&dir.join("state")).unwrap()),
        Recorder::open(&dir.join("log")).unwrap(),
    );
    let mut time = 1000.0;
    let mut tick = |level: i32, occupancy: f64| {
        let mut s = signals::read().expect("live signals");
        time += 1.0;
        s.time = time;
        s.segments = occupancy;
        s.pages = 0.1;
        s.level = level;
        s.pressure = Pressure::Normal;
        s.headroom = s.ram * 4;
        runner.tick(&s);
    };

    // Two tight samples with the compressor at 65% freeze the group...
    tick(35, 0.65);
    tick(35, 0.65);
    let deadline = Instant::now() + Duration::from_secs(5);
    while !jobs::bsd_info(pid).is_some_and(|p| p.stopped) {
        assert!(Instant::now() < deadline, "the group was never stopped");
        std::thread::sleep(Duration::from_millis(10));
    }
    // ...and tight, quiet readings keep it frozen: nothing to decide, one group to look at.
    for _ in 0..20 {
        tick(35, 0.1);
    }
    let before = ALLOCATIONS.with(Cell::get);
    for _ in 0..200 {
        tick(35, 0.1);
    }
    let allocated = ALLOCATIONS.with(Cell::get) - before;
    assert!(
        jobs::bsd_info(pid).is_some_and(|p| p.stopped) && runner.has_records(),
        "still frozen and recorded, so every sample looked at it"
    );
    assert_eq!(
        allocated, 0,
        "the warm loop allocated {allocated} times in 200 samples with a frozen group"
    );
    drop(sleeper);
    let _ = std::fs::remove_dir_all(&dir);
}

/// With the queue served and nobody in it, a sample still allocates nothing: the queue's
/// part of the tick (the scheduler's step, the measuring, the status file) reuses what it has.
#[test]
fn steady_loop_with_the_queue_and_no_clients_does_not_allocate() {
    let dir =
        std::env::temp_dir().join(format!("membrake-test-{}-alloc-queue", std::process::id()));
    let _ = std::fs::remove_dir_all(&dir);
    let state_dir = dir.join("state");
    let opts = RunOptions {
        policy: Policy::default(),
        rules: Rules::new(&[], &[], vec![], vec![], unsafe { libc::getpgrp() }).unwrap(),
        term_seconds: 1.0,
        on_event: None,
    };
    let mut runner = Runner::new(
        opts,
        Some(State::open(&state_dir).unwrap()),
        Recorder::open(&dir.join("log")).unwrap(),
    );
    runner.serve_queue(
        Broker::start(broker::Options {
            socket: dir.join("q.sock"),
            state_dir: state_dir.clone(),
            dry_run: false,
        })
        .unwrap(),
    );
    let mut time = 1000.0;
    let mut tick = || {
        let mut s = signals::read().expect("live signals");
        time += 1.0;
        s.time = time;
        s.segments = 0.1;
        s.pages = 0.1;
        s.level = 80;
        s.pressure = Pressure::Normal;
        s.headroom = s.ram * 4;
        runner.tick(&s);
    };
    // The first samples bring the queue's start-up notes and its first status file.
    for _ in 0..20 {
        tick();
    }
    assert!(
        state_dir.join("queue.tsv").exists(),
        "the queue's part of the tick ran"
    );
    let before = ALLOCATIONS.with(Cell::get);
    for _ in 0..200 {
        tick();
    }
    let allocated = ALLOCATIONS.with(Cell::get) - before;
    assert_eq!(
        allocated, 0,
        "the warm loop allocated {allocated} times in 200 samples with the queue served"
    );
    drop(runner);
    let _ = std::fs::remove_dir_all(&dir);
}

/// A `membrake exec` client with a long command of its own, killed (its command with it)
/// when dropped.
struct Client(Child);

impl Client {
    fn spawn(socket: &std::path::Path) -> Client {
        let mut exec = Command::new(env!("CARGO_BIN_EXE_membrake"));
        // These tests may themselves be running inside an admitted command.
        exec.env_remove("MEMBRAKE_ADMITTED");
        Client(
            exec.arg("exec")
                .arg("--socket")
                .arg(socket)
                .args(["--", "sleep", "300"])
                .stdout(Stdio::null())
                .stderr(Stdio::null())
                .spawn()
                .unwrap(),
        )
    }
}

impl Drop for Client {
    fn drop(&mut self) {
        // SIGTERM first: the client passes it to the group it started.
        // SAFETY: signalling the child this test spawned.
        unsafe { libc::kill(self.0.id() as i32, libc::SIGTERM) };
        let deadline = Instant::now() + Duration::from_secs(3);
        while self.0.try_wait().ok().flatten().is_none() && Instant::now() < deadline {
            std::thread::sleep(Duration::from_millis(10));
        }
        let _ = self.0.kill();
        let _ = self.0.wait();
    }
}

/// With the queue served and busy, a sample still allocates nothing: one command admitted
/// and running (its group measured on every sample), one client connected and waiting (its
/// place worked out and shown on every sample). Both are real: `membrake exec` processes
/// this test starts, with a real `sleep` as the admitted command.
#[test]
fn steady_loop_with_one_client_waiting_and_one_running_does_not_allocate() {
    let dir = std::env::temp_dir().join(format!(
        "membrake-test-{}-alloc-clients",
        std::process::id()
    ));
    let _ = std::fs::remove_dir_all(&dir);
    let state_dir = dir.join("state");
    // A socket path of its own, short enough for `sockaddr_un` wherever the temporary
    // directory is.
    let socket = std::env::temp_dir().join(format!("mbq-{}-alloc.sock", std::process::id()));
    let opts = RunOptions {
        policy: Policy::default(),
        rules: Rules::new(&[], &[], vec![], vec![], unsafe { libc::getpgrp() }).unwrap(),
        term_seconds: 1.0,
        on_event: None,
    };
    let mut runner = Runner::new(
        opts,
        Some(State::open(&state_dir).unwrap()),
        Recorder::open(&dir.join("log")).unwrap(),
    );
    runner.serve_queue(
        Broker::start(broker::Options {
            socket: socket.clone(),
            state_dir: state_dir.clone(),
            dry_run: false,
        })
        .unwrap(),
    );
    let mut time = 1000.0;
    // Memory is calm, or tight enough to hold a request (nothing the brake acts on). Returns
    // on how many samples so far the queue's work was done: a sample on which the socket
    // thread had the queue leaves it for the next, and costs the queue nothing.
    let mut tick = |level: i32| {
        let mut s = signals::read().expect("live signals");
        time += 1.0;
        s.time = time;
        s.segments = 0.1;
        s.pages = 0.1;
        s.level = level;
        s.pressure = Pressure::Normal;
        s.headroom = s.ram * 4;
        runner.tick(&s);
        runner.queue().map_or(0, Broker::scheduled)
    };
    let status = || broker::read_queue(&state_dir).ok().flatten();
    let until = |what: &str, tick: &mut dyn FnMut(), done: &mut dyn FnMut() -> bool| {
        let deadline = Instant::now() + Duration::from_secs(30);
        while !done() {
            assert!(Instant::now() < deadline, "timed out waiting for {what}");
            tick();
            std::thread::sleep(Duration::from_millis(20));
        }
    };

    // One command is admitted and measured...
    let running = Client::spawn(&socket);
    until(
        "the command to run and be measured",
        &mut || {
            tick(80);
        },
        &mut || status().is_some_and(|q| q.running.iter().any(|r| r.pgid > 1 && r.peak > 0)),
    );
    // ...and, with memory tight, the next client is held.
    let waiting = Client::spawn(&socket);
    until(
        "the second client to wait",
        &mut || {
            tick(35);
        },
        &mut || status().is_some_and(|q| q.waiting.len() == 1),
    );
    // Warm: the measured peak has stopped moving, so the status file has stopped changing.
    let mut still = 0;
    let mut last = status().map(|q| q.running[0].peak);
    until(
        "the measured size to settle",
        &mut || {
            tick(35);
        },
        &mut || {
            let now = status().map(|q| q.running[0].peak);
            still = if now == last { still + 1 } else { 0 };
            last = now;
            still >= 50
        },
    );

    // The instrument must be able to see an allocation, or a zero below means nothing.
    let probe = ALLOCATIONS.with(Cell::get);
    drop(std::hint::black_box(Box::new(0u64)));
    assert!(
        ALLOCATIONS.with(Cell::get) > probe,
        "the counting allocator is not counting"
    );
    // Measured over 200 samples on which the queue's work was done, however many samples
    // that takes: a sample that skipped it (the socket thread had the queue) proves nothing
    // about the work, and is not one of the 200.
    let first = tick(35);
    let before = ALLOCATIONS.with(Cell::get);
    let (mut scheduled, mut samples) = (first, 0u32);
    while scheduled - first < 200 {
        samples += 1;
        assert!(
            samples < 5000,
            "the queue's work was done on only {} samples of {samples}",
            scheduled - first
        );
        scheduled = tick(35);
    }
    let allocated = ALLOCATIONS.with(Cell::get) - before;
    assert_eq!(
        scheduled - first,
        200,
        "200 samples of the queue's work were measured"
    );
    let queue = status().expect("a status file");
    assert!(
        queue.running.len() == 1 && queue.waiting.len() == 1,
        "the queue was busy throughout: {} running, {} waiting",
        queue.running.len(),
        queue.waiting.len()
    );
    assert_eq!(
        allocated, 0,
        "the warm loop allocated {allocated} times in 200 samples with a client waiting and one running"
    );
    drop(waiting);
    drop(running);
    drop(runner);
    let _ = std::fs::remove_dir_all(&dir);
}
