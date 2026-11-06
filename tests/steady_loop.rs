//! The sampling loop must not allocate once it is warm: it runs once a second on a machine
//! that may be out of memory. Counts allocations made by the test's own thread.
//!
//! Two paths are measured: the calm one (read, decide, record), and the same with a frozen
//! group on the books, which every sample then has to look at to see whether it is still
//! frozen.

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
use std::process::{Child, Command};
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
        recorder.sample(&s, &step, None).unwrap();
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
    // Only the group this test spawned can be a victim.
    let rules = Rules::new(&[], &[], vec![], vec![], unsafe { libc::getpgrp() })
        .unwrap()
        .only_matching(&format!("^sleep {}$", marker.replace('.', "\\.")))
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
