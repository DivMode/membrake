//! The sampling loop must not allocate once it is warm: it runs once a second on a machine
//! that may be out of memory. Counts allocations made by the test's own thread.

use membrake::engine::{Engine, Policy};
use membrake::recorder::Recorder;
use membrake::signals::{self, Pressure};
use std::alloc::{GlobalAlloc, Layout, System};
use std::cell::Cell;

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
