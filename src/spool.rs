//! A thread that does blocking work for threads that must never block: writing a file, say,
//! on a disk that may stall. The work is handed over through a bounded queue without waiting;
//! if the queue is full (the disk is stuck, or behind) the item is given back and counted as
//! dropped, and the thread reports the count once it has caught up.
//!
//! The brake's sampling thread and the queue's socket thread do no file work of their own,
//! except the one flush of the frozen-state file that must precede a SIGSTOP. Everything else
//! they have to say to a file goes through one of these.

use std::io;
use std::sync::Arc;
use std::sync::atomic::{AtomicU64, Ordering};
use std::sync::mpsc::{self, RecvTimeoutError, SyncSender, TryRecvError, TrySendError};
use std::thread;
use std::time::Duration;

pub struct Spool<T> {
    queue: SyncSender<T>,
    dropped: Arc<AtomicU64>,
}

impl<T> Clone for Spool<T> {
    fn clone(&self) -> Spool<T> {
        Spool {
            queue: self.queue.clone(),
            dropped: Arc::clone(&self.dropped),
        }
    }
}

fn report(dropped: &AtomicU64, lost: &mut impl FnMut(u64)) {
    let count = dropped.swap(0, Ordering::Relaxed);
    if count > 0 {
        lost(count);
    }
}

impl<T: Send + 'static> Spool<T> {
    /// Starts the thread `name`, which calls `work` on every item in the order they were
    /// offered, and `lost` with the number of items dropped since its last call whenever it
    /// has worked through the queue. At most `capacity` items wait. The thread ends when
    /// every handle is gone.
    pub fn spawn(
        name: &str,
        capacity: usize,
        mut work: impl FnMut(T) + Send + 'static,
        lost: impl FnMut(u64) + Send + 'static,
    ) -> io::Result<Spool<T>> {
        Spool::spawn_idling(
            name,
            capacity,
            move |item| {
                if let Some(item) = item {
                    work(item);
                }
            },
            lost,
        )
    }

    /// `spawn`, for a thread with something of its own to do when nothing is offered, such as
    /// writing again what could not be written: `work` is called with None each time a second
    /// goes by with the queue empty.
    pub fn spawn_idling(
        name: &str,
        capacity: usize,
        mut work: impl FnMut(Option<T>) + Send + 'static,
        mut lost: impl FnMut(u64) + Send + 'static,
    ) -> io::Result<Spool<T>> {
        let (queue, inbox) = mpsc::sync_channel(capacity);
        let dropped = Arc::new(AtomicU64::new(0));
        let counter = Arc::clone(&dropped);
        thread::Builder::new().name(name.into()).spawn(move || {
            // The shutdown signals belong to the sampling thread, which wakes at once for them.
            crate::act::block_shutdown_signals();
            loop {
                let item = match inbox.try_recv() {
                    Ok(item) => item,
                    Err(TryRecvError::Disconnected) => break,
                    Err(TryRecvError::Empty) => {
                        report(&counter, &mut lost);
                        match inbox.recv_timeout(Duration::from_secs(1)) {
                            Ok(item) => item,
                            Err(RecvTimeoutError::Timeout) => {
                                work(None);
                                continue;
                            }
                            Err(RecvTimeoutError::Disconnected) => break,
                        }
                    }
                };
                work(Some(item));
            }
            report(&counter, &mut lost);
        })?;
        Ok(Spool { queue, dropped })
    }

    /// Hands `item` over without waiting. If the queue is full, or the thread is gone, the
    /// item comes back and is counted as dropped.
    pub fn offer(&self, item: T) -> Result<(), T> {
        self.queue.try_send(item).map_err(|e| {
            self.dropped.fetch_add(1, Ordering::Relaxed);
            match e {
                TrySendError::Full(item) | TrySendError::Disconnected(item) => item,
            }
        })
    }

    /// `offer` for an item that is not a loss when it is not taken, such as a request to
    /// answer once everything before it is done.
    pub fn try_send(&self, item: T) -> Result<(), TrySendError<T>> {
        self.queue.try_send(item)
    }

    /// Counts one item that was never offered because there was nothing to put it in.
    pub fn count_dropped(&self) {
        self.dropped.fetch_add(1, Ordering::Relaxed);
    }
}
