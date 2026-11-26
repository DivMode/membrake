//! The daemon's side of the queue: the Unix socket, the clients on it, and the two files that
//! tell the world what the queue is doing and has learned.
//!
//! Two threads share one `Core` behind a mutex that is held only for in-memory work, never
//! for I/O and never for formatting: whoever needs more than a moment's look at it copies what
//! it needs out first. The socket thread owns every connection: it accepts, reads what clients
//! say, queues their requests, and tells each client where it stands. The sampling thread (the
//! runner's tick) supplies the memory state and measures the admitted groups. Both threads
//! admit all requests that fit: on a sample, or on arrival using the latest sample. A waiting
//! client therefore never delays a sample, and a sample never waits for a client: the tick only tries
//! the mutex, and leaves the queue's work for the next sample if the socket thread has it.
//!
//! What the socket thread tells a waiting client shows that it is alive, not that anything is
//! being scheduled, so it tells them only while the sampling thread is: a thread whose signals
//! are unreadable, or that is stuck, does not do the queue's work, and once it has not for the
//! silence limit (`Core::stalled`) the waiting clients are sent away with an error, which they
//! retry with backoff. New requests receive the same reply until sampling resumes.
//!
//! Neither thread touches a file. `queue.tsv` and `sizes.tsv` are written by a thread of their
//! own (`spool`), fed through a bounded queue that drops what it cannot take. That thread
//! keeps the latest snapshot of a file it could not write and writes it again (`FileWriter`).
//! (The exceptions are start-up, which reads `sizes.tsv`, and shutdown, which removes the
//! socket.)
//!
//! Clients are identified by the kernel, never by what they claim: the peer pid and uid come
//! from the socket itself, together with the start time of the process that has that pid when
//! the connection is accepted, and the session is worked out here from the peer pid. The group
//! a client says its command runs as is checked against the kernel too (its leader must be a
//! child of that same process, which must still be there), and only that group is ever
//! measured.
//!
//! Trust model: the socket is writable by every local user, so any of them may ask for a turn.
//! That is meant for a Mac with one user. Nothing a client sends can do more than take a place
//! in the line or report its own completion. Strings and protocol lines are bounded.
//! Process identities are checked before measuring groups.

use crate::engine::Policy;
use crate::jobs::{self, AGENT_CLI, Inspector, Probe, Regex};
use crate::queue::{Id, Released, Reservation, Sample, Scheduler, Snapshot, WaitReason};
use crate::runner::{age, gb};
use crate::signals::{self, Signals};
use crate::sizes::Sizes;
use crate::spool::Spool;
use crate::wire::{self, Completion, Message, Reply};
use std::fmt::Write as _;
use std::fs;
use std::io::{self, Read, Write};
use std::os::fd::AsRawFd;
use std::os::unix::fs::{FileExt, FileTypeExt, OpenOptionsExt, PermissionsExt};
use std::os::unix::net::{UnixListener, UnixStream};
use std::path::{Path, PathBuf};
use std::sync::atomic::{AtomicBool, AtomicU64, Ordering};
use std::sync::mpsc;
use std::sync::{Arc, Mutex, MutexGuard, PoisonError};
use std::thread;
use std::time::{Duration, Instant};

/// What the queue has learned, in the state directory.
pub const SIZES_FILE: &str = "sizes.tsv";
/// What the queue is doing, in the state directory, rewritten when it changes.
pub const QUEUE_FILE: &str = "queue.tsv";
/// Request a larger backlog for simultaneous launches. XNU caps it at its own limit.
const LISTEN_BACKLOG: libc::c_int = 1024;
/// Connections taken in one pass of the socket loop, so that a flood of them cannot keep the
/// loop from the clients it already has.
const ACCEPTS_PER_PASS: usize = 16;
/// Notes from the socket thread that wait for the next tick. Past this they are dropped and
/// counted. A burst of thousands of simultaneous launches writes a note each within one
/// tick (#31); each note is a short line, so this bounds the backlog to a few megabytes.
const MAX_NOTES: usize = 16_384;
/// File writes that may wait for the thread that does them. Past this they are dropped.
const FILE_QUEUE: usize = 8;
/// How long dropping the broker waits for its files to be tidied.
const SHUTDOWN_WAIT: Duration = Duration::from_secs(1);
/// How long the socket thread sleeps with no clients, and with some.
const IDLE_POLL_MS: i32 = 1000;
const BUSY_POLL_MS: i32 = 100;
/// A client's unsent replies are cut off at this many bytes: it is not reading them.
const MAX_OUTPUT: usize = 4 * wire::MAX_LINE;

pub struct Options {
    pub socket: PathBuf,
    pub state_dir: PathBuf,
    /// Admit every client at once and log what the queue would have decided without
    /// writing queue status or profiles.
    pub dry_run: bool,
}

/// A line for the event log: its kind and its text.
pub type Note = (&'static str, String);

/// What both threads work on.
struct Core {
    sched: Scheduler,
    /// Shared with the thread that writes the file, which holds the last table it was given
    /// until it has written it; the next change copies the table only if it still does.
    sizes: Arc<Sizes>,
    /// Notes from the socket thread, passed on to the event log by the next tick.
    log: Vec<Note>,
    /// Notes that did not fit in `log`.
    lost_notes: u64,
    dry_run: bool,
    /// When the sampling thread last did the queue's work (see `Broker::schedule`): the start,
    /// until it does. The socket thread grants turns only while this is recent. Monotonic.
    scheduled_at: Instant,
}

fn lock(core: &Mutex<Core>) -> MutexGuard<'_, Core> {
    // A panic on the socket thread must not stop the sampling thread.
    core.lock().unwrap_or_else(PoisonError::into_inner)
}

impl Core {
    /// Whether the sampling thread has not done the queue's work for `after`: it is not
    /// sampling (the signals are unreadable, or it is stuck), so nothing here would ever be
    /// admitted, and the lines the socket thread can still send would only keep clients from
    /// running their commands. A dry run holds nobody, so it never stalls.
    fn stalled(&self, after: Duration) -> bool {
        !self.dry_run && self.scheduled_at.elapsed() >= after
    }

    fn note(&mut self, kind: &'static str, text: String) {
        if self.log.len() < MAX_NOTES {
            self.log.push((kind, text));
        } else {
            self.lost_notes += 1;
        }
    }

    /// Moves the notes so far into `out`, and returns how many were dropped (see
    /// `lost_note`).
    fn take_notes(&mut self, out: &mut Vec<Note>) -> u64 {
        out.append(&mut self.log);
        std::mem::take(&mut self.lost_notes)
    }

    /// A request ends, because its command is done or its client went away. A finished
    /// command's peak joins what is known about its signature. Returns what ended, and the
    /// table of sizes if that changed and is to be written. (What to say about it is left to
    /// the caller, who has let go of the core by then.)
    fn release(
        &mut self,
        id: Id,
        exit: Option<i32>,
        completion: Option<Completion>,
    ) -> Option<(Released, Option<Arc<Sizes>>)> {
        let released = self.sched.release(id)?;
        let table = match &released {
            Released::Running(r) => {
                // A failed first run gives the next sibling a turn to establish the profile.
                if exit.is_some_and(|code| code != 0)
                    && !self.sizes.demand(&r.signature).has_profile()
                {
                    return Some((released, None));
                }
                let completion = completion.filter(|_| exit == Some(0));
                let cpu = if exit == Some(0) {
                    r.measured_cpu().or_else(|| {
                        completion.map(|stats| {
                            let machine =
                                std::thread::available_parallelism().map_or(1, |n| n.get() as u32);
                            let cores =
                                stats.cpu_ns.div_ceil(stats.elapsed_ms.saturating_mul(1000));
                            cores.min(u64::from(machine) * 1000) as u32
                        })
                    })
                } else {
                    None
                };
                let peak = r.peak.max(completion.map_or(0, |stats| stats.peak_bytes));
                let runtime = completion.map(|stats| stats.elapsed_ms);
                if peak == 0 && cpu.is_none() && runtime.is_none() {
                    return Some((released, None));
                }
                Arc::make_mut(&mut self.sizes).record_usage(&r.signature, peak, cpu, runtime);
                (!self.dry_run).then(|| Arc::clone(&self.sizes))
            }
            Released::Waiting(_) => None,
        };
        Some((released, table))
    }
}

/// What the event log says about the notes that did not fit.
fn lost_note(count: u64) -> Note {
    (
        "warn",
        format!("{count} queue note(s) were dropped while the log was not being read"),
    )
}

/// What the event log says about a request that ended: its command is done (`exit` is its
/// code) or its client went away.
fn released_note(released: &Released, exit: Option<i32>) -> String {
    let wall = signals::wall();
    match released {
        Released::Waiting(w) => format!(
            "{} left the queue after {} without running (session {})",
            w.label,
            age(wall - w.since),
            w.session
        ),
        Released::Running(r) => {
            let ending = exit.map_or("its client went away".to_string(), |code| {
                format!("exit {code}")
            });
            format!(
                "{} finished after {} ({ending}), peak {}",
                r.label,
                age(wall - r.since),
                gb(r.peak)
            )
        }
    }
}

/// Both admission paths copy the view under the core lock, then format notes outside it.
/// The socket thread delivers `GO` from that same running state in `Ctx::deliver`.
fn admission_notes(view: &Snapshot, admitted: &[Id], dry_run: bool, out: &mut Vec<Note>) {
    for id in admitted {
        if let Some(job) = view.running.iter().find(|r| r.id == *id) {
            out.push((
                "queue",
                format!(
                    "{}admit {} (session {}) after {}: expects {}, {} in use of a {} cap{}",
                    if dry_run { "dry run, would " } else { "" },
                    job.label,
                    job.session,
                    age(job.waited),
                    gb(job.need),
                    gb(view.used),
                    gb(view.cap),
                    job.exclusive.map_or_else(String::new, |rule| {
                        format!("; exclusive memory turn: {}", rule.reason())
                    }),
                ),
            ));
        }
    }
}

// ---------------------------------------------------------------------------------------
// The files

/// What the broker asks the thread that writes its files to do.
enum FileWork {
    /// The new text of the status file.
    Status(String),
    /// The sizes learned so far, to be written as they are.
    Sizes(Arc<Sizes>),
    /// Remove the status file: the broker is going.
    Remove,
    /// Answer once everything before this is done.
    Done(mpsc::Sender<()>),
}

/// How long the thread that writes the files waits before it tries again what it could not
/// write (it also tries again whenever it is given something new).
const RETRY_EVERY: Duration = Duration::from_secs(5);

/// The thread that writes `queue.tsv` and `sizes.tsv`. A write that fails is not forgotten: the
/// latest snapshot of that file is kept here and written again, every `retry_every` and
/// whenever something new arrives, until a write works. A newer snapshot takes the place of
/// an older one that never got written, so the producers hand over what they have once and
/// never wait to hear whether it was written.
struct FileWriter {
    status: PathBuf,
    status_tmp: PathBuf,
    sizes: PathBuf,
    sizes_tmp: PathBuf,
    /// The latest snapshot of each file that is not on disk yet.
    unwritten_status: Option<String>,
    unwritten_sizes: Option<Arc<Sizes>>,
    retry_every: Duration,
    last_try: Instant,
    failures: Failures,
}

/// Says that a write failed: once for each file, not on every attempt, until a write of it
/// works again.
struct Failures {
    core: Arc<Mutex<Core>>,
    retry_every: Duration,
    said: Vec<PathBuf>,
}

impl Failures {
    /// Whether the write worked; if not, says so.
    fn worked(&mut self, result: io::Result<()>, path: &Path) -> bool {
        let Err(e) = result else {
            self.said.retain(|p| p != path);
            return true;
        };
        if !self.said.iter().any(|p| p == path) {
            self.said.push(path.to_path_buf());
            let text = format!(
                "cannot write {}: {e}; trying again every {} s",
                path.display(),
                self.retry_every.as_secs()
            );
            lock(&self.core).note("warn", text);
        }
        false
    }
}

impl FileWriter {
    /// One turn of the thread: something offered, or (None) a second with nothing offered.
    fn work(&mut self, work: Option<FileWork>) {
        match work {
            Some(FileWork::Status(text)) => self.unwritten_status = Some(text),
            Some(FileWork::Sizes(table)) => self.unwritten_sizes = Some(table),
            Some(FileWork::Remove) => {
                // The broker is going: a snapshot that never got written is not to appear now.
                self.unwritten_status = None;
                let _ = fs::remove_file(&self.status);
                return;
            }
            Some(FileWork::Done(done)) => {
                let _ = done.send(());
                return;
            }
            None if self.last_try.elapsed() < self.retry_every => return,
            None => {}
        }
        self.write_unwritten();
    }

    fn write_unwritten(&mut self) {
        if self.unwritten_status.is_none() && self.unwritten_sizes.is_none() {
            return;
        }
        self.last_try = Instant::now();
        if let Some(text) = &self.unwritten_status {
            let result = write_atomic(&self.status, &self.status_tmp, text.as_bytes());
            if self.failures.worked(result, &self.status) {
                self.unwritten_status = None;
            }
        }
        if let Some(table) = &self.unwritten_sizes {
            let result = write_atomic(&self.sizes, &self.sizes_tmp, table.render().as_bytes());
            if self.failures.worked(result, &self.sizes) {
                self.unwritten_sizes = None;
            }
        }
    }
}

/// Starts the thread that writes `queue.tsv` and `sizes.tsv` in `state_dir`. What it cannot
/// write, and what it is too far behind to be given, is said in the next tick's notes.
fn spawn_files(
    state_dir: &Path,
    core: &Arc<Mutex<Core>>,
    retry_every: Duration,
) -> Result<Spool<FileWork>, String> {
    let mut writer = FileWriter {
        status: state_dir.join(QUEUE_FILE),
        status_tmp: state_dir.join(format!("{QUEUE_FILE}.tmp")),
        sizes: state_dir.join(SIZES_FILE),
        sizes_tmp: state_dir.join(format!("{SIZES_FILE}.tmp")),
        unwritten_status: None,
        unwritten_sizes: None,
        retry_every,
        last_try: Instant::now(),
        failures: Failures {
            core: Arc::clone(core),
            retry_every,
            said: Vec::new(),
        },
    };
    let on_loss = Arc::clone(core);
    Spool::spawn_idling(
        "membrake-queue-files",
        FILE_QUEUE,
        move |work| writer.work(work),
        move |lost| {
            let text = format!(
                "{lost} queue file write(s) were dropped while the disk was not keeping up"
            );
            lock(&on_loss).note("warn", text);
        },
    )
    .map_err(|e| format!("cannot start the thread that writes the queue's files: {e}"))
}

/// Writes `bytes` to `path` through `tmp`, so a reader sees the old file or the new one and
/// never half of either. World-readable: `membrake status` is for every user.
fn write_atomic(path: &Path, tmp: &Path, bytes: &[u8]) -> io::Result<()> {
    let mut file = fs::OpenOptions::new()
        .write(true)
        .create(true)
        .truncate(true)
        .mode(0o644)
        .open(tmp)?;
    file.set_permissions(fs::Permissions::from_mode(0o644))?;
    file.write_all(bytes)?;
    fs::rename(tmp, path)
}

// ---------------------------------------------------------------------------------------
// The sampling thread's side

pub struct Broker {
    core: Arc<Mutex<Core>>,
    stop: Arc<AtomicBool>,
    socket: PathBuf,
    _pidfile: fs::File,
    heartbeat: HeartbeatWriter,
    sampling: Arc<AtomicBool>,
    sample_started: Instant,
    /// None in a dry run, which writes no file.
    files: Option<Spool<FileWork>>,
    dry_run: bool,
    /// The status file as it will be, and as it was last handed to the thread that writes it.
    /// Reused, so a tick that has nothing to say allocates nothing.
    text: String,
    written: String,
    /// The admitted groups to measure this tick: the request, the group, and when its leader
    /// started.
    groups: Vec<(Id, i32, u64)>,
    /// What this tick measured of them.
    seen: Vec<(Id, jobs::GroupUsage)>,
    reservations: Vec<Reservation>,
    inspector: Inspector,
    /// Where the queue stood at the last tick, copied out of the core.
    view: Snapshot,
    /// Reused output of the scheduler's admission loop.
    admitted: Vec<Id>,
    /// On how many samples the queue's work was done (see `scheduled`).
    scheduled: u64,
}

impl Broker {
    /// Loads what was learned, binds the socket and starts the threads that serve it and write
    /// its files. The daemon can keep braking if the queue cannot start.
    pub fn start(opts: Options) -> Result<Broker, String> {
        Self::start_with_cpu_headroom(opts, crate::queue::DEFAULT_CPU_HEADROOM_PERCENT)
    }

    pub(crate) fn start_with_cpu_headroom(
        opts: Options,
        cpu_headroom_percent: u32,
    ) -> Result<Broker, String> {
        let mut log = Vec::new();
        let sizes_path = opts.state_dir.join(SIZES_FILE);
        let sizes = match fs::read_to_string(&sizes_path) {
            Ok(text) => {
                let (sizes, skipped) = Sizes::parse(&text);
                if skipped > 0 {
                    log.push((
                        "warn",
                        format!(
                            "{skipped} unreadable line(s) of {} are ignored",
                            sizes_path.display()
                        ),
                    ));
                }
                sizes
            }
            Err(e) if e.kind() == io::ErrorKind::NotFound => Sizes::default(),
            Err(e) => {
                log.push((
                    "warn",
                    format!(
                        "cannot read {}: {e}; no sizes are known yet",
                        sizes_path.display()
                    ),
                ));
                Sizes::default()
            }
        };
        log.push((
            "queue",
            format!(
                "{} sizes learned; listening on {}{}",
                sizes.len(),
                opts.socket.display(),
                if opts.dry_run {
                    " (dry run: every client is admitted at once)"
                } else {
                    ""
                }
            ),
        ));
        let pidfile = lock_pidfile(&opts.socket)?;
        let heartbeat = HeartbeatWriter::open(&opts.socket)?;
        let listener = bind(&opts.socket)?;
        raise_descriptor_limit();
        let core = Arc::new(Mutex::new(Core {
            sched: Scheduler::with_cpu_headroom(cpu_headroom_percent),
            sizes: Arc::new(sizes),
            log,
            lost_notes: 0,
            dry_run: opts.dry_run,
            scheduled_at: Instant::now(),
        }));
        let files = (!opts.dry_run)
            .then(|| spawn_files(&opts.state_dir, &core, RETRY_EVERY))
            .transpose()?;
        let stop = Arc::new(AtomicBool::new(false));
        let sampling = Arc::new(AtomicBool::new(false));
        let thread_sampling = Arc::clone(&sampling);
        let (thread_core, thread_stop, thread_files) =
            (Arc::clone(&core), Arc::clone(&stop), files.clone());
        thread::Builder::new()
            .name("membrake-queue".into())
            .spawn(move || {
                serve(
                    &listener,
                    &thread_core,
                    &thread_stop,
                    &thread_sampling,
                    thread_files,
                )
            })
            .map_err(|e| format!("cannot start the queue thread: {e}"))?;
        Ok(Broker {
            core,
            stop,
            socket: opts.socket,
            _pidfile: pidfile,
            heartbeat,
            sampling,
            sample_started: Instant::now(),
            files,
            dry_run: opts.dry_run,
            text: String::with_capacity(1024),
            written: String::with_capacity(1024),
            groups: Vec::with_capacity(16),
            seen: Vec::with_capacity(16),
            reservations: Vec::with_capacity(16),
            inspector: Inspector::new(),
            view: Snapshot::default(),
            admitted: Vec::new(),
            scheduled: 0,
        })
    }

    pub fn scheduled(&self) -> u64 {
        self.scheduled
    }

    /// Pause socket-side admission immediately. The brake can act before any group
    /// accounting or queue scheduling.
    pub(crate) fn begin_sample(&mut self) {
        self.sampling.store(true, Ordering::Release);
        self.sample_started = Instant::now();
        self.reservations.clear();
        self.heartbeat.publish(signals::uptime());
    }

    /// Group reads happen outside the scheduler mutex, after pressure actions.
    pub(crate) fn measure(&mut self, now: f64) {
        self.groups.clear();
        {
            let core = lock(&self.core);
            self.groups.extend(
                core.sched
                    .running()
                    .iter()
                    .filter_map(|r| r.pgid.map(|pgid| (r.id, pgid, r.leader_start_us))),
            );
        }
        self.seen.clear();
        for &(id, pgid, leader_start_us) in &self.groups {
            if let Some(usage) = jobs::group_usage(&mut self.inspector, pgid, leader_start_us) {
                self.seen.push((id, usage));
            }
        }
        let mut core = lock(&self.core);
        for (id, usage) in &self.seen {
            core.sched.observe(*id, usage.bytes, now);
            if usage.stopped {
                core.sched.restart_plateau(*id);
            }
            if let Some(cpu) = usage.cpu_ns {
                // Pressure actions can wait through a kill grace before the counters are read.
                core.sched
                    .observe_cpu(*id, cpu, usage.stopped, usage.measured_at);
            }
        }
    }

    /// Fetch after scanning candidates so STARTED messages checked during that scan
    /// also take part in ranking. The returned slice is also what replay records.
    pub(crate) fn refresh_reservations(&mut self) -> &[Reservation] {
        let core = lock(&self.core);
        self.reservations.clear();
        self.reservations
            .extend(core.sched.running().iter().filter_map(|r| {
                Some(Reservation {
                    pgid: r.pgid?,
                    start_us: r.leader_start_us,
                    limit: r.limit(),
                })
            }));
        &self.reservations
    }

    pub(crate) fn reservations(&self) -> &[Reservation] {
        &self.reservations
    }

    pub fn tick(&mut self, s: &Signals, tight: bool, frozen: bool) -> Vec<Note> {
        self.begin_sample();
        self.measure(s.time);
        self.schedule(s, tight, frozen, &Policy::default())
    }

    /// Publish this sample's admission conditions after the engine has acted.
    pub(crate) fn schedule(
        &mut self,
        s: &Signals,
        tight: bool,
        frozen: bool,
        policy: &Policy,
    ) -> Vec<Note> {
        let mut notes = Vec::new();
        let sample = Sample {
            time: s.time,
            // The real clock, whatever drives the samples: it dates what people are shown.
            wall: signals::wall(),
            ram: s.ram,
            level: s.level,
            occupancy: s.occupancy(),
            tight,
            frozen,
        };
        let freeze_pressure =
            policy.freeze_pressure(s, tight) || s.pressure == signals::Pressure::Critical;
        let lost = {
            let mut guard = lock(&self.core);
            let core = &mut *guard;
            core.scheduled_at = self.sample_started;
            let lost = core.take_notes(&mut notes);
            let sizes = &core.sizes;
            let learned = |signature: &str| sizes.demand(signature);
            let now = s.time + self.sample_started.elapsed().as_secs_f64();
            core.sched.step_at(
                &sample,
                now,
                policy,
                freeze_pressure,
                &learned,
                &mut self.admitted,
            );
            if !self.dry_run {
                notes.extend(core.sched.new_stalls(sample.wall).map(|job| {
                    (
                        "warn",
                        format!(
                            "{} (session {}) stalled after {} waiting with nothing running",
                            job.label,
                            job.session,
                            age(sample.wall - job.since)
                        ),
                    )
                }));
            }
            core.sched.snapshot(&learned, &mut self.view);
            lost
        };
        self.sampling.store(false, Ordering::Release);
        // From here on the socket thread has the core again.
        self.scheduled += 1;
        if lost > 0 {
            notes.push(lost_note(lost));
        }
        admission_notes(&self.view, &self.admitted, self.dry_run, &mut notes);
        if let Some(files) = &self.files {
            self.text.clear();
            render(&self.view, &mut self.text);
            // Kept to be offered again at the next sample if the thread could not take it.
            if self.text != self.written && files.offer(FileWork::Status(self.text.clone())).is_ok()
            {
                std::mem::swap(&mut self.text, &mut self.written);
            }
        }
        notes
    }
}

impl Drop for Broker {
    fn drop(&mut self) {
        self.stop.store(true, Ordering::SeqCst);
        let _ = fs::remove_file(&self.socket);
        // The status file goes after whatever is still waiting to be written, and the wait
        // for that is short: a disk that is stuck is not worth holding the daemon's exit for.
        if let Some(files) = &self.files {
            let (done, answer) = mpsc::channel();
            if files.try_send(FileWork::Remove).is_ok()
                && files.try_send(FileWork::Done(done)).is_ok()
            {
                let _ = answer.recv_timeout(SHUTDOWN_WAIT);
            }
        }
    }
}

// ---------------------------------------------------------------------------------------
// The status file

/// The status file, in text. One header line, then one line per fact:
/// `daemon`, `ram`, `cap` and `used` (bytes), `wait` rows in request order
/// (position, session, wall-clock seconds when it asked, need, reason, label) and `run` rows
/// (session, wall-clock seconds when it was admitted, process group or 0, peak, label).
fn render(view: &Snapshot, out: &mut String) {
    let _ = writeln!(
        out,
        "queue\tv2\ndaemon\t{}\nram\t{}\ncap\t{}\nused\t{}",
        std::process::id(),
        view.ram,
        view.cap,
        view.used >> 20 << 20
    );
    for w in &view.waiting {
        let _ = writeln!(
            out,
            "wait\t{}\t{}\t{}\t{}\t{}{}{}\t{}",
            w.position,
            w.session,
            w.since as u64,
            w.need,
            if w.stalled { "stalled: " } else { "" },
            w.reason,
            if !w.stalled {
                ""
            } else if w.reason == WaitReason::Unspecified {
                "nothing running"
            } else {
                ", nothing running"
            },
            w.label
        );
    }
    for r in &view.running {
        let _ = writeln!(
            out,
            "run\t{}\t{}\t{}\t{}\t{}",
            r.session, r.since as u64, r.pgid, r.peak, r.label
        );
    }
}

pub struct WaitRow {
    pub position: usize,
    pub session: i32,
    pub since: f64,
    pub need: u64,
    pub reason: String,
    pub label: String,
}

pub struct RunRow {
    pub session: i32,
    pub since: f64,
    pub pgid: i32,
    pub peak: u64,
    pub label: String,
}

/// The queue as the daemon last wrote it.
pub struct QueueFile {
    pub daemon: i32,
    pub ram: u64,
    pub cap: u64,
    pub used: u64,
    pub waiting: Vec<WaitRow>,
    pub running: Vec<RunRow>,
}

impl QueueFile {
    pub fn parse(text: &str) -> Option<QueueFile> {
        let mut lines = text.lines();
        let has_reason = match lines.next()? {
            "queue\tv1" => false,
            "queue\tv2" => true,
            _ => return None,
        };
        let mut file = QueueFile {
            daemon: 0,
            ram: 0,
            cap: 0,
            used: 0,
            waiting: Vec::new(),
            running: Vec::new(),
        };
        for line in lines {
            let f: Vec<&str> = line.splitn(6, '\t').collect();
            match f.as_slice() {
                ["daemon", v] => file.daemon = v.parse().ok()?,
                ["ram", v] => file.ram = v.parse().ok()?,
                ["cap", v] => file.cap = v.parse().ok()?,
                ["used", v] => file.used = v.parse().ok()?,
                ["wait", position, session, since, need, tail] => {
                    let (reason, label) = if has_reason {
                        tail.split_once('\t')?
                    } else {
                        ("", *tail)
                    };
                    file.waiting.push(WaitRow {
                        position: position.parse().ok()?,
                        session: session.parse().ok()?,
                        since: since.parse().ok()?,
                        need: need.parse().ok()?,
                        reason: reason.to_string(),
                        label: label.to_string(),
                    });
                }
                ["run", session, since, pgid, peak, label] => file.running.push(RunRow {
                    session: session.parse().ok()?,
                    since: since.parse().ok()?,
                    pgid: pgid.parse().ok()?,
                    peak: peak.parse().ok()?,
                    label: label.to_string(),
                }),
                _ => return None,
            }
        }
        Some(file)
    }
}

/// Reads the status file of the daemon that keeps its state in `dir`. None if there is none.
pub fn read_queue(dir: &Path) -> io::Result<Option<QueueFile>> {
    match fs::read_to_string(dir.join(QUEUE_FILE)) {
        Ok(text) => QueueFile::parse(&text)
            .map(Some)
            .ok_or_else(|| io::Error::other("it is not in a format this version reads")),
        Err(e) if e.kind() == io::ErrorKind::NotFound => Ok(None),
        Err(e) => Err(e),
    }
}

// ---------------------------------------------------------------------------------------
// The socket

/// Use the existing hard limit, bounded by the kernel's per-process maximum.
fn raise_descriptor_limit() {
    let mut limit = libc::rlimit {
        rlim_cur: 0,
        rlim_max: 0,
    };
    let mut maximum: libc::c_int = 0;
    let mut size = std::mem::size_of_val(&maximum);
    // SAFETY: valid output buffers, and only this process's soft limit is changed.
    unsafe {
        if libc::getrlimit(libc::RLIMIT_NOFILE, &mut limit) == 0
            && libc::sysctlbyname(
                c"kern.maxfilesperproc".as_ptr(),
                (&mut maximum as *mut libc::c_int).cast(),
                &mut size,
                std::ptr::null_mut(),
                0,
            ) == 0
            && maximum > 0
        {
            let target = limit.rlim_max.min(maximum as libc::rlim_t);
            if limit.rlim_cur < target {
                limit.rlim_cur = target;
                libc::setrlimit(libc::RLIMIT_NOFILE, &limit);
            }
        }
    }
}

fn pidfile_path(socket: &Path) -> PathBuf {
    let mut name = socket.as_os_str().to_os_string();
    name.push(".pid");
    name.into()
}

fn heartbeat_path(socket: &Path) -> PathBuf {
    let mut name = socket.as_os_str().to_os_string();
    name.push(".heartbeat");
    name.into()
}

struct HeartbeatWriter {
    stamp: Arc<AtomicU64>,
    writer: Spool<()>,
}

impl HeartbeatWriter {
    fn open(socket: &Path) -> Result<Self, String> {
        let path = heartbeat_path(socket);
        let result = (|| -> io::Result<Self> {
            let file = fs::OpenOptions::new()
                .write(true)
                .create(true)
                .truncate(false)
                .mode(0o644)
                .custom_flags(libc::O_NOFOLLOW | libc::O_NONBLOCK)
                .open(&path)?;
            if !file.metadata()?.is_file() {
                return Err(io::Error::other("not a regular file"));
            }
            file.set_permissions(fs::Permissions::from_mode(0o644))?;
            let initial = signals::uptime().to_bits();
            file.write_all_at(&initial.to_ne_bytes(), 0)?;
            let stamp = Arc::new(AtomicU64::new(initial));
            let latest = Arc::clone(&stamp);
            let writer = Spool::spawn(
                "membrake-heartbeat",
                1,
                move |()| {
                    let value = latest.load(Ordering::Acquire);
                    let _ = file.write_all_at(&value.to_ne_bytes(), 0);
                },
                |_| {},
            )?;
            Ok(Self { stamp, writer })
        })();
        result.map_err(|e| format!("cannot write {}: {e}", path.display()))
    }

    fn publish(&self, uptime: f64) {
        self.stamp.store(uptime.to_bits(), Ordering::Release);
        let _ = self.writer.try_send(());
    }
}

/// Awake uptime of the last sample published by the broker.
pub(crate) fn heartbeat(socket: &Path) -> Option<f64> {
    let file = fs::OpenOptions::new()
        .read(true)
        .custom_flags(libc::O_NOFOLLOW | libc::O_NONBLOCK)
        .open(heartbeat_path(socket))
        .ok()?;
    if !file.metadata().ok()?.is_file() {
        return None;
    }
    let mut stamp = [0u8; 8];
    file.read_exact_at(&mut stamp, 0).ok()?;
    let stamp = f64::from_ne_bytes(stamp);
    (stamp.is_finite() && stamp > 0.0).then_some(stamp)
}

fn lock_pidfile(socket: &Path) -> Result<fs::File, String> {
    let path = pidfile_path(socket);
    let mut file = fs::OpenOptions::new()
        .write(true)
        .create(true)
        .truncate(false)
        .mode(0o644)
        .custom_flags(libc::O_NOFOLLOW | libc::O_NONBLOCK)
        .open(&path)
        .map_err(|e| format!("cannot open {}: {e}", path.display()))?;
    if !file.metadata().is_ok_and(|m| m.is_file()) {
        return Err(format!("{} is not a regular file", path.display()));
    }
    // SAFETY: file owns the descriptor for the daemon's lifetime.
    if unsafe { libc::flock(file.as_raw_fd(), libc::LOCK_EX | libc::LOCK_NB) } != 0 {
        let error = io::Error::last_os_error();
        return Err(if error.kind() == io::ErrorKind::WouldBlock {
            format!("{} is served by another process", socket.display())
        } else {
            format!("cannot lock {}: {error}", path.display())
        });
    }
    file.set_permissions(fs::Permissions::from_mode(0o644))
        .and_then(|_| file.set_len(0))
        .and_then(|_| writeln!(file, "{}", std::process::id()))
        .map_err(|e| format!("cannot write {}: {e}", path.display()))?;
    Ok(file)
}

/// The kernel releases the pidfile lock when its owner exits. No PID lookup can
/// mistake a restarted or unrelated process for the daemon that owned the socket.
pub(crate) fn daemon_alive(socket: &Path) -> bool {
    let Ok(file) = fs::OpenOptions::new()
        .read(true)
        .custom_flags(libc::O_NOFOLLOW | libc::O_NONBLOCK)
        .open(pidfile_path(socket))
    else {
        return false;
    };
    // SAFETY: this is a nonblocking shared-lock probe on our own descriptor.
    file.metadata().is_ok_and(|m| m.is_file())
        && unsafe { libc::flock(file.as_raw_fd(), libc::LOCK_SH | libc::LOCK_NB) } != 0
        && io::Error::last_os_error().kind() == io::ErrorKind::WouldBlock
}

fn bind(path: &Path) -> Result<UnixListener, String> {
    let listener = match UnixListener::bind(path) {
        Ok(listener) => listener,
        Err(e) if e.kind() == io::ErrorKind::AddrInUse => {
            // A socket nobody answers on is left over from a daemon that died; anything else
            // at that path is not ours to remove.
            let is_socket = fs::symlink_metadata(path).is_ok_and(|m| m.file_type().is_socket());
            match UnixStream::connect(path) {
                Ok(_) => return Err(format!("{} is served by another process", path.display())),
                Err(c) if is_socket && c.kind() == io::ErrorKind::ConnectionRefused => {
                    fs::remove_file(path)
                        .map_err(|e| format!("cannot remove the stale {}: {e}", path.display()))?;
                    UnixListener::bind(path)
                        .map_err(|e| format!("cannot listen on {}: {e}", path.display()))?
                }
                Err(_) => {
                    return Err(format!(
                        "{} exists and is not a free socket",
                        path.display()
                    ));
                }
            }
        }
        Err(e) => return Err(format!("cannot listen on {}: {e}", path.display())),
    };
    // SAFETY: the listener owns a live, bound Unix socket.
    if unsafe { libc::listen(listener.as_raw_fd(), LISTEN_BACKLOG) } != 0 {
        return Err(format!(
            "cannot set listen backlog on {}: {}",
            path.display(),
            io::Error::last_os_error()
        ));
    }
    // Every local user may ask for a turn (see the trust model above). The umask decides the
    // mode bind gives it, so it is set explicitly.
    fs::set_permissions(path, fs::Permissions::from_mode(0o666))
        .map_err(|e| format!("cannot set the mode of {}: {e}", path.display()))?;
    listener
        .set_nonblocking(true)
        .map_err(|e| format!("cannot make {} non-blocking: {e}", path.display()))?;
    Ok(listener)
}

/// Who is on the other end, as the kernel says: the process (pid and start time, which tell it
/// from whoever has the pid next) that made the connection, and its user.
struct Peer {
    pid: i32,
    start_us: u64,
    uid: u32,
}

impl Peer {
    fn of(stream: &UnixStream) -> io::Result<Peer> {
        let fd = stream.as_raw_fd();
        let mut pid: libc::pid_t = 0;
        let mut len = size_of::<libc::pid_t>() as libc::socklen_t;
        // SAFETY: `pid` is a pid_t-sized buffer and `len` says so.
        let rc = unsafe {
            libc::getsockopt(
                fd,
                libc::SOL_LOCAL,
                libc::LOCAL_PEERPID,
                (&raw mut pid).cast(),
                &mut len,
            )
        };
        if rc != 0 {
            return Err(io::Error::last_os_error());
        }
        let (mut uid, mut gid) = (0, 0);
        // SAFETY: `uid` and `gid` are valid out-parameters.
        if unsafe { libc::getpeereid(fd, &mut uid, &mut gid) } != 0 {
            return Err(io::Error::last_os_error());
        }
        // The pid alone is only a number: the connection can outlive the process (handed on
        // by inheritance or by a passed descriptor) and the pid can be given to another.
        let start_us = match jobs::probe(pid) {
            Probe::Found(p) => p.start_us,
            Probe::Gone => return Err(io::Error::other("the client has already exited")),
            Probe::Unreadable(e) => return Err(e),
        };
        Ok(Peer { pid, start_us, uid })
    }

    /// Whether the process that made the connection is still there as itself: its pid still
    /// has the start time it had when it connected.
    fn is_alive(&self) -> bool {
        matches!(jobs::probe(self.pid), Probe::Found(p) if p.start_us == self.start_us)
    }
}

struct Conn {
    stream: UnixStream,
    peer: Peer,
    input: Vec<u8>,
    output: Vec<u8>,
    /// Its place in the scheduler, once it has asked.
    id: Option<Id>,
    /// The last position, need and reason it was told.
    told: Option<(usize, u64, WaitReason)>,
    /// When it was last told, whether or not that changed anything.
    told_at: Instant,
    /// When it was told to go (wall-clock seconds), if it has been: the command it starts
    /// cannot have started before.
    go_at: Option<f64>,
    /// It has said which group its command runs as.
    started: bool,
    /// Close once everything queued for it is sent.
    closing: bool,
    /// Gone, or to be dropped now.
    dead: bool,
}

impl Conn {
    fn say(&mut self, reply: &Reply) {
        self.output.extend_from_slice(reply.line().as_bytes());
    }

    fn go(&mut self) {
        self.go_at = Some(signals::wall());
        self.say(&Reply::Go);
    }

    /// Tells the client what is wrong and hangs up after it.
    fn refuse(&mut self, why: &str) {
        self.say(&Reply::Error(why.to_string()));
        self.closing = true;
    }

    /// Reads what the client has sent, without waiting for more.
    fn read(&mut self) {
        let mut buf = [0u8; 512];
        loop {
            match self.stream.read(&mut buf) {
                Ok(0) => {
                    // It hung up. What it sent before that is still dealt with.
                    self.dead = true;
                    return;
                }
                Ok(n) => {
                    self.input.extend_from_slice(&buf[..n]);
                    if self.input.len() > 4 * wire::MAX_LINE {
                        return;
                    }
                }
                Err(e) if e.kind() == io::ErrorKind::WouldBlock => return,
                Err(e) if e.kind() == io::ErrorKind::Interrupted => {}
                Err(_) => {
                    self.dead = true;
                    return;
                }
            }
        }
    }

    /// Sends as much of what is queued as the client will take now.
    fn flush(&mut self) {
        while !self.output.is_empty() {
            match self.stream.write(&self.output) {
                Ok(0) => break,
                Ok(n) => {
                    self.output.drain(..n);
                }
                Err(e) if e.kind() == io::ErrorKind::WouldBlock => break,
                Err(e) if e.kind() == io::ErrorKind::Interrupted => {}
                Err(_) => {
                    self.dead = true;
                    return;
                }
            }
        }
        if self.output.len() > MAX_OUTPUT || (self.closing && self.output.is_empty()) {
            self.dead = true;
        }
    }
}

/// What the socket thread needs besides the core.
struct Ctx<'a> {
    core: &'a Mutex<Core>,
    sampling: &'a AtomicBool,
    agent: Option<Regex>,
    /// None in a dry run, which writes no file.
    files: Option<Spool<FileWork>>,
    dry_run: bool,
    /// A waiting client is told where it stands at least this often.
    keep_alive: Duration,
    /// Pause admission once the most recent sample is this old.
    stalled_after: Duration,
}

impl Ctx<'_> {
    /// One line from a client.
    fn handle(&self, conn: &mut Conn, line: &str) {
        if line.len() > wire::MAX_LINE {
            return conn.refuse("line too long");
        }
        let message = match Message::parse(line) {
            Ok(message) => message,
            Err(why) => return conn.refuse(&why),
        };
        match (message, conn.id) {
            (
                Message::Request {
                    label,
                    signature,
                    first_attempt_us,
                },
                None,
            ) => {
                let session = match &self.agent {
                    Some(agent) => jobs::session_of(agent, conn.peer.pid),
                    None => conn.peer.pid,
                };
                let label = if label.is_empty() {
                    signature.clone()
                } else {
                    label
                };
                let text = format!(
                    "{label} asks for a turn (pid {}, uid {}, session {session})",
                    conn.peer.pid, conn.peer.uid
                );
                let now = signals::wall();
                let wall = first_attempt_us
                    .filter(|&at| at >= conn.peer.start_us && at <= (now * 1e6) as u64)
                    .map_or(now, |at| at as f64 / 1e6);
                let mut guard = lock(self.core);
                guard.note("queue", text);
                conn.id = Some(guard.sched.enqueue(session, label, signature, wall));
                drop(guard);
                if self.dry_run {
                    // A dry run holds nobody: the queue still decides, to be logged, but the
                    // client goes now.
                    conn.go();
                }
            }
            (Message::Request { .. }, Some(_)) => conn.refuse("a connection asks once"),
            (Message::Started(pgid), Some(id)) => self.started(conn, id, pgid),
            (Message::Started(_), None) => conn.refuse("started before a request"),
            (Message::Done(code, completion), Some(id)) if conn.go_at.is_some() => {
                conn.id = None;
                self.release(id, Some(code), completion);
                conn.closing = true;
            }
            (Message::Done(..), None) => conn.refuse("done before a request"),
            (Message::Done(..), Some(_)) => conn.refuse("done before the go-ahead"),
        }
    }

    /// The client says its command runs as group `pgid`. Once per connection, after it was
    /// told to go, and only if the group is what a command started by this client is: led by
    /// a process that is its child, of the same user, that started after the go-ahead. The
    /// client is the process that connected, still: a pid that has since gone to another
    /// process is not it, and a child of that process is not its command. That leader's
    /// identity is what is measured from then on; nothing else a client names is ever looked
    /// at.
    fn started(&self, conn: &mut Conn, id: Id, pgid: i32) {
        if conn.started {
            return conn.refuse("a connection starts once");
        }
        let Some(go_at) = conn.go_at else {
            return conn.refuse("started before the go-ahead");
        };
        let leader = match jobs::probe_including_exited(pgid) {
            // The child may already be reaped. DONE still carries the client's own usage.
            Probe::Gone if pgid > 1 => {
                conn.started = true;
                return;
            }
            Probe::Found(l)
                if pgid > 1
                    && l.pgid == pgid
                    && l.ppid == conn.peer.pid
                    && l.uid == conn.peer.uid
                    && l.start_us > (go_at * 1e6) as u64
                    && conn.peer.is_alive() =>
            {
                l
            }
            _ => return conn.refuse("that is not the group of the command you were let go"),
        };
        conn.started = true;
        lock(self.core).sched.started(id, pgid, leader.start_us);
    }

    /// The request ends; hands the sizes to the thread that writes them if they changed.
    fn release(&self, id: Id, exit: Option<i32>, completion: Option<Completion>) {
        let ended = lock(self.core).release(id, exit, completion);
        let Some((released, table)) = ended else {
            return;
        };
        let text = released_note(&released, exit);
        lock(self.core).note("queue", text);
        if let (Some(table), Some(files)) = (table, &self.files) {
            // If the thread cannot take it, it is counted and said at the next tick; the
            // next command to finish writes the whole table again.
            let _ = files.offer(FileWork::Sizes(table));
        }
    }

    /// Tells every waiting client where it stands, and every admitted one to go; if the
    /// sampling thread has stopped scheduling, sends the waiting ones away instead.
    fn deliver(&self, conns: &mut [Conn], view: &mut Snapshot, admitted: &mut Vec<Id>) {
        if conns.iter().all(|c| c.id.is_none()) {
            return;
        }
        {
            let mut guard = lock(self.core);
            let core = &mut *guard;
            let learned = |signature: &str| core.sizes.demand(signature);
            admitted.clear();
            if !core.stalled(self.stalled_after) && !self.sampling.load(Ordering::Acquire) {
                let now = core.sched.sample_time() + core.scheduled_at.elapsed().as_secs_f64();
                core.sched
                    .admit_now(now, signals::wall(), &learned, admitted);
            }
            core.sched.snapshot(&learned, view);
        }
        if !admitted.is_empty() {
            let mut notes = Vec::new();
            admission_notes(view, admitted, self.dry_run, &mut notes);
            let mut core = lock(self.core);
            for (kind, text) in notes {
                core.note(kind, text);
            }
        }
        for conn in conns.iter_mut() {
            let Some(id) = conn.id.filter(|_| !conn.closing && !conn.dead) else {
                continue;
            };
            if let Some(w) = view.waiting.iter().find(|w| w.id == id) {
                let standing = (w.position, w.need, w.reason);
                if !self.dry_run
                    && (conn.told != Some(standing) || conn.told_at.elapsed() >= self.keep_alive)
                {
                    // A healthy scheduler sends keep-alives even when everything is waiting.
                    conn.told = Some(standing);
                    conn.told_at = Instant::now();
                    conn.say(&Reply::Queued {
                        position: w.position,
                        need: w.need,
                        reason: w.reason,
                    });
                }
            } else if view.running.iter().any(|r| r.id == id) {
                if conn.go_at.is_none() {
                    conn.go();
                }
            } else {
                conn.dead = true;
            }
        }
    }
}

/// The socket thread.
fn serve(
    listener: &UnixListener,
    core: &Mutex<Core>,
    stop: &AtomicBool,
    sampling: &AtomicBool,
    files: Option<Spool<FileWork>>,
) {
    crate::act::block_shutdown_signals();
    let dry_run = lock(core).dry_run;
    let ctx = Ctx {
        core,
        sampling,
        // Without the regex every session is the client's own pid: no turns, but it works.
        agent: Regex::new(AGENT_CLI).ok(),
        files,
        dry_run,
        keep_alive: wire::timing().keep_alive,
        stalled_after: wire::timing().silence.min(Duration::from_secs(15)),
    };
    let mut conns: Vec<Conn> = Vec::new();
    let mut view = Snapshot::default();
    let mut admitted = Vec::new();
    let mut accept_failed = false;
    while !stop.load(Ordering::SeqCst) && !crate::act::shutdown_requested() {
        let mut fds = Vec::with_capacity(conns.len() + 1);
        fds.push(libc::pollfd {
            fd: listener.as_raw_fd(),
            events: libc::POLLIN,
            revents: 0,
        });
        fds.extend(conns.iter().map(|c| libc::pollfd {
            fd: c.stream.as_raw_fd(),
            events: libc::POLLIN
                | if c.output.is_empty() {
                    0
                } else {
                    libc::POLLOUT
                },
            revents: 0,
        }));
        let timeout = if conns.is_empty() {
            IDLE_POLL_MS
        } else {
            BUSY_POLL_MS
        };
        // SAFETY: `fds` is a valid array of the length passed.
        let ready = unsafe { libc::poll(fds.as_mut_ptr(), fds.len() as libc::nfds_t, timeout) };
        if ready < 0 {
            if io::Error::last_os_error().kind() != io::ErrorKind::Interrupted {
                thread::sleep(Duration::from_millis(100));
            }
            continue;
        }
        for (conn, fd) in conns.iter_mut().zip(&fds[1..]) {
            if fd.revents & (libc::POLLIN | libc::POLLHUP | libc::POLLERR) != 0 {
                conn.read();
            }
        }
        if fds[0].revents & libc::POLLIN != 0 {
            accept_some(listener, &mut conns, core, &mut accept_failed);
        }
        for conn in &mut conns {
            while !conn.closing
                && let Some(line) = wire::take_line(&mut conn.input)
            {
                ctx.handle(conn, &line);
            }
            if conn.closing {
                // Told to go away: whatever more it sends is not read.
                conn.input.clear();
            } else if conn.input.len() > wire::MAX_LINE {
                conn.refuse("line too long");
                conn.input.clear();
            }
        }
        ctx.deliver(&mut conns, &mut view, &mut admitted);
        for conn in &mut conns {
            conn.flush();
        }
        conns.retain_mut(|conn| {
            if conn.dead
                && let Some(id) = conn.id.take()
            {
                ctx.release(id, None, None);
            }
            !conn.dead
        });
    }
}

/// Takes a batch of waiting connections so existing clients are served between accepts.
fn accept_some(
    listener: &UnixListener,
    conns: &mut Vec<Conn>,
    core: &Mutex<Core>,
    failed: &mut bool,
) {
    for _ in 0..ACCEPTS_PER_PASS {
        match listener.accept() {
            Ok((stream, _)) => {
                // A client the kernel cannot identify is not served.
                let Ok(peer) = Peer::of(&stream) else {
                    continue;
                };
                if stream.set_nonblocking(true).is_err() {
                    continue;
                }
                conns.push(Conn {
                    stream,
                    peer,
                    input: Vec::new(),
                    output: Vec::new(),
                    id: None,
                    told: None,
                    told_at: Instant::now(),
                    go_at: None,
                    started: false,
                    closing: false,
                    dead: false,
                });
            }
            Err(e) if e.kind() == io::ErrorKind::WouldBlock => return,
            Err(e) if e.kind() == io::ErrorKind::Interrupted => {}
            Err(e) => {
                // Out of descriptors, most likely. Say so once, and do not spin on it.
                if !std::mem::replace(failed, true) {
                    lock(core).note("warn", format!("cannot accept a queue client: {e}"));
                }
                thread::sleep(Duration::from_millis(100));
                return;
            }
        }
    }
}

#[cfg(test)]
mod tests {
    use super::*;
    use crate::signals::Pressure;
    use crate::testutil::{mkfifo, orphan_group, scratch};
    use std::os::unix::fs::PermissionsExt;
    use std::os::unix::process::CommandExt;
    use std::process::{Child, Command};
    use std::time::Instant;

    /// Tight readings reach the freeze tier so tests can hold the queue with nothing running.
    fn reading(time: f64, level: i32) -> Signals {
        Signals {
            time,
            wall: signals::wall(),
            segments: if level <= 40 { 0.65 } else { 0.1 },
            pages: 0.0,
            swap_used: 0,
            headroom: 500 << 30,
            ram: 96 << 30,
            level,
            pressure: Pressure::Normal,
        }
    }

    /// A broker on a socket of its own, in a state directory of its own.
    fn broker(name: &str, dry_run: bool) -> (Broker, PathBuf, PathBuf) {
        let dir = scratch(name);
        fs::create_dir_all(&dir).unwrap();
        let socket = dir.join("q.sock");
        let broker = Broker::start(Options {
            socket: socket.clone(),
            state_dir: dir.clone(),
            dry_run,
        })
        .unwrap();
        (broker, socket, dir)
    }

    /// A client that speaks the protocol by hand.
    struct Client {
        stream: UnixStream,
        input: Vec<u8>,
    }

    impl Client {
        fn connect(socket: &Path) -> Client {
            let stream = UnixStream::connect(socket).unwrap();
            stream
                .set_read_timeout(Some(Duration::from_millis(50)))
                .unwrap();
            Client {
                stream,
                input: Vec::new(),
            }
        }

        fn send(&mut self, text: &str) {
            self.stream.write_all(text.as_bytes()).unwrap();
        }

        fn request(&mut self, label: &str) {
            self.send(&format!("REQUEST\tv1\t{label}\tsig\n"));
        }

        /// The next line, if one comes within `wait`; None if the daemon hung up first.
        fn line(&mut self, wait: Duration) -> Option<String> {
            let deadline = Instant::now() + wait;
            loop {
                if let Some(line) = wire::take_line(&mut self.input) {
                    return Some(line);
                }
                let mut chunk = [0u8; 256];
                match self.stream.read(&mut chunk) {
                    Ok(0) => return None,
                    Ok(n) => self.input.extend_from_slice(&chunk[..n]),
                    Err(_) if Instant::now() < deadline => {}
                    Err(_) => return None,
                }
            }
        }

        fn next(&mut self) -> String {
            self.line(Duration::from_secs(10))
                .expect("the daemon answered")
        }

        /// True if the daemon has closed the connection.
        fn hung_up(&mut self) -> bool {
            let deadline = Instant::now() + Duration::from_secs(10);
            while Instant::now() < deadline {
                let mut chunk = [0u8; 16];
                if matches!(self.stream.read(&mut chunk), Ok(0)) {
                    return true;
                }
            }
            false
        }
    }

    /// `count` readings at `level`, a second apart; the notes they produced.
    fn tick_n(broker: &mut Broker, time: &mut f64, level: i32, count: u32) -> Vec<Note> {
        let mut notes = Vec::new();
        for _ in 0..count {
            *time += 1.0;
            notes.extend(broker.tick(&reading(*time, level), level <= 40, false));
        }
        notes
    }

    /// Readings at `level`, a second apart, until `done` says so; the notes they produced.
    fn tick_until(
        broker: &mut Broker,
        from: &mut f64,
        level: i32,
        mut done: impl FnMut() -> bool,
    ) -> Vec<Note> {
        let mut notes = Vec::new();
        let deadline = Instant::now() + Duration::from_secs(10);
        while !done() {
            assert!(Instant::now() < deadline, "the queue never got there");
            *from += 1.0;
            notes.extend(broker.tick(&reading(*from, level), level <= 40, false));
            thread::sleep(Duration::from_millis(20));
        }
        notes
    }

    /// A `sleep` in a group of its own, killed when dropped: a command that a client started.
    struct Sleeping(Child);

    impl Sleeping {
        fn spawn() -> Sleeping {
            Sleeping(
                Command::new("sleep")
                    .arg("300")
                    .process_group(0)
                    .spawn()
                    .unwrap(),
            )
        }

        fn pgid(&self) -> i32 {
            self.0.id() as i32
        }
    }

    impl Drop for Sleeping {
        fn drop(&mut self) {
            let _ = self.0.kill();
            let _ = self.0.wait();
        }
    }

    /// The client is told where it stands, and then to go, and only when memory allows. The
    /// daemon knows who it is from the kernel: the log names this process, however the client
    /// words its request.
    #[test]
    fn a_client_is_told_its_place_and_then_to_go_and_the_kernel_says_who_it_is() {
        let (mut broker, socket, dir) = broker("grant", false);
        let mut client = Client::connect(&socket);
        client.request("tests");
        assert_eq!(client.next(), format!("QUEUED\t1\t{}", 4u64 << 30));

        // Freeze-tier pressure holds the request.
        let mut time = 100.0;
        let notes = tick_n(&mut broker, &mut time, 35, 3);
        assert!(client.next().ends_with("\ttight"));
        assert!(client.line(Duration::from_millis(300)).is_none());
        let asked = notes
            .iter()
            .find(|n| n.1.contains("asks for a turn"))
            .unwrap();
        let me = format!("pid {}, uid {}", std::process::id(), unsafe {
            libc::getuid()
        });
        assert!(asked.1.contains(&me), "{}", asked.1);

        // Calm: it goes.
        let mut go = None;
        tick_until(&mut broker, &mut time, 80, || {
            go = client.line(Duration::from_millis(20));
            go.as_deref() == Some("GO")
        });
        assert_eq!(go.as_deref(), Some("GO"));
        let _ = fs::remove_dir_all(&dir);
    }

    /// Both clients get the go-ahead from the last calm sample, with no further tick. Their
    /// admission notes are queued for the event log through the same formatter as a tick.
    #[test]
    fn clients_that_fit_get_go_on_arrival_without_another_sample() {
        let (mut broker, socket, dir) = broker("arrival", false);
        give_test_signature_a_cpu_estimate(&broker);
        broker.tick(&reading(signals::uptime(), 80), false, false);
        assert_eq!(broker.scheduled(), 1);
        let mut first = Client::connect(&socket);
        first.request("first");
        assert_eq!(first.next(), "GO");
        let mut second = Client::connect(&socket);
        second.request("second");
        assert_eq!(second.next(), "GO");
        assert_eq!(
            broker.scheduled(),
            1,
            "neither request needed another sample"
        );
        let core = lock(&broker.core);
        assert_eq!(core.sched.running().len(), 2);
        for label in ["first", "second"] {
            assert_eq!(
                core.log
                    .iter()
                    .filter(|n| n.1.starts_with(&format!("admit {label} ")))
                    .count(),
                1
            );
        }
        drop(core);
        drop(broker);
        let _ = fs::remove_dir_all(&dir);
    }

    #[test]
    fn a_sample_admits_and_logs_every_client_that_fits() {
        let (mut broker, socket, dir) = broker("batch", false);
        give_test_signature_a_cpu_estimate(&broker);
        let mut first = Client::connect(&socket);
        first.request("first");
        assert!(first.next().starts_with("QUEUED\t1\t"));
        let mut second = Client::connect(&socket);
        second.request("second");
        assert!(second.next().ends_with("\tbehind"));
        let mut time = 0.0;
        let notes = tick_until(&mut broker, &mut time, 80, || {
            first.line(Duration::from_millis(20)).as_deref() == Some("GO")
        });
        assert_eq!(second.next(), "GO");
        for label in ["first", "second"] {
            assert_eq!(
                notes
                    .iter()
                    .filter(|n| n.1.starts_with(&format!("admit {label} ")))
                    .count(),
                1
            );
        }
        drop(broker);
        let _ = fs::remove_dir_all(&dir);
    }

    fn give_test_signature_a_cpu_estimate(broker: &Broker) {
        let mut core = lock(&broker.core);
        core.sched = Scheduler::with_cpu_budget(8000);
        Arc::make_mut(&mut core.sizes).record_usage("sig", crate::queue::GIB, Some(1000), None);
    }

    #[test]
    fn one_cold_request_per_signature_runs_until_done_teaches_its_siblings() {
        let (mut broker, socket, dir) = broker("cold-siblings", false);
        lock(&broker.core).sched = Scheduler::with_cpu_budget(24_000);
        let mut siblings: Vec<_> = (0..10)
            .map(|i| {
                let mut client = Client::connect(&socket);
                client.request(&format!("sibling-{i}"));
                assert!(client.next().starts_with("QUEUED\t"));
                client
            })
            .collect();
        let mut other = Client::connect(&socket);
        other.send("REQUEST\tv1\tother\tother-signature\n");
        assert!(other.next().starts_with("QUEUED\t"));

        broker.tick(&reading(signals::uptime(), 80), false, false);
        {
            let core = lock(&broker.core);
            let running: Vec<_> = core
                .sched
                .running()
                .iter()
                .map(|r| r.label.as_str())
                .collect();
            assert_eq!(running, ["sibling-0", "other"], "admissions before DONE");
        }
        assert_eq!(siblings[0].next(), "GO");
        assert_eq!(other.next(), "GO");
        assert!(siblings[1].next().ends_with("\tbehind\tmeasuring"));

        let peak = crate::queue::GIB;
        siblings[0].send(&format!("DONE\t0\t5\t1000000\t{peak}\n"));
        for client in &mut siblings[1..] {
            loop {
                let reply = client.next();
                if reply == "GO" {
                    break;
                }
                assert!(reply.starts_with("QUEUED\t"), "{reply}");
            }
        }
        {
            let core = lock(&broker.core);
            assert_eq!(core.sizes.get("sig"), Some(peak));
            assert!(core.sched.waiting().is_empty());
            let view = &mut Snapshot::default();
            core.sched.snapshot(&|sig| core.sizes.demand(sig), view);
            let learned: Vec<_> = view
                .running
                .iter()
                .filter(|r| r.label.starts_with("sibling-"))
                .collect();
            assert_eq!(learned.len(), 9);
            for sibling in learned {
                assert_eq!(
                    sibling.need,
                    peak + peak / 4,
                    "{} after DONE",
                    sibling.label
                );
            }
        }
        drop(broker);
        let _ = fs::remove_dir_all(&dir);
    }

    #[test]
    fn a_failed_cold_run_or_disconnect_without_usage_lets_the_next_sibling_measure() {
        let (mut broker, socket, dir) = broker("cold-retry", false);
        lock(&broker.core).sched = Scheduler::with_cpu_budget(8_000);
        let now = signals::uptime();
        broker.tick(&reading(now, 80), false, false);

        let mut first = Client::connect(&socket);
        first.request("first");
        assert_eq!(first.next(), "GO");
        let mut second = Client::connect(&socket);
        second.request("second");
        assert!(second.next().ends_with("\tbehind\tmeasuring"));
        let mut third = Client::connect(&socket);
        third.request("third");
        assert!(third.next().ends_with("\tbehind\tmeasuring"));

        {
            let mut core = lock(&broker.core);
            let id = core.sched.running()[0].id;
            core.sched.observe(id, crate::queue::GIB, now);
        }
        first.send("DONE\t1\n");
        assert_eq!(second.next(), "GO");
        assert!(third.next().ends_with("\tbehind\tmeasuring"));
        drop(second);
        assert_eq!(third.next(), "GO");

        let core = lock(&broker.core);
        assert!(!core.sizes.demand("sig").has_profile());
        assert_eq!(core.sched.running()[0].label, "third");
        drop(core);
        drop(broker);
        let _ = fs::remove_dir_all(&dir);
    }

    /// A connection that is closed gives up its place, at any point: the next client moves
    /// up, and the one that left never starts.
    #[test]
    fn a_client_that_goes_away_releases_its_place() {
        let (mut broker, socket, dir) = broker("leave", false);
        let (mut first, mut second) = (Client::connect(&socket), Client::connect(&socket));
        first.request("first");
        assert!(first.next().starts_with("QUEUED\t1\t"));
        second.request("second");
        assert!(second.next().starts_with("QUEUED\t2\t"));

        drop(first);
        assert!(second.next().starts_with("QUEUED\t1\t"), "moved up");
        let mut time = 200.0;
        let notes = tick_until(&mut broker, &mut time, 80, || {
            second.line(Duration::from_millis(20)).as_deref() == Some("GO")
        });
        let said: Vec<&str> = notes.iter().map(|n| n.1.as_str()).collect();
        assert!(
            said.iter().any(|t| t.contains("first left the queue")),
            "{said:?}"
        );
        assert!(!said.iter().any(|t| t.contains("admit first")), "{said:?}");
        let _ = fs::remove_dir_all(&dir);
    }

    /// Whatever a client sends, it gets an error and a closed connection, never more than its
    /// own line of trouble, and the next client is served as usual.
    #[test]
    fn protocol_violations_get_an_error_and_do_not_disturb_the_next_client() {
        let (_broker, socket, dir) = broker("violations", false);
        let long = format!("REQUEST\tv1\t{}\n", "x".repeat(2 * wire::MAX_LINE));
        for (sent, expect) in [
            ("REQUEST\tv2\tl\ts\n", "ERROR\tunsupported protocol version"),
            ("HELLO\n", "ERROR\tunknown message"),
            ("DONE\t0\n", "ERROR\tdone before a request"),
            ("STARTED\t4242\n", "ERROR\tstarted before a request"),
            ("REQUEST\tv1\tl\n", "ERROR\tmalformed message"),
            (long.as_str(), "ERROR\tline too long"),
        ] {
            let mut client = Client::connect(&socket);
            client.send(sent);
            assert!(client.next().starts_with(expect), "{sent:?}");
        }
        // A second request on one connection is refused, and the connection closed.
        let mut twice = Client::connect(&socket);
        twice.send("REQUEST\tv1\ta\ts\nREQUEST\tv1\tb\ts\n");
        assert_eq!(twice.next(), "ERROR\ta connection asks once");
        assert!(twice.hung_up());

        let mut good = Client::connect(&socket);
        good.request("fine");
        assert!(good.next().starts_with("QUEUED\t"));
        let _ = fs::remove_dir_all(&dir);
    }

    /// Waits for the status file to say something `done` is satisfied with. The file is
    /// written by a thread of its own, so it trails the tick that decided it.
    fn status_until(
        broker: &mut Broker,
        time: &mut f64,
        dir: &Path,
        level: i32,
        done: impl Fn(&QueueFile) -> bool,
    ) -> QueueFile {
        let deadline = Instant::now() + Duration::from_secs(10);
        loop {
            *time += 1.0;
            broker.tick(&reading(*time, level), level <= 40, false);
            if let Ok(Some(file)) = read_queue(dir)
                && done(&file)
            {
                return file;
            }
            assert!(Instant::now() < deadline, "the status file never said it");
            thread::sleep(Duration::from_millis(20));
        }
    }

    /// The status file shows who waits and who runs, with strings made safe, to everybody; and
    /// it is gone when the broker is.
    #[test]
    fn the_status_file_lists_the_queue_and_goes_with_the_broker() {
        let (mut broker, socket, dir) = broker("status", false);
        let mut client = Client::connect(&socket);
        client.send("REQUEST\tv1\tbuild \x1b[31mred\tcargo build\n");
        client.next();
        let mut time = 300.0;
        let file = status_until(&mut broker, &mut time, &dir, 35, |f| f.waiting.len() == 1);
        assert_eq!(file.daemon as u32, std::process::id());
        assert_eq!((file.waiting.len(), file.running.len()), (1, 0));
        assert_eq!(file.waiting[0].label, "build ?[31mred");
        assert_eq!(
            (file.waiting[0].position, file.waiting[0].need),
            (1, (file.cap / 8).max(4 << 30))
        );
        assert_eq!(file.ram, 96 << 30);
        assert_eq!(
            file.cap,
            (96u64 << 30) * 65 / 100,
            "learned from the tight reading"
        );
        let mode = fs::metadata(dir.join(QUEUE_FILE))
            .unwrap()
            .permissions()
            .mode()
            & 0o777;
        assert_eq!(mode, 0o644);

        tick_until(&mut broker, &mut time, 80, || {
            client.line(Duration::from_millis(20)).as_deref() == Some("GO")
        });
        let command = Sleeping::spawn();
        client.send(&format!("STARTED\t{}\n", command.pgid()));
        let file = status_until(&mut broker, &mut time, &dir, 80, |f| {
            f.running.first().is_some_and(|r| r.pgid == command.pgid())
        });
        assert_eq!((file.waiting.len(), file.running.len()), (0, 1));

        drop(broker);
        assert!(!dir.join(QUEUE_FILE).exists());
        assert!(!socket.exists());
        let _ = fs::remove_dir_all(&dir);
    }

    #[test]
    fn low_swap_headroom_holds_the_idle_queue_and_stalled_jobs_warn_and_appear_in_status() {
        let (mut broker, _socket, dir) = broker("idle-stall", false);
        lock(&broker.core).sched.enqueue(
            123,
            "build".into(),
            "build".into(),
            signals::wall() - 11.0 * 60.0,
        );
        let mut sample = reading(100.0, 35);
        sample.segments = 0.1;
        sample.headroom = sample.ram - 1;
        let notes = broker.tick(&sample, true, false);
        assert!(
            notes.iter().any(|(kind, text)| {
                *kind == "warn"
                    && text == "build (session 123) stalled after 11m waiting with nothing running"
            }),
            "{notes:?}"
        );
        let mut text = String::new();
        render(&broker.view, &mut text);
        let status = QueueFile::parse(&text).unwrap();
        assert!(status.running.is_empty());
        assert_eq!(
            status.waiting[0].reason,
            "stalled: memory is tight, nothing running"
        );

        sample.time += 1.0;
        assert!(
            broker
                .tick(&sample, true, false)
                .iter()
                .all(|(kind, _)| *kind != "warn")
        );
        sample.time += 1.0;
        sample.headroom = sample.ram;
        let notes = broker.tick(&sample, true, false);
        assert!(
            notes
                .iter()
                .any(|(kind, text)| *kind == "queue" && text.starts_with("admit build")),
            "{notes:?}"
        );
        drop(broker);
        let _ = fs::remove_dir_all(&dir);
    }

    /// A dry run holds nobody and writes nothing, but says what the queue would have done.
    #[test]
    fn a_dry_run_admits_at_once_and_writes_no_file() {
        let (mut broker, socket, dir) = broker("dry", true);
        let mut client = Client::connect(&socket);
        client.request("tests");
        assert_eq!(client.next(), "GO");
        let mut time = 400.0;
        let notes = tick_n(&mut broker, &mut time, 80, 5);
        assert!(
            notes
                .iter()
                .any(|n| n.1.starts_with("dry run, would admit tests")),
            "{notes:?}"
        );
        assert!(!dir.join(QUEUE_FILE).exists());
        drop(client);
        let _ = fs::remove_dir_all(&dir);
    }

    /// A socket file that nothing listens on, as a daemon that died leaves one. Observed: in
    /// a parallel run of the whole suite (once in four runs, never when this test ran alone)
    /// a connection to such a file succeeded, as if the dropped listener were still open.
    /// Hypothesis, not shown: macOS makes a new descriptor close-on-exec a moment after
    /// creating it, and a test thread that forks in that moment carries it into a child. So
    /// the file is made again until it really refuses connections.
    fn stale_socket(path: &Path) {
        for _ in 0..50 {
            drop(UnixListener::bind(path).unwrap());
            let refused = UnixStream::connect(path)
                .is_err_and(|e| e.kind() == io::ErrorKind::ConnectionRefused);
            if refused {
                return;
            }
            fs::remove_file(path).unwrap();
            thread::sleep(Duration::from_millis(20));
        }
        panic!("could not make a socket that refuses connections");
    }

    /// The socket is open to every user; one a dead daemon left behind is replaced, and
    /// neither a live daemon's nor anything that is not a socket is touched.
    #[test]
    fn binding_replaces_a_stale_socket_and_refuses_anything_else() {
        let dir = scratch("bind");
        fs::create_dir_all(&dir).unwrap();
        let options = |socket: &Path| Options {
            socket: socket.to_path_buf(),
            state_dir: dir.clone(),
            dry_run: true,
        };

        let stale = dir.join("stale.sock");
        stale_socket(&stale);
        assert!(stale.exists());
        let broker = Broker::start(options(&stale)).unwrap();
        let mode = fs::metadata(&stale).unwrap().permissions().mode() & 0o777;
        assert_eq!(mode, 0o666, "every local user may connect");

        let Err(taken) = Broker::start(options(&stale)) else {
            panic!("a live socket was taken over");
        };
        assert!(taken.contains("served by another process"), "{taken}");
        drop(broker);

        let file = dir.join("file.sock");
        fs::write(&file, "mine").unwrap();
        let Err(not_a_socket) = Broker::start(options(&file)) else {
            panic!("a file was replaced");
        };
        assert!(not_a_socket.contains("not a free socket"), "{not_a_socket}");
        assert_eq!(fs::read_to_string(&file).unwrap(), "mine");
        let _ = fs::remove_dir_all(&dir);
    }

    // -----------------------------------------------------------------------------------
    // STARTED names a group the daemon will measure on the sampling thread, and records the
    // footprint under the client's signature: it must be the client's own command.

    /// Asks for a turn, for a command of this signature, on a fresh connection and waits for
    /// the go-ahead.
    fn go(broker: &mut Broker, time: &mut f64, socket: &Path, signature: &str) -> Client {
        let mut client = Client::connect(socket);
        client.send(&format!("REQUEST\tv1\ttests\t{signature}\n"));
        tick_until(broker, time, 80, || {
            client.line(Duration::from_millis(20)).as_deref() == Some("GO")
        });
        client
    }

    #[test]
    fn a_completion_is_learned_even_if_the_child_was_reaped_before_started_arrived() {
        let (mut broker, socket, dir) = broker("reaped-before-started", false);
        let mut time = 0.0;
        let mut client = go(&mut broker, &mut time, &socket, "short command");
        let mut child = Command::new("/usr/bin/true")
            .process_group(0)
            .spawn()
            .unwrap();
        let pgid = child.id();
        assert!(child.wait().unwrap().success());
        client.send(&format!("STARTED\t{pgid}\nDONE\t0\t10\t8000000\t4096\n"));
        assert!(client.hung_up());
        let demand = lock(&broker.core).sizes.demand("short command");
        assert_eq!(
            (demand.bytes, demand.millicores, demand.runtime_ms),
            (Some(4096), Some(800), Some(10))
        );
        let _ = fs::remove_dir_all(&dir);
    }

    /// The group of a command the client started is accepted, once. A group that is not a
    /// command of this client's is an error and a hung-up connection, and nothing is learned
    /// about it, whichever way it fails to be one: it started before the go-ahead, it is a
    /// member and not the leader of its group, or it is not this client's child (a group that
    /// belongs to somebody else, which the client could otherwise make the daemon measure).
    #[test]
    fn started_must_name_the_group_of_a_command_the_client_started_after_the_go() {
        let (mut broker, socket, dir) = broker("started", false);
        let mut time = 500.0;
        // Each is made, or named, after the go-ahead, bar the first: so that each fails for
        // one reason only.
        let early = Sleeping::spawn(); // a child of this client, but older than any go
        let rejected =
            |broker: &mut Broker, time: &mut f64, why: &str, pgid: &mut dyn FnMut() -> i32| {
                let mut client = go(broker, time, &socket, "lie");
                let pgid = pgid();
                client.send(&format!("STARTED\t{pgid}\n"));
                assert_eq!(
                    client.next(),
                    "ERROR\tthat is not the group of the command you were let go",
                    "{why}"
                );
                assert!(client.hung_up(), "{why}");
            };
        rejected(
            &mut broker,
            &mut time,
            "it started before the go-ahead",
            &mut || early.pgid(),
        );
        let (mut leader, mut member) = (None, None);
        rejected(
            &mut broker,
            &mut time,
            "a member, not the leader of its group",
            &mut || {
                let led = Sleeping::spawn();
                let joined = Sleeping(
                    Command::new("sleep")
                        .arg("300")
                        .process_group(led.pgid())
                        .spawn()
                        .unwrap(),
                );
                let pid = joined.0.id() as i32;
                (leader, member) = (Some(led), Some(joined));
                pid
            },
        );
        let mut stranger = None;
        rejected(
            &mut broker,
            &mut time,
            "nobody's child of this client",
            &mut || {
                let orphan = orphan_group(&format!("3600.{}", std::process::id()));
                let pid = orphan.pid;
                stranger = Some(orphan);
                pid
            },
        );
        rejected(&mut broker, &mut time, "launchd", &mut || 1);
        rejected(&mut broker, &mut time, "no group", &mut || 0);
        drop((leader, member, stranger));

        // The command of this client is accepted, once.
        let mut client = go(&mut broker, &mut time, &socket, "real");
        let command = Sleeping::spawn();
        client.send(&format!("STARTED\t{}\n", command.pgid()));
        let file = status_until(&mut broker, &mut time, &dir, 80, |f| {
            f.running
                .first()
                .is_some_and(|r| r.pgid == command.pgid() && r.peak > 0)
        });
        assert_eq!(file.running.len(), 1, "only the one that was accepted");
        client.send(&format!("STARTED\t{}\n", command.pgid()));
        assert_eq!(client.next(), "ERROR\ta connection starts once");
        // What the accepted command used is learned; about the groups that were refused,
        // nothing is.
        let core = Arc::clone(&broker.core);
        tick_until(&mut broker, &mut time, 80, || {
            lock(&core).sizes.get("real").is_some()
        });
        assert!(lock(&core).sizes.get("lie").is_none());
        let _ = fs::remove_dir_all(&dir);
    }

    /// A command that is accepted and measured teaches the queue its size; a client that
    /// named a group that was not its own does not.
    #[test]
    fn a_command_that_finishes_is_learned_and_one_that_lied_is_not() {
        let (mut broker, socket, dir) = broker("learn", false);
        let mut time = 600.0;
        let mut honest = go(&mut broker, &mut time, &socket, "sig");
        let command = Sleeping::spawn();
        honest.send(&format!("STARTED\t{}\n", command.pgid()));
        status_until(&mut broker, &mut time, &dir, 80, |f| {
            f.running.first().is_some_and(|r| r.pgid > 1 && r.peak > 0)
        });
        let core = Arc::clone(&broker.core);
        tick_until(&mut broker, &mut time, 80, || {
            lock(&core).sched.running()[0].measured_cpu().is_some()
        });
        honest.send("DONE\t0\n");
        tick_until(&mut broker, &mut time, 80, || {
            let demand = lock(&core).sizes.demand("sig");
            demand.bytes.is_some() && demand.millicores.is_some()
        });

        let mut liar = Client::connect(&socket);
        liar.send("REQUEST\tv1\tliar\tlie\n");
        tick_until(&mut broker, &mut time, 80, || {
            liar.line(Duration::from_millis(20)).as_deref() == Some("GO")
        });
        liar.send(&format!("STARTED\t{}\n", std::process::id()));
        assert!(liar.next().starts_with("ERROR"));
        tick_n(&mut broker, &mut time, 80, 3);
        assert!(lock(&core).sizes.get("lie").is_none());
        let _ = fs::remove_dir_all(&dir);
    }

    /// The client is the process that connected, and not whoever has its pid by the time it
    /// says STARTED: the connection can outlive the process (inherited, or passed on) and the
    /// pid can be reissued, and a child of the process that has it then is not the client's
    /// command. A pid cannot be made to be reissued on demand, so the connection here carries
    /// a start time that is not the one this process has, as if the pid had gone to another
    /// process since. This shows the comparison of the start time noted at accept with the
    /// pid's start time now, and that `STARTED` is refused when they differ; it is not a real
    /// reissue of a pid. The command is a real child of this process, in a group of its own,
    /// that started after the go-ahead, and what the same connection says with the start time
    /// the kernel gave is accepted.
    #[test]
    fn started_is_refused_when_the_pids_start_time_is_not_the_one_noted_at_accept() {
        let dir = scratch("peer-lifetime");
        fs::create_dir_all(&dir).unwrap();
        let socket = dir.join("q.sock");
        let listener = bind(&socket).unwrap();
        let _client = UnixStream::connect(&socket).unwrap();
        let accept = || loop {
            match listener.accept() {
                Ok((stream, _)) => return stream,
                Err(e) if e.kind() == io::ErrorKind::WouldBlock => {
                    thread::sleep(Duration::from_millis(10))
                }
                Err(e) => panic!("{e}"),
            }
        };
        let stream = accept();
        let kernel_says = Peer::of(&stream).unwrap();
        let me = std::process::id() as i32;
        assert_eq!(kernel_says.pid, me);
        assert!(kernel_says.is_alive());
        assert!(kernel_says.start_us > 0);

        let core = test_core();
        let id = {
            let mut core = lock(&core);
            let id = core
                .sched
                .enqueue(1, "tests".into(), "sig".into(), signals::wall());
            let sample = Sample {
                time: 1.0,
                wall: signals::wall(),
                ram: 96 << 30,
                level: 80,
                occupancy: 0.0,
                tight: false,
                frozen: false,
            };
            core.sched.step(
                &sample,
                &|_| crate::sizes::Demand::default(),
                &mut Vec::new(),
            );
            assert_eq!(core.sched.running().len(), 1, "admitted");
            id
        };
        let go_at = signals::wall() - 1.0;
        let command = Sleeping::spawn();
        let ctx = Ctx {
            core: &core,
            sampling: &AtomicBool::new(false),
            agent: None,
            files: None,
            dry_run: false,
            keep_alive: wire::KEEP_ALIVE,
            stalled_after: wire::SILENCE,
        };
        let conn = |start_us: u64| Conn {
            stream: stream.try_clone().unwrap(),
            peer: Peer {
                pid: kernel_says.pid,
                start_us,
                uid: kernel_says.uid,
            },
            input: Vec::new(),
            output: Vec::new(),
            id: Some(id),
            told: None,
            told_at: Instant::now(),
            go_at: Some(go_at),
            started: false,
            closing: false,
            dead: false,
        };
        let measured = || lock(&core).sched.running()[0].pgid;

        let mut stale = conn(kernel_says.start_us + 1);
        ctx.started(&mut stale, id, command.pgid());
        assert!(stale.closing && !stale.started);
        assert!(
            String::from_utf8_lossy(&stale.output).starts_with("ERROR\t"),
            "{:?}",
            String::from_utf8_lossy(&stale.output)
        );
        assert_eq!(measured(), None, "nothing is measured for it");

        let mut real = conn(kernel_says.start_us);
        ctx.started(&mut real, id, command.pgid());
        assert!(real.started && !real.closing);
        assert_eq!(measured(), Some(command.pgid()));
        let _ = fs::remove_dir_all(&dir);
    }

    // -----------------------------------------------------------------------------------
    // Reservation identity must be refreshed before an emergency uses a sample.
    #[test]
    fn a_contended_sample_uses_the_newly_verified_victim_reservations() {
        use crate::engine::{Action, Engine, Policy};
        use crate::jobs::Job;
        use crate::queue::GIB;

        let (mut broker, _socket, dir) = broker("held", false);
        let core = Arc::clone(&broker.core);
        let mut guard = lock(&core);
        let runaway = guard.sched.enqueue(1, "build".into(), "build".into(), 0.0);
        let healthy = guard.sched.enqueue(2, "lint".into(), "lint".into(), 0.0);
        guard.sched.step(
            &Sample {
                time: 1.0,
                wall: 1.0,
                ram: 96 * GIB,
                level: 80,
                occupancy: 0.0,
                tight: false,
                frozen: false,
            },
            &|sig| crate::sizes::Demand {
                bytes: Some(if sig == "build" { 4 * GIB } else { 12 * GIB }),
                ..crate::sizes::Demand::default()
            },
            &mut Vec::new(),
        );
        let build = Sleeping::spawn();
        let lint = Sleeping::spawn();
        let identity = |pid| jobs::bsd_info(pid).unwrap().start_us;
        let mut candidates = Vec::new();
        for (id, pgid, bytes) in [
            (runaway, build.pgid(), 6 * GIB),
            (healthy, lint.pgid(), 8 * GIB),
        ] {
            let start_us = identity(pgid);
            assert!(guard.sched.started(id, pgid, start_us));
            candidates.push(Job {
                pgid,
                leader_pid: pgid,
                start_us,
                uid: 501,
                bytes,
                procs: 1,
                prefer: false,
                protected: false,
                stopped: false,
                command: String::new(),
            });
        }
        let (started, ready) = mpsc::channel();
        let (returned, answer) = mpsc::channel();
        let sampler = thread::spawn(move || {
            started.send(()).unwrap();
            broker.measure(2.0);
            let mut e = Engine::new(Policy::default());
            let mut s = reading(2.0, 35);
            s.segments = 0.93;
            let step = e.step_with_candidates(&s, || (candidates, broker.refresh_reservations()));
            returned.send(step.actions).unwrap();
        });
        ready.recv().unwrap();
        let early = answer.recv_timeout(Duration::from_millis(100));
        drop(guard);
        let actions =
            early.unwrap_or_else(|_| answer.recv_timeout(Duration::from_secs(5)).unwrap());
        sampler.join().unwrap();
        assert!(
            matches!(actions.as_slice(), [Action::Kill { job, .. }] if job.pgid == build.pgid()),
            "{actions:?}"
        );
        let _ = fs::remove_dir_all(&dir);
    }

    /// The silence limit, as this process has it, and a moment more.
    fn past_the_silence() -> Duration {
        wire::timing().silence + Duration::from_secs(1)
    }

    #[test]
    fn stalled_sampling_keeps_waiting_clients_in_order_until_readings_return() {
        let (mut broker, socket, dir) = broker("stalled", false);
        let core = Arc::clone(&broker.core);
        let mut time = 1000.0;
        tick_n(&mut broker, &mut time, 35, 3);
        let mut old = Client::connect(&socket);
        old.request("first");
        assert!(old.next().starts_with("QUEUED\t1\t"));
        lock(&core).scheduled_at = Instant::now().checked_sub(past_the_silence()).unwrap();
        let mut late = Client::connect(&socket);
        late.request("second");
        assert!(late.next().starts_with("QUEUED\t2\t"));
        tick_until(&mut broker, &mut time, 80, || {
            old.line(Duration::from_millis(50)).as_deref() == Some("GO")
        });
        old.send("DONE\t0\n");
        tick_until(&mut broker, &mut time, 80, || {
            late.line(Duration::from_millis(50)).as_deref() == Some("GO")
        });
        let _ = fs::remove_dir_all(&dir);
    }

    #[test]
    fn review3_socket_backfill_uses_elapsed_time_since_the_sample() {
        let (broker, socket, dir) = broker("socket-clock", false);
        {
            let mut core = lock(&broker.core);
            core.sched = Scheduler::with_cpu_budget(4000);
            let sizes = Arc::make_mut(&mut core.sizes);
            for (name, gib, cpu, runtime) in [
                ("watch", 8, 2000, None),
                ("blocker", 16, 1000, Some(10_000)),
                ("head", 24, 2000, None),
                ("late", 1, 1000, Some(500)),
            ] {
                sizes.record_usage(name, gib << 30, Some(cpu), runtime);
            }
            core.sched.enqueue(1, "watch".into(), "watch".into(), 1.0);
            core.sched
                .enqueue(1, "blocker".into(), "blocker".into(), 1.0);
            let sizes = Arc::clone(&core.sizes);
            core.sched.step(
                &Sample {
                    time: 10.0,
                    wall: 1.0,
                    ram: 96 << 30,
                    level: 80,
                    occupancy: 0.0,
                    tight: false,
                    frozen: false,
                },
                &|name| sizes.demand(name),
                &mut Vec::new(),
            );
            core.sched.enqueue(1, "head".into(), "head".into(), 1.0);
            // At 19.8, a 500 ms job cannot finish by the head's promised 20.0 start.
            core.scheduled_at = Instant::now()
                .checked_sub(Duration::from_millis(9800))
                .unwrap();
        }
        let mut client = Client::connect(&socket);
        client.send("REQUEST\tv1\tlate\tlate\n");
        assert!(client.next().starts_with("QUEUED\t2\t"));
        let _ = fs::remove_dir_all(&dir);
    }

    /// A dry run holds nobody: its clients have their go-ahead at once and are never sent
    /// away, whatever the sampling thread is doing.
    #[test]
    fn a_dry_run_never_sends_a_client_away_for_a_stalled_sampler() {
        let (broker, socket, dir) = broker("stalled-dry", true);
        lock(&broker.core).scheduled_at = Instant::now().checked_sub(past_the_silence()).unwrap();
        let mut client = Client::connect(&socket);
        client.request("tests");
        assert_eq!(client.next(), "GO");
        assert!(client.line(Duration::from_millis(500)).is_none());
        let _ = fs::remove_dir_all(&dir);
    }

    // -----------------------------------------------------------------------------------
    // Neither thread touches a file: a file that blocks holds up only its own writes.

    /// `queue.tsv` is a path a writer waits at for a reader that never comes. The tick still
    /// returns at once, sample after sample, and clients are still told to go.
    #[test]
    fn a_status_file_that_blocks_does_not_stop_the_tick_or_the_clients() {
        let (mut broker, socket, dir) = broker("stuck-status", false);
        mkfifo(&dir.join(format!("{QUEUE_FILE}.tmp")));
        let mut client = Client::connect(&socket);
        client.request("tests");
        assert!(client.next().starts_with("QUEUED"));

        let started = Instant::now();
        let mut time = 800.0;
        // Every tick has a changed status to hand over (the memory in use moves), and the
        // thread that writes it is stuck on the first.
        for i in 0..100 {
            time += 1.0;
            let level = 30 + i % 7;
            broker.tick(&reading(time, level), true, false);
        }
        assert!(
            started.elapsed() < Duration::from_secs(3),
            "100 ticks took {:?} with the status file blocked",
            started.elapsed()
        );
        let mut go = None;
        tick_until(&mut broker, &mut time, 80, || {
            go = client.line(Duration::from_millis(20));
            go.is_some()
        });
        assert_eq!(go.as_deref(), Some("GO"));
        let started = Instant::now();
        drop(broker);
        assert!(
            started.elapsed() < Duration::from_secs(3),
            "shutdown waited"
        );
        let _ = fs::remove_dir_all(&dir);
    }

    /// `sizes.tsv` blocks. The command that just finished wanted it written, from the socket
    /// thread's connection; the next client is told to go all the same.
    #[test]
    fn a_sizes_file_that_blocks_does_not_stop_the_next_client() {
        let (mut broker, socket, dir) = broker("stuck-sizes", false);
        mkfifo(&dir.join(format!("{SIZES_FILE}.tmp")));
        let mut time = 900.0;
        let mut first = go(&mut broker, &mut time, &socket, "sig");
        let command = Sleeping::spawn();
        first.send(&format!("STARTED\t{}\n", command.pgid()));
        status_until(&mut broker, &mut time, &dir, 80, |f| {
            f.running.first().is_some_and(|r| r.peak > 0)
        });
        first.send("DONE\t0\n");
        let core = Arc::clone(&broker.core);
        tick_until(&mut broker, &mut time, 80, || {
            lock(&core).sizes.get("sig").is_some()
        });

        let mut second = go(&mut broker, &mut time, &socket, "sig");
        second.send("DONE\t0\n");
        assert!(second.hung_up(), "and the socket thread still answers");
        let _ = fs::remove_dir_all(&dir);
    }

    // -----------------------------------------------------------------------------------
    // A write that fails is written again.

    /// Waits for the file at `path` to have `text`.
    fn file_says(path: &Path, text: &str) {
        let deadline = Instant::now() + Duration::from_secs(10);
        while fs::read_to_string(path).ok().as_deref() != Some(text) {
            assert!(
                Instant::now() < deadline,
                "{} never said it",
                path.display()
            );
            thread::sleep(Duration::from_millis(20));
        }
    }

    /// The disk refuses the status file and the sizes file, and then takes them again, with
    /// nothing new offered: the latest snapshot of each, which the thread kept, is written.
    /// The newest snapshot is the one written, not an older one that was waiting, and the
    /// failure is said once, not at every attempt.
    #[test]
    fn a_snapshot_that_could_not_be_written_is_written_when_the_disk_recovers() {
        let dir = scratch("rewrite");
        fs::create_dir_all(&dir).unwrap();
        let core = Arc::new(test_core());
        let files = spawn_files(&dir, &core, Duration::ZERO).unwrap();

        fs::set_permissions(&dir, fs::Permissions::from_mode(0o555)).unwrap();
        let mut table = Sizes::default();
        table.record("tsc web", 3 << 30);
        for text in ["queue\tv1\nold\n", "queue\tv1\nnewer\n"] {
            assert!(files.offer(FileWork::Status(text.into())).is_ok());
        }
        assert!(
            files
                .offer(FileWork::Sizes(Arc::new(table.clone())))
                .is_ok()
        );
        // Everything offered has been tried (and refused).
        let (done, answered) = mpsc::channel();
        files.try_send(FileWork::Done(done)).unwrap();
        answered.recv_timeout(Duration::from_secs(10)).unwrap();
        assert!(!dir.join(QUEUE_FILE).exists() && !dir.join(SIZES_FILE).exists());

        fs::set_permissions(&dir, fs::Permissions::from_mode(0o755)).unwrap();
        file_says(&dir.join(QUEUE_FILE), "queue\tv1\nnewer\n");
        file_says(&dir.join(SIZES_FILE), &table.render());
        let said = lock(&core).log.clone();
        assert_eq!(said.len(), 2, "once for each file: {said:?}");
        let _ = fs::remove_dir_all(&dir);
    }

    /// The broker going takes the status file with it, and a snapshot that never got written
    /// is not written after that.
    #[test]
    fn a_status_snapshot_that_never_got_written_is_not_written_after_the_broker_has_gone() {
        let dir = scratch("rewrite-removed");
        fs::create_dir_all(&dir).unwrap();
        let core = Arc::new(test_core());
        let files = spawn_files(&dir, &core, Duration::ZERO).unwrap();
        fs::set_permissions(&dir, fs::Permissions::from_mode(0o555)).unwrap();
        assert!(
            files
                .offer(FileWork::Status("queue\tv1\nlate\n".into()))
                .is_ok()
        );
        assert!(files.offer(FileWork::Remove).is_ok());
        let (done, answered) = mpsc::channel();
        files.try_send(FileWork::Done(done)).unwrap();
        answered.recv_timeout(Duration::from_secs(10)).unwrap();

        fs::set_permissions(&dir, fs::Permissions::from_mode(0o755)).unwrap();
        // The disk is back. A snapshot of the other file makes the thread try everything it
        // still has to write, and it says when it is done: the sizes are written, and had it
        // kept the status snapshot, that would be too.
        let mut table = Sizes::default();
        table.record("tsc web", 3 << 30);
        assert!(
            files
                .offer(FileWork::Sizes(Arc::new(table.clone())))
                .is_ok()
        );
        let (done, answered) = mpsc::channel();
        files.try_send(FileWork::Done(done)).unwrap();
        answered.recv_timeout(Duration::from_secs(10)).unwrap();
        assert_eq!(
            fs::read_to_string(dir.join(SIZES_FILE)).unwrap(),
            table.render(),
            "the thread did try to write"
        );
        assert!(!dir.join(QUEUE_FILE).exists());
        let _ = fs::remove_dir_all(&dir);
    }

    // -----------------------------------------------------------------------------------
    // Bounded work on the socket thread.

    fn test_core() -> Mutex<Core> {
        Mutex::new(Core {
            sched: Scheduler::new(),
            sizes: Arc::new(Sizes::default()),
            log: Vec::new(),
            lost_notes: 0,
            dry_run: false,
            scheduled_at: Instant::now(),
        })
    }

    /// Connections are taken a few at a time, whatever is waiting, so that the clients
    /// already served are looked after between the batches.
    #[test]
    fn a_pass_of_the_socket_loop_takes_only_so_many_connections() {
        let dir = scratch("budget");
        fs::create_dir_all(&dir).unwrap();
        let socket = dir.join("q.sock");
        let listener = bind(&socket).unwrap();
        let waiting: Vec<UnixStream> = (0..40)
            .map(|_| UnixStream::connect(&socket).unwrap())
            .collect();
        let (core, mut conns, mut failed) = (test_core(), Vec::new(), false);
        accept_some(&listener, &mut conns, &core, &mut failed);
        assert_eq!(conns.len(), ACCEPTS_PER_PASS);
        accept_some(&listener, &mut conns, &core, &mut failed);
        assert_eq!(conns.len(), 2 * ACCEPTS_PER_PASS);
        accept_some(&listener, &mut conns, &core, &mut failed);
        assert_eq!(conns.len(), 40, "and the rest on the third");
        drop(waiting);
        let _ = fs::remove_dir_all(&dir);
    }

    /// A connection beyond the former 64-client limit is accepted like the others.
    #[test]
    fn connections_past_the_old_limit_are_accepted() {
        let dir = scratch("limit");
        fs::create_dir_all(&dir).unwrap();
        let socket = dir.join("q.sock");
        let listener = bind(&socket).unwrap();
        let (core, mut conns, mut failed) = (test_core(), Vec::new(), false);
        let held: Vec<UnixStream> = (0..65)
            .map(|_| UnixStream::connect(&socket).unwrap())
            .collect();
        for _ in 0..5 {
            accept_some(&listener, &mut conns, &core, &mut failed);
        }
        assert_eq!(conns.len(), 65);
        drop(held);
        let _ = fs::remove_dir_all(&dir);
    }

    /// Arrivals that never stop do not stop the client that is already waiting from being
    /// told to go: it is served between the batches of arrivals.
    #[test]
    fn a_flood_of_connections_does_not_keep_the_queue_from_its_client() {
        let (mut broker, socket, dir) = broker("flood", false);
        let mut client = Client::connect(&socket);
        client.request("tests");
        assert!(client.next().starts_with("QUEUED"));

        let flooding = {
            let socket = socket.clone();
            thread::spawn(move || {
                // Past the limit many times over; each is held open, as a stuck client is.
                (0..400)
                    .filter_map(|_| UnixStream::connect(&socket).ok())
                    .collect::<Vec<_>>()
            })
        };
        let mut time = 1000.0;
        let mut go = None;
        tick_until(&mut broker, &mut time, 80, || {
            go = client.line(Duration::from_millis(20));
            go.is_some()
        });
        assert_eq!(go.as_deref(), Some("GO"));
        let flood = flooding.join().unwrap();
        assert!(
            flood.len() > 64,
            "test must exercise the previous connection limit"
        );
        let _ = fs::remove_dir_all(&dir);
    }

    /// A burst of 2,200 simultaneous launches loses none of its notes in one tick (#31).
    #[test]
    fn a_burst_of_launches_keeps_every_note() {
        let core = test_core();
        let mut guard = lock(&core);
        for i in 0..2_200 {
            guard.note("queue", format!("burst-{i} asks for a turn"));
        }
        let mut notes = Vec::new();
        assert_eq!(guard.take_notes(&mut notes), 0, "nothing dropped");
        assert_eq!(notes.len(), 2_200);
    }

    /// Notes that pile up while the sampling thread is busy are bounded, and the ones dropped
    /// are counted and said.
    #[test]
    fn pending_notes_are_bounded_and_the_dropped_ones_counted() {
        let core = test_core();
        let mut guard = lock(&core);
        for i in 0..10 * MAX_NOTES {
            guard.note("queue", format!("note {i}"));
        }
        assert_eq!(guard.log.len(), MAX_NOTES);
        let mut notes = Vec::new();
        let lost = guard.take_notes(&mut notes);
        assert_eq!(notes.len(), MAX_NOTES);
        assert_eq!(notes[0].1, "note 0", "the first ones are kept");
        assert_eq!(lost, 9 * MAX_NOTES as u64, "the rest are counted");
        let said = lost_note(lost);
        assert_eq!(said.0, "warn");
        assert!(
            said.1
                .starts_with(&format!("{} queue note(s)", 9 * MAX_NOTES)),
            "{}",
            said.1
        );
        let mut again = Vec::new();
        assert_eq!(guard.take_notes(&mut again), 0, "counted once");
        assert!(again.is_empty());
    }
}
