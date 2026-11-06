//! The daemon's side of the queue: the Unix socket, the clients on it, and the two files that
//! tell the world what the queue is doing and has learned.
//!
//! Two threads share one `Core` behind a mutex that is only ever held for in-memory work,
//! never for I/O. The socket thread owns every connection: it accepts, reads what clients say,
//! queues their requests, and tells each client where it stands. The sampling thread (the
//! runner's tick) owns the clock: once a sample it asks the scheduler whether the next request
//! may start, measures the groups of the admitted commands, and rewrites `queue.tsv`. A
//! client waiting for its turn therefore never delays a sample, and a sample never waits for a
//! client.
//!
//! Clients are identified by the kernel, never by what they claim: the peer pid and uid come
//! from the socket itself, and the session is worked out here from the peer pid.
//!
//! Trust model: the socket is writable by every local user, so any of them may ask for a turn.
//! That is meant for a Mac with one user. Nothing a client sends can do more than take a place
//! in the line (its strings are sanitized and bounded, and there are at most 64 clients at
//! once); admitted groups are only ever measured, never signalled.

use crate::jobs::{self, AGENT_CLI, Inspector, Regex};
use crate::queue::{Id, Released, Sample, Scheduler, Standing};
use crate::runner::{age, gb};
use crate::signals::{self, Signals};
use crate::sizes::Sizes;
use crate::wire::{self, Message, Reply};
use std::fmt::Write as _;
use std::fs;
use std::io::{self, Read, Write};
use std::os::fd::AsRawFd;
use std::os::unix::fs::{FileTypeExt, OpenOptionsExt, PermissionsExt};
use std::os::unix::net::{UnixListener, UnixStream};
use std::path::{Path, PathBuf};
use std::sync::atomic::{AtomicBool, Ordering};
use std::sync::{Arc, Mutex, MutexGuard, PoisonError};
use std::thread;
use std::time::Duration;

/// What the queue has learned, in the state directory.
pub const SIZES_FILE: &str = "sizes.tsv";
/// What the queue is doing, in the state directory, rewritten when it changes.
pub const QUEUE_FILE: &str = "queue.tsv";
/// Clients at once. The daemon has few descriptors to spare and braking needs them more.
const MAX_CONNECTIONS: usize = 64;
/// How long the socket thread sleeps with no clients, and with some.
const IDLE_POLL_MS: i32 = 1000;
const BUSY_POLL_MS: i32 = 100;
/// A client's unsent replies are cut off at this many bytes: it is not reading them.
const MAX_OUTPUT: usize = 4 * wire::MAX_LINE;

pub struct Options {
    pub socket: PathBuf,
    pub state_dir: PathBuf,
    /// Admit every client at once, log what the queue would have decided, and write no file.
    pub dry_run: bool,
}

/// A line for the event log: its kind and its text.
pub type Note = (&'static str, String);

/// What both threads work on.
struct Core {
    sched: Scheduler,
    sizes: Sizes,
    /// Notes from the socket thread, passed on to the event log by the next tick.
    log: Vec<Note>,
    dry_run: bool,
}

fn lock(core: &Mutex<Core>) -> MutexGuard<'_, Core> {
    // A panic on the socket thread must not stop the sampling thread.
    core.lock().unwrap_or_else(PoisonError::into_inner)
}

impl Core {
    fn note(&mut self, kind: &'static str, text: String) {
        self.log.push((kind, text));
    }

    /// A request ends, because its command is done (`exit` is its code) or its client went
    /// away. A finished command's peak joins what is known about its signature. Returns the
    /// text of the sizes file if that changed and is to be written.
    fn release(&mut self, id: Id, exit: Option<i32>) -> Option<String> {
        let wall = signals::wall();
        match self.sched.release(id)? {
            Released::Waiting(w) => {
                let text = format!(
                    "{} left the queue after {} without running (session {})",
                    w.label,
                    age(wall - w.since),
                    w.session
                );
                self.note("queue", text);
                None
            }
            Released::Running(r) => {
                let ending = exit.map_or("its client went away".to_string(), |code| {
                    format!("exit {code}")
                });
                let text = format!(
                    "{} finished after {} ({ending}), peak {}",
                    r.label,
                    age(wall - r.since),
                    gb(r.peak)
                );
                self.note("queue", text);
                self.sizes.record(&r.signature, r.peak);
                (!self.dry_run && r.peak > 0).then(|| self.sizes.render())
            }
        }
    }
}

// ---------------------------------------------------------------------------------------
// The sampling thread's side

pub struct Broker {
    core: Arc<Mutex<Core>>,
    stop: Arc<AtomicBool>,
    socket: PathBuf,
    status_path: PathBuf,
    status_tmp: PathBuf,
    dry_run: bool,
    /// The status file as it will be, and as it was last written. Reused, so a tick that has
    /// nothing to say allocates nothing.
    text: String,
    written: String,
    /// The admitted groups to measure this tick.
    groups: Vec<(Id, i32)>,
    inspector: Inspector,
}

impl Broker {
    /// Loads what was learned, binds the socket and starts the thread that serves it. A
    /// failure means no queue: the daemon goes on braking, and clients run at once.
    pub fn start(opts: Options) -> Result<Broker, String> {
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
        let listener = bind(&opts.socket)?;
        let core = Arc::new(Mutex::new(Core {
            sched: Scheduler::new(),
            sizes,
            log,
            dry_run: opts.dry_run,
        }));
        let stop = Arc::new(AtomicBool::new(false));
        let (thread_core, thread_stop) = (Arc::clone(&core), Arc::clone(&stop));
        let sizes_file = (!opts.dry_run).then_some(sizes_path);
        thread::Builder::new()
            .name("membrake-queue".into())
            .spawn(move || serve(&listener, &thread_core, &thread_stop, sizes_file.as_deref()))
            .map_err(|e| format!("cannot start the queue thread: {e}"))?;
        Ok(Broker {
            core,
            stop,
            socket: opts.socket,
            status_path: opts.state_dir.join(QUEUE_FILE),
            status_tmp: opts.state_dir.join(format!("{QUEUE_FILE}.tmp")),
            dry_run: opts.dry_run,
            text: String::with_capacity(1024),
            written: String::with_capacity(1024),
            groups: Vec::with_capacity(16),
            inspector: Inspector::new(),
        })
    }

    /// One sample: decides whether the next request starts, measures the admitted groups, and
    /// rewrites the status file if it changed. Returns what the event log should say.
    /// Allocation-free when no client is connected and nothing has changed.
    pub fn tick(&mut self, s: &Signals, tight: bool, frozen: bool) -> Vec<Note> {
        let sample = Sample {
            time: s.time,
            // The real clock, whatever drives the samples: it dates what people are shown.
            wall: signals::wall(),
            ram: s.ram,
            level: s.level,
            tight,
            frozen,
        };
        let mut notes = Vec::new();
        {
            let mut guard = lock(&self.core);
            let core = &mut *guard;
            notes.append(&mut core.log);
            let sizes = &core.sizes;
            if let Some(id) = core.sched.step(&sample, &|signature| sizes.get(signature))
                && let Some(job) = core.sched.running().iter().find(|r| r.id == id)
            {
                notes.push((
                    "queue",
                    format!(
                        "{}admit {} (session {}) after {}: expects {}, {} in use of a {} cap",
                        if self.dry_run { "dry run, would " } else { "" },
                        job.label,
                        job.session,
                        age(job.waited),
                        gb(job.need),
                        gb(core.sched.used()),
                        gb(core.sched.cap().unwrap_or(0)),
                    ),
                ));
            }
            self.groups.clear();
            self.groups.extend(
                core.sched
                    .running()
                    .iter()
                    .filter_map(|r| r.pgid.map(|pgid| (r.id, pgid))),
            );
            self.text.clear();
            render(&core.sched, &core.sizes, &mut self.text);
        }
        if !self.groups.is_empty() {
            // Measured with the lock released: it reads the process table.
            let mut seen = Vec::with_capacity(self.groups.len());
            for &(id, pgid) in &self.groups {
                if let Some(bytes) = jobs::group_footprint(&mut self.inspector, pgid) {
                    seen.push((id, bytes));
                }
            }
            let mut core = lock(&self.core);
            for (id, bytes) in seen {
                core.sched.observe(id, bytes);
            }
        }
        if !self.dry_run && self.text != self.written {
            match write_atomic(&self.status_path, &self.status_tmp, self.text.as_bytes()) {
                Ok(()) => std::mem::swap(&mut self.text, &mut self.written),
                Err(e) => notes.push((
                    "warn",
                    format!("cannot write {}: {e}", self.status_path.display()),
                )),
            }
        }
        notes
    }
}

impl Drop for Broker {
    fn drop(&mut self) {
        self.stop.store(true, Ordering::SeqCst);
        let _ = fs::remove_file(&self.socket);
        if !self.dry_run {
            let _ = fs::remove_file(&self.status_path);
        }
    }
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
// The status file

/// The status file, in text. One header line, then one line per fact:
/// `daemon`, `ram`, `cap` and `used` (bytes), `wait` rows in the order the requests will be
/// admitted (position, session, wall-clock seconds when it asked, need, label) and `run` rows
/// (session, wall-clock seconds when it was admitted, process group or 0, peak, label).
fn render(sched: &Scheduler, sizes: &Sizes, out: &mut String) {
    let _ = writeln!(
        out,
        "queue\tv1\ndaemon\t{}\nram\t{}\ncap\t{}\nused\t{}",
        std::process::id(),
        sched.ram(),
        sched.cap().unwrap_or(0),
        sched.used() >> 20 << 20
    );
    if !sched.waiting().is_empty() {
        let standings = sched.standings(&|signature| sizes.get(signature));
        for (id, standing) in standings {
            if let (Standing::Waiting { position, need }, Some(w)) =
                (standing, sched.waiting().iter().find(|w| w.id == id))
            {
                let _ = writeln!(
                    out,
                    "wait\t{position}\t{}\t{}\t{need}\t{}",
                    w.session, w.since as u64, w.label
                );
            }
        }
    }
    for r in sched.running() {
        let _ = writeln!(
            out,
            "run\t{}\t{}\t{}\t{}\t{}",
            r.session,
            r.since as u64,
            r.pgid.unwrap_or(0),
            r.peak,
            r.label
        );
    }
}

pub struct WaitRow {
    pub position: usize,
    pub session: i32,
    pub since: f64,
    pub need: u64,
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
        if lines.next()? != "queue\tv1" {
            return None;
        }
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
                ["wait", position, session, since, need, label] => file.waiting.push(WaitRow {
                    position: position.parse().ok()?,
                    session: session.parse().ok()?,
                    since: since.parse().ok()?,
                    need: need.parse().ok()?,
                    label: label.to_string(),
                }),
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
    // Every local user may ask for a turn (see the trust model above). The umask decides the
    // mode bind gives it, so it is set explicitly.
    fs::set_permissions(path, fs::Permissions::from_mode(0o666))
        .map_err(|e| format!("cannot set the mode of {}: {e}", path.display()))?;
    listener
        .set_nonblocking(true)
        .map_err(|e| format!("cannot make {} non-blocking: {e}", path.display()))?;
    Ok(listener)
}

/// Who is on the other end, as the kernel says.
struct Peer {
    pid: i32,
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
        Ok(Peer { pid, uid })
    }
}

struct Conn {
    stream: UnixStream,
    peer: Peer,
    input: Vec<u8>,
    output: Vec<u8>,
    /// Its place in the scheduler, once it has asked.
    id: Option<Id>,
    /// The last position and need it was told.
    told: Option<(usize, u64)>,
    sent_go: bool,
    /// Close once everything queued for it is sent.
    closing: bool,
    /// Gone, or to be dropped now.
    dead: bool,
}

impl Conn {
    fn say(&mut self, reply: &Reply) {
        self.output.extend_from_slice(reply.line().as_bytes());
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
    agent: Option<Regex>,
    sizes_file: Option<&'a Path>,
    dry_run: bool,
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
            (Message::Request { label, signature }, None) => {
                let session = match &self.agent {
                    Some(agent) => jobs::session_of(agent, conn.peer.pid),
                    None => conn.peer.pid,
                };
                let label = if label.is_empty() {
                    signature.clone()
                } else {
                    label
                };
                let mut core = lock(self.core);
                let text = format!(
                    "{label} asks for a turn (pid {}, uid {}, session {session})",
                    conn.peer.pid, conn.peer.uid
                );
                core.note("queue", text);
                conn.id = Some(
                    core.sched
                        .enqueue(session, label, signature, signals::wall()),
                );
                drop(core);
                if self.dry_run {
                    // A dry run holds nobody: the queue still decides, to be logged, but the
                    // client goes now.
                    conn.sent_go = true;
                    conn.say(&Reply::Go);
                }
            }
            (Message::Request { .. }, Some(_)) => conn.refuse("a connection asks once"),
            (Message::Started(pgid), Some(id)) if pgid > 1 => {
                lock(self.core).sched.started(id, pgid);
            }
            (Message::Started(_), _) => {}
            (Message::Done(code), Some(id)) => {
                conn.id = None;
                self.release(id, Some(code));
                conn.closing = true;
            }
            (Message::Done(_), None) => conn.refuse("done before a request"),
        }
    }

    /// The request ends; writes the sizes if they changed.
    fn release(&self, id: Id, exit: Option<i32>) {
        let text = lock(self.core).release(id, exit);
        if let (Some(text), Some(path)) = (text, self.sizes_file) {
            let tmp = path.with_extension("tsv.tmp");
            if let Err(e) = write_atomic(path, &tmp, text.as_bytes()) {
                lock(self.core).note("warn", format!("cannot write {}: {e}", path.display()));
            }
        }
    }

    /// Tells every waiting client where it stands, and every admitted one to go.
    fn deliver(&self, conns: &mut [Conn]) {
        if conns.iter().all(|c| c.id.is_none()) {
            return;
        }
        let standings = {
            let core = lock(self.core);
            core.sched.standings(&|signature| core.sizes.get(signature))
        };
        for conn in conns.iter_mut() {
            let Some(id) = conn.id.filter(|_| !conn.closing && !conn.dead) else {
                continue;
            };
            match standings.iter().find(|(i, _)| *i == id).map(|(_, s)| *s) {
                Some(Standing::Waiting { position, need }) => {
                    if !self.dry_run && conn.told != Some((position, need)) {
                        conn.told = Some((position, need));
                        conn.say(&Reply::Queued { position, need });
                    }
                }
                Some(Standing::Admitted) => {
                    if !conn.sent_go {
                        conn.sent_go = true;
                        conn.say(&Reply::Go);
                    }
                }
                None => conn.dead = true,
            }
        }
    }
}

fn block_shutdown_signals() {
    // SAFETY: a zeroed sigset_t is emptied before use; the mask change is for this thread only.
    unsafe {
        let mut set: libc::sigset_t = std::mem::zeroed();
        libc::sigemptyset(&mut set);
        for sig in [libc::SIGTERM, libc::SIGINT, libc::SIGHUP] {
            libc::sigaddset(&mut set, sig);
        }
        libc::pthread_sigmask(libc::SIG_BLOCK, &set, std::ptr::null_mut());
    }
}

/// The socket thread.
fn serve(
    listener: &UnixListener,
    core: &Mutex<Core>,
    stop: &AtomicBool,
    sizes_file: Option<&Path>,
) {
    // The shutdown signals belong to the sampling thread, which wakes at once for them.
    block_shutdown_signals();
    let dry_run = lock(core).dry_run;
    let ctx = Ctx {
        core,
        // Without the regex every session is the client's own pid: no turns, but it works.
        agent: Regex::new(AGENT_CLI).ok(),
        sizes_file,
        dry_run,
    };
    let mut conns: Vec<Conn> = Vec::new();
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
            accept_all(listener, &mut conns, core, &mut accept_failed);
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
        ctx.deliver(&mut conns);
        for conn in &mut conns {
            conn.flush();
        }
        conns.retain_mut(|conn| {
            if conn.dead
                && let Some(id) = conn.id.take()
            {
                ctx.release(id, None);
            }
            !conn.dead
        });
    }
}

fn accept_all(
    listener: &UnixListener,
    conns: &mut Vec<Conn>,
    core: &Mutex<Core>,
    failed: &mut bool,
) {
    loop {
        match listener.accept() {
            Ok((stream, _)) => {
                if conns.len() >= MAX_CONNECTIONS {
                    continue; // dropped: the client runs at once
                }
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
                    sent_go: false,
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
    use crate::testutil::scratch;
    use std::os::unix::fs::PermissionsExt;
    use std::time::Instant;

    fn reading(time: f64, level: i32) -> Signals {
        Signals {
            time,
            wall: signals::wall(),
            segments: 0.1,
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

    /// The client is told where it stands, and then to go, and only when memory allows. The
    /// daemon knows who it is from the kernel: the log names this process, however the client
    /// words its request.
    #[test]
    fn a_client_is_told_its_place_and_then_to_go_and_the_kernel_says_who_it_is() {
        let (mut broker, socket, dir) = broker("grant", false);
        let mut client = Client::connect(&socket);
        client.request("tests");
        assert_eq!(client.next(), format!("QUEUED\t1\t{}", 1u64 << 30));

        // Tight: no go-ahead however long it waits.
        let mut time = 100.0;
        let notes = tick_n(&mut broker, &mut time, 35, 3);
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
            go.is_some()
        });
        assert_eq!(go.as_deref(), Some("GO"));
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

    /// The status file shows who waits and who runs, with strings made safe, to everybody; and
    /// it is gone when the broker is.
    #[test]
    fn the_status_file_lists_the_queue_and_goes_with_the_broker() {
        let (mut broker, socket, dir) = broker("status", false);
        let mut client = Client::connect(&socket);
        client.send("REQUEST\tv1\tbuild \x1b[31mred\tcargo build\n");
        client.next();
        let mut time = 300.0;
        tick_n(&mut broker, &mut time, 35, 1);

        let file = read_queue(&dir).unwrap().expect("a status file");
        assert_eq!(file.daemon as u32, std::process::id());
        assert_eq!((file.waiting.len(), file.running.len()), (1, 0));
        assert_eq!(file.waiting[0].label, "build ?[31mred");
        assert_eq!(
            (file.waiting[0].position, file.waiting[0].need),
            (1, 1 << 30)
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
        client.send("STARTED\t4242\n");
        let started = Instant::now();
        let file = loop {
            time += 1.0;
            broker.tick(&reading(time, 80), false, false);
            let file = read_queue(&dir).unwrap().unwrap();
            if file.running.first().is_some_and(|r| r.pgid == 4242) {
                break file;
            }
            assert!(started.elapsed() < Duration::from_secs(10));
            thread::sleep(Duration::from_millis(20));
        };
        assert_eq!((file.waiting.len(), file.running.len()), (0, 1));

        drop(broker);
        assert!(!dir.join(QUEUE_FILE).exists());
        assert!(!socket.exists());
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
}
