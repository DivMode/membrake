//! Everything membrake does to the system: signalling process groups, remembering what it
//! froze so a restart can thaw it, running hooks, and lifting itself out of jetsam band 0.
//!
//! Every signal to a group first checks that it is still the group that was recorded: its
//! leader (pid plus start time) is alive, or, if the leader has died, a surviving member
//! started before the group was recorded. A pid that was reused for a new group has only
//! members that started later, so it is never signalled by mistake.

use crate::jobs::{self, Job, Proc, one_line};
use crate::signals;
use std::fmt;
use std::fs;
use std::io::{self, Write};
use std::os::fd::AsRawFd;
use std::os::unix::fs::{DirBuilderExt, OpenOptionsExt};
use std::path::{Path, PathBuf};
use std::process::{Command, Stdio};
use std::sync::atomic::{AtomicBool, Ordering};
use std::thread::{self, JoinHandle};
use std::time::Duration;

const STATE_FILE: &str = "frozen.tsv";
const LOCK_FILE: &str = "membrake.lock";
/// How long to wait for a group to disappear after SIGKILL.
const KILL_WAIT: f64 = 10.0;
const POLL: Duration = Duration::from_millis(50);
/// No sleep is ever longer than the sampling interval, whatever the clock says.
pub const MAX_SLEEP: f64 = 1.0;

// ---------------------------------------------------------------------------------------
// Shutdown

static SHUTDOWN: AtomicBool = AtomicBool::new(false);

extern "C" fn on_signal(_: libc::c_int) {
    SHUTDOWN.store(true, Ordering::SeqCst);
}

pub fn shutdown_requested() -> bool {
    SHUTDOWN.load(Ordering::SeqCst)
}

/// SIGTERM, SIGINT and SIGHUP only set a flag; the loop thaws frozen jobs and exits. No
/// SA_RESTART, so a sleeping loop wakes at once.
pub fn install_signal_handlers() {
    for sig in [libc::SIGTERM, libc::SIGINT, libc::SIGHUP] {
        // SAFETY: a zeroed sigaction with a handler that only stores to an atomic.
        unsafe {
            let mut sa: libc::sigaction = std::mem::zeroed();
            sa.sa_sigaction = on_signal as extern "C" fn(libc::c_int) as usize;
            libc::sigemptyset(&mut sa.sa_mask);
            libc::sigaction(sig, &sa, std::ptr::null_mut());
        }
    }
}

/// Sleeps until `deadline` (engine time, `signals::uptime`), never longer than `MAX_SLEEP`;
/// returns early when a signal arrives.
pub fn sleep_until(deadline: f64) {
    let left = sleep_length(deadline - signals::uptime());
    if left <= 0.0 {
        return;
    }
    let ts = libc::timespec {
        tv_sec: left.trunc() as libc::time_t,
        tv_nsec: (left.fract() * 1e9) as libc::c_long,
    };
    // SAFETY: `ts` is a valid timespec; the remaining-time pointer may be null.
    unsafe { libc::nanosleep(&ts, std::ptr::null_mut()) };
}

/// How long to sleep when `left` seconds remain until the deadline: never negative, never
/// more than the sampling interval.
fn sleep_length(left: f64) -> f64 {
    left.clamp(0.0, MAX_SLEEP)
}

/// Holds an exclusive lock on `<dir>/membrake.lock` for as long as the returned file lives,
/// so two daemons never freeze and thaw each other's jobs. The lock goes away with the
/// process, however it dies.
pub fn lock_instance(dir: &Path) -> Result<fs::File, String> {
    fs::DirBuilder::new()
        .recursive(true)
        .mode(0o755)
        .create(dir)
        .map_err(|e| format!("cannot create the state directory {}: {e}", dir.display()))?;
    let path = dir.join(LOCK_FILE);
    let file = fs::OpenOptions::new()
        .write(true)
        .create(true)
        .truncate(false)
        .mode(0o600)
        .open(&path)
        .map_err(|e| format!("cannot open {}: {e}", path.display()))?;
    // SAFETY: flock on a file descriptor we own.
    if unsafe { libc::flock(file.as_raw_fd(), libc::LOCK_EX | libc::LOCK_NB) } != 0 {
        let e = io::Error::last_os_error();
        return Err(if e.raw_os_error() == Some(libc::EWOULDBLOCK) {
            format!(
                "another membrake is already running (it holds {}); only one may run at a time",
                path.display()
            )
        } else {
            format!("cannot lock {}: {e}", path.display())
        });
    }
    Ok(file)
}

// ---------------------------------------------------------------------------------------
// The state file: what is frozen right now.

#[derive(Clone, Debug, PartialEq)]
pub struct Record {
    pub pgid: i32,
    pub leader_pid: i32,
    pub leader_start_us: u64,
    /// Wall-clock Unix seconds. A group member that started after this is not the group
    /// that was frozen.
    pub frozen_at: f64,
    pub bytes: u64,
    pub uid: u32,
    pub command: String,
}

impl Record {
    fn line(&self) -> String {
        format!(
            "{}\t{}\t{}\t{:.3}\t{}\t{}\t{}\n",
            self.pgid,
            self.leader_pid,
            self.leader_start_us,
            self.frozen_at,
            self.bytes,
            self.uid,
            one_line(&self.command, 200)
        )
    }

    fn parse(line: &str) -> Option<Record> {
        let mut f = line.splitn(7, '\t');
        Some(Record {
            pgid: f.next()?.parse().ok()?,
            leader_pid: f.next()?.parse().ok()?,
            leader_start_us: f.next()?.parse().ok()?,
            frozen_at: f.next()?.parse().ok()?,
            bytes: f.next()?.parse().ok()?,
            uid: f.next()?.parse().ok()?,
            command: f.next()?.to_string(),
        })
    }

    fn bound_us(&self) -> u64 {
        wall_us(self.frozen_at)
    }
}

fn wall_us(seconds: f64) -> u64 {
    (seconds.max(0.0) * 1e6) as u64
}

/// Reads the state file without touching it: the records, and any lines that could not be
/// parsed (kept, never dropped). A missing file is an empty state.
pub fn read_state(dir: &Path) -> io::Result<(Vec<Record>, Vec<String>)> {
    let text = match fs::read_to_string(dir.join(STATE_FILE)) {
        Ok(text) => text,
        Err(e) if e.kind() == io::ErrorKind::NotFound => return Ok((Vec::new(), Vec::new())),
        Err(e) => return Err(e),
    };
    let (mut records, mut unparsed) = (Vec::new(), Vec::new());
    for line in text.lines().filter(|l| !l.trim().is_empty()) {
        match Record::parse(line) {
            Some(r) => records.push(r),
            None => unparsed.push(line.to_string()),
        }
    }
    Ok((records, unparsed))
}

pub struct State {
    dir: PathBuf,
    records: Vec<Record>,
    /// Lines of the file that are not records. They stay in the file: a line we cannot read
    /// may be a frozen job a newer or damaged writer left, and silently deleting it could
    /// leave that job frozen for good.
    unparsed: Vec<String>,
}

impl State {
    /// Creates the directory (readable by every user, so `membrake status` can list frozen
    /// jobs; it holds only group ids and command lines, which `ps` already shows to all)
    /// and loads what a previous run left.
    pub fn open(dir: &Path) -> io::Result<State> {
        fs::DirBuilder::new()
            .recursive(true)
            .mode(0o755)
            .create(dir)?;
        let (records, unparsed) = read_state(dir)?;
        Ok(State {
            dir: dir.to_path_buf(),
            records,
            unparsed,
        })
    }

    /// Re-reads the file: it, not this copy, is the truth after a crash or an outside edit.
    pub fn reload(&mut self) -> io::Result<()> {
        (self.records, self.unparsed) = read_state(&self.dir)?;
        Ok(())
    }

    pub fn records(&self) -> &[Record] {
        &self.records
    }

    pub fn record(&self, pgid: i32) -> Option<&Record> {
        self.records.iter().find(|r| r.pgid == pgid)
    }

    pub fn unparsed(&self) -> &[String] {
        &self.unparsed
    }

    /// Writes the whole file atomically and flushes it to disk: the record must outlive a
    /// crash, or a frozen job would stay frozen forever.
    fn save(&self) -> io::Result<()> {
        let tmp = self.dir.join(format!("{STATE_FILE}.tmp"));
        let mut file = fs::OpenOptions::new()
            .write(true)
            .create(true)
            .truncate(true)
            .mode(0o644)
            .open(&tmp)?;
        for r in &self.records {
            file.write_all(r.line().as_bytes())?;
        }
        for line in &self.unparsed {
            file.write_all(line.as_bytes())?;
            file.write_all(b"\n")?;
        }
        file.sync_all()?;
        fs::rename(tmp, self.dir.join(STATE_FILE))
    }

    fn add(&mut self, record: Record) -> io::Result<()> {
        self.records.retain(|r| r.pgid != record.pgid);
        self.records.push(record);
        self.save()
    }

    /// Drops the record for `pgid`, if any.
    pub fn remove(&mut self, pgid: i32) -> io::Result<()> {
        let before = self.records.len();
        self.records.retain(|r| r.pgid != pgid);
        if self.records.len() == before {
            return Ok(());
        }
        self.save()
    }
}

// ---------------------------------------------------------------------------------------
// Signalling groups

#[derive(Debug)]
pub enum ActError {
    /// The group is gone, or its pid now belongs to a newer group.
    Gone,
    Io(io::Error),
}

impl fmt::Display for ActError {
    fn fmt(&self, f: &mut fmt::Formatter<'_>) -> fmt::Result {
        match self {
            ActError::Gone => write!(f, "the job has exited"),
            ActError::Io(e) => write!(f, "{e}"),
        }
    }
}

impl From<io::Error> for ActError {
    fn from(e: io::Error) -> ActError {
        ActError::Io(e)
    }
}

/// The live (non-zombie) members of process group `pgid`.
fn live_members(pgid: i32) -> Vec<Proc> {
    let mut pids = vec![0i32; 4096];
    let bytes = (pids.len() * size_of::<i32>()) as libc::c_int;
    // SAFETY: `pids` is `bytes` long.
    let got = unsafe { libc::proc_listpgrppids(pgid, pids.as_mut_ptr().cast(), bytes) };
    // Like proc_listallpids, this wrapper returns a count of pids, not bytes.
    let count = (got.max(0) as usize).min(pids.len());
    pids[..count]
        .iter()
        .filter_map(|&pid| jobs::bsd_info(pid))
        .filter(|p| !p.zombie && p.pgid == pgid)
        .collect()
}

/// The members of the group that was recorded, or None if it is gone. It is the same group
/// if its leader is alive, or if the leader has died but a surviving member started before
/// `bound_us` (when the group was recorded). A group that merely reuses the pgid has only
/// members that started later.
fn recorded_group(
    pgid: i32,
    leader_pid: i32,
    leader_start_us: u64,
    bound_us: u64,
) -> Option<Vec<Proc>> {
    let members = live_members(pgid);
    let leader = members
        .iter()
        .any(|p| p.pid == leader_pid && p.start_us == leader_start_us);
    let survivor = members.iter().any(|p| p.start_us < bound_us);
    (leader || survivor).then_some(members)
}

fn signal_group(pgid: i32, sig: libc::c_int) -> Result<(), ActError> {
    // SAFETY: killpg has no memory-safety preconditions.
    if unsafe { libc::killpg(pgid, sig) } == 0 {
        return Ok(());
    }
    let e = io::Error::last_os_error();
    Err(if e.raw_os_error() == Some(libc::ESRCH) {
        ActError::Gone
    } else {
        ActError::Io(e)
    })
}

/// SIGSTOP the group. The record is written and flushed first: a crash between the two
/// steps leaves a record for a running job, which recovery thaws harmlessly. `now` is the
/// wall clock.
pub fn freeze(state: &mut State, job: &Job, now: f64) -> Result<(), ActError> {
    recorded_group(job.pgid, job.leader_pid, job.start_us, wall_us(now)).ok_or(ActError::Gone)?;
    state.add(Record {
        pgid: job.pgid,
        leader_pid: job.leader_pid,
        leader_start_us: job.start_us,
        frozen_at: now,
        bytes: job.bytes,
        uid: job.uid,
        command: job.command.clone(),
    })?;
    if let Err(e) = signal_group(job.pgid, libc::SIGSTOP) {
        let _ = state.remove(job.pgid);
        return Err(e);
    }
    Ok(())
}

/// SIGCONT the recorded group, if it is still the same group.
fn thaw(rec: &Record) -> Result<(), ActError> {
    recorded_group(
        rec.pgid,
        rec.leader_pid,
        rec.leader_start_us,
        rec.bound_us(),
    )
    .ok_or(ActError::Gone)?;
    signal_group(rec.pgid, libc::SIGCONT)
}

/// SIGCONT the group and forget it. A job that has already gone, or was never recorded, is
/// not an error worth more than `Gone`.
pub fn resume(state: &mut State, job: &Job) -> Result<(), ActError> {
    let Some(rec) = state.record(job.pgid).cloned() else {
        return Err(ActError::Gone);
    };
    let result = thaw(&rec);
    state.remove(job.pgid)?;
    result
}

#[derive(Debug, PartialEq, Eq)]
pub enum Recovered {
    Resumed,
    /// The recorded group is gone, or its pgid now belongs to a newer group: nothing was
    /// signalled.
    Skipped,
    Failed(String),
}

/// Thaws every recorded job whose group is still the recorded one, then empties the records
/// (unreadable lines stay). Run at start and on shutdown, before anything else.
pub fn recover(state: &mut State) -> io::Result<Vec<(Record, Recovered)>> {
    let mut report = Vec::new();
    for r in std::mem::take(&mut state.records) {
        let outcome = match thaw(&r) {
            Ok(()) => Recovered::Resumed,
            Err(ActError::Gone) => Recovered::Skipped,
            Err(ActError::Io(e)) => Recovered::Failed(e.to_string()),
        };
        report.push((r, outcome));
    }
    state.save()?;
    Ok(report)
}

/// `recover` on whatever the state file in `dir` holds now.
pub fn recover_dir(dir: &Path) -> io::Result<Vec<(Record, Recovered)>> {
    recover(&mut State::open(dir)?)
}

#[derive(Debug, PartialEq, Eq)]
pub enum FrozenState {
    /// Every live member is stopped, as membrake left it.
    Stopped,
    /// Someone continued it (`fg`, `kill -CONT`).
    ResumedOutside,
    Gone,
}

/// What became of a job membrake froze. Cheap: it looks at one group only.
pub fn frozen_state(rec: &Record) -> FrozenState {
    match recorded_group(
        rec.pgid,
        rec.leader_pid,
        rec.leader_start_us,
        rec.bound_us(),
    ) {
        None => FrozenState::Gone,
        Some(members) if members.iter().all(|p| p.stopped) => FrozenState::Stopped,
        Some(_) => FrozenState::ResumedOutside,
    }
}

fn wait_for_exit(pgid: i32, seconds: f64) -> bool {
    let deadline = signals::uptime() + seconds;
    while !live_members(pgid).is_empty() {
        if signals::uptime() >= deadline || shutdown_requested() {
            return false;
        }
        thread::sleep(POLL);
    }
    true
}

/// SIGSTOP the group so it stops growing, then (unless `immediate`) SIGTERM and SIGCONT and
/// wait up to `term_seconds`, then SIGKILL and wait for the group to exit. Returns whether
/// it exited. Blocks: only one kill is ever in flight. `frozen_at` is when membrake froze
/// the job, if it did (see `recorded_group`).
pub fn kill(
    job: &Job,
    immediate: bool,
    term_seconds: f64,
    frozen_at: Option<f64>,
) -> Result<bool, ActError> {
    let bound = wall_us(frozen_at.unwrap_or_else(signals::wall));
    recorded_group(job.pgid, job.leader_pid, job.start_us, bound).ok_or(ActError::Gone)?;
    signal_group(job.pgid, libc::SIGSTOP)?;
    // The SIGSTOP succeeded, so we are allowed to signal this group. A later error therefore
    // means the group has already exited: macOS answers EPERM, not ESRCH, for a zombie that
    // its parent has not reaped yet. Whether it is gone is decided by `wait_for_exit`.
    if !immediate && term_seconds > 0.0 {
        let _ = signal_group(job.pgid, libc::SIGTERM);
        // A stopped process cannot run its SIGTERM handler.
        let _ = signal_group(job.pgid, libc::SIGCONT);
        if wait_for_exit(job.pgid, term_seconds) {
            return Ok(true);
        }
    }
    let _ = signal_group(job.pgid, libc::SIGKILL);
    Ok(wait_for_exit(job.pgid, KILL_WAIT))
}

// ---------------------------------------------------------------------------------------
// Hooks

/// Runs `/bin/sh -c command` with the event described in `MEMBRAKE_*` variables. Does not
/// wait: the returned thread reaps the child (join it only in tests).
pub fn spawn_hook(command: &str, event: &str, job: &Job) -> io::Result<JoinHandle<()>> {
    let mut child = Command::new("/bin/sh")
        .arg("-c")
        .arg(command)
        .env("MEMBRAKE_EVENT", event)
        .env("MEMBRAKE_PGID", job.pgid.to_string())
        .env("MEMBRAKE_BYTES", job.bytes.to_string())
        .env("MEMBRAKE_COMMAND", &job.command)
        .env("MEMBRAKE_UID", job.uid.to_string())
        .stdin(Stdio::null())
        .spawn()?;
    Ok(thread::spawn(move || {
        let _ = child.wait();
    }))
}

// ---------------------------------------------------------------------------------------
// Self-protection

/// The band ordinary user processes live in (`JETSAM_PRIORITY_DEFAULT`).
const BAND: i32 = 180;
/// `MEMORYSTATUS_CMD_SET_PRIORITY_PROPERTIES`, from xnu's `kern_memorystatus.h`.
const CMD_SET_PRIORITY_PROPERTIES: u32 = 2;
/// Set in the environment by the re-exec, so the new image does not re-exec again.
const BAND_ENV: &str = "MEMBRAKE_JETSAM_BAND";

/// `memorystatus_priority_properties_t`
#[repr(C)]
struct PriorityProperties {
    priority: i32,
    user_data: u64,
}

type MemorystatusControl = unsafe extern "C" fn(u32, i32, u32, *mut libc::c_void, usize) -> i32;
type SetJetsamExt = unsafe extern "C" fn(*mut libc::posix_spawnattr_t, i16, i32, i32, i32) -> i32;

/// Looks a private libSystem symbol up at run time, so a macOS without it degrades to a
/// logged failure instead of a binary that will not launch.
fn private_symbol(name: &std::ffi::CStr) -> Option<*mut libc::c_void> {
    // SAFETY: dlsym with RTLD_DEFAULT and a NUL-terminated name.
    let p = unsafe { libc::dlsym(libc::RTLD_DEFAULT, name.as_ptr()) };
    (!p.is_null()).then_some(p)
}

/// As root, `memorystatus_control` raises our own band directly.
fn raise_with_memorystatus_control() -> Result<(), String> {
    let f =
        private_symbol(c"memorystatus_control").ok_or("memorystatus_control is not exported")?;
    // SAFETY: the symbol has this C signature (kern_memorystatus.h).
    let f: MemorystatusControl = unsafe { std::mem::transmute(f) };
    let mut props = PriorityProperties {
        priority: BAND,
        user_data: 0,
    };
    // SAFETY: `props` matches memorystatus_priority_properties_t and the size passed.
    let rc = unsafe {
        f(
            CMD_SET_PRIORITY_PROPERTIES,
            std::process::id() as i32,
            0,
            (&raw mut props).cast(),
            size_of::<PriorityProperties>(),
        )
    };
    if rc == 0 {
        Ok(())
    } else {
        Err(io::Error::last_os_error().to_string())
    }
}

/// Without root: re-execute ourselves with a jetsam band in the spawn attributes. Returns
/// only on failure.
fn reexec_in_band() -> Result<(), String> {
    use std::ffi::CString;
    use std::os::unix::ffi::OsStrExt;
    let set = private_symbol(c"posix_spawnattr_setjetsam_ext")
        .ok_or("posix_spawnattr_setjetsam_ext is not exported")?;
    // SAFETY: the symbol has this C signature (libsyscall).
    let set: SetJetsamExt = unsafe { std::mem::transmute(set) };
    let exe = std::env::current_exe().map_err(|e| e.to_string())?;
    let exe = CString::new(exe.as_os_str().as_bytes()).map_err(|e| e.to_string())?;
    let argv: Vec<CString> = std::env::args_os()
        .map(|a| CString::new(a.as_bytes()).map_err(|e| e.to_string()))
        .collect::<Result<_, _>>()?;
    let mut envp: Vec<CString> = std::env::vars_os()
        .map(|(k, v)| {
            let mut kv = k.as_bytes().to_vec();
            kv.push(b'=');
            kv.extend_from_slice(v.as_bytes());
            CString::new(kv).map_err(|e| e.to_string())
        })
        .collect::<Result<_, _>>()?;
    envp.push(CString::new(format!("{BAND_ENV}={BAND}")).map_err(|e| e.to_string())?);
    let argv_ptrs: Vec<*mut libc::c_char> = argv
        .iter()
        .map(|a| a.as_ptr().cast_mut())
        .chain([std::ptr::null_mut()])
        .collect();
    let envp_ptrs: Vec<*mut libc::c_char> = envp
        .iter()
        .map(|a| a.as_ptr().cast_mut())
        .chain([std::ptr::null_mut()])
        .collect();

    let mut attr: libc::posix_spawnattr_t = std::ptr::null_mut();
    // SAFETY: standard posix_spawn attribute setup; `attr` is destroyed on every path. With
    // POSIX_SPAWN_SETEXEC a successful posix_spawn replaces this process and never returns.
    unsafe {
        let rc = libc::posix_spawnattr_init(&mut attr);
        if rc != 0 {
            return Err(io::Error::from_raw_os_error(rc).to_string());
        }
        // POSIX_SPAWN_JETSAM_USE_EFFECTIVE_PRIORITY (1), like launchd; no memory limit (-1).
        let mut rc = set(&mut attr, 1, BAND, -1, -1);
        if rc == 0 {
            rc = libc::posix_spawnattr_setflags(&mut attr, libc::POSIX_SPAWN_SETEXEC as i16);
        }
        if rc == 0 {
            let mut pid: libc::pid_t = 0;
            rc = libc::posix_spawn(
                &mut pid,
                exe.as_ptr(),
                std::ptr::null(),
                &attr,
                argv_ptrs.as_ptr(),
                envp_ptrs.as_ptr(),
            );
        }
        libc::posix_spawnattr_destroy(&mut attr);
        Err(io::Error::from_raw_os_error(rc).to_string())
    }
}

/// Moves membrake out of jetsam band 0, the only band the kernel's low-swap and
/// compressor-shortage kills take. Failure is reported in the returned line, never fatal.
/// Call first thing: the fallback re-executes the process.
pub fn protect_self() -> String {
    if let Ok(band) = std::env::var(BAND_ENV) {
        return format!("jetsam: requested band {band} via re-exec (not verifiable without root)");
    }
    let direct = match raise_with_memorystatus_control() {
        Ok(()) => return format!("jetsam: raised to band {BAND} with memorystatus_control"),
        Err(e) => e,
    };
    match reexec_in_band() {
        Ok(()) => unreachable!("a successful re-exec does not return"),
        Err(e) => format!(
            "jetsam: WARNING could not leave band 0 (memorystatus_control: {direct}; re-exec: {e}); \
             the kernel's low-swap kill may take membrake"
        ),
    }
}

#[cfg(test)]
mod tests {
    use super::*;
    use crate::testutil::{Sleeper, scratch};

    /// A clock that jumps or a bad deadline must not put the brake to sleep.
    #[test]
    fn no_sleep_is_longer_than_the_sampling_interval() {
        assert_eq!(sleep_length(3600.0), MAX_SLEEP);
        assert_eq!(sleep_length(f64::INFINITY), MAX_SLEEP);
        assert_eq!(sleep_length(-5.0), 0.0);
        assert_eq!(sleep_length(0.4), 0.4);
    }

    fn records_on_disk(dir: &Path) -> usize {
        read_state(dir).unwrap().0.len()
    }

    /// The crash-safety contract: a frozen job recorded by one run is thawed by the next
    /// run's start-up recovery, found only through the state file.
    #[test]
    fn restart_resumes_a_recorded_frozen_group() {
        let dir = scratch("recover");
        let sleeper = Sleeper::spawn();
        let job = sleeper.job(1);
        {
            let mut first_run = State::open(&dir).unwrap();
            freeze(&mut first_run, &job, signals::wall()).unwrap();
            assert!(sleeper.settles(true), "freeze stops the group");
        } // the first run "crashes": its State is gone, the file stays

        let mut second_run = State::open(&dir).unwrap();
        assert_eq!(second_run.records().len(), 1);
        let report = recover(&mut second_run).unwrap();
        assert_eq!(report.len(), 1);
        assert_eq!(report[0].1, Recovered::Resumed);
        assert!(sleeper.settles(false), "recovery continues the group");
        assert_eq!(records_on_disk(&dir), 0, "state file is emptied");
        let _ = fs::remove_dir_all(&dir);
    }

    /// Pid reuse: a record describing an earlier group must not signal a newer group that
    /// has the same pgid. The newer group's members all started after the record.
    #[test]
    fn recovery_leaves_alone_a_pid_that_was_reused() {
        let dir = scratch("reuse");
        let sleeper = Sleeper::spawn();
        let job = sleeper.job(1);
        let mut state = State::open(&dir).unwrap();
        freeze(&mut state, &job, signals::wall()).unwrap();
        assert!(sleeper.settles(true));
        // Rewrite the record as if it described an earlier group frozen long before this
        // process existed.
        state.records[0].leader_start_us -= 5_000_000;
        state.records[0].frozen_at = job.start_us as f64 / 1e6 - 100.0;
        state.save().unwrap();

        let mut next = State::open(&dir).unwrap();
        let report = recover(&mut next).unwrap();
        assert_eq!(report[0].1, Recovered::Skipped);
        assert!(
            sleeper.stopped(),
            "the unrelated stopped process is untouched"
        );
        assert_eq!(records_on_disk(&dir), 0, "the stale record is dropped");
        let _ = fs::remove_dir_all(&dir);
    }

    /// The leader of a frozen group dies (killed by hand) and a member survives, still
    /// stopped. Recovery must find the survivor through the group itself, not through the
    /// dead leader's pid.
    #[test]
    fn recovery_resumes_the_survivor_of_a_group_whose_leader_died() {
        let dir = scratch("survivor");
        let mut leader = Sleeper::spawn();
        let member = Sleeper::spawn_in(leader.pid(), false);
        let job = leader.job(2);
        let mut state = State::open(&dir).unwrap();
        freeze(&mut state, &job, signals::wall()).unwrap();
        assert!(member.settles(true), "the whole group is stopped");

        leader.die();
        assert!(
            jobs::bsd_info(job.leader_pid).is_none(),
            "the leader is gone"
        );
        assert!(member.stopped());

        let report = recover(&mut state).unwrap();
        assert_eq!(report[0].1, Recovered::Resumed);
        assert!(member.settles(false), "the survivor runs again");
        assert_eq!(records_on_disk(&dir), 0);
        let _ = fs::remove_dir_all(&dir);
    }

    /// Kill reaches a frozen group's survivors the same way.
    #[test]
    fn kill_reaches_the_survivor_of_a_group_whose_leader_died() {
        let mut leader = Sleeper::spawn();
        let member = Sleeper::spawn_in(leader.pid(), false);
        let job = leader.job(2);
        let frozen_at = signals::wall();
        leader.die();
        assert!(kill(&job, true, 5.0, Some(frozen_at)).unwrap());
        assert!(live_members(job.pgid).is_empty());
        drop(member);
    }

    /// A record whose group has exited has nothing to resume and is dropped.
    #[test]
    fn record_of_an_exited_group_is_dropped() {
        let dir = scratch("exited");
        let mut sleeper = Sleeper::spawn();
        let job = sleeper.job(1);
        let mut state = State::open(&dir).unwrap();
        freeze(&mut state, &job, signals::wall()).unwrap();
        sleeper.die();
        let report = recover(&mut state).unwrap();
        assert_eq!(report[0].1, Recovered::Skipped);
        assert_eq!(records_on_disk(&dir), 0);
        let _ = fs::remove_dir_all(&dir);
    }

    #[test]
    fn freeze_then_resume_round_trips() {
        let dir = scratch("roundtrip");
        let sleeper = Sleeper::spawn();
        let job = sleeper.job(1);
        let mut state = State::open(&dir).unwrap();
        freeze(&mut state, &job, signals::wall()).unwrap();
        assert!(sleeper.settles(true));
        assert_eq!(records_on_disk(&dir), 1);
        resume(&mut state, &job).unwrap();
        assert!(sleeper.settles(false));
        assert_eq!(records_on_disk(&dir), 0);
        let _ = fs::remove_dir_all(&dir);
    }

    /// The runner drops a job someone resumed by hand, or that exited, from its frozen set.
    #[test]
    fn frozen_state_notices_a_job_resumed_outside_or_gone() {
        let dir = scratch("frozen-state");
        let mut sleeper = Sleeper::spawn();
        let job = sleeper.job(1);
        let mut state = State::open(&dir).unwrap();
        freeze(&mut state, &job, signals::wall()).unwrap();
        assert!(sleeper.settles(true));
        let rec = state.record(job.pgid).unwrap().clone();
        assert_eq!(frozen_state(&rec), FrozenState::Stopped);

        signal_group(job.pgid, libc::SIGCONT).unwrap(); // `kill -CONT`, not membrake
        assert!(sleeper.settles(false));
        assert_eq!(frozen_state(&rec), FrozenState::ResumedOutside);

        sleeper.die();
        assert_eq!(frozen_state(&rec), FrozenState::Gone);
        let _ = fs::remove_dir_all(&dir);
    }

    /// A non-emergency kill stops the group, sends SIGTERM and returns once the group is gone,
    /// well before the grace period runs out. (A plain `sleep` dies of SIGTERM even while
    /// stopped, so this does not show that SIGCONT is needed for a process with a handler.)
    #[test]
    fn kill_ends_a_stopped_group() {
        let sleeper = Sleeper::spawn();
        let job = sleeper.job(1);
        assert!(!live_members(job.pgid).is_empty());
        let started = std::time::Instant::now();
        assert!(kill(&job, false, 5.0, None).unwrap(), "group exited");
        assert!(live_members(job.pgid).is_empty());
        // It ended on SIGTERM, not by waiting out the 5 s grace period for SIGKILL.
        assert!(
            started.elapsed() < Duration::from_secs(3),
            "{:?}",
            started.elapsed()
        );
    }

    /// A job that ignores SIGTERM is waited for through the grace period and then SIGKILLed.
    #[test]
    fn kill_escalates_to_sigkill_when_sigterm_is_ignored() {
        let sleeper = Sleeper::spawn_in(0, true);
        let job = sleeper.job(1);
        let started = std::time::Instant::now();
        assert!(kill(&job, false, 1.0, None).unwrap(), "group exited");
        let took = started.elapsed();
        assert!(live_members(job.pgid).is_empty());
        assert!(
            took >= Duration::from_secs(1) && took < Duration::from_secs(8),
            "SIGTERM was ignored, so the 1 s grace period must pass first: {took:?}"
        );
    }

    #[test]
    fn immediate_kill_skips_the_grace_period() {
        let sleeper = Sleeper::spawn_in(0, true);
        let job = sleeper.job(1);
        let started = std::time::Instant::now();
        assert!(kill(&job, true, 5.0, None).unwrap());
        assert!(started.elapsed() < Duration::from_secs(3));
    }

    /// Two daemons must not run at once; the lock frees when the first goes away.
    #[test]
    fn a_second_instance_is_refused() {
        let dir = scratch("lock");
        let first = lock_instance(&dir).unwrap();
        let err = lock_instance(&dir).unwrap_err();
        assert!(err.contains("already running"), "{err}");
        drop(first);
        // Other tests fork children in this process; a child holds the descriptor until its
        // exec closes it, so the release can lag by an instant.
        let deadline = std::time::Instant::now() + Duration::from_secs(2);
        while lock_instance(&dir).is_err() {
            assert!(std::time::Instant::now() < deadline, "the lock never freed");
            thread::sleep(Duration::from_millis(10));
        }
        let _ = fs::remove_dir_all(&dir);
    }

    /// A state line we cannot parse may be a frozen job: it stays in the file through every
    /// rewrite, and is reported.
    #[test]
    fn unparseable_state_lines_are_kept() {
        let dir = scratch("unparsed");
        fs::create_dir_all(&dir).unwrap();
        fs::write(dir.join(STATE_FILE), "not a record at all\n").unwrap();
        let sleeper = Sleeper::spawn();
        let job = sleeper.job(1);
        let mut state = State::open(&dir).unwrap();
        assert_eq!(state.unparsed(), ["not a record at all"]);
        freeze(&mut state, &job, signals::wall()).unwrap();
        recover(&mut state).unwrap();
        let text = fs::read_to_string(dir.join(STATE_FILE)).unwrap();
        assert_eq!(text, "not a record at all\n");
        let _ = fs::remove_dir_all(&dir);
    }

    /// The hook contract: the five MEMBRAKE_* variables reach the command.
    #[test]
    fn hook_receives_the_event_environment() {
        let dir = scratch("hook");
        fs::create_dir_all(&dir).unwrap();
        let out = dir.join("out");
        let job = Job {
            pgid: 4321,
            leader_pid: 4321,
            start_us: 1,
            uid: 777,
            bytes: 2048,
            procs: 1,
            prefer: false,
            protected: false,
            stopped: false,
            command: "tsc --build".into(),
        };
        let cmd = format!(
            "printf '%s|%s|%s|%s|%s' \"$MEMBRAKE_EVENT\" \"$MEMBRAKE_PGID\" \"$MEMBRAKE_BYTES\" \"$MEMBRAKE_COMMAND\" \"$MEMBRAKE_UID\" > {}",
            out.display()
        );
        spawn_hook(&cmd, "freeze", &job).unwrap().join().unwrap();
        assert_eq!(
            fs::read_to_string(&out).unwrap(),
            "freeze|4321|2048|tsc --build|777"
        );
        let _ = fs::remove_dir_all(&dir);
    }
}
