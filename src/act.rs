//! Everything membrake does to the system: signalling process groups, remembering what it
//! stopped so a restart can continue it, running hooks, and lifting itself out of jetsam
//! band 0.
//!
//! Identity. Every signal to a group is preceded, immediately, by a look at the group, and the
//! group must still be the one that was recorded or scanned. It is, if its leader (pid plus
//! start time) is alive; or if a member is one the freeze was sent to (the record lists them,
//! pid plus start time); or if a member started before the bound; or if a member is a child
//! of a recorded process. The bound is the time the process table was scanned (or, for a
//! frozen job, the time of its record); it is never worked out when a signal is dispatched,
//! which may be seconds later. A pid that was reused for a new group has only members that
//! started later and are nobody's children, so it is never signalled by mistake. A group that
//! none of this recognises, but in which a member is stopped, is not "gone" either: stopped
//! members may be membrake's, so the record stays and nothing is signalled (`Unknown`).
//!
//! What is left. macOS has no pidfd, so between that look and the `killpg` there are a few
//! microseconds in which the whole group would have to exit and the kernel hand its pgid to a
//! new group; the kernel does not reissue a pgid while any process is still in the group, so
//! it also needs the pid counter to wrap around inside that gap. SIGKILL is not sent to the
//! group at all: each member is signalled by its own pid, after its start time is read again,
//! so a pid that was reissued to a new process in the meantime is skipped.
//!
//! Records. A record is the promise to continue a group. It is written (and flushed) before
//! every SIGSTOP, including the one that starts a kill, and kept until the group is confirmed
//! gone or SIGCONT has been sent to it. A group that cannot be inspected, or a SIGCONT that
//! fails, is neither: the record stays and the signal is tried again.

use crate::jobs::{Inspector, Job, Look, Probe, Seen, one_line, probe};
use crate::signals;
use std::fmt::{self, Write as _};
use std::fs;
use std::io::{self, Write};
use std::os::fd::AsRawFd;
use std::os::unix::fs::{DirBuilderExt, OpenOptionsExt, PermissionsExt};
use std::path::{Path, PathBuf};
use std::process::{Command, Stdio};
use std::sync::atomic::{AtomicBool, AtomicI32, Ordering};
use std::thread::{self, JoinHandle};
use std::time::Duration;

const STATE_FILE: &str = "frozen.tsv";
const LOCK_FILE: &str = "membrake.lock";
/// How long to keep killing a group, after SIGKILL, until it is gone.
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

/// Keeps the shutdown signals away from the calling thread, so that they reach the sampling
/// thread, which wakes at once for them. Call first thing in every thread that is started.
pub(crate) fn block_shutdown_signals() {
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

/// Creates the state directory if needed and sets its mode, whatever the umask and whatever
/// an earlier version left. Readable by every user, so `membrake status` can list frozen jobs;
/// it holds only group ids and command lines, which `ps` already shows to all.
fn secure_dir(dir: &Path) -> io::Result<()> {
    fs::DirBuilder::new()
        .recursive(true)
        .mode(0o755)
        .create(dir)?;
    fs::set_permissions(dir, fs::Permissions::from_mode(0o755))
}

/// Holds an exclusive lock on `<dir>/membrake.lock` for as long as the returned file lives,
/// so two daemons never freeze and thaw each other's jobs. The lock goes away with the
/// process, however it dies.
pub fn lock_instance(dir: &Path) -> Result<fs::File, String> {
    secure_dir(dir)
        .map_err(|e| format!("cannot create the state directory {}: {e}", dir.display()))?;
    let path = dir.join(LOCK_FILE);
    let file = fs::OpenOptions::new()
        .write(true)
        .create(true)
        .truncate(false)
        .mode(0o600)
        .open(&path)
        .map_err(|e| format!("cannot open {}: {e}", path.display()))?;
    file.set_permissions(fs::Permissions::from_mode(0o600))
        .map_err(|e| format!("cannot set the mode of {}: {e}", path.display()))?;
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
    /// The processes the SIGSTOP was sent to (pid, start time in microseconds). A group whose
    /// leader has died is still the recorded one while any of them is in it.
    pub members: Vec<(i32, u64)>,
}

impl Record {
    fn of(job: &Job, frozen_at: f64, members: &[Seen]) -> Record {
        Record {
            pgid: job.pgid,
            leader_pid: job.leader_pid,
            leader_start_us: job.start_us,
            frozen_at,
            bytes: job.bytes,
            uid: job.uid,
            command: job.command.clone(),
            members: members.iter().map(|m| (m.pid, m.start_us)).collect(),
        }
    }

    /// The command ends the line a version without `members` wrote, so `members` follows it
    /// as an eighth field: an older reader sees them as part of the command, and a newer one
    /// reads an older record as having none.
    fn line(&self) -> String {
        let mut line = format!(
            "{}\t{}\t{}\t{:.3}\t{}\t{}\t{}",
            self.pgid,
            self.leader_pid,
            self.leader_start_us,
            self.frozen_at,
            self.bytes,
            self.uid,
            one_line(&self.command, 200)
        );
        for (i, (pid, start_us)) in self.members.iter().enumerate() {
            line.push(if i == 0 { '\t' } else { ',' });
            let _ = write!(line, "{pid}:{start_us}");
        }
        line.push('\n');
        line
    }

    /// A member entry that cannot be read costs only itself: the record is still a record,
    /// and a line that is not one is a job that is never resumed.
    fn parse(line: &str) -> Option<Record> {
        let mut f = line.splitn(8, '\t');
        Some(Record {
            pgid: f.next()?.parse().ok()?,
            leader_pid: f.next()?.parse().ok()?,
            leader_start_us: f.next()?.parse().ok()?,
            frozen_at: f.next()?.parse().ok()?,
            bytes: f.next()?.parse().ok()?,
            uid: f.next()?.parse().ok()?,
            command: f.next()?.to_string(),
            members: f
                .next()
                .unwrap_or("")
                .split(',')
                .filter_map(|m| {
                    let (pid, start_us) = m.split_once(':')?;
                    Some((pid.parse().ok()?, start_us.parse().ok()?))
                })
                .collect(),
        })
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
    /// Creates the directory (see `secure_dir`), repairs the mode of a state file an earlier
    /// version left unreadable to other users, and loads what a previous run left.
    pub fn open(dir: &Path) -> io::Result<State> {
        secure_dir(dir)?;
        match fs::set_permissions(dir.join(STATE_FILE), fs::Permissions::from_mode(0o644)) {
            Err(e) if e.kind() != io::ErrorKind::NotFound => return Err(e),
            _ => {}
        }
        let (records, unparsed) = read_state(dir)?;
        Ok(State {
            dir: dir.to_path_buf(),
            records,
            unparsed,
        })
    }

    /// Re-reads the file: it, not this copy, is the truth after a crash or an outside edit.
    /// If the file cannot be read, this copy is left as it was.
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
    /// crash, or a frozen job would stay frozen forever. The mode is set on the file itself,
    /// so neither the umask nor a leftover temporary file decides it.
    fn save(&self) -> io::Result<()> {
        let tmp = self.dir.join(format!("{STATE_FILE}.tmp"));
        let mut file = fs::OpenOptions::new()
            .write(true)
            .create(true)
            .truncate(true)
            .mode(0o644)
            .open(&tmp)?;
        file.set_permissions(fs::Permissions::from_mode(0o644))?;
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

    /// Records the group and writes the file. If the write fails nothing has been promised, and
    /// the records are as they were.
    fn add(&mut self, record: Record) -> io::Result<()> {
        let replaced = self
            .records
            .iter()
            .position(|r| r.pgid == record.pgid)
            .map(|at| self.records.remove(at));
        self.records.push(record);
        self.save().inspect_err(|_| {
            self.records.pop();
            self.records.extend(replaced);
        })
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
    /// Confirmed: the group is gone, or its pid now belongs to a newer group.
    Gone,
    /// The group could not be inspected. Nothing was signalled and nothing is known: this is
    /// not `Gone`, and no record may be dropped because of it.
    Unknown(String),
    /// The rules, asked again once the record was written and just before the signal, no
    /// longer allow this group to be touched (the reason is the text). Nothing was signalled.
    Ineligible(String),
    Io(io::Error),
}

impl fmt::Display for ActError {
    fn fmt(&self, f: &mut fmt::Formatter<'_>) -> fmt::Result {
        match self {
            ActError::Gone => write!(f, "the job has exited"),
            ActError::Unknown(why) => write!(f, "the job cannot be inspected: {why}"),
            ActError::Ineligible(why) => write!(f, "it is no longer eligible: {why}"),
            ActError::Io(e) => write!(f, "{e}"),
        }
    }
}

impl From<io::Error> for ActError {
    fn from(e: io::Error) -> ActError {
        ActError::Io(e)
    }
}

/// The group a signal is meant for.
struct Identity {
    pgid: i32,
    leader_pid: i32,
    leader_start_us: u64,
    /// A member that started at or after this is not, for that reason alone, part of the
    /// group that was recorded.
    bound_us: u64,
    /// The processes the freeze went to (pid, start time), as recorded.
    members: Vec<(i32, u64)>,
}

/// What a look at the members of a pgid says about whose group it is.
#[derive(Debug, PartialEq, Eq)]
enum Claim {
    /// The recorded group.
    Ours,
    /// Another group that has the pgid now.
    Elsewhere,
    /// Not recognised, but a member is stopped: it may be one membrake stopped, whose leader
    /// and recorded members have since died. Not to be signalled, and not to be forgotten.
    Unproven,
}

/// Whether `members` (the live members of the pgid) are the group whose leader is
/// (`leader.0`, `leader.1`), recorded as `recorded`, with its bound `bound_us`. The cheap
/// checks come first, and the one that asks the kernel about a parent only if they all fail.
fn claim(members: &[Seen], leader: (i32, u64), recorded: &[(i32, u64)], bound_us: u64) -> Claim {
    let is = |m: &Seen, (pid, start_us): (i32, u64)| m.pid == pid && m.start_us == start_us;
    let direct = members
        .iter()
        .any(|m| is(m, leader) || recorded.iter().any(|&r| is(m, r)) || m.start_us < bound_us);
    // A member that is the child of a recorded process (which may have left the group): the
    // group is one job, so one member that is recognised is enough.
    let by_parent = || {
        members.iter().any(|m| {
            std::iter::once(&leader)
                .chain(recorded)
                .any(|&(pid, start_us)| {
                    pid == m.ppid && matches!(probe(pid), Probe::Found(p) if p.start_us == start_us)
                })
        })
    };
    if direct || by_parent() {
        Claim::Ours
    } else if members.iter().any(|m| m.stopped) {
        Claim::Unproven
    } else {
        Claim::Elsewhere
    }
}

impl Identity {
    /// `bound` is wall-clock Unix seconds. The members are learnt by `remember`.
    fn of_job(job: &Job, bound: f64) -> Identity {
        Identity {
            pgid: job.pgid,
            leader_pid: job.leader_pid,
            leader_start_us: job.start_us,
            bound_us: wall_us(bound),
            members: Vec::new(),
        }
    }

    fn of_record(rec: &Record) -> Identity {
        Identity {
            pgid: rec.pgid,
            leader_pid: rec.leader_pid,
            leader_start_us: rec.leader_start_us,
            bound_us: wall_us(rec.frozen_at),
            members: rec.members.clone(),
        }
    }

    fn claim(&self, members: &[Seen]) -> Claim {
        claim(
            members,
            (self.leader_pid, self.leader_start_us),
            &self.members,
            self.bound_us,
        )
    }

    /// Adds the live members just seen, so that they are still recognised when the leader
    /// and the others are gone.
    fn remember(&mut self, members: &[Seen]) {
        for m in members {
            if !self.members.contains(&(m.pid, m.start_us)) {
                self.members.push((m.pid, m.start_us));
            }
        }
    }
}

/// Test only (`run --simulate`): the process whose children a signal may reach. 0 when there
/// is no such confinement, which is always the case outside a simulated run.
static SIMULATION_PARENT: AtomicI32 = AtomicI32::new(0);

/// Test only (`run --simulate`): from now on, nothing is signalled unless the leader of its
/// group is alive and a child of `parent`, the test that started the simulated daemon and
/// spawned the processes it may touch. This is the last line of the confinement, behind the
/// eligibility rules: it holds for every signal, including the recovery of a record that an
/// earlier run left, which the rules never see.
pub fn confine_to_children_of(parent: i32) {
    SIMULATION_PARENT.store(parent, Ordering::SeqCst);
}

/// Whether the simulation, if there is one, allows signalling the group `id` names.
fn inside_simulation(id: &Identity) -> bool {
    match SIMULATION_PARENT.load(Ordering::SeqCst) {
        0 => true,
        parent => matches!(
            probe(id.leader_pid),
            Probe::Found(l) if l.start_us == id.leader_start_us && l.ppid == parent
        ),
    }
}

/// Looks at the group and checks that it is still the one in `id`. Leaves its members in
/// `insp`. `Gone` means absent or replaced by another group; a group that could not be
/// listed or read, or that has stopped members that cannot be told from ours, is `Unknown`,
/// never `Gone`.
fn verify(insp: &mut Inspector, id: &Identity) -> Result<(), ActError> {
    match insp.look(id.pgid) {
        Look::Absent => Err(ActError::Gone),
        Look::Unknown(why) => Err(ActError::Unknown(why)),
        Look::Present => match id.claim(insp.members()) {
            Claim::Ours if inside_simulation(id) => Ok(()),
            Claim::Ours => Err(ActError::Unknown(
                "it is outside the simulation's confinement (test only)".into(),
            )),
            Claim::Elsewhere => Err(ActError::Gone),
            Claim::Unproven => Err(ActError::Unknown(
                "its members are stopped, and none is shown to be one membrake stopped".into(),
            )),
        },
    }
}

fn signal_group(pgid: i32, sig: libc::c_int) -> Result<(), ActError> {
    #[cfg(test)]
    if crate::testutil::fault::take(&crate::testutil::fault::SIGNAL) {
        return Err(ActError::Io(io::Error::from_raw_os_error(libc::EPERM)));
    }
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

/// Signals the group, after looking at it once more: every signal is preceded by its own
/// identity check, however recently the last one was made.
fn signal_verified(insp: &mut Inspector, id: &Identity, sig: libc::c_int) -> Result<(), ActError> {
    verify(insp, id)?;
    signal_group(id.pgid, sig)
}

/// Drops the record of a group that is confirmed gone; any other error leaves it. Returns
/// the error for the caller.
fn settle(state: &mut State, pgid: i32, error: ActError) -> ActError {
    if matches!(error, ActError::Gone) {
        let _ = state.remove(pgid);
    }
    error
}

/// How many times `freeze` rewrites its record because the group gained members while the
/// last one was being written.
const RECORD_ROUNDS: usize = 4;

/// SIGSTOP the group. The record, with the members the signal is about to reach, is written
/// and flushed first; the group is then looked at again, and the rules asked again
/// (`allowed` returns the reason the group may no longer be touched, if so), immediately
/// before the signal. The write can take a while, in which time the group can gain members
/// (they are written down too, and the group looked at again) or become something the rules
/// forbid (it is not stopped). A crash between the write and the signal leaves a record for
/// a running job, which recovery thaws harmlessly. `now` is the wall-clock time the process
/// table was scanned, the bound of the group's identity.
pub fn freeze(
    state: &mut State,
    job: &Job,
    now: f64,
    allowed: &mut dyn FnMut() -> Option<String>,
) -> Result<(), ActError> {
    let mut insp = Inspector::new();
    verify(&mut insp, &Identity::of_job(job, now))?;
    let mut record = Record::of(job, now, insp.members());
    state.add(record.clone())?;
    let mut rounds = 0;
    let stopped = loop {
        if let Some(why) = allowed() {
            let _ = state.remove(job.pgid);
            return Err(ActError::Ineligible(why));
        }
        let looked = verify(&mut insp, &Identity::of_record(&record));
        let grown = insp
            .members()
            .iter()
            .any(|m| !record.members.contains(&(m.pid, m.start_us)));
        match looked {
            Ok(()) if grown && rounds < RECORD_ROUNDS => {
                rounds += 1;
                record = Record::of(job, now, insp.members());
                if let Err(e) = state.add(record.clone()) {
                    let _ = state.remove(job.pgid);
                    return Err(e.into());
                }
            }
            // Looked at just now: signal without another look.
            Ok(()) => break signal_group(job.pgid, libc::SIGSTOP),
            Err(e) => break Err(e),
        }
    };
    if let Err(e) = stopped {
        // Nothing was stopped, so there is nothing to promise.
        let _ = state.remove(job.pgid);
        return Err(e);
    }
    Ok(())
}

/// SIGCONT the recorded group, if it is still the same group.
fn thaw(rec: &Record, insp: &mut Inspector) -> Result<(), ActError> {
    signal_verified(insp, &Identity::of_record(rec), libc::SIGCONT)
}

/// SIGCONT the recorded group `pgid`. The record is dropped only once the group is confirmed
/// gone (or replaced) or the SIGCONT has been sent; if the signal fails, or the group cannot
/// be inspected, the record stays so that it can be tried again. A group that was never
/// recorded is `Gone`.
pub fn resume(state: &mut State, pgid: i32) -> Result<(), ActError> {
    let Some(rec) = state.record(pgid).cloned() else {
        return Err(ActError::Gone);
    };
    let result = thaw(&rec, &mut Inspector::new());
    if matches!(result, Ok(()) | Err(ActError::Gone)) {
        // If this write fails the file keeps a record that recovery will find gone or
        // continued: harmless.
        let _ = state.remove(pgid);
    }
    result
}

#[derive(Debug, PartialEq, Eq)]
pub enum Recovered {
    Resumed,
    /// The recorded group is gone, or its pgid now belongs to a newer group: nothing was
    /// signalled.
    Skipped,
    /// The signal failed or the group could not be inspected. The record is kept.
    Failed(String),
}

/// Thaws every recorded job whose group is still the recorded one and drops its record, and
/// the record of every group that is gone. A record whose job could not be continued stays
/// (unreadable lines stay too). Run at start and on shutdown, before anything else.
pub fn recover(state: &mut State) -> io::Result<Vec<(Record, Recovered)>> {
    let mut insp = Inspector::new();
    let (mut report, mut kept) = (Vec::new(), Vec::new());
    for r in std::mem::take(&mut state.records) {
        let outcome = match thaw(&r, &mut insp) {
            Ok(()) => Recovered::Resumed,
            Err(ActError::Gone) => Recovered::Skipped,
            Err(e) => Recovered::Failed(e.to_string()),
        };
        if matches!(outcome, Recovered::Failed(_)) {
            kept.push(r.clone());
        }
        report.push((r, outcome));
    }
    state.records = kept;
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
    /// The group could not be inspected: nothing can be said about it.
    Unknown,
}

/// What became of a job membrake froze. Cheap: it looks at one group only, through the
/// buffers in `insp`, and allocates nothing once they are warm.
pub fn frozen_state(rec: &Record, insp: &mut Inspector) -> FrozenState {
    match insp.look(rec.pgid) {
        Look::Absent => FrozenState::Gone,
        Look::Unknown(_) => FrozenState::Unknown,
        Look::Present => match claim(
            insp.members(),
            (rec.leader_pid, rec.leader_start_us),
            &rec.members,
            wall_us(rec.frozen_at),
        ) {
            Claim::Elsewhere => FrozenState::Gone,
            Claim::Unproven => FrozenState::Unknown,
            Claim::Ours if insp.members().iter().all(|p| p.stopped) => FrozenState::Stopped,
            Claim::Ours => FrozenState::ResumedOutside,
        },
    }
}

/// True once the group is gone or replaced by a newer one; false at the deadline or when
/// shutdown is requested. A group that cannot be inspected is not gone.
fn wait_gone(insp: &mut Inspector, id: &Identity, seconds: f64) -> bool {
    let deadline = signals::uptime() + seconds;
    loop {
        if matches!(verify(insp, id), Err(ActError::Gone)) {
            return true;
        }
        if signals::uptime() >= deadline || shutdown_requested() {
            return false;
        }
        thread::sleep(POLL);
    }
}

/// SIGKILL one member by its own pid, if it is still the process that was seen: same start
/// time, same group, not a zombie. The start time is read again immediately before `kill(2)`,
/// so a pid reissued to a new process since the group was looked at is skipped.
fn kill_member(seen: &Seen) -> bool {
    #[cfg(test)]
    if crate::testutil::fault::take(&crate::testutil::fault::KILL) {
        return false;
    }
    match probe(seen.pid) {
        Probe::Found(now)
            if now.start_us == seen.start_us && now.pgid == seen.pgid && !now.zombie =>
        {
            // SAFETY: kill has no memory-safety preconditions.
            unsafe { libc::kill(seen.pid, libc::SIGKILL) == 0 }
        }
        _ => false,
    }
}

#[derive(Debug, PartialEq, Eq)]
pub enum Killed {
    /// The group is gone (or its pgid now belongs to a newer group). Its record is dropped.
    Exited,
    /// Processes were still there when the wait for SIGKILL ran out. The record is kept, and
    /// the group is continued on later samples until it is confirmed gone.
    Survivors,
    /// `allowed` refused: the group must not be killed (the reason is the text). If it was
    /// frozen it has been continued.
    Spared(String),
}

/// The rules, asked again before a signal: if they now forbid touching the group it is
/// continued (if membrake stopped it) and spared.
fn spare(
    state: &mut State,
    pgid: i32,
    allowed: &mut dyn FnMut() -> Option<String>,
) -> Option<Killed> {
    let why = allowed()?;
    let _ = resume(state, pgid);
    Some(Killed::Spared(why))
}

/// SIGSTOP the group so it stops growing, then (unless `immediate`) SIGTERM and SIGCONT and
/// wait up to `term_seconds`, then SIGKILL the members until the group is gone. An emergency
/// (`immediate`) has no seconds to spare: SIGSTOP, then SIGKILL, with no SIGTERM and no grace.
/// Blocks: only one kill is ever in flight.
///
/// The SIGSTOP of a job that is not frozen is recorded in `state` first, like a freeze, with
/// the members it is about to reach; the record goes when the group is confirmed gone.
/// `scanned` is the wall-clock time the process table was scanned to choose the job (the
/// bound of its identity), unless the job was frozen, when the record's time is the bound.
/// `allowed` is asked, after the record is written, before every pass of signals: the
/// SIGSTOP, the SIGTERM and SIGCONT, and each round of SIGKILLs. It returns the reason the
/// group may no longer be killed, if so, and then nothing more is sent.
pub fn kill(
    state: &mut State,
    job: &Job,
    scanned: f64,
    immediate: bool,
    term_seconds: f64,
    allowed: &mut dyn FnMut() -> Option<String>,
) -> Result<Killed, ActError> {
    let pgid = job.pgid;
    let mut id = match state.record(pgid) {
        Some(rec) => Identity::of_record(rec),
        None => Identity::of_job(job, scanned),
    };
    let mut insp = Inspector::new();

    verify(&mut insp, &id).map_err(|e| settle(state, pgid, e))?;
    // The record first, then the stop. If the record cannot be written the group is not
    // stopped: it is terminated (or killed) running, which no crash can leave frozen.
    let adds_record = state.record(pgid).is_none();
    let may_stop = !adds_record || state.add(Record::of(job, scanned, insp.members())).is_ok();
    id.remember(insp.members());
    if let Some(spared) = spare(state, pgid, allowed) {
        return Ok(spared);
    }
    if may_stop && let Err(e) = signal_verified(&mut insp, &id, libc::SIGSTOP) {
        if adds_record {
            let _ = state.remove(pgid);
        }
        return Err(settle(state, pgid, e));
    }
    // The SIGSTOP succeeded, so we are allowed to signal this group. A later error therefore
    // means the group has already exited or been replaced (every signal looks first, and
    // macOS answers EPERM, not ESRCH, for a zombie that its parent has not reaped yet).
    // Whether it is gone is decided by `wait_gone`.
    if !immediate && term_seconds > 0.0 {
        if let Some(spared) = spare(state, pgid, allowed) {
            return Ok(spared);
        }
        let _ = signal_verified(&mut insp, &id, libc::SIGTERM);
        // A stopped process cannot run its SIGTERM handler.
        let _ = signal_verified(&mut insp, &id, libc::SIGCONT);
        if wait_gone(&mut insp, &id, term_seconds) {
            let _ = state.remove(pgid);
            return Ok(Killed::Exited);
        }
    }
    let deadline = signals::uptime() + KILL_WAIT;
    loop {
        match verify(&mut insp, &id) {
            Err(ActError::Gone) => {
                let _ = state.remove(pgid);
                return Ok(Killed::Exited);
            }
            Ok(()) => {
                // The grace period, or the last round, was long enough for the group to
                // change: ask again before the signal that cannot be taken back.
                if let Some(spared) = spare(state, pgid, allowed) {
                    return Ok(spared);
                }
                id.remember(insp.members());
                for member in insp.members() {
                    kill_member(member);
                }
            }
            // Could not look: signal nothing, look again.
            Err(_) => {}
        }
        if signals::uptime() >= deadline || shutdown_requested() {
            return Ok(Killed::Survivors);
        }
        thread::sleep(POLL);
    }
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
pub(crate) fn private_symbol(name: &std::ffi::CStr) -> Option<*mut libc::c_void> {
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
    use crate::jobs;
    use crate::testutil::{Sleeper, fault, mkfifo, scratch};
    use std::io::Read;

    /// A clock that jumps or a bad deadline must not put the brake to sleep.
    #[test]
    fn no_sleep_is_longer_than_the_sampling_interval() {
        assert_eq!(sleep_length(3600.0), MAX_SLEEP);
        assert_eq!(sleep_length(f64::INFINITY), MAX_SLEEP);
        assert_eq!(sleep_length(-5.0), 0.0);
        assert_eq!(sleep_length(0.4), 0.4);
    }

    /// Freezes with the rules never objecting, as a plain eligible job.
    fn freeze_plain(state: &mut State, job: &Job, now: f64) -> Result<(), ActError> {
        freeze(state, job, now, &mut || None)
    }

    fn records_on_disk(dir: &Path) -> usize {
        read_state(dir).unwrap().0.len()
    }

    /// Kills with the rules never objecting, as a plain eligible job.
    fn kill_job(
        state: &mut State,
        job: &Job,
        immediate: bool,
        term_seconds: f64,
        scanned: f64,
    ) -> Result<Killed, ActError> {
        kill(state, job, scanned, immediate, term_seconds, &mut || None)
    }

    /// The wall-clock time, in seconds, `micros` after the start of a process.
    fn secs_after(start_us: u64, micros: i64) -> f64 {
        (start_us as i64 + micros) as f64 / 1e6
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
            freeze_plain(&mut first_run, &job, signals::wall()).unwrap();
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
    /// has the same pgid. The newer group's members all started after the record, and none is
    /// one the record lists.
    #[test]
    fn recovery_leaves_alone_a_pid_that_was_reused() {
        let dir = scratch("reuse");
        let sleeper = Sleeper::spawn();
        let job = sleeper.job(1);
        let mut state = State::open(&dir).unwrap();
        freeze_plain(&mut state, &job, signals::wall()).unwrap();
        assert!(sleeper.settles(true));
        // Rewrite the record as if it described an earlier group frozen long before this
        // process existed, and the process is running: someone else's.
        signal_group(job.pgid, libc::SIGCONT).unwrap();
        assert!(sleeper.settles(false));
        state.records[0].leader_start_us -= 5_000_000;
        state.records[0].frozen_at = job.start_us as f64 / 1e6 - 100.0;
        state.records[0].members.clear();
        state.save().unwrap();

        let mut next = State::open(&dir).unwrap();
        let report = recover(&mut next).unwrap();
        assert_eq!(report[0].1, Recovered::Skipped);
        assert_eq!(records_on_disk(&dir), 0, "the stale record is dropped");
        let _ = fs::remove_dir_all(&dir);
    }

    /// The same record, but what has the pgid now is stopped: stopped members are what a
    /// frozen job leaves behind, so the group is neither signalled nor forgotten, and the
    /// record stays to be looked at again. (A group nothing recognises in which nothing is
    /// stopped is plainly somebody else's; see above.)
    #[test]
    fn a_stopped_group_nothing_recognises_is_unknown_never_gone() {
        let dir = scratch("unproven");
        let sleeper = Sleeper::spawn();
        let job = sleeper.job(1);
        let mut state = State::open(&dir).unwrap();
        freeze_plain(&mut state, &job, signals::wall()).unwrap();
        assert!(sleeper.settles(true));
        state.records[0].leader_start_us -= 5_000_000;
        state.records[0].frozen_at = job.start_us as f64 / 1e6 - 100.0;
        state.records[0].members.clear();
        state.save().unwrap();
        let rec = state.records[0].clone();

        let mut insp = Inspector::new();
        assert_eq!(frozen_state(&rec, &mut insp), FrozenState::Unknown);
        assert!(matches!(
            resume(&mut state, job.pgid),
            Err(ActError::Unknown(_))
        ));
        let report = recover(&mut state).unwrap();
        assert!(matches!(report[0].1, Recovered::Failed(_)), "{report:?}");
        assert_eq!(records_on_disk(&dir), 1, "the record stays");
        assert!(sleeper.stopped(), "and nothing was signalled");
        let _ = fs::remove_dir_all(&dir);
    }

    /// The case that was lost: a member forked after the bound (the scan) and before the
    /// freeze, both stopped, the leader killed by hand. The child is not older than the
    /// bound, but it is one of the processes the freeze went to, and it must be continued.
    #[test]
    fn recovery_resumes_a_child_forked_after_the_scan_when_the_leader_died() {
        let dir = scratch("forked-after");
        let mut leader = Sleeper::spawn();
        let job = leader.job(2);
        let scanned = signals::wall();
        let child = Sleeper::spawn_in(leader.pid(), false);
        assert!(
            jobs::bsd_info(child.pid()).unwrap().start_us as f64 / 1e6 > scanned,
            "the premise: the child started after the bound"
        );
        let mut state = State::open(&dir).unwrap();
        freeze_plain(&mut state, &job, scanned).unwrap();
        assert!(child.settles(true), "the whole group is stopped");
        assert_eq!(state.records()[0].members.len(), 2, "and both are recorded");

        leader.die();
        // A restart: the record is read back from the file.
        let mut next = State::open(&dir).unwrap();
        let rec = next.records()[0].clone();
        assert_eq!(rec.members.len(), 2);
        assert_eq!(
            frozen_state(&rec, &mut Inspector::new()),
            FrozenState::Stopped,
            "still the frozen group, not a forgotten one"
        );
        let report = recover(&mut next).unwrap();
        assert_eq!(report[0].1, Recovered::Resumed);
        assert!(child.settles(false), "the child runs again");
        assert_eq!(records_on_disk(&dir), 0);
        let _ = fs::remove_dir_all(&dir);
    }

    /// The write of the record can take a while, and the group gains a member in it. That
    /// member is written down too (the record is rewritten and the group looked at again)
    /// before anything is stopped, so it too is found after the leader has gone.
    #[test]
    fn freeze_records_the_members_the_group_gains_while_its_record_is_written() {
        let dir = scratch("gains");
        let mut leader = Sleeper::spawn();
        let job = leader.job(1);
        let scanned = signals::wall();
        let mut state = State::open(&dir).unwrap();
        let tmp = dir.join(format!("{STATE_FILE}.tmp"));
        mkfifo(&tmp);

        let freezing = thread::spawn(move || {
            let result = freeze_plain(&mut state, &job, scanned);
            (state, result)
        });
        thread::sleep(Duration::from_millis(300));
        let child = Sleeper::spawn_in(leader.pid(), false); // while the first write waits
        let mut first = String::new();
        fs::File::open(&tmp)
            .unwrap()
            .read_to_string(&mut first)
            .unwrap();
        assert_eq!(first.matches(':').count(), 1, "one member: {first}");

        // The group is looked at again, found to have grown, and the record written again,
        // before the signal.
        let (mut state, result) = freezing.join().unwrap();
        result.unwrap();
        assert_eq!(state.records()[0].members.len(), 2, "both are listed");
        assert_eq!(
            read_state(&dir).unwrap().0[0].members.len(),
            2,
            "and on disk"
        );
        assert!(
            child.settles(true) && leader.settles(true),
            "both are stopped"
        );
        leader.die();
        let report = recover(&mut state).unwrap();
        assert_eq!(report[0].1, Recovered::Resumed);
        assert!(child.settles(false));
        let _ = fs::remove_dir_all(&dir);
    }

    /// Who is recognised as a recorded group's, member by member: its leader, a process the
    /// freeze went to, anything older than the bound, and a child of a recorded process even
    /// if that has left the group. Anything else is another group's, or, if everything in the
    /// group is stopped, not to be told apart from ours.
    #[test]
    fn a_member_is_recognised_by_identity_by_age_or_by_its_recorded_parent() {
        let parent = Sleeper::spawn();
        let parent_start = jobs::bsd_info(parent.pid()).unwrap().start_us;
        let seen = |pid: i32, ppid: i32, start_us: u64, stopped: bool| Seen {
            pid,
            ppid,
            pgid: 4000,
            uid: 501,
            start_us,
            zombie: false,
            stopped,
        };
        let bound = 1_000_000;
        let recorded = [(7001, 500_000), (parent.pid(), parent_start)];
        let leader = (7000, 400_000);
        let c = |members: &[Seen]| claim(members, leader, &recorded, bound);

        assert_eq!(
            c(&[seen(7000, 1, 400_000, true)]),
            Claim::Ours,
            "the leader"
        );
        assert_eq!(
            c(&[seen(7001, 1, 500_000, true)]),
            Claim::Ours,
            "a recorded member"
        );
        assert_eq!(
            c(&[seen(7002, 1, 900_000, true)]),
            Claim::Ours,
            "older than the bound"
        );
        assert_eq!(
            c(&[seen(7003, parent.pid(), 2_000_000, true)]),
            Claim::Ours,
            "the child of a recorded process that is still that process"
        );
        // The pids are the same and the processes are not: reissued.
        assert_eq!(
            c(&[seen(7000, 1, 2_000_001, true)]),
            Claim::Unproven,
            "the leader's pid, reissued"
        );
        assert_eq!(
            c(&[seen(7001, 1, 2_000_001, false)]),
            Claim::Elsewhere,
            "a recorded member's pid, reissued to a running process"
        );
        let reissued = [(parent.pid(), parent_start + 1)];
        assert_eq!(
            claim(
                &[seen(7003, parent.pid(), 2_000_000, true)],
                leader,
                &reissued,
                bound
            ),
            Claim::Unproven,
            "the child of a pid that is another process now"
        );
        assert_eq!(
            c(&[
                seen(7003, 1, 2_000_000, false),
                seen(7004, 1, 2_000_000, false)
            ]),
            Claim::Elsewhere,
            "nothing stopped: somebody else's, running"
        );
        assert_eq!(
            c(&[
                seen(7003, 1, 2_000_000, true),
                seen(7004, 1, 2_000_000, false)
            ]),
            Claim::Unproven,
            "a stopped member that cannot be told from ours is never called gone"
        );
        assert_eq!(
            c(&[
                seen(7003, 1, 2_000_000, true),
                seen(7000, 1, 400_000, false)
            ]),
            Claim::Ours,
            "one member that is ours is enough"
        );
    }

    /// A record from before records listed their members (an older version wrote it) is
    /// still read, and one with a damaged member entry loses that entry and no more: a line
    /// that cannot be read is a job that is never resumed.
    #[test]
    fn records_with_and_without_members_are_read_and_written_back() {
        let old = "4000\t4000\t123\t1700000000.000\t2048\t501\ttsc --build";
        let rec = Record::parse(old).unwrap();
        assert!(rec.members.is_empty());
        assert_eq!(rec.line(), format!("{old}\n"), "written as it was read");

        let new = format!("{old}\t4000:123,4001:456");
        let rec = Record::parse(&new).unwrap();
        assert_eq!(rec.members, [(4000, 123), (4001, 456)]);
        assert_eq!(rec.line(), format!("{new}\n"));

        let damaged = Record::parse(&format!("{old}\t4000:123,junk,4002:x,4003:9")).unwrap();
        assert_eq!(damaged.members, [(4000, 123), (4003, 9)]);
        assert_eq!(damaged.command, "tsc --build");
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
        freeze_plain(&mut state, &job, signals::wall()).unwrap();
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
        let dir = scratch("kill-survivor");
        let mut leader = Sleeper::spawn();
        let member = Sleeper::spawn_in(leader.pid(), false);
        let job = leader.job(2);
        let frozen_at = signals::wall();
        leader.die();
        let mut state = State::open(&dir).unwrap();
        assert_eq!(
            kill_job(&mut state, &job, true, 5.0, frozen_at).unwrap(),
            Killed::Exited
        );
        let mut insp = Inspector::new();
        assert!(matches!(insp.look(job.pgid), Look::Absent));
        drop(member);
        let _ = fs::remove_dir_all(&dir);
    }

    /// A record whose group has exited has nothing to resume and is dropped.
    #[test]
    fn record_of_an_exited_group_is_dropped() {
        let dir = scratch("exited");
        let mut sleeper = Sleeper::spawn();
        let job = sleeper.job(1);
        let mut state = State::open(&dir).unwrap();
        freeze_plain(&mut state, &job, signals::wall()).unwrap();
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
        freeze_plain(&mut state, &job, signals::wall()).unwrap();
        assert!(sleeper.settles(true));
        assert_eq!(records_on_disk(&dir), 1);
        resume(&mut state, job.pgid).unwrap();
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
        freeze_plain(&mut state, &job, signals::wall()).unwrap();
        assert!(sleeper.settles(true));
        let rec = state.record(job.pgid).unwrap().clone();
        let mut insp = Inspector::new();
        assert_eq!(frozen_state(&rec, &mut insp), FrozenState::Stopped);

        signal_group(job.pgid, libc::SIGCONT).unwrap(); // `kill -CONT`, not membrake
        assert!(sleeper.settles(false));
        assert_eq!(frozen_state(&rec, &mut insp), FrozenState::ResumedOutside);

        sleeper.die();
        assert_eq!(frozen_state(&rec, &mut insp), FrozenState::Gone);
        let _ = fs::remove_dir_all(&dir);
    }

    // -----------------------------------------------------------------------------------
    // Findings: a record outlives anything short of the group being gone or continued.

    /// A group the kernel could not list is not a group that is gone. Nothing that follows
    /// from "gone" (a dropped record, a forgotten job) may follow from "unknown".
    #[test]
    fn a_group_that_cannot_be_listed_is_unknown_never_gone() {
        let dir = scratch("unknown");
        let sleeper = Sleeper::spawn();
        let job = sleeper.job(1);
        let mut state = State::open(&dir).unwrap();
        freeze_plain(&mut state, &job, signals::wall()).unwrap();
        assert!(sleeper.settles(true));
        let rec = state.record(job.pgid).unwrap().clone();
        let mut insp = Inspector::new();

        fault::arm(&fault::LIST, 1);
        assert_eq!(frozen_state(&rec, &mut insp), FrozenState::Unknown);

        fault::arm(&fault::LIST, 1);
        assert!(matches!(
            resume(&mut state, job.pgid),
            Err(ActError::Unknown(_))
        ));
        assert_eq!(records_on_disk(&dir), 1, "the record stays");
        assert!(sleeper.stopped(), "and nothing was signalled");

        fault::arm(&fault::LIST, 1);
        let report = recover(&mut state).unwrap();
        assert!(matches!(report[0].1, Recovered::Failed(_)));
        assert_eq!(records_on_disk(&dir), 1, "recovery keeps it too");
        assert_eq!(state.records().len(), 1);

        // Once the group can be read again the same record is resolved.
        resume(&mut state, job.pgid).unwrap();
        assert!(sleeper.settles(false));
        assert_eq!(records_on_disk(&dir), 0);
        let _ = fs::remove_dir_all(&dir);
    }

    /// A SIGCONT that fails is not a SIGCONT that was sent: `resume` and `recover` keep the
    /// record, and the job is continued the next time it works.
    #[test]
    fn a_failed_sigcont_leaves_the_record_to_be_tried_again() {
        let dir = scratch("sigcont");
        let sleeper = Sleeper::spawn();
        let job = sleeper.job(1);
        let mut state = State::open(&dir).unwrap();
        freeze_plain(&mut state, &job, signals::wall()).unwrap();
        assert!(sleeper.settles(true));

        fault::arm(&fault::SIGNAL, 1);
        assert!(matches!(resume(&mut state, job.pgid), Err(ActError::Io(_))));
        assert_eq!(records_on_disk(&dir), 1);
        assert!(sleeper.stopped());

        fault::arm(&fault::SIGNAL, 1);
        let report = recover(&mut state).unwrap();
        assert!(matches!(report[0].1, Recovered::Failed(_)));
        assert_eq!(records_on_disk(&dir), 1);

        let report = recover(&mut state).unwrap();
        assert_eq!(report[0].1, Recovered::Resumed);
        assert!(sleeper.settles(false));
        assert_eq!(records_on_disk(&dir), 0);
        let _ = fs::remove_dir_all(&dir);
    }

    // -----------------------------------------------------------------------------------
    // Findings: identity is checked at the signal, from the bound taken at scan time.

    /// A group that merely reuses the pgid of the one that was scanned: its leader is another
    /// process, and everything in it started after the sample. Nothing is signalled to it.
    /// (Worked out at dispatch, the bound would be later than that group's start.)
    #[test]
    fn a_kill_never_signals_a_group_that_only_reuses_the_pgid() {
        let dir = scratch("reuse-kill");
        let sleeper = Sleeper::spawn();
        let mut job = sleeper.job(1);
        let scanned = secs_after(job.start_us, -1_000_000);
        job.start_us -= 5_000_000; // the leader that was scanned is not this process
        let mut state = State::open(&dir).unwrap();

        let result = kill_job(&mut state, &job, true, 1.0, scanned);
        assert!(matches!(result, Err(ActError::Gone)), "{result:?}");
        assert!(sleeper.0.id() > 0 && !sleeper.stopped(), "not even stopped");
        assert!(state.records().is_empty() && records_on_disk(&dir) == 0);
        let _ = fs::remove_dir_all(&dir);
    }

    /// Writing the record can take a while. The group is looked at again after it, and a
    /// group that has changed meanwhile is not stopped. Here the write is held on a named
    /// pipe while a member joins and the leader dies, leaving only a member that started
    /// after the bound and that the record does not list: another group's.
    #[test]
    fn freeze_looks_at_the_group_again_after_the_record_is_written() {
        let dir = scratch("freeze-again");
        let mut leader = Sleeper::spawn();
        let job = leader.job(1);
        let bound = signals::wall();
        let mut state = State::open(&dir).unwrap();
        let tmp = dir.join(format!("{STATE_FILE}.tmp"));
        mkfifo(&tmp);

        let freezing = thread::spawn(move || {
            let result = freeze_plain(&mut state, &job, bound);
            (state, result)
        });
        thread::sleep(Duration::from_millis(300));
        assert!(!freezing.is_finished(), "the write is waiting for a reader");
        let member = Sleeper::spawn_in(leader.pid(), false);
        leader.die(); // what is left is a member the bound says is a newer group
        let mut written = String::new();
        fs::File::open(&tmp)
            .unwrap()
            .read_to_string(&mut written)
            .unwrap();
        assert!(written.starts_with(&job_pgid_text(&member)), "{written}");

        let (state, result) = freezing.join().unwrap();
        assert!(matches!(result, Err(ActError::Gone)), "{result:?}");
        assert!(!member.stopped(), "the newer group was not stopped");
        assert!(state.records().is_empty(), "and nothing is promised");
        let _ = fs::remove_dir_all(&dir);
    }

    fn job_pgid_text(member: &Sleeper) -> String {
        format!("{}\t", jobs::bsd_info(member.pid()).unwrap().pgid)
    }

    /// While a kill waits out the grace period the group is looked at again each time: once
    /// the leader is gone and only a member that started after the bound, and that was not
    /// there when the kill began, is left, the group that was meant is gone, and the member
    /// is not killed.
    #[test]
    fn a_kill_stops_when_what_is_left_of_the_group_is_a_newer_one() {
        let dir = scratch("kill-newer");
        let mut leader = Sleeper::spawn_in(0, true);
        let job = leader.job(1);
        let scanned = signals::wall();
        let mut state = State::open(&dir).unwrap();

        let killing = thread::spawn(move || {
            let result = kill_job(&mut state, &job, false, 3.0, scanned);
            (state, result)
        });
        thread::sleep(Duration::from_millis(500)); // stopped, TERM ignored, waiting
        let member = Sleeper::spawn_in(leader.pid(), true);
        leader.die();
        let started = std::time::Instant::now();
        let (state, result) = killing.join().unwrap();
        assert_eq!(result.unwrap(), Killed::Exited);
        assert!(started.elapsed() < Duration::from_secs(2), "not the grace");
        assert!(state.records().is_empty());
        assert!(
            member.0.id() > 0 && member.settles(false),
            "the member runs on"
        );
        assert!(!member.stopped());
        let _ = fs::remove_dir_all(&dir);
    }

    /// SIGKILL goes to a member only if it is still the process that was seen.
    #[test]
    fn sigkill_skips_a_member_whose_start_time_changed() {
        let mut sleeper = Sleeper::spawn();
        let p = jobs::probe(sleeper.pid());
        let jobs::Probe::Found(seen) = p else {
            panic!("{p:?}")
        };
        let reissued = Seen {
            start_us: seen.start_us + 1,
            ..seen
        };
        assert!(!kill_member(&reissued), "a pid with another start time");
        assert!(sleeper.0.try_wait().unwrap().is_none(), "is left alone");
        assert!(kill_member(&seen));
        assert!(sleeper.0.wait().is_ok());
        sleeper.die();
    }

    // -----------------------------------------------------------------------------------
    // Findings: every SIGSTOP is recorded first.

    /// A kill starts with a SIGSTOP of a job membrake never froze. That stop is on disk
    /// before it is sent: here the write is held on a named pipe, and nothing is stopped
    /// until it is let through.
    #[test]
    fn a_kill_records_its_stop_before_it_sends_it() {
        let dir = scratch("kill-record");
        let sleeper = Sleeper::spawn();
        let job = sleeper.job(1);
        let record_starts = job_pgid_text(&sleeper);
        let mut state = State::open(&dir).unwrap();
        let tmp = dir.join(format!("{STATE_FILE}.tmp"));
        mkfifo(&tmp);

        let killing = thread::spawn(move || {
            let result = kill_job(&mut state, &job, true, 1.0, signals::wall());
            (state, result)
        });
        thread::sleep(Duration::from_millis(300));
        assert!(!killing.is_finished(), "the kill waits for its record");
        assert!(!sleeper.stopped(), "so nothing has been stopped yet");
        let mut written = String::new();
        fs::File::open(&tmp)
            .unwrap()
            .read_to_string(&mut written)
            .unwrap();
        assert!(written.starts_with(&record_starts), "{written}");

        let (state, result) = killing.join().unwrap();
        assert_eq!(result.unwrap(), Killed::Exited);
        assert!(state.records().is_empty(), "gone, so no promise remains");
        let _ = fs::remove_dir_all(&dir);
    }

    /// If the record cannot be written the group is not stopped, but it is still killed:
    /// no crash can leave it frozen, and a full disk must not stop the brake.
    #[test]
    fn a_kill_still_kills_when_its_record_cannot_be_written() {
        let dir = scratch("kill-no-record");
        let sleeper = Sleeper::spawn();
        let job = sleeper.job(1);
        let mut state = State::open(&dir).unwrap();
        fs::set_permissions(&dir, fs::Permissions::from_mode(0o555)).unwrap();

        let result = kill_job(&mut state, &job, true, 1.0, signals::wall());
        fs::set_permissions(&dir, fs::Permissions::from_mode(0o755)).unwrap();
        assert_eq!(result.unwrap(), Killed::Exited);
        assert!(state.records().is_empty());
        let _ = fs::remove_dir_all(&dir);
    }

    // -----------------------------------------------------------------------------------
    // Findings: the rules are asked again before anything is killed.

    /// A job that is no longer eligible is not killed, and one membrake froze is continued.
    #[test]
    fn a_kill_is_refused_for_a_group_the_rules_now_exclude_and_it_is_continued() {
        let dir = scratch("spared");
        let mut sleeper = Sleeper::spawn();
        let job = sleeper.job(1);
        let mut state = State::open(&dir).unwrap();
        freeze_plain(&mut state, &job, signals::wall()).unwrap();
        assert!(sleeper.settles(true));

        let result = kill(&mut state, &job, 0.0, true, 1.0, &mut || {
            Some("it is now excluded".into())
        });
        assert_eq!(result.unwrap(), Killed::Spared("it is now excluded".into()));
        assert!(sleeper.settles(false), "continued");
        assert!(sleeper.0.try_wait().unwrap().is_none(), "and alive");
        assert_eq!(records_on_disk(&dir), 0);
        sleeper.die();
        let _ = fs::remove_dir_all(&dir);
    }

    /// The rules are asked before every pass of signals, the last of them after the grace
    /// period and before the first SIGKILL: a group that has become ineligible by then is
    /// continued, not killed.
    #[test]
    fn the_rules_are_asked_again_before_sigkill() {
        let dir = scratch("spared-late");
        let sleeper = Sleeper::spawn_in(0, true);
        let job = sleeper.job(1);
        let mut state = State::open(&dir).unwrap();
        let mut asked = 0;
        let started = std::time::Instant::now();
        let result = kill(&mut state, &job, signals::wall(), false, 1.0, &mut || {
            asked += 1;
            (asked > 2).then(|| "it changed".to_string())
        });
        assert_eq!(result.unwrap(), Killed::Spared("it changed".into()));
        assert!(
            started.elapsed() >= Duration::from_secs(1),
            "after the grace"
        );
        assert_eq!(
            asked, 3,
            "before the stop, before the SIGTERM, before the SIGKILL"
        );
        assert!(sleeper.settles(false) && sleeper.job(1).pgid == job.pgid);
        assert!(state.records().is_empty());
        let _ = fs::remove_dir_all(&dir);
    }

    /// And before every round of SIGKILLs, not only the first: here the first round misses
    /// its member, the rules then forbid, and the second round is not sent.
    #[test]
    fn the_rules_are_asked_again_before_every_round_of_sigkills() {
        let dir = scratch("spared-rounds");
        let sleeper = Sleeper::spawn();
        let job = sleeper.job(1);
        let mut state = State::open(&dir).unwrap();
        fault::arm(&fault::KILL, 1);
        let mut asked = 0;
        let result = kill(&mut state, &job, signals::wall(), true, 1.0, &mut || {
            asked += 1;
            (asked > 2).then(|| "it changed".to_string())
        });
        assert_eq!(result.unwrap(), Killed::Spared("it changed".into()));
        assert_eq!(asked, 3, "before the stop, and before each of two rounds");
        assert!(sleeper.settles(false), "continued, and still alive");
        assert!(state.records().is_empty() && records_on_disk(&dir) == 0);
        let _ = fs::remove_dir_all(&dir);
    }

    /// The record is written, then the rules are asked, then the group is stopped: a group
    /// the rules forbid by the time the write is done is not stopped (here a leader that
    /// becomes something else while the write is held on a named pipe), and nothing is
    /// promised.
    #[test]
    fn freeze_asks_the_rules_again_after_its_record_is_written() {
        let dir = scratch("freeze-rules");
        let sleeper = Sleeper::spawn();
        let job = sleeper.job(1);
        let mut state = State::open(&dir).unwrap();
        let tmp = dir.join(format!("{STATE_FILE}.tmp"));
        mkfifo(&tmp);
        let forbidden = std::sync::Arc::new(AtomicBool::new(false));
        let rules = std::sync::Arc::clone(&forbidden);

        let freezing = thread::spawn(move || {
            let result = freeze(&mut state, &job, signals::wall(), &mut || {
                rules
                    .load(Ordering::SeqCst)
                    .then(|| "it changed".to_string())
            });
            (state, result)
        });
        thread::sleep(Duration::from_millis(300));
        assert!(!freezing.is_finished(), "the write is waiting for a reader");
        forbidden.store(true, Ordering::SeqCst); // what the rules saw before the write is stale
        let mut written = String::new();
        fs::File::open(&tmp)
            .unwrap()
            .read_to_string(&mut written)
            .unwrap();

        let (state, result) = freezing.join().unwrap();
        assert!(matches!(result, Err(ActError::Ineligible(_))), "{result:?}");
        assert!(!sleeper.stopped(), "not stopped");
        assert!(state.records().is_empty() && records_on_disk(&dir) == 0);
        let _ = fs::remove_dir_all(&dir);
    }

    /// The same for a kill: the record of its stop is written, and the rules are asked once
    /// that is done. A group they forbid by then is neither stopped nor killed.
    #[test]
    fn a_kill_asks_the_rules_again_after_its_record_is_written() {
        let dir = scratch("kill-rules");
        let sleeper = Sleeper::spawn();
        let job = sleeper.job(1);
        let mut state = State::open(&dir).unwrap();
        let tmp = dir.join(format!("{STATE_FILE}.tmp"));
        mkfifo(&tmp);
        let forbidden = std::sync::Arc::new(AtomicBool::new(false));
        let rules = std::sync::Arc::clone(&forbidden);

        let killing = thread::spawn(move || {
            let result = kill(&mut state, &job, signals::wall(), true, 1.0, &mut || {
                rules
                    .load(Ordering::SeqCst)
                    .then(|| "it changed".to_string())
            });
            (state, result)
        });
        thread::sleep(Duration::from_millis(300));
        assert!(!killing.is_finished(), "the write is waiting for a reader");
        forbidden.store(true, Ordering::SeqCst);
        let mut written = String::new();
        fs::File::open(&tmp)
            .unwrap()
            .read_to_string(&mut written)
            .unwrap();

        let (state, result) = killing.join().unwrap();
        assert_eq!(result.unwrap(), Killed::Spared("it changed".into()));
        assert!(!sleeper.stopped(), "not stopped");
        assert!(sleeper.0.id() > 0 && sleeper.settles(false), "and alive");
        assert!(state.records().is_empty() && records_on_disk(&dir) == 0);
        let _ = fs::remove_dir_all(&dir);
    }

    // -----------------------------------------------------------------------------------
    // Kill sequences

    /// A non-emergency kill stops the group, sends SIGTERM and returns once the group is gone,
    /// well before the grace period runs out. (A plain `sleep` dies of SIGTERM even while
    /// stopped, so this does not show that SIGCONT is needed for a process with a handler.)
    #[test]
    fn kill_ends_a_stopped_group() {
        let dir = scratch("kill-term");
        let sleeper = Sleeper::spawn();
        let job = sleeper.job(1);
        let mut state = State::open(&dir).unwrap();
        let started = std::time::Instant::now();
        assert_eq!(
            kill_job(&mut state, &job, false, 5.0, signals::wall()).unwrap(),
            Killed::Exited
        );
        let mut insp = Inspector::new();
        assert!(matches!(insp.look(job.pgid), Look::Absent));
        // It ended on SIGTERM, not by waiting out the 5 s grace period for SIGKILL.
        assert!(
            started.elapsed() < Duration::from_secs(3),
            "{:?}",
            started.elapsed()
        );
        let _ = fs::remove_dir_all(&dir);
    }

    /// A job that ignores SIGTERM is waited for through the grace period and then SIGKILLed.
    #[test]
    fn kill_escalates_to_sigkill_when_sigterm_is_ignored() {
        let dir = scratch("kill-escalate");
        let sleeper = Sleeper::spawn_in(0, true);
        let job = sleeper.job(1);
        let mut state = State::open(&dir).unwrap();
        let started = std::time::Instant::now();
        assert_eq!(
            kill_job(&mut state, &job, false, 1.0, signals::wall()).unwrap(),
            Killed::Exited
        );
        let took = started.elapsed();
        let mut insp = Inspector::new();
        assert!(matches!(insp.look(job.pgid), Look::Absent));
        assert!(
            took >= Duration::from_secs(1) && took < Duration::from_secs(8),
            "SIGTERM was ignored, so the 1 s grace period must pass first: {took:?}"
        );
        let _ = fs::remove_dir_all(&dir);
    }

    /// An emergency has no seconds to spare: no SIGTERM, no grace. The job here ignores
    /// SIGTERM, and still dies at once.
    #[test]
    fn immediate_kill_skips_the_grace_period() {
        let dir = scratch("kill-now");
        let sleeper = Sleeper::spawn_in(0, true);
        let job = sleeper.job(1);
        let mut state = State::open(&dir).unwrap();
        let started = std::time::Instant::now();
        assert_eq!(
            kill_job(&mut state, &job, true, 5.0, signals::wall()).unwrap(),
            Killed::Exited
        );
        assert!(started.elapsed() < Duration::from_secs(3));
        let _ = fs::remove_dir_all(&dir);
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
        freeze_plain(&mut state, &job, signals::wall()).unwrap();
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
