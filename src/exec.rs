//! `membrake exec -- COMMAND ...`: asks the daemon for a turn, waits for the go-ahead, runs
//! the command, and reports how it ended.
//!
//! The client never stands between a command and the machine for longer than the daemon says.
//! If the socket is missing or refuses, or the daemon sends an error, hangs up, does not answer
//! the request within `FIRST_ANSWER`, or says nothing at all for `wire::SILENCE` after that (it
//! tells every client that waits where it stands at least every `wire::KEEP_ALIVE`, for as long
//! as its sampling loop is scheduling the queue), it says so
//! on one line of stderr and runs the command at once. A command
//! started by one that was already admitted (`MEMBRAKE_ADMITTED` is set) is run directly, so
//! the heavy commands a heavy command starts do not queue behind it.
//!
//! The command runs in a process group of its own (the job the brake freezes and the queue
//! measures), below interactive apps: at nice 10 and under macOS's utility QoS clamp, the way
//! `taskpolicy -c utility nice -n 10` would run it. The exit code is the command's; a command
//! that dies of a signal gives 128 plus the signal, and 127 and 126 are a command that is not
//! found and one that cannot be run, as in a shell.
//!
//! When the command has exited, whatever it left running in that group is stopped if the job
//! was interrupted (a signal was passed on to it, or the command died of one) or `--reap` was
//! given: `SIGTERM` to the group, then `SIGKILL` if anything is left after `REAP_GRACE`, until
//! the group is empty or `KILL_WAIT` has passed. Without either, one line on stderr says the
//! group still has processes and they are left alone. The group is the one this process
//! created when it started the command, never one it looked up; its number stays the command's
//! throughout, because the command is reaped only afterwards (see "What the command leaves
//! running"). Whether anything is left is read from one kernel list of the group's pids (see
//! `group_state`), so a member that forks and exits cannot hide its child; a group that keeps
//! forking faster than it is stopped may still outlast `KILL_WAIT`, and is then reported. A
//! process that left the group (`setsid`, `setpgid`) is not followed. Stopping a
//! job's leftovers is this, so that scripts and agents never search the process table for them
//! (nix-config issue 270: on 2026-11-11 such a search reached every process group).

use crate::act::private_symbol;
use crate::jobs;
use crate::wire::{self, Message, Reply};
use std::ffi::{CString, OsStr, OsString};
use std::io::{self, Read, Write};
use std::os::unix::ffi::OsStrExt;
use std::os::unix::net::UnixStream;
use std::os::unix::process::CommandExt;
use std::path::{Path, PathBuf};
use std::process::Command;
use std::sync::atomic::{AtomicBool, AtomicI32, Ordering};
use std::time::{Duration, Instant};

/// Set in the environment of every command the queue let through (or that ran because the
/// queue was unavailable). A `membrake exec` inside such a command runs its command directly.
pub const ADMITTED: &str = "MEMBRAKE_ADMITTED";
/// How long the daemon has to answer a request at all. Waiting for a turn takes as long as it
/// takes, but a daemon that never says even where the request stands is broken. After the
/// first answer the limit is `wire::SILENCE` between lines, which the daemon's keep-alive
/// (`wire::KEEP_ALIVE`) never lets a running daemon reach.
const FIRST_ANSWER: Duration = Duration::from_secs(10);
/// How often a waiting client looks up from the socket to see whether it was signalled.
const LOOK_UP: Duration = Duration::from_millis(250);
/// `POSIX_SPAWN_PROC_CLAMP_UTILITY`, from xnu's `spawn_internal.h`: what `taskpolicy -c
/// utility` asks for.
const CLAMP_UTILITY: u64 = 1;
/// Heavy work runs below interactive apps. A caller's lower priority is never raised.
const BACKGROUND_NICE: libc::c_int = 10;
/// Where a shell looks for a program when `PATH` is not set.
const DEFAULT_PATH: &str = "/usr/bin:/bin";

type SetQosClamp = unsafe extern "C" fn(*mut libc::posix_spawnattr_t, u64) -> libc::c_int;

pub struct Args {
    /// What the queue shows for this command; its signature if None.
    pub label: Option<String>,
    pub socket: PathBuf,
    pub command: Vec<OsString>,
    /// Stop what the command leaves running in its group when it ends, even when nothing
    /// interrupted it.
    pub reap: bool,
}

/// Best-effort stderr line. Not `eprintln!`, which panics when stderr is closed.
fn say(message: &str) {
    let _ = writeln!(io::stderr(), "membrake: {message}");
}

// ---------------------------------------------------------------------------------------
// Signals

/// A signal that arrived before there was a command to pass it to.
static PENDING: AtomicI32 = AtomicI32::new(0);
/// The command's process group, once it runs.
static GROUP: AtomicI32 = AtomicI32::new(0);
/// Set once a signal has been passed on to the command's group: the job was interrupted.
static FORWARDED: AtomicBool = AtomicBool::new(false);

extern "C" fn on_signal(sig: libc::c_int) {
    let group = GROUP.load(Ordering::SeqCst);
    if group > 0 {
        // SAFETY: killpg is async-signal-safe.
        unsafe { libc::killpg(group, sig) };
        FORWARDED.store(true, Ordering::SeqCst);
    } else {
        PENDING.store(sig, Ordering::SeqCst);
    }
}

/// SIGINT, SIGTERM and SIGHUP go to the command's whole group (the terminal and an outer
/// killer reach only this process, not the group the command leads). No SA_RESTART, so a
/// client waiting for its turn wakes at once. A signal that was ignored stays ignored, as it
/// would be for the command itself (`nohup`, a shell's background command).
fn install_signal_handlers() {
    for sig in [libc::SIGINT, libc::SIGTERM, libc::SIGHUP] {
        // SAFETY: zeroed sigactions, read and then set with a handler that only touches atomics.
        unsafe {
            let mut old: libc::sigaction = std::mem::zeroed();
            if libc::sigaction(sig, std::ptr::null(), &mut old) == 0
                && old.sa_sigaction == libc::SIG_IGN
            {
                continue;
            }
            let mut new: libc::sigaction = std::mem::zeroed();
            new.sa_sigaction = on_signal as extern "C" fn(libc::c_int) as usize;
            libc::sigemptyset(&mut new.sa_mask);
            libc::sigaction(sig, &new, std::ptr::null_mut());
        }
    }
}

// ---------------------------------------------------------------------------------------
// Asking for a turn

const UNEXPECTED: &str = "the daemon sent something unexpected";

/// Why the wait ended without a go-ahead.
enum Stopped {
    /// The queue cannot be used; the command runs at once.
    Unavailable(String),
    /// This process was signalled while it waited: it never ran anything.
    Signalled(i32),
}

/// A turn granted within this long is not worth a line: on an idle machine every command
/// is told "position 1" for up to a tick before it starts.
const QUIET: Duration = Duration::from_secs(2);

/// The line printed whenever the position changes, once the wait has lasted `QUIET`.
fn queued_line(position: usize, need: u64) -> String {
    format!(
        "queued for memory (position {position}, needs {:.1} GB)",
        need as f64 / (1u64 << 30) as f64
    )
}

fn connect_error(socket: &Path, e: &io::Error) -> String {
    match e.kind() {
        io::ErrorKind::NotFound | io::ErrorKind::ConnectionRefused => {
            format!("no daemon is listening on {}", socket.display())
        }
        io::ErrorKind::PermissionDenied => format!("no permission to use {}", socket.display()),
        _ => format!("{}: {e}", socket.display()),
    }
}

/// Asks for a turn and waits for `GO`. Prints a line whenever the position changes. Gives up on
/// a daemon that says nothing for too long: `FIRST_ANSWER` for the first line, `wire::SILENCE`
/// after each line.
fn wait_for_turn(socket: &Path, label: &str, signature: &str) -> Result<UnixStream, Stopped> {
    let mut stream =
        UnixStream::connect(socket).map_err(|e| Stopped::Unavailable(connect_error(socket, &e)))?;
    let request = Message::Request {
        label: label.to_string(),
        signature: signature.to_string(),
    };
    // The timeout is set before the request goes out: once the daemon has read the request it
    // may hang up at any moment, and macOS refuses setsockopt on a socket whose peer has closed
    // (EINVAL), which would hide the hang-up behind "Invalid argument".
    stream
        .set_read_timeout(Some(LOOK_UP))
        .and_then(|()| stream.write_all(request.line().as_bytes()))
        .map_err(|e| Stopped::Unavailable(e.to_string()))?;

    let asked = Instant::now();
    let silence = wire::timing().silence;
    // When the daemon last said anything.
    let mut heard = asked;
    let mut told = false;
    let mut position: Option<(usize, u64)> = None;
    let mut shown = None;
    let mut input = Vec::new();
    loop {
        let sig = PENDING.load(Ordering::SeqCst);
        if sig != 0 {
            return Err(Stopped::Signalled(sig));
        }
        while let Some(line) = wire::take_line(&mut input) {
            heard = Instant::now();
            match Reply::parse(&line) {
                Some(Reply::Go) => return Ok(stream),
                Some(Reply::Queued {
                    position: now,
                    need,
                }) => {
                    told = true;
                    position = Some((now, need));
                }
                Some(Reply::Error(why)) => return Err(Stopped::Unavailable(why)),
                None => return Err(Stopped::Unavailable(UNEXPECTED.into())),
            }
        }
        if let Some((now, need)) = position
            && asked.elapsed() >= QUIET
            && shown != Some(now)
        {
            shown = Some(now);
            say(&queued_line(now, need));
        }
        if !told && heard.elapsed() > FIRST_ANSWER {
            return Err(Stopped::Unavailable("the daemon did not answer".into()));
        }
        if told && heard.elapsed() > silence {
            return Err(Stopped::Unavailable("the daemon stopped answering".into()));
        }
        if input.len() > wire::MAX_LINE {
            return Err(Stopped::Unavailable(UNEXPECTED.into()));
        }
        let mut chunk = [0u8; 256];
        match stream.read(&mut chunk) {
            Ok(0) => {
                return Err(Stopped::Unavailable(
                    "the daemon closed the connection".into(),
                ));
            }
            Ok(n) => input.extend_from_slice(&chunk[..n]),
            Err(e)
                if matches!(
                    e.kind(),
                    io::ErrorKind::WouldBlock
                        | io::ErrorKind::TimedOut
                        | io::ErrorKind::Interrupted
                ) => {}
            Err(e) => return Err(Stopped::Unavailable(e.to_string())),
        }
    }
}

// ---------------------------------------------------------------------------------------
// Running the command

/// What a shell would say, and exit with, when it cannot start a program.
struct NotRun {
    code: i32,
    why: String,
}

impl NotRun {
    fn of(errno: i32) -> NotRun {
        if errno == libc::ENOENT {
            NotRun {
                code: 127,
                why: "command not found".into(),
            }
        } else {
            NotRun {
                code: 126,
                why: io::Error::from_raw_os_error(errno).to_string(),
            }
        }
    }
}

/// Below interactive apps, but never above a priority the caller chose.
fn lower_priority() {
    // SAFETY: plain priority calls on this process; a refusal never stops the command.
    unsafe {
        if libc::getpriority(libc::PRIO_PROCESS, 0) < BACKGROUND_NICE {
            libc::setpriority(libc::PRIO_PROCESS, 0, BACKGROUND_NICE);
        }
    }
}

/// The programs to try for `name`: itself if it has a directory, else it in every directory
/// of `PATH`, in order.
fn candidates(name: &OsStr, path: Option<&OsStr>) -> Vec<PathBuf> {
    if name.as_bytes().contains(&b'/') {
        return vec![PathBuf::from(name)];
    }
    let path = path.unwrap_or(OsStr::new(DEFAULT_PATH));
    path.as_bytes()
        .split(|&b| b == b':')
        .map(|dir| match dir {
            [] => Path::new(".").join(name),
            dir => Path::new(OsStr::from_bytes(dir)).join(name),
        })
        .collect()
}

fn c_string(text: &OsStr) -> Result<CString, NotRun> {
    CString::new(text.as_bytes()).map_err(|_| NotRun {
        code: 126,
        why: "an argument contains a NUL byte".into(),
    })
}

/// Starts the command as the leader of a new process group, under the utility QoS clamp, with
/// `MEMBRAKE_ADMITTED` set; returns its pid, which is also its group. The program is looked up
/// the way a shell does, trying each directory of `PATH` and reporting not-found only if none
/// has it.
fn spawn(command: &[OsString]) -> Result<i32, NotRun> {
    let argv = command
        .iter()
        .map(|a| c_string(a))
        .collect::<Result<Vec<_>, _>>()?;
    let mut env: Vec<(OsString, OsString)> =
        std::env::vars_os().filter(|(k, _)| k != ADMITTED).collect();
    env.push((ADMITTED.into(), "1".into()));
    let envp = env
        .into_iter()
        .map(|(mut name, value)| {
            name.push("=");
            name.push(value);
            c_string(&name)
        })
        .collect::<Result<Vec<_>, _>>()?;
    let pointers = |strings: &[CString]| -> Vec<*mut libc::c_char> {
        strings
            .iter()
            .map(|s| s.as_ptr().cast_mut())
            .chain([std::ptr::null_mut()])
            .collect()
    };
    let (argv_ptrs, envp_ptrs) = (pointers(&argv), pointers(&envp));
    let path = std::env::var_os("PATH");

    let mut attr: libc::posix_spawnattr_t = std::ptr::null_mut();
    // SAFETY: standard posix_spawn attribute setup; `attr` is destroyed on every path below.
    let rc = unsafe { libc::posix_spawnattr_init(&mut attr) };
    if rc != 0 {
        return Err(NotRun::of(rc));
    }
    let outcome = (|| -> Result<i32, NotRun> {
        // SAFETY: `attr` is initialised; the signal sets are initialised before use.
        unsafe {
            let mut default_signals: libc::sigset_t = std::mem::zeroed();
            libc::sigemptyset(&mut default_signals);
            // Rust ignores SIGPIPE, and an ignored signal stays ignored across exec.
            libc::sigaddset(&mut default_signals, libc::SIGPIPE);
            let mut no_signals: libc::sigset_t = std::mem::zeroed();
            libc::sigemptyset(&mut no_signals);
            let flags = libc::POSIX_SPAWN_SETPGROUP
                | libc::POSIX_SPAWN_SETSIGDEF
                | libc::POSIX_SPAWN_SETSIGMASK;
            let rc = libc::posix_spawnattr_setflags(&mut attr, flags as libc::c_short)
                | libc::posix_spawnattr_setpgroup(&mut attr, 0)
                | libc::posix_spawnattr_setsigdefault(&mut attr, &default_signals)
                | libc::posix_spawnattr_setsigmask(&mut attr, &no_signals);
            if rc != 0 {
                return Err(NotRun::of(rc));
            }
            // A macOS without the clamp still runs the command, only not clamped.
            if let Some(set) = private_symbol(c"posix_spawnattr_set_qos_clamp_np") {
                let set: SetQosClamp = std::mem::transmute(set);
                set(&mut attr, CLAMP_UTILITY);
            }
        }
        let mut refused = None;
        let mut last = libc::ENOENT;
        for program in candidates(&command[0], path.as_deref()) {
            let program = c_string(program.as_os_str())?;
            let mut pid: libc::pid_t = 0;
            // SAFETY: the strings and the pointer arrays outlive the call, which copies them.
            let rc = unsafe {
                libc::posix_spawn(
                    &mut pid,
                    program.as_ptr(),
                    std::ptr::null(),
                    &attr,
                    argv_ptrs.as_ptr(),
                    envp_ptrs.as_ptr(),
                )
            };
            match rc {
                0 => return Ok(pid),
                libc::ENOENT | libc::ENOTDIR => last = rc,
                _ => refused = refused.or(Some(rc)),
            }
        }
        Err(NotRun::of(refused.unwrap_or(last)))
    })();
    // SAFETY: `attr` was initialised above and is destroyed exactly once.
    unsafe { libc::posix_spawnattr_destroy(&mut attr) };
    outcome
}

/// Waits until the command has exited, and leaves it unreaped: a zombie keeps its pid, and so
/// its group's number, until `release` collects it. Says whether a signal ended it.
fn wait_for_exit(pid: i32) -> Result<bool, String> {
    loop {
        // SAFETY: zeroed siginfo is a valid out-parameter.
        let mut info: libc::siginfo_t = unsafe { std::mem::zeroed() };
        // SAFETY: `info` outlives the call.
        let rc = unsafe {
            libc::waitid(
                libc::P_PID,
                pid as libc::id_t,
                &mut info,
                libc::WEXITED | libc::WNOWAIT,
            )
        };
        if rc == 0 {
            return Ok(matches!(info.si_code, libc::CLD_KILLED | libc::CLD_DUMPED));
        }
        let e = io::Error::last_os_error();
        if e.kind() != io::ErrorKind::Interrupted {
            return Err(e.to_string());
        }
    }
}

/// Collects the command, and with it the hold on its group's number, and gives its exit code.
/// The group is unpublished first, so no signal handler sends to the number after this.
fn release(pid: i32) -> i32 {
    GROUP.store(0, Ordering::SeqCst);
    let mut status = 0;
    loop {
        // SAFETY: `status` is a valid out-parameter.
        let rc = unsafe { libc::waitpid(pid, &mut status, 0) };
        if rc == pid {
            break;
        }
        if rc < 0 && io::Error::last_os_error().kind() != io::ErrorKind::Interrupted {
            say(&format!(
                "cannot wait for the command: {}",
                io::Error::last_os_error()
            ));
            return 1;
        }
    }
    if libc::WIFEXITED(status) {
        libc::WEXITSTATUS(status)
    } else if libc::WIFSIGNALED(status) {
        128 + libc::WTERMSIG(status)
    } else {
        1
    }
}

// ---------------------------------------------------------------------------------------
// What the command leaves running
//
// The group's number cannot go to anyone else while this runs. The command's process, which
// leads the group, is an unreaped zombie here (`wait_for_exit`), and XNU keeps a zombie in the
// pid hash and in its process group until its parent reaps it: `forkproc` rejects a pid that
// `phash_find_locked`, `pghash_exists_locked` or `session_find_locked` finds, and
// `reap_child_locked` is where `phash_remove_locked` and `pgrp_leave_locked` happen
// (bsd/kern/kern_fork.c, kern_exit.c). So no new process, and no new group, takes the number,
// and every signal below, and every one the handler passes on meanwhile, reaches only the
// command's group. `release` reaps it only afterwards, unpublishing the group first.

/// How long the group's leftovers have between `SIGTERM` and `SIGKILL`.
const REAP_GRACE: Duration = Duration::from_secs(10);
/// How long `SIGKILL` is repeated, for members forked after a previous one went out.
const KILL_WAIT: Duration = Duration::from_secs(2);
/// How often the group is looked at while they stop.
const REAP_LOOK: Duration = Duration::from_millis(100);

/// What is left in the command's group, as far as the kernel can say.
#[derive(Debug, PartialEq)]
enum Group {
    /// Nothing but the unreaped leader (and members that have exited, waiting to be reaped).
    Empty,
    /// A live member this user may signal.
    Signallable,
    /// `n` live members, none of which this user may signal (another user's).
    Foreign(usize),
    /// The group could not be listed, or a member could not be read.
    Unknown,
}

/// What is left in the command's group. The group's pids come from one call,
/// `proc_listpgrppids`, which XNU fills while holding `proc_list_lock` and which lists exited
/// members (zombies) too (bsd/kern/proc_info.c, `PROC_PGRP_ONLY`). A member that forks and
/// exits therefore cannot hide its child from it: the child is in the list, and so is the
/// member until its own parent reaps it. "Only the held leader" in that list is the empty
/// group. The other pids are then read one by one, only to tell live from exited and this
/// user's from another's; a pid that is gone by then counts as exited.
fn group_state(group: i32) -> Group {
    let mut inspector = jobs::Inspector::new();
    let Ok(pids) = inspector.pids(group) else {
        return Group::Unknown;
    };
    // SAFETY: geteuid has no preconditions.
    let me = unsafe { libc::geteuid() };
    let (mut mine, mut theirs, mut unreadable) = (0usize, 0usize, false);
    for &pid in pids.iter().filter(|&&pid| pid != group) {
        match jobs::probe(pid) {
            jobs::Probe::Found(p) if p.zombie || p.pgid != group => {}
            jobs::Probe::Found(p) if p.uid == me => mine += 1,
            jobs::Probe::Found(_) => theirs += 1,
            jobs::Probe::Gone => {}
            jobs::Probe::Unreadable(_) => unreadable = true,
        }
    }
    match (mine, theirs, unreadable) {
        (1.., _, _) => Group::Signallable,
        (0, _, true) => Group::Unknown,
        (0, 1.., false) => Group::Foreign(theirs),
        (0, 0, false) => Group::Empty,
    }
}

/// Waits up to `limit` while the group may still hold something this user can stop, calling
/// `each` before every wait, and gives the state it ended in.
fn settle(group: i32, limit: Duration, mut each: impl FnMut()) -> Group {
    let deadline = Instant::now() + limit;
    loop {
        let state = group_state(group);
        let open = matches!(state, Group::Signallable | Group::Unknown);
        if !open || Instant::now() >= deadline {
            return state;
        }
        each();
        std::thread::sleep(REAP_LOOK);
    }
}

/// The one line saying how stopping went. `how`: what was sent last.
fn say_outcome(group: i32, how: &str, state: &Group) {
    say(&match state {
        Group::Empty => {
            format!("stopped what the command left running in its process group ({how})")
        }
        Group::Foreign(n) => format!(
            "stopped what this user could in the command's process group {group} ({how}); \
             {n} process(es) of another user remain, which this user cannot signal"
        ),
        Group::Unknown => format!(
            "sent {how} to the command's process group {group}; could not read the group \
             afterwards, so whether anything is left is not known"
        ),
        Group::Signallable => format!(
            "the command's process group {group} still had processes after {} s of {how} \
             (still forking); left",
            KILL_WAIT.as_secs()
        ),
    });
}

fn signal_group(group: i32, sig: libc::c_int) {
    // SAFETY: the command's group, its number held by the unreaped leader (see above).
    unsafe { libc::killpg(group, sig) };
}

/// The signals passed on to the command (`install_signal_handlers`), as a set.
fn passed_on() -> libc::sigset_t {
    // SAFETY: an initialised, then filled, signal set.
    unsafe {
        let mut set: libc::sigset_t = std::mem::zeroed();
        libc::sigemptyset(&mut set);
        for sig in [libc::SIGINT, libc::SIGTERM, libc::SIGHUP] {
            libc::sigaddset(&mut set, sig);
        }
        set
    }
}

/// Blocks the signals passed on to the command, on top of whatever is blocked already, and
/// gives the mask as it was, for `restore_signals`.
fn hold_signals() -> libc::sigset_t {
    let set = passed_on();
    // SAFETY: both sets are initialised; the kernel fills `old`.
    unsafe {
        let mut old: libc::sigset_t = std::mem::zeroed();
        libc::pthread_sigmask(libc::SIG_BLOCK, &set, &mut old);
        old
    }
}

/// Puts back the mask `hold_signals` found: a signal the caller had blocked stays blocked.
fn restore_signals(old: &libc::sigset_t) {
    // SAFETY: `old` came from `pthread_sigmask`.
    unsafe { libc::pthread_sigmask(libc::SIG_SETMASK, old, std::ptr::null_mut()) };
}

/// True when one of the signals passed on to the command is pending only because
/// `hold_signals` blocked it: one the caller had blocked itself was never going to interrupt.
fn signal_pending(before: &libc::sigset_t) -> bool {
    // SAFETY: zeroed, then filled by the kernel.
    unsafe {
        let mut pending: libc::sigset_t = std::mem::zeroed();
        libc::sigpending(&mut pending);
        [libc::SIGINT, libc::SIGTERM, libc::SIGHUP]
            .iter()
            .any(|&sig| {
                libc::sigismember(&pending, sig) == 1 && libc::sigismember(before, sig) == 0
            })
    }
}

/// Stops what is left in the command's group once the command has exited, or says it is there.
/// `stop`: the job was interrupted, or `--reap` was given. When it left the group alone it
/// returns with the signals passed on to the command blocked, and the mask to restore once the
/// command is reaped (see `run_command`).
fn reap(group: i32, stop: bool) -> Option<libc::sigset_t> {
    match group_state(group) {
        Group::Empty => return None,
        Group::Foreign(n) => {
            say(&format!(
                "{n} process(es) of another user are still in the command's process group \
                 {group}; this user cannot signal them"
            ));
            return None;
        }
        Group::Unknown => {
            say(&format!(
                "could not read what is left in the command's process group {group}; left alone"
            ));
            return None;
        }
        Group::Signallable => {}
    }
    // The decision to leave the group alone is made with the signals that would interrupt the
    // job blocked, and one already pending counts: a signal cannot arrive between the decision
    // and acting on it. If the group is left, they stay blocked until the command is reaped
    // and the group unpublished (`release`), so none is passed on to it afterwards either.
    if !stop {
        let before = hold_signals();
        if !FORWARDED.load(Ordering::SeqCst) && !signal_pending(&before) {
            say(&format!(
                "the command ended, but its process group {group} still has processes \
                 running; left alone (--reap stops them when the command ends)"
            ));
            return Some(before);
        }
        restore_signals(&before);
    }
    signal_group(group, libc::SIGTERM);
    let state = settle(group, REAP_GRACE, || {});
    if state != Group::Signallable {
        say_outcome(group, "SIGTERM", &state);
        return None;
    }
    // Again at every look: a member may fork after a SIGKILL has gone out to the others.
    signal_group(group, libc::SIGKILL);
    let state = settle(group, KILL_WAIT, || signal_group(group, libc::SIGKILL));
    say_outcome(group, "SIGKILL after SIGTERM was ignored", &state);
    None
}

/// Runs the command to its end and reports it to the daemon, if there is one on the line.
fn run_command(command: &[OsString], mut daemon: Option<UnixStream>, reap_always: bool) -> i32 {
    let tell = |daemon: &mut Option<UnixStream>, message: Message| {
        if let Some(stream) = daemon.as_mut()
            && stream.write_all(message.line().as_bytes()).is_err()
        {
            *daemon = None;
        }
    };
    lower_priority();
    let pid = match spawn(command) {
        Ok(pid) => pid,
        Err(NotRun { code, why }) => {
            say(&format!("{}: {why}", command[0].to_string_lossy()));
            tell(&mut daemon, Message::Done(code));
            return code;
        }
    };
    GROUP.store(pid, Ordering::SeqCst);
    // A signal that came in between the spawn and the line above.
    let early = PENDING.swap(0, Ordering::SeqCst);
    if early != 0 {
        // SAFETY: signalling the group this process just started.
        unsafe { libc::killpg(pid, early) };
        FORWARDED.store(true, Ordering::SeqCst);
    }
    tell(&mut daemon, Message::Started(pid));
    let held: Option<libc::sigset_t> = match wait_for_exit(pid) {
        Ok(signalled) => reap(
            pid,
            reap_always || signalled || FORWARDED.load(Ordering::SeqCst),
        ),
        // Not known to have exited, so its group's number is not known to be held: the group
        // is unpublished before anything else, and nothing is sent to it.
        Err(why) => {
            GROUP.store(0, Ordering::SeqCst);
            say(&format!("cannot wait for the command: {why}"));
            None
        }
    };
    let code = release(pid);
    if let Some(before) = held {
        restore_signals(&before);
    }
    tell(&mut daemon, Message::Done(code));
    code
}

/// Runs `args.command`, queued for memory. Returns the exit code.
pub fn run(args: Args) -> i32 {
    let Args {
        label,
        socket,
        command,
        reap: reap_always,
    } = args;
    if std::env::var_os(ADMITTED).is_some() {
        // Replaces this process: the command is as direct as it can be.
        let error = Command::new(&command[0]).args(&command[1..]).exec();
        let not_run = NotRun::of(error.raw_os_error().unwrap_or(libc::ENOEXEC));
        say(&format!(
            "{}: {}",
            command[0].to_string_lossy(),
            not_run.why
        ));
        return not_run.code;
    }
    install_signal_handlers();
    let signature = wire::signature(&command);
    let label = wire::sanitize(label.as_deref().unwrap_or(&signature), wire::MAX_LABEL);
    match wait_for_turn(&socket, &label, &signature) {
        Ok(stream) => run_command(&command, Some(stream), reap_always),
        Err(Stopped::Unavailable(why)) => {
            say(&format!("queue unavailable ({why}); running now"));
            run_command(&command, None, reap_always)
        }
        Err(Stopped::Signalled(sig)) => 128 + sig,
    }
}
