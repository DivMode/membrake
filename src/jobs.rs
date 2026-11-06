//! Jobs are process groups: shells and coding agents start every command in its own group,
//! so a worker pool is one job and one `killpg` reaches all of it.
//!
//! This module scans the process table through libproc (no subprocess, no `ps`), groups
//! processes, and decides which groups may ever be touched.

use std::collections::HashMap;
use std::ffi::CString;
use std::io;
use std::mem::MaybeUninit;
use std::ops::Deref;

/// Groups led by a user below this uid (root and the system accounts) are never touched,
/// unless the user is named by `--include-user`. (`--protect-user` only changes the ranking
/// of jobs that are already eligible; it does not lift this floor.)
pub const FIRST_USER_UID: u32 = 500;

/// The agent CLIs, as a POSIX extended regex matched against a process name, executable path
/// or full command line. Written once, as a macro, so `ALWAYS_NEVER` and `AGENT_CLI` cannot
/// drift apart (`concat!` takes literals, not constants):
/// - the names;
/// - Claude Code by install directory, because its process name is its version string, and
///   by package name when it runs under node;
/// - a `codex` binary, including one run via node (`node /path/to/codex ...`).
macro_rules! agent_cli {
    () => {
        r"^(claude|codex)$|/\.local/share/claude/|@anthropic-ai/claude-code|/codex( |$)"
    };
}

/// The agent CLIs. A queue session is the nearest ancestor of a client that matches this.
pub const AGENT_CLI: &str = agent_cli!();

/// Always protected, whatever `--never` says: the agent CLIs themselves (`AGENT_CLI`),
/// terminal multiplexers and terminals, including the Nix-wrapped `.tmux-wrapped` style.
///
/// Matched against every member's complete command line, not only the leader's, so an agent
/// hosted by an interpreter (`node /path/to/codex ...`) protects the shell-led group it runs in.
pub const ALWAYS_NEVER: &str = concat!(
    agent_cli!(),
    r"|^(tmux|herdr|zellij|screen|ghostty|alacritty|kitty|wezterm|wezterm-gui|\.(herdr|tmux|zellij)-wrapped)$|/\.(herdr|tmux|zellij)-wrapped$"
);

const COMMAND_CHARS: usize = 200;

/// A process group that membrake may act on.
#[derive(Clone, Debug, PartialEq)]
pub struct Job {
    pub pgid: i32,
    /// The member whose pid is the pgid, or else the earliest-started member.
    pub leader_pid: i32,
    /// Start time of that leader, microseconds since the epoch. With the pid it identifies
    /// the process even after pid reuse.
    pub start_us: u64,
    pub uid: u32,
    /// Sum of `ri_phys_footprint` over the members: Activity Monitor's "Memory".
    pub bytes: u64,
    pub procs: u32,
    pub prefer: bool,
    /// Owned by a `--protect-user` user: never frozen, killed only when nothing else is left.
    pub protected: bool,
    /// Every live member is already stopped (for instance a shell job suspended with ^Z).
    pub stopped: bool,
    pub command: String,
}

/// Replaces control characters so a command fits on one line of a log or TSV. Display only:
/// never shorten text that is being matched against the rules.
pub fn one_line(text: &str, max_chars: usize) -> String {
    text.chars()
        .take(max_chars)
        .map(|c| if c.is_control() { ' ' } else { c })
        .collect()
}

// ---------------------------------------------------------------------------------------
// POSIX extended regular expressions from libc: no dependency, same syntax as `grep -E`.

pub struct Regex {
    re: libc::regex_t,
}

impl Regex {
    pub fn new(pattern: &str) -> Result<Regex, String> {
        let c = CString::new(pattern).map_err(|_| "pattern contains a NUL byte".to_string())?;
        let mut re = MaybeUninit::<libc::regex_t>::zeroed();
        // SAFETY: `re` is a zeroed regex_t and `c` is NUL-terminated.
        let rc = unsafe {
            libc::regcomp(
                re.as_mut_ptr(),
                c.as_ptr(),
                libc::REG_EXTENDED | libc::REG_NOSUB,
            )
        };
        if rc != 0 {
            let mut buf = [0 as libc::c_char; 256];
            // SAFETY: the buffer length is passed along; regerror NUL-terminates.
            unsafe { libc::regerror(rc, re.as_ptr(), buf.as_mut_ptr(), buf.len()) };
            // SAFETY: regerror wrote a NUL-terminated string into `buf`.
            let msg = unsafe { std::ffi::CStr::from_ptr(buf.as_ptr()) };
            return Err(format!("{pattern:?}: {}", msg.to_string_lossy()));
        }
        // SAFETY: regcomp succeeded.
        Ok(Regex {
            re: unsafe { re.assume_init() },
        })
    }

    pub fn is_match(&self, text: &str) -> bool {
        let Ok(c) = CString::new(text) else {
            return false;
        };
        // SAFETY: `re` was compiled by regcomp; REG_NOSUB needs no match array.
        unsafe { libc::regexec(&self.re, c.as_ptr(), 0, std::ptr::null_mut(), 0) == 0 }
    }
}

impl Drop for Regex {
    fn drop(&mut self) {
        // SAFETY: `re` came from a successful regcomp and is freed exactly once.
        unsafe { libc::regfree(&mut self.re) };
    }
}

// ---------------------------------------------------------------------------------------
// Eligibility: a pure function of facts about a group.

#[derive(Clone, Debug, PartialEq, Eq)]
pub struct Member {
    pub name: String,
    pub exe: String,
    /// The member's effective uid.
    pub uid: u32,
    /// The member's complete arguments joined by spaces (`KERN_PROCARGS2`), untruncated: the
    /// only place an interpreter-hosted agent (`node /path/to/codex`) shows.
    pub command: String,
}

#[derive(Clone, Copy, Debug, PartialEq, Eq)]
pub enum Reason {
    /// pgid 0 or 1: the kernel and launchd.
    System,
    /// membrake's own process group.
    OwnGroup,
    /// A member runs from inside an application, XPC service or extension bundle, or from
    /// the system volume's `/System`: operating-system and app machinery, not a worker.
    Bundle,
    /// Led by, or containing a process of, root or a system account.
    SystemUser,
    /// Members run as different users: a set-uid helper (`sudo`, `login`) joined the group.
    MixedUsers,
    /// Matched `--never` or the built-in agent and terminal list, or (test only) is outside
    /// the `--simulate-only` filter.
    Never,
}

impl Reason {
    /// For log lines.
    pub fn text(self) -> &'static str {
        match self {
            Reason::System => "it is a system process group",
            Reason::OwnGroup => "it is membrake's own group",
            Reason::Bundle => "a member runs from an application or system bundle",
            Reason::SystemUser => "it belongs to root or a system account",
            Reason::MixedUsers => "its members run as different users",
            Reason::Never => "it matches the agent, terminal or --never protections",
        }
    }
}

#[derive(Clone, Copy, Debug, PartialEq, Eq)]
pub enum Verdict {
    Ineligible(Reason),
    Eligible { prefer: bool, protected: bool },
}

fn in_bundle_or_system(exe: &str) -> bool {
    exe.starts_with("/System/")
        || [".app/Contents/", ".xpc/Contents/", ".appex/Contents/"]
            .iter()
            .any(|marker| exe.contains(marker))
}

pub struct Rules {
    never: Vec<Regex>,
    prefer: Vec<Regex>,
    protect_uids: Vec<u32>,
    include_uids: Vec<u32>,
    own_pgid: i32,
    /// Test only (`run --simulate-only`): when set, only groups whose leader's command line
    /// matches are eligible at all.
    only: Option<Regex>,
}

impl Rules {
    pub fn new(
        never: &[String],
        prefer: &[String],
        protect_uids: Vec<u32>,
        include_uids: Vec<u32>,
        own_pgid: i32,
    ) -> Result<Rules, String> {
        let mut all_never = vec![Regex::new(ALWAYS_NEVER)?];
        for p in never {
            all_never.push(Regex::new(p).map_err(|e| format!("--never {e}"))?);
        }
        let prefer = prefer
            .iter()
            .map(|p| Regex::new(p).map_err(|e| format!("--prefer {e}")))
            .collect::<Result<_, _>>()?;
        Ok(Rules {
            never: all_never,
            prefer,
            protect_uids,
            include_uids,
            own_pgid,
            only: None,
        })
    }

    /// Test only (`run --simulate-only`): makes every group whose leader's command line does
    /// not match `pattern` ineligible, so a run driven by scripted readings can reach the
    /// processes a test spawned and no others. The caller's own uid counts as a user's even
    /// below 500, because a Nix build sandbox runs its tests as a build user with such a uid.
    pub fn only_matching(mut self, pattern: &str) -> Result<Rules, String> {
        self.only = Some(Regex::new(pattern).map_err(|e| format!("--simulate-only {e}"))?);
        // SAFETY: getuid has no preconditions.
        self.include_uids.push(unsafe { libc::getuid() });
        Ok(self)
    }

    /// The checks that need only the pgid and the leader's uid, so a scan can skip a group
    /// before reading any of its memory.
    pub fn precheck(&self, pgid: i32, leader_uid: u32) -> Option<Reason> {
        if pgid <= 1 {
            return Some(Reason::System);
        }
        if pgid == self.own_pgid {
            return Some(Reason::OwnGroup);
        }
        if self.below_floor(leader_uid) {
            return Some(Reason::SystemUser);
        }
        None
    }

    /// A uid below 500 is a system account unless `--include-user` names it. `--protect-user`
    /// does not: it ranks jobs that are already eligible, it must not make a system account's
    /// jobs killable.
    fn below_floor(&self, uid: u32) -> bool {
        uid < FIRST_USER_UID && !self.include_uids.contains(&uid)
    }

    /// `leader_command` is the leader's full, untruncated command line; members are matched by
    /// name, executable path and their own full command line, so a protected process anywhere
    /// in the group protects the group. Every member's uid is checked, not only the leader's:
    /// a group is ineligible if any member is a system account (unless named by
    /// `--include-user`) or runs as a different user than the leader.
    pub fn classify(
        &self,
        pgid: i32,
        leader_uid: u32,
        members: &[Member],
        leader_command: &str,
    ) -> Verdict {
        if let Some(reason) = self.precheck(pgid, leader_uid) {
            return Verdict::Ineligible(reason);
        }
        if members.iter().any(|m| self.below_floor(m.uid)) {
            return Verdict::Ineligible(Reason::SystemUser);
        }
        if members.iter().any(|m| m.uid != leader_uid) {
            return Verdict::Ineligible(Reason::MixedUsers);
        }
        if members.iter().any(|m| in_bundle_or_system(&m.exe)) {
            return Verdict::Ineligible(Reason::Bundle);
        }
        if self
            .only
            .as_ref()
            .is_some_and(|re| !re.is_match(leader_command))
        {
            return Verdict::Ineligible(Reason::Never);
        }
        let hit = |list: &[Regex], text: &str| list.iter().any(|r| r.is_match(text));
        if hit(&self.never, leader_command)
            || members.iter().any(|m| {
                hit(&self.never, &m.name)
                    || hit(&self.never, &m.exe)
                    || hit(&self.never, &m.command)
            })
        {
            return Verdict::Ineligible(Reason::Never);
        }
        let prefer = hit(&self.prefer, leader_command)
            || members
                .iter()
                .any(|m| hit(&self.prefer, &m.name) || hit(&self.prefer, &m.exe));
        Verdict::Eligible {
            prefer,
            protected: self.protect_uids.contains(&leader_uid),
        }
    }
}

// ---------------------------------------------------------------------------------------
// libproc

/// What `proc_pidinfo(PROC_PIDTBSDINFO)` says about one process, without its name: plain
/// data, so polling a group allocates nothing.
#[derive(Clone, Copy, Debug)]
pub struct Seen {
    pub pid: i32,
    pub ppid: i32,
    pub pgid: i32,
    pub uid: u32,
    pub start_us: u64,
    pub zombie: bool,
    pub stopped: bool,
}

/// `Seen` and the process name.
#[derive(Clone, Debug)]
pub struct Proc {
    seen: Seen,
    pub name: String,
}

impl Deref for Proc {
    type Target = Seen;
    fn deref(&self) -> &Seen {
        &self.seen
    }
}

/// One fact about one process. Three outcomes, never two: a process that has exited is not
/// the same as one that could not be read, and taking the second for the first is how a
/// frozen group gets forgotten.
#[derive(Debug)]
pub enum Probe<T> {
    Found(T),
    /// The process does not exist (any more).
    Gone,
    Unreadable(io::Error),
}

impl<T> Probe<T> {
    fn map<U>(self, f: impl FnOnce(T) -> U) -> Probe<U> {
        match self {
            Probe::Found(t) => Probe::Found(f(t)),
            Probe::Gone => Probe::Gone,
            Probe::Unreadable(e) => Probe::Unreadable(e),
        }
    }
}

fn clear_errno() {
    // SAFETY: __error returns the calling thread's errno slot.
    unsafe { *libc::__error() = 0 };
}

/// Sorts a failed libproc call by errno: ESRCH means the process does not exist, anything
/// else (or no errno at all) means it may exist and could not be read.
fn failure<T>(what: &str) -> Probe<T> {
    let e = io::Error::last_os_error();
    match e.raw_os_error() {
        Some(libc::ESRCH) => Probe::Gone,
        Some(0) | None => Probe::Unreadable(io::Error::other(format!("{what} returned no data"))),
        Some(_) => Probe::Unreadable(io::Error::new(e.kind(), format!("{what}: {e}"))),
    }
}

fn bsd_info_raw(pid: i32) -> Probe<libc::proc_bsdinfo> {
    let mut info = MaybeUninit::<libc::proc_bsdinfo>::zeroed();
    let size = size_of::<libc::proc_bsdinfo>() as libc::c_int;
    clear_errno();
    // SAFETY: the buffer is a zeroed proc_bsdinfo of `size` bytes.
    let got = unsafe {
        libc::proc_pidinfo(
            pid,
            libc::PROC_PIDTBSDINFO,
            0,
            info.as_mut_ptr().cast(),
            size,
        )
    };
    match got {
        // SAFETY: the kernel filled the whole struct.
        n if n == size => Probe::Found(unsafe { info.assume_init() }),
        0 => failure("proc_pidinfo"),
        _ => Probe::Unreadable(io::Error::other("proc_pidinfo returned a short read")),
    }
}

fn seen_from(pid: i32, info: &libc::proc_bsdinfo) -> Seen {
    Seen {
        pid,
        ppid: info.pbi_ppid as i32,
        pgid: info.pbi_pgid as i32,
        uid: info.pbi_uid,
        start_us: info.pbi_start_tvsec * 1_000_000 + info.pbi_start_tvusec,
        zombie: info.pbi_status == libc::SZOMB,
        stopped: info.pbi_status == libc::SSTOP,
    }
}

/// Reads one process. Allocation-free.
pub fn probe(pid: i32) -> Probe<Seen> {
    bsd_info_raw(pid).map(|info| seen_from(pid, &info))
}

fn probe_proc(pid: i32) -> Probe<Proc> {
    bsd_info_raw(pid).map(|info| {
        let name = match c_chars(&info.pbi_name) {
            n if n.is_empty() => c_chars(&info.pbi_comm),
            n => n,
        };
        Proc {
            seen: seen_from(pid, &info),
            name,
        }
    })
}

fn c_chars(raw: &[libc::c_char]) -> String {
    let bytes: Vec<u8> = raw
        .iter()
        .take_while(|&&c| c != 0)
        .map(|&c| c as u8)
        .collect();
    String::from_utf8_lossy(&bytes).into_owned()
}

/// The process, or None if it is gone or unreadable. Where the difference matters, use
/// `probe`.
pub fn bsd_info(pid: i32) -> Option<Proc> {
    match probe_proc(pid) {
        Probe::Found(p) => Some(p),
        Probe::Gone | Probe::Unreadable(_) => None,
    }
}

pub fn list_pids() -> Vec<i32> {
    // SAFETY: a null buffer asks only for the count.
    let count = unsafe { libc::proc_listallpids(std::ptr::null_mut(), 0) };
    if count <= 0 {
        return Vec::new();
    }
    // Room for processes started between the two calls.
    let mut pids = vec![0i32; count as usize + 64];
    let bytes = (pids.len() * size_of::<i32>()) as libc::c_int;
    // SAFETY: `pids` is `bytes` long.
    let filled = unsafe { libc::proc_listallpids(pids.as_mut_ptr().cast(), bytes) };
    pids.truncate(filled.max(0) as usize);
    pids
}

/// `ri_phys_footprint`, or 0 for a process we may not inspect.
pub fn footprint(pid: i32) -> u64 {
    let mut ri = MaybeUninit::<libc::rusage_info_v2>::zeroed();
    // SAFETY: the buffer is a zeroed rusage_info_v2, the flavor that matches it.
    let rc = unsafe { libc::proc_pid_rusage(pid, libc::RUSAGE_INFO_V2, ri.as_mut_ptr().cast()) };
    if rc != 0 {
        return 0;
    }
    // SAFETY: the kernel filled the struct.
    unsafe { ri.assume_init() }.ri_phys_footprint
}

fn exe_path(pid: i32) -> Probe<String> {
    #[cfg(test)]
    if crate::testutil::fault::take(&crate::testutil::fault::READ) {
        return Probe::Unreadable(io::Error::other("injected failure"));
    }
    let mut buf = vec![0u8; libc::PROC_PIDPATHINFO_MAXSIZE as usize];
    clear_errno();
    // SAFETY: `buf` is as long as the size passed.
    let n = unsafe { libc::proc_pidpath(pid, buf.as_mut_ptr().cast(), buf.len() as u32) };
    if n > 0 {
        Probe::Found(String::from_utf8_lossy(&buf[..n as usize]).into_owned())
    } else {
        failure("proc_pidpath")
    }
}

/// A process's complete arguments joined by spaces, from `KERN_PROCARGS2`: every one of them,
/// however long. An empty string if it has none. Read only for candidates, never every sample.
fn args_line(pid: i32) -> Probe<String> {
    let mut mib = [libc::CTL_KERN, libc::KERN_PROCARGS2, pid];
    let mut size = 0usize;
    clear_errno();
    // SAFETY: a null buffer asks only for the size.
    let rc = unsafe {
        libc::sysctl(
            mib.as_mut_ptr(),
            3,
            std::ptr::null_mut(),
            &mut size,
            std::ptr::null_mut(),
            0,
        )
    };
    if rc != 0 {
        return failure("KERN_PROCARGS2");
    }
    if size <= size_of::<i32>() {
        return Probe::Found(String::new());
    }
    let mut buf = vec![0u8; size];
    // SAFETY: `buf` is `size` bytes long.
    let rc = unsafe {
        libc::sysctl(
            mib.as_mut_ptr(),
            3,
            buf.as_mut_ptr().cast(),
            &mut size,
            std::ptr::null_mut(),
            0,
        )
    };
    if rc != 0 {
        return failure("KERN_PROCARGS2");
    }
    buf.truncate(size);
    let Some(head) = buf.first_chunk::<4>() else {
        return Probe::Unreadable(io::Error::other("KERN_PROCARGS2 returned a short read"));
    };
    let argc = i32::from_ne_bytes(*head).max(0) as usize;
    let mut at = 4;
    while at < buf.len() && buf[at] != 0 {
        at += 1; // the executable path
    }
    while at < buf.len() && buf[at] == 0 {
        at += 1; // its padding
    }
    let mut args: Vec<String> = Vec::new();
    while args.len() < argc && at < buf.len() {
        let end = buf[at..]
            .iter()
            .position(|&b| b == 0)
            .map_or(buf.len(), |p| at + p);
        args.push(String::from_utf8_lossy(&buf[at..end]).into_owned());
        at = end + 1;
    }
    Probe::Found(args.join(" "))
}

/// The command line as shown to people: one line, cut at 200 characters.
pub fn command_line(pid: i32) -> Option<String> {
    match args_line(pid) {
        Probe::Found(line) if !line.is_empty() => Some(one_line(&line, COMMAND_CHARS)),
        _ => None,
    }
}

/// What a look at one process group found. Three outcomes, never two.
#[derive(Debug)]
pub enum Look {
    /// At least one live member; see `Inspector::members`.
    Present,
    /// Confirmed: the kernel lists no live process in the group.
    Absent,
    /// The kernel could not say, or a member could not be read. This is not `Absent`: nothing
    /// may be concluded about the group, least of all that it is gone.
    Unknown(String),
}

/// Looks at process groups through buffers it keeps, so polling a frozen group on every
/// sample allocates nothing once they have grown to fit.
pub struct Inspector {
    pids: Vec<i32>,
    members: Vec<Seen>,
}

impl Default for Inspector {
    fn default() -> Inspector {
        Inspector::new()
    }
}

impl Inspector {
    pub fn new() -> Inspector {
        Inspector {
            pids: vec![0; 1024],
            members: Vec::with_capacity(64),
        }
    }

    /// The live (non-zombie) members of the group found by the last `look`.
    pub fn members(&self) -> &[Seen] {
        &self.members
    }

    /// Fills `pids` with the group's pids and says how many. Err if the kernel could not say.
    fn list(&mut self, pgid: i32) -> Result<usize, String> {
        #[cfg(test)]
        if crate::testutil::fault::take(&crate::testutil::fault::LIST) {
            return Err("injected failure".into());
        }
        loop {
            let bytes = (self.pids.len() * size_of::<i32>()) as libc::c_int;
            clear_errno();
            // SAFETY: `pids` is `bytes` long.
            let got =
                unsafe { libc::proc_listpgrppids(pgid, self.pids.as_mut_ptr().cast(), bytes) };
            // Like proc_listallpids, this wrapper returns a count of pids, not bytes. It also
            // returns zero when the enumeration itself failed (libproc.c), so only errno says
            // whether a zero is an empty group.
            if got <= 0 {
                let e = io::Error::last_os_error();
                return match (got, e.raw_os_error()) {
                    (0, Some(0) | None) => Ok(0),
                    _ => Err(format!("proc_listpgrppids({pgid}): {e}")),
                };
            }
            let count = got as usize;
            if count < self.pids.len() {
                return Ok(count);
            }
            // Possibly cut off by the end of the buffer: grow it and ask again.
            self.pids.resize(self.pids.len() * 2, 0);
        }
    }

    /// Lists group `pgid` and reads each member. Allocation-free once warm.
    pub fn look(&mut self, pgid: i32) -> Look {
        let count = match self.list(pgid) {
            Ok(count) => count,
            Err(why) => return Look::Unknown(why),
        };
        self.members.clear();
        for i in 0..count {
            let pid = self.pids[i];
            match probe(pid) {
                Probe::Found(s) if !s.zombie && s.pgid == pgid => self.members.push(s),
                // Exited, a zombie, or moved to another group since the list was made.
                Probe::Found(_) | Probe::Gone => {}
                Probe::Unreadable(e) => return Look::Unknown(format!("pid {pid}: {e}")),
            }
        }
        if self.members.is_empty() {
            Look::Absent
        } else {
            Look::Present
        }
    }
}

/// Everything the rules need to know about a group, every member fully read.
pub struct Facts {
    pub procs: Vec<Proc>,
    /// One per entry of `procs`, in the same order.
    pub members: Vec<Member>,
}

pub enum Read {
    Facts(Facts),
    Absent,
    /// The kernel could not list the group, or a member's uid, path or arguments could not be
    /// read: the rules cannot be applied, so the group is ineligible.
    Unreadable(String),
}

fn why(probe: &Probe<String>) -> Option<String> {
    match probe {
        Probe::Unreadable(e) => Some(e.to_string()),
        Probe::Found(_) | Probe::Gone => None,
    }
}

/// A member's facts, None if it exited while being read, Err if it is still there and could
/// not be read.
fn read_member(p: &Proc) -> Result<Option<Member>, String> {
    match (exe_path(p.pid), args_line(p.pid)) {
        (Probe::Found(exe), Probe::Found(command)) => Ok(Some(Member {
            name: p.name.clone(),
            exe,
            uid: p.uid,
            command,
        })),
        (exe, command) => match probe(p.pid) {
            Probe::Gone => Ok(None),
            Probe::Found(now) if now.zombie || now.pgid != p.pgid => Ok(None),
            _ => Err(format!(
                "pid {}: {}",
                p.pid,
                why(&exe)
                    .or_else(|| why(&command))
                    .unwrap_or_else(|| "unreadable".into())
            )),
        },
    }
}

/// Reads group `pgid` from the kernel's own list of its members and each member in full.
pub fn read_group(pgid: i32, insp: &mut Inspector) -> Read {
    let count = match insp.list(pgid) {
        Ok(count) => count,
        Err(why) => return Read::Unreadable(why),
    };
    let (mut procs, mut members) = (Vec::new(), Vec::new());
    for i in 0..count {
        let pid = insp.pids[i];
        let p = match probe_proc(pid) {
            Probe::Found(p) if !p.zombie && p.pgid == pgid => p,
            Probe::Found(_) | Probe::Gone => continue,
            Probe::Unreadable(e) => return Read::Unreadable(format!("pid {pid}: {e}")),
        };
        match read_member(&p) {
            Ok(Some(member)) => {
                procs.push(p);
                members.push(member);
            }
            Ok(None) => {}
            Err(why) => return Read::Unreadable(why),
        }
    }
    if procs.is_empty() {
        Read::Absent
    } else {
        Read::Facts(Facts { procs, members })
    }
}

fn leader_of(pgid: i32, procs: &[Proc]) -> Option<&Proc> {
    procs
        .iter()
        .find(|p| p.pid == pgid)
        .or_else(|| procs.iter().min_by_key(|p| p.start_us))
}

/// The leader of a fully read group and its complete command line.
fn leader_and_command(pgid: i32, group: &Facts) -> Option<(&Proc, &str)> {
    let leader = leader_of(pgid, &group.procs)?;
    let at = group.procs.iter().position(|p| p.pid == leader.pid)?;
    Some((leader, group.members[at].command.as_str()))
}

/// Every eligible group of at least `min_bytes`. The one place that reads the whole process
/// table.
pub fn scan(rules: &Rules, min_bytes: u64) -> Vec<Job> {
    let mut groups: HashMap<i32, Vec<Proc>> = HashMap::new();
    for pid in list_pids() {
        if let Probe::Found(p) = probe_proc(pid)
            && !p.zombie
        {
            groups.entry(p.pgid).or_default().push(p);
        }
    }
    let mut insp = Inspector::new();
    let mut jobs = Vec::new();
    for (pgid, procs) in groups {
        let Some(leader) = leader_of(pgid, &procs) else {
            continue;
        };
        if rules.precheck(pgid, leader.uid).is_some() {
            continue;
        }
        let bytes: u64 = procs.iter().map(|p| footprint(p.pid)).sum();
        if bytes < min_bytes {
            continue;
        }
        // Heavy enough to matter: read the group afresh from the kernel's list of its members
        // (a process that failed to read above is in no group of `groups`, but is in this
        // list). A member that cannot be read in full makes the group ineligible.
        let Read::Facts(group) = read_group(pgid, &mut insp) else {
            continue;
        };
        let Some((leader, full)) = leader_and_command(pgid, &group) else {
            continue;
        };
        if let Verdict::Eligible { prefer, protected } =
            rules.classify(pgid, leader.uid, &group.members, full)
        {
            jobs.push(Job {
                pgid,
                leader_pid: leader.pid,
                start_us: leader.start_us,
                uid: leader.uid,
                bytes,
                procs: group.procs.len() as u32,
                prefer,
                protected,
                stopped: group.procs.iter().all(|p| p.stopped),
                command: one_line(
                    if full.is_empty() { &leader.name } else { full },
                    COMMAND_CHARS,
                ),
            });
        }
    }
    jobs
}

/// How many generations up the process tree `session_of` looks.
const SESSION_DEPTH: usize = 32;

/// Whether `p` is an agent CLI: `agent` matches its name, its executable path or its complete
/// arguments, the three places `Rules::classify` looks.
fn is_agent(agent: &Regex, p: &Proc) -> bool {
    agent.is_match(&p.name)
        || matches!(exe_path(p.pid), Probe::Found(exe) if agent.is_match(&exe))
        || matches!(args_line(p.pid), Probe::Found(command) if agent.is_match(&command))
}

/// The queue session of the process `pid`: its nearest ancestor that is an agent CLI
/// (`agent` is `AGENT_CLI`), or else its parent. One agent's commands share a session, so the
/// queue can take turns between agents. An ancestor that cannot be read (another user's, to a
/// normal user) is judged by what can be read, which is at least its name.
pub fn session_of(agent: &Regex, pid: i32) -> i32 {
    let Some(client) = bsd_info(pid) else {
        return pid;
    };
    let mut at = client.ppid;
    for _ in 0..SESSION_DEPTH {
        if at <= 1 {
            break;
        }
        let Some(p) = bsd_info(at) else {
            break;
        };
        if is_agent(agent, &p) {
            return at;
        }
        at = p.ppid;
    }
    client.ppid
}

/// The summed footprint of the live members of group `pgid`, or None if it is gone or the
/// kernel could not list it. Allocation-free once `insp` is warm.
pub fn group_footprint(insp: &mut Inspector, pgid: i32) -> Option<u64> {
    match insp.look(pgid) {
        Look::Present => Some(insp.members().iter().map(|m| footprint(m.pid)).sum()),
        Look::Absent | Look::Unknown(_) => None,
    }
}

/// Why group `pgid` must not be killed right now, or None if the full rules still allow it.
/// Judged on the group as it is now, whatever size it has: a frozen job that shrank, or one the
/// scan no longer lists because it has since been excluded (it exec'd an agent, a helper from
/// another user joined it), is told apart from one that is merely small. A group that cannot be
/// read in full is excluded, as in `scan`.
pub fn spared(rules: &Rules, pgid: i32) -> Option<String> {
    match read_group(pgid, &mut Inspector::new()) {
        // Nothing left to kill; the caller's own check finds that out.
        Read::Absent => None,
        Read::Unreadable(why) => Some(format!("it cannot be inspected: {why}")),
        Read::Facts(group) => {
            let (leader, full) = leader_and_command(pgid, &group)?;
            match rules.classify(pgid, leader.uid, &group.members, full) {
                Verdict::Eligible { .. } => None,
                Verdict::Ineligible(reason) => Some(reason.text().to_string()),
            }
        }
    }
}

/// Looks a user name up in the account database.
pub fn uid_of(name: &str) -> Result<u32, String> {
    let c = CString::new(name).map_err(|_| format!("bad user name {name:?}"))?;
    let mut pwd = MaybeUninit::<libc::passwd>::zeroed();
    let mut buf = vec![0 as libc::c_char; 4096];
    let mut out: *mut libc::passwd = std::ptr::null_mut();
    // SAFETY: all pointers are valid for the lengths passed.
    let rc = unsafe {
        libc::getpwnam_r(
            c.as_ptr(),
            pwd.as_mut_ptr(),
            buf.as_mut_ptr(),
            buf.len(),
            &mut out,
        )
    };
    if rc != 0 || out.is_null() {
        return Err(format!("no such user: {name}"));
    }
    // SAFETY: getpwnam_r succeeded, so `pwd` is initialised.
    Ok(unsafe { pwd.assume_init() }.pw_uid)
}

#[cfg(test)]
mod tests {
    use super::*;
    use crate::testutil::fault;
    use std::os::unix::process::CommandExt;
    use std::process::{Child, Command};

    fn rules(protect: Vec<u32>, include: Vec<u32>, never: &[&str]) -> Rules {
        let never: Vec<String> = never.iter().map(|s| s.to_string()).collect();
        Rules::new(&never, &["^vitest$".to_string()], protect, include, 4242).unwrap()
    }

    fn member(name: &str, exe: &str) -> Member {
        Member {
            name: name.into(),
            exe: exe.into(),
            uid: 501,
            command: name.into(),
        }
    }

    /// Classifies a group whose members all run as `uid`, the leader's user.
    fn classify(r: &Rules, pgid: i32, uid: u32, members: &[Member]) -> Verdict {
        let members: Vec<Member> = members
            .iter()
            .map(|m| Member { uid, ..m.clone() })
            .collect();
        r.classify(pgid, uid, &members, "node build.js")
    }

    const PLAIN: Verdict = Verdict::Eligible {
        prefer: false,
        protected: false,
    };

    /// Everything membrake must never touch, and the ordinary job that it may.
    #[test]
    fn never_eligible_groups_are_excluded() {
        let r = rules(vec![], vec![], &[]);
        let tsc = [member("tsc", "/usr/local/bin/node")];
        assert_eq!(classify(&r, 5000, 501, &tsc), PLAIN);

        // pgid <= 1 and membrake's own group.
        assert_eq!(
            classify(&r, 1, 501, &tsc),
            Verdict::Ineligible(Reason::System)
        );
        assert_eq!(
            classify(&r, 0, 501, &tsc),
            Verdict::Ineligible(Reason::System)
        );
        assert_eq!(
            classify(&r, 4242, 501, &tsc),
            Verdict::Ineligible(Reason::OwnGroup)
        );
        // Leader owned by root or a system account.
        assert_eq!(
            classify(&r, 5000, 0, &tsc),
            Verdict::Ineligible(Reason::SystemUser)
        );
        assert_eq!(
            classify(&r, 5000, 499, &tsc),
            Verdict::Ineligible(Reason::SystemUser)
        );
        // A member inside an app bundle.
        let app = [
            member("tsc", "/usr/local/bin/node"),
            member(
                "Helper",
                "/Applications/Some.app/Contents/Frameworks/Helper",
            ),
        ];
        assert_eq!(
            classify(&r, 5000, 501, &app),
            Verdict::Ineligible(Reason::Bundle)
        );
        for exe in [
            "/System/Library/Frameworks/WebKit.framework/Versions/A/XPCServices/com.apple.WebKit.WebContent.xpc/Contents/MacOS/com.apple.WebKit.WebContent",
            "/System/Library/CoreServices/Something",
            "/opt/tools/Helper.xpc/Contents/MacOS/Helper",
            "/opt/tools/Plugin.appex/Contents/MacOS/Plugin",
        ] {
            let m = [member("worker", exe)];
            assert_eq!(
                classify(&r, 5000, 501, &m),
                Verdict::Ineligible(Reason::Bundle),
                "{exe}"
            );
        }
        // The agent CLIs: by name, and Claude Code by install path because its process name
        // is its version string.
        let by_name = [member("claude", "/opt/bin/claude")];
        let by_path = [member(
            "2.1.318",
            "/opt/agent/.local/share/claude/versions/2.1.318",
        )];
        let codex = [member("codex", "/opt/bin/codex")];
        for members in [&by_name[..], &by_path[..], &codex[..]] {
            assert_eq!(
                classify(&r, 5000, 501, members),
                Verdict::Ineligible(Reason::Never)
            );
        }
        // An agent that is not the leader still protects the whole group.
        let mixed = [member("sh", "/bin/sh"), member("claude", "/opt/bin/claude")];
        assert_eq!(
            classify(&r, 5000, 501, &mixed),
            Verdict::Ineligible(Reason::Never)
        );
        // --never adds to the built-ins and matches the leader's command line too.
        let custom = rules(vec![], vec![], &["build\\.js"]);
        assert_eq!(
            classify(&custom, 5000, 501, &tsc),
            Verdict::Ineligible(Reason::Never)
        );
    }

    /// uid < 500 is lifted by `--include-user` only. `--protect-user` ranks jobs that are
    /// already eligible; it must not make a system account's jobs killable. --never still
    /// wins over an included user.
    #[test]
    fn only_include_user_lifts_the_system_user_rule() {
        let tsc = [member("tsc", "/usr/local/bin/node")];
        let r = rules(vec![200], vec![], &[]);
        assert_eq!(
            classify(&r, 5000, 200, &tsc),
            Verdict::Ineligible(Reason::SystemUser),
            "protecting a system account does not make its jobs eligible"
        );
        assert_eq!(
            r.precheck(5000, 200),
            Some(Reason::SystemUser),
            "nor does it pass the cheap pre-check the scan applies"
        );
        let r = rules(vec![], vec![201], &[]);
        assert_eq!(classify(&r, 5000, 201, &tsc), PLAIN);
        assert_eq!(
            classify(&r, 5000, 202, &tsc),
            Verdict::Ineligible(Reason::SystemUser)
        );
        // Protected and included: eligible, and ranked last.
        let r = rules(vec![200], vec![200], &[]);
        assert_eq!(
            classify(&r, 5000, 200, &tsc),
            Verdict::Eligible {
                prefer: false,
                protected: true
            }
        );
        let agent = [member("codex", "/opt/bin/codex")];
        assert_eq!(
            classify(&r, 5000, 200, &agent),
            Verdict::Ineligible(Reason::Never)
        );
    }

    /// Every member's uid counts, not only the leader's: a set-uid helper (sudo, login) that
    /// joined an ordinary user's group makes the whole group untouchable.
    #[test]
    fn a_member_of_another_user_makes_the_group_ineligible() {
        let r = rules(vec![], vec![], &[]);
        let as_user = |uid| Member {
            uid,
            ..member("tsc", "/usr/local/bin/node")
        };
        let group = |uids: &[u32]| {
            let members: Vec<Member> = uids.iter().map(|&u| as_user(u)).collect();
            r.classify(5000, uids[0], &members, "node build.js")
        };
        assert_eq!(group(&[501, 501]), PLAIN);
        assert_eq!(
            group(&[501, 501, 0]),
            Verdict::Ineligible(Reason::SystemUser)
        );
        assert_eq!(group(&[501, 499]), Verdict::Ineligible(Reason::SystemUser));
        assert_eq!(group(&[501, 502]), Verdict::Ineligible(Reason::MixedUsers));
        // Including a system user lifts the uid rule for it, but never mixes users in a group.
        let r = rules(vec![], vec![0], &[]);
        let members = [as_user(0), as_user(0)];
        assert_eq!(r.classify(5000, 0, &members, "node build.js"), PLAIN);
        let members = [as_user(501), as_user(0)];
        assert_eq!(
            r.classify(5000, 501, &members, "node build.js"),
            Verdict::Ineligible(Reason::MixedUsers)
        );
    }

    /// The agent CLIs are found however they were launched: by name, Nix-wrapped, or as a
    /// script run by node. Look-alike names are left alone.
    #[test]
    fn agent_clis_are_recognised_in_every_launch_form() {
        let r = rules(vec![], vec![], &[]);
        let leader = |name: &str, exe: &str, command: &str| {
            let members = [member(name, exe)];
            r.classify(5000, 501, &members, command)
        };
        let never = Verdict::Ineligible(Reason::Never);
        assert_eq!(
            leader(
                "node",
                "/usr/local/bin/node",
                "node /x/node_modules/@anthropic-ai/claude-code/cli.js --resume"
            ),
            never
        );
        assert_eq!(
            leader(
                "node",
                "/usr/local/bin/node",
                "node /opt/agents/bin/codex exec fix"
            ),
            never
        );
        assert_eq!(leader("codex", "/opt/agents/bin/codex", "codex"), never);
        for wrapped in [".tmux-wrapped", ".herdr-wrapped", ".zellij-wrapped"] {
            let exe = format!("/nix/store/abc-pkg/bin/{wrapped}");
            assert_eq!(leader(wrapped, &exe, wrapped), never, "{wrapped}");
            assert_eq!(leader("node", &exe, "node"), never, "{wrapped} by path");
        }
        assert_eq!(
            leader(
                "node",
                "/usr/local/bin/node",
                "node /opt/codex-tools/run.js"
            ),
            PLAIN
        );
        assert_eq!(
            leader(
                "tmuxinator",
                "/usr/local/bin/tmuxinator",
                "tmuxinator start"
            ),
            PLAIN
        );
    }

    /// An agent hosted by an interpreter shows only in that member's own arguments: its name
    /// and path are `node`'s. It protects the group it runs in, whoever leads it.
    #[test]
    fn an_interpreter_hosted_agent_in_any_member_protects_the_group() {
        let r = rules(vec![], vec![], &[]);
        let shell = member("sh", "/bin/sh");
        let hosted = Member {
            command: "node /opt/agents/bin/codex exec fix".into(),
            ..member("node", "/usr/local/bin/node")
        };
        assert_eq!(
            classify(&r, 5000, 501, &[shell.clone(), hosted]),
            Verdict::Ineligible(Reason::Never)
        );
        let look_alike = Member {
            command: "node /opt/codex-tools/run.js".into(),
            ..member("node", "/usr/local/bin/node")
        };
        assert_eq!(classify(&r, 5000, 501, &[shell, look_alike]), PLAIN);
    }

    #[test]
    fn prefer_marks_matching_jobs() {
        let r = rules(vec![], vec![], &[]);
        let vitest = [member("vitest", "/usr/local/bin/node")];
        assert_eq!(
            classify(&r, 5000, 501, &vitest),
            Verdict::Eligible {
                prefer: true,
                protected: false
            }
        );
    }

    #[test]
    fn bad_regex_is_reported() {
        assert!(Rules::new(&["(".to_string()], &[], vec![], vec![], 1).is_err());
    }

    struct Sleeper(Child);
    impl Drop for Sleeper {
        fn drop(&mut self) {
            let _ = self.0.kill();
            let _ = self.0.wait();
        }
    }

    /// The riskiest FFI in the crate: struct layouts and units of the libproc calls, checked
    /// against a real process group whose facts the test knows.
    #[test]
    fn scan_sees_a_real_process_group() {
        let child = Sleeper(
            Command::new("sleep")
                .arg("300")
                .process_group(0)
                .spawn()
                .unwrap(),
        );
        let pid = child.0.id() as i32;
        // Include the current user explicitly: a build sandbox runs as a uid below 500.
        let me = unsafe { libc::getuid() };
        let r = Rules::new(&[], &[], vec![], vec![me], unsafe { libc::getpgrp() }).unwrap();
        let jobs = scan(&r, 0);
        let job = jobs
            .iter()
            .find(|j| j.pgid == pid)
            .expect("the sleep group is listed");
        assert_eq!(job.leader_pid, pid);
        assert_eq!(job.procs, 1);
        assert_eq!(job.uid, unsafe { libc::getuid() });
        assert!(!job.stopped);
        assert!(job.command.starts_with("sleep 300"), "{}", job.command);
        let now_us = crate::signals::wall() * 1e6;
        assert!(job.start_us as f64 <= now_us && now_us - (job.start_us as f64) < 300e6);
        // membrake's own group is never listed.
        assert!(jobs.iter().all(|j| j.pgid != unsafe { libc::getpgrp() }));
    }

    fn spawn(arg0: Option<&str>, pgid: i32) -> Sleeper {
        let mut cmd = Command::new("sleep");
        if let Some(arg0) = arg0 {
            cmd.arg0(arg0);
        }
        Sleeper(cmd.arg("300").process_group(pgid).spawn().unwrap())
    }

    fn pid_of(s: &Sleeper) -> i32 {
        s.0.id() as i32
    }

    /// Rules that include the current user (a build sandbox runs as a uid below 500).
    fn my_rules() -> Rules {
        let me = unsafe { libc::getuid() };
        Rules::new(&[], &[], vec![], vec![me], unsafe { libc::getpgrp() }).unwrap()
    }

    /// Kills the whole process group when dropped.
    struct KillGroup(i32);
    impl Drop for KillGroup {
        fn drop(&mut self) {
            // SAFETY: signalling a group this test spawned.
            unsafe { libc::killpg(self.0, libc::SIGKILL) };
        }
    }

    /// The agent protections read the complete arguments of every member, not only the
    /// leader's, and not the shortened text shown to people: here an agent is a shell running
    /// a script called codex in a directory 240 characters deep, a member of a group whose
    /// leader is a plain `sleep`. The leader's command line and the member's name and path
    /// (`sh`, `/bin/sh`) say nothing of an agent, and the shortened command line ends before
    /// its name.
    #[test]
    fn scan_protects_a_group_by_the_full_arguments_of_any_member() {
        let dir = crate::testutil::scratch("agent-path");
        let deep = dir.join("a".repeat(120)).join("b".repeat(120));
        std::fs::create_dir_all(&deep).unwrap();
        let agent = deep.join("codex");
        std::fs::write(&agent, "#!/bin/sh\nsleep 300\n").unwrap();

        let leader = spawn(None, 0);
        let pgid = pid_of(&leader);
        let _group = KillGroup(pgid);
        let host = Sleeper(
            Command::new("/bin/sh")
                .arg(&agent)
                .process_group(pgid)
                .spawn()
                .unwrap(),
        );
        let control = spawn(None, 0);
        let jobs = scan(&my_rules(), 0);
        assert!(
            jobs.iter().all(|j| j.pgid != pgid),
            "a member's arguments name an agent"
        );
        assert!(
            jobs.iter().any(|j| j.pgid == pid_of(&control)),
            "an ordinary group is listed"
        );
        drop(host);
        let _ = std::fs::remove_dir_all(&dir);
    }

    /// What cannot be read is not judged: a member whose path cannot be read makes its group
    /// ineligible, and `spared` says so before anything is killed.
    #[test]
    fn a_group_with_an_unreadable_member_is_not_eligible() {
        let sleeper = spawn(None, 0);
        let pgid = pid_of(&sleeper);
        let rules = my_rules();
        assert!(scan(&rules, 0).iter().any(|j| j.pgid == pgid), "readable");
        assert_eq!(spared(&rules, pgid), None);

        fault::arm(&fault::READ, u32::MAX);
        assert!(scan(&rules, 0).iter().all(|j| j.pgid != pgid));
        assert!(matches!(
            read_group(pgid, &mut Inspector::new()),
            Read::Unreadable(_)
        ));
        let why = spared(&rules, pgid).expect("a group that cannot be read is not killed");
        fault::arm(&fault::READ, 0);
        assert!(why.contains("cannot be inspected"), "{why}");
    }

    /// The same without injecting anything: launchd's group holds a process owned by root,
    /// which a normal user cannot read at all.
    #[test]
    fn a_member_owned_by_root_is_unreadable_to_a_normal_user() {
        assert_ne!(
            unsafe { libc::getuid() },
            0,
            "the tests run as a normal user"
        );
        assert!(matches!(
            read_group(1, &mut Inspector::new()),
            Read::Unreadable(_)
        ));
    }

    /// A queue session is the nearest ancestor that is an agent CLI, however many other
    /// processes lie between, and the parent when there is none. The agent here is a shell
    /// running a script called `codex`; between it and the process asking there is an
    /// ordinary shell.
    #[test]
    fn a_session_is_the_nearest_agent_ancestor_else_the_parent() {
        let dir = crate::testutil::scratch("session");
        std::fs::create_dir_all(&dir).unwrap();
        let pid_file = dir.join("pid");
        let agent = dir.join("codex");
        std::fs::write(
            &agent,
            "#!/bin/sh\nsh -c 'sleep 300 & echo $! > \"$0\"; wait' \"$1\"\n",
        )
        .unwrap();
        let mut host = Command::new("/bin/sh")
            .arg(&agent)
            .arg(&pid_file)
            .spawn()
            .unwrap();
        let deadline = std::time::Instant::now() + std::time::Duration::from_secs(10);
        let asking = loop {
            if let Some(pid) = std::fs::read_to_string(&pid_file)
                .ok()
                .and_then(|t| t.trim().parse::<i32>().ok())
            {
                break pid;
            }
            assert!(std::time::Instant::now() < deadline, "the script never ran");
            std::thread::sleep(std::time::Duration::from_millis(10));
        };

        let agents = Regex::new(AGENT_CLI).unwrap();
        assert_eq!(session_of(&agents, asking), host.id() as i32, "the agent");
        let nobody = Regex::new("^no process is called this$").unwrap();
        let parent = bsd_info(asking).unwrap().ppid;
        assert_ne!(parent, host.id() as i32, "an ordinary shell lies between");
        assert_eq!(session_of(&nobody, asking), parent, "else the parent");
        assert_eq!(
            session_of(&agents, i32::MAX),
            i32::MAX,
            "a process that is gone"
        );

        // SAFETY: signalling the process the script started, which this test owns.
        unsafe { libc::kill(asking, libc::SIGKILL) };
        let _ = host.kill();
        let _ = host.wait();
        let _ = std::fs::remove_dir_all(&dir);
    }

    /// A process that exited is told apart from one that cannot be read, and a group nobody
    /// is in from a group the kernel failed to list: only the first of each is "gone".
    #[test]
    fn gone_absent_and_unknown_are_told_apart() {
        let mut child = spawn(None, 0);
        let pid = pid_of(&child);
        let mut insp = Inspector::new();
        assert!(matches!(probe(pid), Probe::Found(_)));
        assert!(matches!(insp.look(pid), Look::Present));
        assert_eq!(insp.members().len(), 1);

        fault::arm(&fault::LIST, 1);
        assert!(
            matches!(insp.look(pid), Look::Unknown(_)),
            "a failed listing is not an empty group"
        );
        assert!(matches!(insp.look(pid), Look::Present));

        child.0.kill().unwrap();
        child.0.wait().unwrap();
        assert!(matches!(probe(pid), Probe::Gone));
        assert!(matches!(insp.look(pid), Look::Absent));
    }
}
