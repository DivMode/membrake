//! Jobs are process groups: shells and coding agents start every command in its own group,
//! so a worker pool is one job and one `killpg` reaches all of it.
//!
//! This module scans the process table through libproc (no subprocess, no `ps`), groups
//! processes, and decides which groups may ever be touched.

use std::collections::HashMap;
use std::ffi::CString;
use std::mem::MaybeUninit;

/// Groups led by a user below this uid (root and the system accounts) are never touched,
/// unless the user is named by `--protect-user` or `--include-user`.
pub const FIRST_USER_UID: u32 = 500;

/// Always protected, whatever `--never` says: the agent CLIs themselves, terminal
/// multiplexers and terminals. A POSIX extended regex matched against each member's process
/// name and executable path and against the leader's command line:
/// - the names, including the Nix-wrapped `.tmux-wrapped` style;
/// - Claude Code by install directory, because its process name is its version string, and
///   by package name when it runs under node;
/// - a `codex` binary, including one run via node (`node /path/to/codex ...`).
pub const ALWAYS_NEVER: &str = r"^(claude|codex|tmux|herdr|zellij|screen|ghostty|alacritty|kitty|wezterm|wezterm-gui|\.(herdr|tmux|zellij)-wrapped)$|/\.(herdr|tmux|zellij)-wrapped$|/\.local/share/claude/|@anthropic-ai/claude-code|/codex( |$)";

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

/// Replaces control characters so a command fits on one line of a log or TSV.
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
    /// Matched `--never` or the built-in agent and terminal list.
    Never,
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
        })
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
        if leader_uid < FIRST_USER_UID && !self.named(leader_uid) {
            return Some(Reason::SystemUser);
        }
        None
    }

    /// A user named by `--protect-user` or `--include-user` may have a uid below 500.
    fn named(&self, uid: u32) -> bool {
        self.protect_uids.contains(&uid) || self.include_uids.contains(&uid)
    }

    /// `leader_command` is the leader's full command line; members are matched by name and
    /// executable path, so a protected process anywhere in the group protects the group.
    /// Every member's uid is checked, not only the leader's: a group is ineligible if any
    /// member is a system account (unless named) or runs as a different user than the leader.
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
        if members
            .iter()
            .any(|m| m.uid < FIRST_USER_UID && !self.named(m.uid))
        {
            return Verdict::Ineligible(Reason::SystemUser);
        }
        if members.iter().any(|m| m.uid != leader_uid) {
            return Verdict::Ineligible(Reason::MixedUsers);
        }
        if members.iter().any(|m| in_bundle_or_system(&m.exe)) {
            return Verdict::Ineligible(Reason::Bundle);
        }
        let hit = |list: &[Regex], text: &str| list.iter().any(|r| r.is_match(text));
        if hit(&self.never, leader_command)
            || members
                .iter()
                .any(|m| hit(&self.never, &m.name) || hit(&self.never, &m.exe))
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

/// What `proc_pidinfo(PROC_PIDTBSDINFO)` says about one process.
#[derive(Clone, Debug)]
pub struct Proc {
    pub pid: i32,
    pub pgid: i32,
    pub uid: u32,
    pub start_us: u64,
    pub name: String,
    pub zombie: bool,
    pub stopped: bool,
}

fn c_chars(raw: &[libc::c_char]) -> String {
    let bytes: Vec<u8> = raw
        .iter()
        .take_while(|&&c| c != 0)
        .map(|&c| c as u8)
        .collect();
    String::from_utf8_lossy(&bytes).into_owned()
}

pub fn bsd_info(pid: i32) -> Option<Proc> {
    let mut info = MaybeUninit::<libc::proc_bsdinfo>::zeroed();
    let size = size_of::<libc::proc_bsdinfo>() as libc::c_int;
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
    if got != size {
        return None;
    }
    // SAFETY: the kernel filled the whole struct.
    let info = unsafe { info.assume_init() };
    let name = match c_chars(&info.pbi_name) {
        n if n.is_empty() => c_chars(&info.pbi_comm),
        n => n,
    };
    Some(Proc {
        pid,
        pgid: info.pbi_pgid as i32,
        uid: info.pbi_uid,
        start_us: info.pbi_start_tvsec * 1_000_000 + info.pbi_start_tvusec,
        name,
        zombie: info.pbi_status == libc::SZOMB,
        stopped: info.pbi_status == libc::SSTOP,
    })
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

pub fn exe_path(pid: i32) -> Option<String> {
    let mut buf = vec![0u8; libc::PROC_PIDPATHINFO_MAXSIZE as usize];
    // SAFETY: `buf` is as long as the size passed.
    let n = unsafe { libc::proc_pidpath(pid, buf.as_mut_ptr().cast(), buf.len() as u32) };
    (n > 0).then(|| String::from_utf8_lossy(&buf[..n as usize]).into_owned())
}

/// The command line, from `KERN_PROCARGS2`. Read only for candidates, never every sample.
pub fn command_line(pid: i32) -> Option<String> {
    let mut mib = [libc::CTL_KERN, libc::KERN_PROCARGS2, pid];
    let mut size = 0usize;
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
    if rc != 0 || size <= size_of::<i32>() {
        return None;
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
        return None;
    }
    buf.truncate(size);
    let argc = i32::from_ne_bytes(buf[..4].try_into().ok()?).max(0) as usize;
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
    (!args.is_empty()).then(|| one_line(&args.join(" "), COMMAND_CHARS))
}

fn leader_of(pgid: i32, procs: &[Proc]) -> Option<&Proc> {
    procs
        .iter()
        .find(|p| p.pid == pgid)
        .or_else(|| procs.iter().min_by_key(|p| p.start_us))
}

/// Every eligible group of at least `min_bytes`. The one place that reads the process table.
pub fn scan(rules: &Rules, min_bytes: u64) -> Vec<Job> {
    let mut groups: HashMap<i32, Vec<Proc>> = HashMap::new();
    for pid in list_pids() {
        if let Some(p) = bsd_info(pid).filter(|p| !p.zombie) {
            groups.entry(p.pgid).or_default().push(p);
        }
    }
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
        let members: Vec<Member> = procs
            .iter()
            .map(|p| Member {
                name: p.name.clone(),
                exe: exe_path(p.pid).unwrap_or_default(),
                uid: p.uid,
            })
            .collect();
        let command =
            command_line(leader.pid).unwrap_or_else(|| one_line(&leader.name, COMMAND_CHARS));
        if let Verdict::Eligible { prefer, protected } =
            rules.classify(pgid, leader.uid, &members, &command)
        {
            jobs.push(Job {
                pgid,
                leader_pid: leader.pid,
                start_us: leader.start_us,
                uid: leader.uid,
                bytes,
                procs: procs.len() as u32,
                prefer,
                protected,
                stopped: procs.iter().all(|p| p.stopped),
                command,
            });
        }
    }
    jobs
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

    /// uid < 500 is lifted for named users only; --never still wins over a protected user.
    #[test]
    fn named_users_lift_the_system_user_rule() {
        let tsc = [member("tsc", "/usr/local/bin/node")];
        let r = rules(vec![200], vec![], &[]);
        assert_eq!(
            classify(&r, 5000, 200, &tsc),
            Verdict::Eligible {
                prefer: false,
                protected: true
            }
        );
        let r = rules(vec![], vec![201], &[]);
        assert_eq!(classify(&r, 5000, 201, &tsc), PLAIN);
        assert_eq!(
            classify(&r, 5000, 202, &tsc),
            Verdict::Ineligible(Reason::SystemUser)
        );
        let r = rules(vec![200], vec![], &[]);
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
        // Naming a system user lifts the uid rule for it, but never mixes users in a group.
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
}
