//! The flight recorder: one TSV line per sample and a human-readable event log, both
//! size-capped with one rotated generation. `replay` reads the TSV back through the engine.
//!
//! Nothing here can stop the brake: every write is best effort, a file that cannot be opened
//! or reopened is retried on the next write, and no event is fsynced inline (the runner syncs
//! after it has signalled). A gap in the samples means membrake itself was not running.

use crate::engine::{Action, Engine, Policy, Step};
use crate::jobs::{Job, one_line};
use crate::signals::{Pressure, Signals};
use std::fmt::Write as _;
use std::fs::{self, File, OpenOptions};
use std::io::{self, BufRead, Read, Seek, SeekFrom, Write};
use std::path::{Path, PathBuf};

pub const SAMPLES_FILE: &str = "samples.tsv";
pub const EVENTS_FILE: &str = "events.log";
const SAMPLES_CAP: u64 = 32 << 20;
const EVENTS_CAP: u64 = 8 << 20;
const SYNC_EVERY: f64 = 10.0;

/// `time` is the wall clock (for people); `uptime` is the engine's clock, which `replay`
/// uses. The next seven columns are the raw readings `replay` feeds the engine; `occupancy`,
/// `eta_s` and `tight` are derived and informational.
///
/// `jobs` is the candidate list the engine was shown on that sample. Empty means the process
/// table was not scanned; `-` means it was scanned and nothing qualified. Otherwise entries
/// `pgid:leader_pid:start_us:uid:bytes:procs:flags:command` joined by `;`, with flags among
/// `p` (preferred), `P` (protected user), `s` (stopped) or `-`.
pub const HEADER: &str = "time\tuptime\tsegments\tpages\tswap_used\theadroom\tram\tlevel\tpressure\toccupancy\teta_s\ttight\tjobs\n";

/// An append-only file that moves itself to `<name>.1` when the next write would pass the
/// cap. It opens lazily and re-opens after any failure, so a full disk or a missing
/// directory costs log lines, never the daemon.
struct Log {
    path: PathBuf,
    file: Option<File>,
    written: u64,
    cap: u64,
    header: &'static str,
}

impl Log {
    fn new(path: PathBuf, cap: u64, header: &'static str) -> Log {
        Log {
            path,
            file: None,
            written: 0,
            cap,
            header,
        }
    }

    /// Opens the file (and its directory) if it is not open, and makes it well formed: a
    /// header on an empty file, a newline after a torn last line.
    fn ensure_open(&mut self) -> io::Result<()> {
        if self.file.is_some() {
            return Ok(());
        }
        if let Some(dir) = self.path.parent() {
            fs::create_dir_all(dir)?;
        }
        let mut file = OpenOptions::new()
            .create(true)
            .append(true)
            .open(&self.path)?;
        let len = file.metadata()?.len();
        let mut written = len;
        if len == 0 {
            file.write_all(self.header.as_bytes())?;
            written += self.header.len() as u64;
        } else if !ends_with_newline(&self.path, len)? {
            file.write_all(b"\n")?;
            written += 1;
        }
        self.file = Some(file);
        self.written = written;
        Ok(())
    }

    fn rotate(&mut self) -> io::Result<()> {
        let mut old = self.path.clone().into_os_string();
        old.push(".1");
        fs::rename(&self.path, old)?;
        // If the new file cannot be created `file` stays None and the next write retries.
        self.file = None;
        self.ensure_open()
    }

    fn append(&mut self, bytes: &[u8]) -> io::Result<()> {
        self.ensure_open()?;
        if self.written + bytes.len() as u64 > self.cap && self.written > self.header.len() as u64 {
            self.rotate()?;
        }
        let Some(file) = self.file.as_mut() else {
            return Err(io::Error::other("log file is not open"));
        };
        if let Err(e) = file.write_all(bytes) {
            // Possibly a partial line: reopen next time, which also restores the newline.
            self.file = None;
            return Err(e);
        }
        self.written += bytes.len() as u64;
        Ok(())
    }

    fn sync(&self) -> io::Result<()> {
        self.file.as_ref().map_or(Ok(()), File::sync_data)
    }
}

fn ends_with_newline(path: &Path, len: u64) -> io::Result<bool> {
    let mut file = File::open(path)?;
    file.seek(SeekFrom::Start(len - 1))?;
    let mut last = [0u8; 1];
    file.read_exact(&mut last)?;
    Ok(last[0] == b'\n')
}

pub struct Recorder {
    samples: Log,
    events: Log,
    line: String,
    last_sync: f64,
    /// Events have been written since the last sync.
    dirty: bool,
}

impl Recorder {
    /// Never fails: the files open on first use (see `ensure_open` to learn of a problem).
    pub fn new(dir: &Path) -> Recorder {
        Recorder::with_caps(dir, SAMPLES_CAP, EVENTS_CAP)
    }

    /// `new`, then open both files now so a problem is reported at start-up.
    pub fn open(dir: &Path) -> io::Result<Recorder> {
        let mut rec = Recorder::new(dir);
        rec.ensure_open()?;
        Ok(rec)
    }

    fn with_caps(dir: &Path, samples_cap: u64, events_cap: u64) -> Recorder {
        Recorder {
            samples: Log::new(dir.join(SAMPLES_FILE), samples_cap, HEADER),
            events: Log::new(dir.join(EVENTS_FILE), events_cap, ""),
            line: String::with_capacity(1024),
            last_sync: 0.0,
            dirty: false,
        }
    }

    /// Tries to open both files; reports the first failure after trying both.
    pub fn ensure_open(&mut self) -> io::Result<()> {
        let samples = self.samples.ensure_open();
        let events = self.events.ensure_open();
        samples.and(events)
    }

    /// Appends one sample. `jobs` is the candidate list the engine was shown, if it asked
    /// for one (`Some(&[])` records a scan that found nothing). Allocation-free once the
    /// line buffer has grown to fit.
    pub fn sample(&mut self, s: &Signals, step: &Step, jobs: Option<&[Job]>) -> io::Result<()> {
        format_row(&mut self.line, s, step, jobs);
        self.samples.append(self.line.as_bytes())?;
        if s.time - self.last_sync >= SYNC_EVERY {
            self.last_sync = s.time;
            self.samples.sync()?;
        }
        Ok(())
    }

    /// Writes one event to stdout (the service log) and the event log. Best effort and never
    /// blocks on the disk: call `sync_events` later, after any signal has been sent.
    pub fn event(&mut self, wall: f64, kind: &str, text: &str) {
        let mut line = String::new();
        timestamp(&mut line, wall);
        let _ = writeln!(line, " {kind} {}", one_line(text, 1000));
        // Not print!: that panics when stdout is closed, and logging must never stop the brake.
        let _ = io::stdout().write_all(line.as_bytes());
        let _ = io::stdout().flush();
        let _ = self.events.append(line.as_bytes());
        self.dirty = true;
    }

    /// Flushes events (and the samples around them) to disk if any were written.
    pub fn sync_events(&mut self) {
        if self.dirty {
            self.dirty = false;
            let _ = self.events.sync();
            let _ = self.samples.sync();
        }
    }
}

/// One TSV line (with its newline) in `line`, which is cleared first. Allocation-free once
/// the buffer has grown to fit.
pub fn format_row(line: &mut String, s: &Signals, step: &Step, jobs: Option<&[Job]>) {
    line.clear();
    let _ = write!(
        line,
        "{:.3}\t{:.3}\t{:.4}\t{:.4}\t{}\t{}\t{}\t{}\t{}\t{:.4}\t",
        s.wall,
        s.time,
        s.segments,
        s.pages,
        s.swap_used,
        s.headroom,
        s.ram,
        s.level,
        s.pressure as u8,
        step.occupancy
    );
    if step.eta.is_finite() {
        let _ = write!(line, "{}", step.eta as u64);
    }
    let _ = write!(line, "\t{}\t", u8::from(step.tight));
    match jobs {
        None => {}
        Some([]) => line.push('-'),
        Some(jobs) => {
            for (i, j) in jobs.iter().enumerate() {
                if i > 0 {
                    line.push(';');
                }
                let _ = write!(
                    line,
                    "{}:{}:{}:{}:{}:{}:",
                    j.pgid, j.leader_pid, j.start_us, j.uid, j.bytes, j.procs
                );
                for (set, flag) in [(j.prefer, 'p'), (j.protected, 'P'), (j.stopped, 's')] {
                    if set {
                        line.push(flag);
                    }
                }
                if line.ends_with(':') {
                    line.push('-');
                }
                line.push(':');
                line.extend(
                    j.command
                        .chars()
                        .map(|c| if c == ';' || c.is_control() { ' ' } else { c }),
                );
            }
        }
    }
    line.push('\n');
}

/// `2026-11-05T16:20:01Z`
pub fn timestamp(out: &mut String, time: f64) {
    let secs = time.floor() as libc::time_t;
    // SAFETY: gmtime_r writes into the zeroed tm we pass.
    let tm = unsafe {
        let mut tm: libc::tm = std::mem::zeroed();
        libc::gmtime_r(&secs, &mut tm);
        tm
    };
    let _ = write!(
        out,
        "{:04}-{:02}-{:02}T{:02}:{:02}:{:02}Z",
        tm.tm_year + 1900,
        tm.tm_mon + 1,
        tm.tm_mday,
        tm.tm_hour,
        tm.tm_min,
        tm.tm_sec
    );
}

pub fn job_text(job: &Job) -> String {
    format!(
        "{:.1}GB group {} ({} procs, uid {}): {}",
        job.bytes as f64 / (1u64 << 30) as f64,
        job.pgid,
        job.procs,
        job.uid,
        job.command
    )
}

/// An action as an event kind and line of text; the runner and `replay` print the same words.
pub fn describe(action: &Action) -> (&'static str, String) {
    match action {
        Action::Stall { gap } => (
            "stall",
            format!("{gap:.1}s since the previous sample; the fill rate was reset"),
        ),
        Action::Warn { text } => ("warn", text.clone()),
        Action::Freeze { job, reason } => ("freeze", format!("{} [{reason}]", job_text(job))),
        Action::Resume { job } => ("resume", job_text(job)),
        Action::Kill {
            job,
            immediate,
            reason,
        } => (
            "kill",
            format!(
                "{} [{reason}{}]",
                job_text(job),
                if *immediate { "; immediate" } else { "" }
            ),
        ),
    }
}

// ---------------------------------------------------------------------------------------
// Reading it back

pub struct Row {
    pub signals: Signals,
    /// None: the process table was not scanned on this sample.
    pub jobs: Option<Vec<Job>>,
}

fn parse_job(entry: &str) -> Option<Job> {
    let mut f = entry.splitn(8, ':');
    let (pgid, leader_pid, start_us, uid, bytes, procs, flags) = (
        f.next()?.parse().ok()?,
        f.next()?.parse().ok()?,
        f.next()?.parse().ok()?,
        f.next()?.parse().ok()?,
        f.next()?.parse().ok()?,
        f.next()?.parse().ok()?,
        f.next()?,
    );
    Some(Job {
        pgid,
        leader_pid,
        start_us,
        uid,
        bytes,
        procs,
        prefer: flags.contains('p'),
        protected: flags.contains('P'),
        stopped: flags.contains('s'),
        command: f.next()?.to_string(),
    })
}

pub fn parse_row(line: &str) -> Result<Row, String> {
    let f: Vec<&str> = line.split('\t').collect();
    if f.len() != 13 {
        return Err(format!(
            "expected 13 tab-separated columns, found {}",
            f.len()
        ));
    }
    let num = |i: usize| -> Result<f64, String> {
        f[i].parse()
            .map_err(|_| format!("column {} is not a number: {:?}", i + 1, f[i]))
    };
    let int = |i: usize| -> Result<u64, String> {
        f[i].parse()
            .map_err(|_| format!("column {} is not an integer: {:?}", i + 1, f[i]))
    };
    let jobs = match f[12] {
        "" => None,
        "-" => Some(Vec::new()),
        list => Some(
            list.split(';')
                .map(|e| parse_job(e).ok_or_else(|| format!("bad job entry {e:?}")))
                .collect::<Result<_, _>>()?,
        ),
    };
    Ok(Row {
        signals: Signals {
            wall: num(0)?,
            time: num(1)?,
            segments: num(2)?,
            pages: num(3)?,
            swap_used: int(4)?,
            headroom: int(5)?,
            ram: int(6)?,
            level: i32::try_from(int(7)?).map_err(|_| "level out of range".to_string())?,
            pressure: Pressure::from_raw(int(8)?),
        },
        jobs,
    })
}

/// First field of the line that stands for a tick whose signals could not be read, in a
/// script for `run --simulate`: `unreadable<TAB>UPTIME`. Never written by the recorder.
pub const UNREADABLE: &str = "unreadable";

/// One line of a script for `run --simulate` (test only).
pub enum ScriptedRow {
    Signals(Signals),
    Unreadable { time: f64 },
}

/// A samples.tsv row, or the `unreadable` marker line. The jobs column of a row is ignored.
pub fn parse_scripted(line: &str) -> Result<ScriptedRow, String> {
    let Some(rest) = line
        .strip_prefix(UNREADABLE)
        .and_then(|r| r.strip_prefix('\t'))
    else {
        return parse_row(line).map(|row| ScriptedRow::Signals(row.signals));
    };
    match rest.parse::<f64>() {
        Ok(time) if time.is_finite() => Ok(ScriptedRow::Unreadable { time }),
        _ => Err(format!("{UNREADABLE} needs an uptime, found {rest:?}")),
    }
}

#[derive(Debug, Default, PartialEq, Eq)]
pub struct Summary {
    pub samples: u64,
    pub freezes: u64,
    pub resumes: u64,
    pub kills: u64,
    pub stalls: u64,
}

/// Feeds a recorded TSV through a fresh engine and prints what it would do. The candidate
/// list is the last one recorded (a scan that found nothing clears it), less jobs the replay
/// has killed; nothing is signalled. A torn final line, as left by a crash, is skipped with
/// a warning; a malformed complete line is an error.
pub fn replay(input: impl BufRead, policy: Policy, out: &mut impl Write) -> io::Result<Summary> {
    replay_with(input, policy, out, false)
}

/// The colour of an event kind on a terminal: freeze blue, resume green, kill red.
fn paint(kind: &str) -> &'static str {
    match kind {
        "freeze" => "\x1b[1;34m",
        "resume" => "\x1b[1;32m",
        "kill" => "\x1b[1;31m",
        _ => "\x1b[33m",
    }
}

/// `replay`, optionally colouring each event kind (for a terminal).
pub fn replay_with(
    mut input: impl BufRead,
    policy: Policy,
    out: &mut impl Write,
    color: bool,
) -> io::Result<Summary> {
    let mut engine = Engine::new(policy);
    let mut summary = Summary::default();
    let mut jobs: Vec<Job> = Vec::new();
    let mut killed: Vec<(i32, u64)> = Vec::new();
    let mut raw = Vec::new();
    let mut n = 0;
    loop {
        raw.clear();
        if input.read_until(b'\n', &mut raw)? == 0 {
            break;
        }
        n += 1;
        let complete = raw.ends_with(b"\n");
        let text = String::from_utf8_lossy(&raw);
        let line = text.trim_end_matches(['\n', '\r']);
        if line.is_empty() || line.starts_with("time\t") {
            continue;
        }
        let row = match parse_row(line) {
            Ok(row) => row,
            Err(e) if !complete => {
                writeln!(out, "warn: line {n} is a torn final line, skipped ({e})")?;
                break;
            }
            Err(e) => {
                return Err(io::Error::new(
                    io::ErrorKind::InvalidData,
                    format!("line {n}: {e}"),
                ));
            }
        };
        if let Some(mut list) = row.jobs {
            // A recording made without these kills (or a synthetic one) still lists the
            // jobs; a killed job is gone, so it must not be chosen again.
            list.retain(|j| {
                !killed
                    .iter()
                    .any(|&(pgid, start)| j.pgid == pgid && j.start_us == start)
            });
            jobs = list;
        }
        summary.samples += 1;
        let step = engine.step(&row.signals, || jobs.clone());
        for action in &step.actions {
            match action {
                Action::Stall { .. } => summary.stalls += 1,
                Action::Freeze { .. } => summary.freezes += 1,
                Action::Resume { .. } => summary.resumes += 1,
                Action::Kill { job, .. } => {
                    summary.kills += 1;
                    killed.push((job.pgid, job.start_us));
                    jobs.retain(|j| j.pgid != job.pgid);
                }
                Action::Warn { .. } => {}
            }
            let (kind, text) = describe(action);
            let mut ts = String::new();
            timestamp(&mut ts, row.signals.wall);
            if color {
                writeln!(
                    out,
                    "\x1b[2m{ts}\x1b[0m {}{kind}\x1b[0m {text}",
                    paint(kind)
                )?;
            } else {
                writeln!(out, "{ts} {kind} {text}")?;
            }
        }
    }
    writeln!(
        out,
        "replayed {} samples: {} freezes, {} resumes, {} kills, {} stalls",
        summary.samples, summary.freezes, summary.resumes, summary.kills, summary.stalls
    )?;
    Ok(summary)
}

#[cfg(test)]
mod tests {
    use super::*;

    fn scratch(name: &str) -> PathBuf {
        let dir =
            std::env::temp_dir().join(format!("membrake-test-{}-rec-{name}", std::process::id()));
        let _ = fs::remove_dir_all(&dir);
        dir
    }

    /// Sample `i`: engine time 5000+i, wall clock 1_700_000_000+i.
    fn signals(i: u32, occupancy: f64, level: i32) -> Signals {
        Signals {
            time: 5000.0 + f64::from(i),
            wall: 1_700_000_000.0 + f64::from(i),
            segments: occupancy,
            pages: 0.125,
            swap_used: 3 << 30,
            headroom: 500 << 30,
            ram: 96 << 30,
            level,
            pressure: Pressure::Normal,
        }
    }

    fn job(pgid: i32, start_us: u64, gb: u64, flags: (bool, bool, bool), command: &str) -> Job {
        Job {
            pgid,
            leader_pid: pgid,
            start_us,
            uid: 501,
            bytes: gb << 30,
            procs: 3,
            prefer: flags.0,
            protected: flags.1,
            stopped: flags.2,
            command: command.into(),
        }
    }

    fn jobs3() -> [Job; 3] {
        [
            job(10, 1_000, 8, (false, false, false), "tsc --build"),
            job(
                11,
                2_000,
                4,
                (true, false, false),
                "vitest run; --reporter dot",
            ),
            job(12, 3_000, 6, (false, true, true), "node ci.js"),
        ]
    }

    /// What the recorder writes is exactly what replay reads, and replay then makes the
    /// engine's decision on it: the preferred job is frozen. Both clocks survive.
    #[test]
    fn recorded_samples_replay_through_the_engine() {
        let dir = scratch("roundtrip");
        let mut rec = Recorder::open(&dir).unwrap();
        let jobs = jobs3();
        let mut engine = Engine::new(Policy::default());
        let mut written = Vec::new();
        for i in 0..4 {
            let s = signals(i, 0.65, 35);
            let step = engine.step(&s, || jobs.to_vec());
            rec.sample(&s, &step, (i == 1).then_some(&jobs[..]))
                .unwrap();
            written.push(s);
        }
        let text = fs::read_to_string(dir.join(SAMPLES_FILE)).unwrap();
        assert!(text.starts_with(HEADER));
        let rows: Vec<Row> = text
            .lines()
            .skip(1)
            .map(|l| parse_row(l).unwrap())
            .collect();
        assert_eq!(rows.len(), 4);
        for (row, s) in rows.iter().zip(&written) {
            assert_eq!(row.signals, *s);
        }
        let listed = rows[1].jobs.as_ref().unwrap();
        assert_eq!(listed.len(), 3);
        assert_eq!(
            listed[1].command, "vitest run  --reporter dot",
            "';' is not allowed in a command"
        );
        assert_eq!(
            (listed[1].prefer, listed[2].protected, listed[2].stopped),
            (true, true, true)
        );
        assert_eq!(listed[0].bytes, 8 << 30);
        assert!(rows[0].jobs.is_none(), "not scanned");

        let mut out = Vec::new();
        let summary = replay(text.as_bytes(), Policy::default(), &mut out).unwrap();
        // The engine saw the jobs only on the second sample, so that is where it can pick
        // one: the preferred job ranks first, then it is frozen.
        assert_eq!(summary.samples, 4);
        let printed = String::from_utf8(out).unwrap();
        assert!(printed.contains("freeze 4.0GB group 11"), "{printed}");
        assert!(printed.contains("2023-11-14T22:13:2"), "{printed}");
    }

    /// "Scanned and found nothing" is not "not scanned": replay must drop the old candidates
    /// on the first and keep them on the second.
    #[test]
    fn an_empty_scan_is_recorded_differently_from_no_scan() {
        let jobs = jobs3();
        // Candidates at sample 0, then sample 1 either found none or did not scan, then a
        // crunch at samples 2-3 (two strikes freeze).
        let tsv = |at_one: Option<&[Job]>| {
            let mut tsv = String::from(HEADER);
            let mut line = String::new();
            let samples = [
                (0, 0.10, 80, Some(&jobs[..])),
                (1, 0.10, 80, at_one),
                (2, 0.65, 35, None),
                (3, 0.65, 35, None),
            ];
            for (i, occ, level, list) in samples {
                let s = signals(i, occ, level);
                let step = Engine::new(Policy::default()).step(&s, Vec::new);
                format_row(&mut line, &s, &step, list);
                tsv.push_str(&line);
            }
            tsv
        };
        let rows: Vec<Row> = tsv(Some(&[]))
            .lines()
            .skip(1)
            .map(|l| parse_row(l).unwrap())
            .collect();
        assert_eq!(rows[1].jobs.as_deref(), Some(&[][..]));
        assert!(rows[2].jobs.is_none());

        let replayed = |text: String| {
            let mut out = Vec::new();
            let summary = replay(text.as_bytes(), Policy::default(), &mut out).unwrap();
            (summary, String::from_utf8(out).unwrap())
        };
        let (kept, _) = replayed(tsv(None));
        assert_eq!(
            kept.freezes, 1,
            "not scanned: the carried candidates are still there"
        );
        let (cleared, printed) = replayed(tsv(Some(&[])));
        assert_eq!(cleared.freezes, 0, "{printed}");
        assert!(printed.contains("no eligible job"), "{printed}");
    }

    #[test]
    fn replay_rejects_a_malformed_line() {
        let text = format!("{HEADER}1\t2\t3\n");
        let err = replay(text.as_bytes(), Policy::default(), &mut Vec::new()).unwrap_err();
        assert!(err.to_string().contains("line 2"), "{err}");
    }

    /// A crash can leave half a line at the end. Replay keeps everything before it.
    #[test]
    fn replay_skips_a_torn_final_line_with_a_warning() {
        let dir = scratch("torn");
        let mut rec = Recorder::open(&dir).unwrap();
        let mut engine = Engine::new(Policy::default());
        for i in 0..3 {
            let s = signals(i, 0.10, 80);
            let step = engine.step(&s, Vec::new);
            rec.sample(&s, &step, None).unwrap();
        }
        let mut text = fs::read_to_string(dir.join(SAMPLES_FILE)).unwrap();
        text.push_str("1700000003.000\t5003.000\t0.10"); // cut off mid-line, no newline
        let mut out = Vec::new();
        let summary = replay(text.as_bytes(), Policy::default(), &mut out).unwrap();
        assert_eq!(summary.samples, 3);
        assert!(
            String::from_utf8(out).unwrap().contains("torn final line"),
            "a warning names the skipped line"
        );
    }

    /// The log never grows past its cap: the full file moves to `.1` and a fresh one starts
    /// with the header again.
    #[test]
    fn logs_rotate_at_their_cap() {
        let dir = scratch("rotate");
        let mut rec = Recorder::with_caps(&dir, 400, 400);
        let mut engine = Engine::new(Policy::default());
        for i in 0..20 {
            let s = signals(i, 0.1, 80);
            let step = engine.step(&s, Vec::new);
            rec.sample(&s, &step, None).unwrap();
        }
        let main = fs::read_to_string(dir.join(SAMPLES_FILE)).unwrap();
        let old = fs::read_to_string(dir.join(format!("{SAMPLES_FILE}.1"))).unwrap();
        assert!(main.starts_with(HEADER) && old.starts_with(HEADER));
        assert!(
            main.len() <= 400 && old.len() <= 400,
            "{} {}",
            main.len(),
            old.len()
        );
        assert!(main.lines().count() > 1);
    }

    /// A file left with half a line gets its newline before anything is appended, so the
    /// torn line cannot swallow the next row.
    #[test]
    fn reopening_a_torn_log_starts_on_a_new_line() {
        let dir = scratch("newline");
        fs::create_dir_all(&dir).unwrap();
        fs::write(
            dir.join(EVENTS_FILE),
            "2026-01-01T00:00:00Z start ok\n2026-01-01T00:00:01Z fre",
        )
        .unwrap();
        let mut rec = Recorder::open(&dir).unwrap();
        rec.event(1_700_000_000.0, "warn", "after the crash");
        let text = fs::read_to_string(dir.join(EVENTS_FILE)).unwrap();
        let lines: Vec<&str> = text.lines().collect();
        assert_eq!(lines.len(), 3, "{text}");
        assert_eq!(lines[1], "2026-01-01T00:00:01Z fre");
        assert!(lines[2].ends_with("warn after the crash"));
    }

    /// If the file cannot be opened (here: a directory stands in its place) writes fail but
    /// the log recovers by itself once the obstacle is gone, instead of failing forever.
    #[test]
    fn a_log_that_cannot_open_retries_on_the_next_write() {
        let dir = scratch("retry");
        fs::create_dir_all(&dir).unwrap();
        let path = dir.join(EVENTS_FILE);
        fs::create_dir(&path).unwrap();
        let mut log = Log::new(path.clone(), 1 << 20, "");
        assert!(log.append(b"lost\n").is_err());
        assert!(log.append(b"lost again\n").is_err());
        fs::remove_dir(&path).unwrap();
        log.append(b"kept\n").unwrap();
        assert_eq!(fs::read_to_string(&path).unwrap(), "kept\n");
    }

    /// A rotation that fails (here the `.1` name is taken by a directory) loses lines but
    /// does not wedge the log: the next write rotates and carries on.
    #[test]
    fn a_failed_rotation_is_retried_on_the_next_write() {
        let dir = scratch("rotate-retry");
        fs::create_dir_all(&dir).unwrap();
        let path = dir.join(EVENTS_FILE);
        let old = dir.join(format!("{EVENTS_FILE}.1"));
        let mut log = Log::new(path.clone(), 30, "");
        log.append(b"0123456789012345678901234\n").unwrap();
        fs::create_dir(&old).unwrap();
        assert!(log.append(b"next line\n").is_err());
        fs::remove_dir(&old).unwrap();
        log.append(b"next line\n").unwrap();
        assert_eq!(
            fs::read_to_string(&old).unwrap(),
            "0123456789012345678901234\n"
        );
        assert_eq!(fs::read_to_string(&path).unwrap(), "next line\n");
    }
}
