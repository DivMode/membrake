//! Where the runner's readings, and its sense of time, come from.
//!
//! The runner asks a `SignalSource` for the next reading and decides from the reading alone:
//! the time of every decision (the fail-safe window, the freeze grace, the outside-resume
//! check count) is the reading's `time`. `Live` reads the kernel once a second. `Scripted`
//! exists only for tests (`membrake run --simulate`): it replays readings from a file so a
//! test can drive the real daemon, signalling real processes, through a whole lifecycle in
//! seconds of real time rather than minutes.

use crate::act;
use crate::recorder::{self, ScriptedRow};
use crate::signals::{self, ReadError, Signals};
use std::fs::File;
use std::io::{self, BufRead, BufReader};
use std::path::Path;
use std::time::Duration;

/// Seconds between live readings.
const INTERVAL: f64 = 1.0;
/// Real time between scripted readings, and between looks at a script that has run out of
/// rows. The engine's clock is the script's, but the processes it signals are real, and a
/// SIGSTOP shows in the process table a moment later: without this pause the checks that
/// decide a frozen job was "resumed outside" could all run before the kernel showed the stop.
const SCRIPT_PAUSE: Duration = Duration::from_millis(20);

/// A reading that could not be taken, and when the attempt was made (engine time).
#[derive(Debug)]
pub struct Unreadable {
    pub time: f64,
    pub error: ReadError,
}

pub type Reading = Result<Signals, Unreadable>;

pub trait SignalSource {
    /// Waits for the next sampling instant and takes a reading. `Ok(None)` means there will
    /// be no more (shutdown was requested); `Err` is a source that is broken, not a reading
    /// that failed: the runner thaws everything and stops.
    fn next_reading(&mut self) -> Result<Option<Reading>, String>;
}

/// The kernel, read straight from sysctl and statfs.
#[derive(Default)]
pub struct Live {
    /// When the next reading is due; None before the first, which is taken at once.
    next_tick: Option<f64>,
}

impl Live {
    pub fn new() -> Live {
        Live::default()
    }
}

impl SignalSource for Live {
    fn next_reading(&mut self) -> Result<Option<Reading>, String> {
        match self.next_tick {
            None => self.next_tick = Some(signals::uptime()),
            Some(due) => {
                // A slow tick (a kill that waited) does not make the next ones bunch up.
                let next = (due + INTERVAL).max(signals::uptime());
                self.next_tick = Some(next);
                act::sleep_until(next);
                if act::shutdown_requested() {
                    return Ok(None);
                }
            }
        }
        Ok(Some(signals::read().map_err(|error| Unreadable {
            time: signals::uptime(),
            error,
        })))
    }
}

/// `membrake run --simulate FILE`: TEST ONLY. Readings come from `FILE` in the samples.tsv
/// format, one row per tick, the `uptime` column driving the engine's clock. The `jobs`
/// column is ignored: the process table is still scanned, and real groups are still
/// signalled, which is why the caller must also restrict eligibility (`--simulate-only`) and
/// why this refuses to be created by root. A line `unreadable<TAB>UPTIME` stands for a tick
/// whose signals could not be read.
///
/// The file is followed like `tail -f`: when the rows run out the source waits for more, so
/// a test can append a stage, watch its effect on the real processes, and append the next.
/// It ends only at shutdown.
pub struct Scripted {
    input: BufReader<File>,
    /// The line being read; kept across polls because a writer may be half way through it.
    line: Vec<u8>,
    number: usize,
}

impl Scripted {
    pub fn open(path: &Path) -> Result<Scripted, String> {
        // SAFETY: getuid and geteuid have no preconditions.
        refuse_root(unsafe { libc::getuid() }, unsafe { libc::geteuid() })?;
        let file = File::open(path).map_err(|e| format!("{}: {e}", path.display()))?;
        Ok(Scripted {
            input: BufReader::new(file),
            line: Vec::new(),
            number: 0,
        })
    }
}

/// Simulated readings must never drive signals as root, against the whole machine.
pub fn refuse_root(uid: u32, euid: u32) -> Result<(), String> {
    if uid == 0 || euid == 0 {
        return Err("--simulate is a test hook and refuses to run as root".into());
    }
    Ok(())
}

impl SignalSource for Scripted {
    fn next_reading(&mut self) -> Result<Option<Reading>, String> {
        loop {
            if act::shutdown_requested() {
                return Ok(None);
            }
            std::thread::sleep(SCRIPT_PAUSE);
            match self.input.read_until(b'\n', &mut self.line) {
                Err(e) => return Err(format!("cannot read the script: {e}")),
                Ok(_) if self.line.ends_with(b"\n") => {}
                Ok(_) => continue,
            }
            self.number += 1;
            let text = String::from_utf8_lossy(&self.line);
            let text = text.trim_end_matches(['\n', '\r']);
            let row = if text.is_empty() || text.starts_with("time\t") {
                None
            } else {
                Some(
                    recorder::parse_scripted(text)
                        .map_err(|e| format!("script line {}: {e}", self.number))?,
                )
            };
            self.line.clear();
            match row {
                None => {}
                Some(ScriptedRow::Signals(s)) => return Ok(Some(Ok(s))),
                Some(ScriptedRow::Unreadable { time }) => {
                    return Ok(Some(Err(Unreadable {
                        time,
                        error: ReadError {
                            what: "scripted reading",
                            source: io::Error::other("unreadable this tick"),
                        },
                    })));
                }
            }
        }
    }
}

#[cfg(test)]
mod tests {
    use super::*;
    use crate::testutil::scratch;
    use std::fs;

    /// The check behind `--simulate`'s refusal, not a run as root: real or effective uid 0
    /// (a set-uid copy) is refused, an ordinary user is not.
    #[test]
    fn simulated_readings_are_refused_to_root() {
        assert!(refuse_root(0, 501).is_err());
        assert!(refuse_root(501, 0).is_err());
        assert!(refuse_root(0, 0).is_err());
        assert!(refuse_root(501, 501).is_ok());
    }

    /// A script is read row by row: a reading, a marker for an unreadable tick with its own
    /// time, and a malformed row that is an error naming its line, never a silent end (a
    /// test that stopped feeding itself would pass without having tested anything).
    #[test]
    fn a_script_yields_readings_and_unreadable_ticks_and_rejects_a_bad_row() {
        let dir = scratch("script");
        fs::create_dir_all(&dir).unwrap();
        let path = dir.join("script.tsv");
        let calm = "1700000000.000\t5000.000\t0.1000\t0.0000\t0\t536870912000\t103079215104\t80\t1\t0.1000\t\t0\t\n";
        fs::write(
            &path,
            format!(
                "{}{calm}{}\t5001.000\ngarbage\n",
                recorder::HEADER,
                recorder::UNREADABLE
            ),
        )
        .unwrap();
        let mut script = Scripted::open(&path).unwrap();

        let s = script.next_reading().unwrap().unwrap().unwrap();
        assert_eq!((s.time, s.level, s.segments), (5000.0, 80, 0.1));
        let u = script.next_reading().unwrap().unwrap().unwrap_err();
        assert_eq!(u.time, 5001.0);
        let e = script.next_reading().unwrap_err();
        assert!(e.contains("line 4"), "{e}");
        let _ = fs::remove_dir_all(&dir);
    }
}
