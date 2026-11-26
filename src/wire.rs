//! The line protocol between `membrake exec` and the daemon's queue, and the text rules both
//! ends share. One message per line, fields separated by a tab, version-tagged:
//!
//! ```text
//! client -> daemon   REQUEST  v1  <label>  <signature>  [first-attempt unix micros]
//!                    STARTED  <pgid>
//!                    DONE     <exit code>  [elapsed ms, cpu ns, peak bytes]
//!                    IS_LIVE  <admitting client pid:start time in unix micros>
//! daemon -> client   QUEUED   <position>  <need in bytes>  <reason>  (whenever any changes, and
//!                                                             at least every `KEEP_ALIVE`)
//!                    GO
//!                    LIVE     <yes or no>
//!                    ERROR    <text>
//! ```
//!
//! Reasons are `tight`, `frozen`, `behind`, or `cap` followed by used, reserved and cap bytes.
//! A three-field `QUEUED` from an older daemon has an unspecified reason.
//!
//! Waiting clients receive regular QUEUED updates. The daemon publishes sampling
//! liveness separately, so a full socket backlog does not impose a request deadline.
//!
//! Nothing a client sends is trusted: the daemon sanitizes every string and bounds every line.
//! `STARTED` is sent once, after `GO`. Only a verified child group is sampled. A child that
//! has already been reaped can still be represented by the usage in `DONE`.

use crate::queue::WaitReason;

pub(crate) const NOT_SCHEDULING: &str = "the queue is not scheduling";
pub(crate) const TOO_MANY_CLIENTS: &str = "too many clients"; // Older brokers.
use std::ffi::{OsStr, OsString};
use std::fmt;
use std::path::Path;
use std::time::Duration;

/// Where the daemon listens and `membrake exec` connects, unless told otherwise.
pub const DEFAULT_SOCKET: &str = "/var/run/membrake.sock";
/// The protocol version a request names; a daemon refuses any other.
pub const VERSION: &str = "v1";
/// The longest line either end accepts.
pub const MAX_LINE: usize = 1024;
pub const MAX_LABEL: usize = 80;
pub const MAX_SIGNATURE: usize = 2 * MAX_LABEL + 4;
/// The longest a client that has been answered waits for the next line, of any kind, before it
/// takes the daemon to have stopped answering and retries.
pub const SILENCE: Duration = Duration::from_secs(30);
/// How often at least the daemon tells a waiting client where it stands: a sixth of `SILENCE`
/// (5 s), so five lines in a row have to be lost for a client to give up on a daemon that is
/// running.
pub const KEEP_ALIVE: Duration = Duration::from_secs(SILENCE.as_secs() / 6);

/// The two times above, as this process uses them.
pub struct Timing {
    pub keep_alive: Duration,
    pub silence: Duration,
}

/// The times of the keep-alive, normally `KEEP_ALIVE` and `SILENCE`. Test only:
/// `MEMBRAKE_TEST_SILENCE_MS` shortens `SILENCE` to that many milliseconds (at least 6) and
/// `KEEP_ALIVE` to a sixth of it, in the daemon and the client that read it, so that a test of
/// the silence rule does not take 30 s. It is not a setting for anyone else: it is not in the
/// README, and nothing sets it in a real run.
pub fn timing() -> Timing {
    let silence = std::env::var("MEMBRAKE_TEST_SILENCE_MS")
        .ok()
        .and_then(|ms| ms.parse::<u64>().ok())
        .filter(|&ms| ms >= 6)
        .map_or(SILENCE, Duration::from_millis);
    Timing {
        keep_alive: silence / 6,
        silence,
    }
}

/// Printable ASCII only, at most `max` characters: everything else becomes `?`. The text
/// ends up in one line of a log, a TSV file and a terminal, whoever sent it.
pub fn sanitize(text: &str, max: usize) -> String {
    let clean: String = text
        .chars()
        .take(max)
        .map(|c| if (' '..='~').contains(&c) { c } else { '?' })
        .collect();
    clean.trim().to_string()
}

/// A command's program and up to two plain arguments, with directories stripped.
pub fn command_signature(argv: &[OsString]) -> String {
    let name = |arg: &OsStr| {
        let name = Path::new(arg).file_name().unwrap_or(arg);
        name.to_string_lossy().into_owned()
    };
    let mut words: Vec<String> = argv.iter().take(1).map(|a| name(a)).collect();
    words.extend(
        argv.iter()
            .skip(1)
            .filter(|a| !a.is_empty() && !a.as_encoded_bytes().starts_with(b"-"))
            .take(2)
            .map(|a| name(a)),
    );
    sanitize(&words.join(" "), MAX_LABEL)
}

/// The label is an additional part of the key. A length prefix makes the boundary
/// unambiguous even when a display label contains punctuation.
pub fn signature(argv: &[OsString], label: Option<&str>) -> String {
    let command = command_signature(argv);
    let label = label.map_or_else(String::new, |label| sanitize(label, MAX_LABEL));
    if label.is_empty() {
        command
    } else {
        format!("#{}:{label}{command}", label.len())
    }
}

/// The command-signature portion of a labeled key, for reading pre-upgrade profiles.
pub(crate) fn legacy_signature(signature: &str) -> Option<&str> {
    let (length, parts) = signature.strip_prefix('#')?.split_once(':')?;
    let length: usize = length.parse().ok()?;
    if !(1..=MAX_LABEL).contains(&length) {
        return None;
    }
    parts.get(length..).filter(|command| !command.is_empty())
}

#[derive(Clone, Copy, Debug, PartialEq, Eq)]
pub struct Completion {
    pub elapsed_ms: u64,
    pub cpu_ns: u64,
    pub peak_bytes: u64,
}

/// The process that holds the admission connection. A reused pid names a different client.
#[derive(Clone, Copy, Debug, PartialEq, Eq)]
pub struct Admission {
    pub pid: i32,
    pub start_us: u64,
}

impl Admission {
    pub(crate) fn parse(text: &str) -> Option<Self> {
        let (pid, start_us) = text.split_once(':')?;
        let pid = pid.parse().ok()?;
        let start_us = start_us.parse().ok()?;
        (pid > 0 && start_us > 0).then_some(Self { pid, start_us })
    }
}

impl fmt::Display for Admission {
    fn fmt(&self, f: &mut fmt::Formatter<'_>) -> fmt::Result {
        write!(f, "{}:{}", self.pid, self.start_us)
    }
}

/// Removes and returns the first complete line of `buf`, without its newline.
pub fn take_line(buf: &mut Vec<u8>) -> Option<String> {
    let end = buf.iter().position(|&b| b == b'\n')?;
    let line = String::from_utf8_lossy(&buf[..end])
        .trim_end_matches('\r')
        .to_string();
    buf.drain(..=end);
    Some(line)
}

/// What a client says.
#[derive(Debug, PartialEq, Eq)]
pub enum Message {
    Request {
        label: String,
        signature: String,
        first_attempt_us: Option<u64>,
    },
    /// The command is running as this process group.
    Started(i32),
    /// The command has ended with this exit code.
    Done(i32, Option<Completion>),
    IsLive(Admission),
}

impl Message {
    pub fn line(&self) -> String {
        match self {
            Message::Request {
                label,
                signature,
                first_attempt_us,
            } => {
                let mut line = format!("REQUEST\t{VERSION}\t{label}\t{signature}");
                if let Some(at) = first_attempt_us {
                    use std::fmt::Write as _;
                    let _ = write!(line, "\t{at}");
                }
                line.push('\n');
                line
            }
            Message::Started(pgid) => format!("STARTED\t{pgid}\n"),
            Message::Done(code, None) => format!("DONE\t{code}\n"),
            Message::Done(code, Some(stats)) => format!(
                "DONE\t{code}\t{}\t{}\t{}\n",
                stats.elapsed_ms, stats.cpu_ns, stats.peak_bytes
            ),
            Message::IsLive(admission) => format!("IS_LIVE\t{admission}\n"),
        }
    }

    /// Reads one line from a client. The strings of a request come out sanitized. The error
    /// is what the daemon tells the client.
    pub fn parse(line: &str) -> Result<Message, String> {
        let mut fields = line.split('\t');
        let number = |field: Option<&str>| {
            field
                .and_then(|f| f.parse::<i32>().ok())
                .ok_or_else(|| "malformed message".to_string())
        };
        match fields.next() {
            Some("REQUEST") => {
                if fields.next() != Some(VERSION) {
                    return Err(format!("unsupported protocol version (this is {VERSION})"));
                }
                let (Some(label), Some(signature)) = (fields.next(), fields.next()) else {
                    return Err("malformed message".into());
                };
                Ok(Message::Request {
                    label: sanitize(label, MAX_LABEL),
                    signature: sanitize(signature, MAX_SIGNATURE),
                    first_attempt_us: fields.next().and_then(|at| at.parse().ok()),
                })
            }
            Some("STARTED") => Ok(Message::Started(number(fields.next())?)),
            Some("IS_LIVE") => fields
                .next()
                .and_then(Admission::parse)
                .map(Message::IsLive)
                .ok_or_else(|| "malformed admission identity".into()),
            Some("DONE") => {
                let code = number(fields.next())?;
                let stats = if let Some(elapsed) = fields.next() {
                    let value = |field: Option<&str>| {
                        field
                            .and_then(|field| field.parse::<u64>().ok())
                            .ok_or_else(|| "malformed completion".to_string())
                    };
                    Some(Completion {
                        elapsed_ms: value(Some(elapsed))?.max(1),
                        cpu_ns: value(fields.next())?,
                        peak_bytes: value(fields.next())?,
                    })
                } else {
                    None
                };
                Ok(Message::Done(code, stats))
            }
            _ => Err("unknown message".into()),
        }
    }
}

/// What the daemon says.
#[derive(Debug, PartialEq, Eq)]
pub enum Reply {
    /// Waiting: this place in the order, and the memory the request is expected to need.
    Queued {
        position: usize,
        need: u64,
        reason: WaitReason,
    },
    /// Start the command now.
    Go,
    Live(bool),
    Error(String),
}

impl Reply {
    pub fn line(&self) -> String {
        match self {
            Reply::Queued {
                position,
                need,
                reason,
            } => {
                let mut line = format!("QUEUED\t{position}\t{need}");
                match reason {
                    WaitReason::Unspecified => {}
                    WaitReason::Tight => line.push_str("\ttight"),
                    WaitReason::Frozen => line.push_str("\tfrozen"),
                    WaitReason::Behind => line.push_str("\tbehind"),
                    // Older clients read "behind" and stay queued.
                    WaitReason::Measuring => line.push_str("\tbehind\tmeasuring"),
                    WaitReason::Cap {
                        used,
                        reserved,
                        cap,
                    } => {
                        use std::fmt::Write as _;
                        let _ = write!(line, "\tcap\t{used}\t{reserved}\t{cap}");
                    }
                    WaitReason::Cpu { committed, budget } => {
                        // An older client reads "behind" and stays queued. An unknown
                        // reason would make it treat the daemon as unavailable and start.
                        use std::fmt::Write as _;
                        // Keep the former requested-CPU field for clients on the old format.
                        let _ = write!(line, "\tbehind\tcpu\t{committed}\t0\t{budget}");
                    }
                }
                line.push('\n');
                line
            }
            Reply::Go => "GO\n".to_string(),
            Reply::Live(live) => format!("LIVE\t{}\n", if *live { "yes" } else { "no" }),
            Reply::Error(text) => format!("ERROR\t{}\n", sanitize(text, MAX_LINE / 2)),
        }
    }

    pub fn parse(line: &str) -> Option<Reply> {
        let mut fields = line.split('\t');
        match fields.next()? {
            "QUEUED" => Some(Reply::Queued {
                position: fields.next()?.parse().ok()?,
                need: fields.next()?.parse().ok()?,
                reason: match fields.next() {
                    None => WaitReason::Unspecified,
                    Some("tight") => WaitReason::Tight,
                    Some("frozen") => WaitReason::Frozen,
                    Some("behind") => match fields.next() {
                        Some("measuring") => WaitReason::Measuring,
                        Some("cpu") => {
                            let committed = fields.next()?.parse().ok()?;
                            fields.next()?;
                            WaitReason::Cpu {
                                committed,
                                budget: fields.next()?.parse().ok()?,
                            }
                        }
                        _ => WaitReason::Behind,
                    },
                    Some("cap") => WaitReason::Cap {
                        used: fields.next()?.parse().ok()?,
                        reserved: fields.next()?.parse().ok()?,
                        cap: fields.next()?.parse().ok()?,
                    },
                    _ => return None,
                },
            }),
            "GO" => Some(Reply::Go),
            "LIVE" => match fields.next()? {
                "yes" => Some(Reply::Live(true)),
                "no" => Some(Reply::Live(false)),
                _ => None,
            },
            "ERROR" => Some(Reply::Error(sanitize(fields.next()?, MAX_LINE))),
            _ => None,
        }
    }
}

#[cfg(test)]
mod tests {
    use super::*;

    fn argv(words: &[&str]) -> Vec<OsString> {
        words.iter().map(OsString::from).collect()
    }

    /// The signature is the program and up to two plain arguments, directories stripped:
    /// what tells one kind of heavy job from another, and nothing that varies per run.
    #[test]
    fn a_signature_is_the_program_and_up_to_two_plain_arguments() {
        let sig = |words: &[&str]| signature(&argv(words), None);
        assert_eq!(sig(&["bun", "run", "test"]), "bun run test");
        assert_eq!(sig(&["bun", "run", "test", "--watch", "x"]), "bun run test");
        assert_eq!(
            sig(&["/opt/bin/tsc", "-p", "apps/web/", "--noEmit"]),
            "tsc web"
        );
        assert_eq!(
            sig(&["cargo", "--locked", "build", "--release"]),
            "cargo build"
        );
        assert_eq!(sig(&["make"]), "make");
        assert_eq!(sig(&["./gradlew", "", "assemble"]), "gradlew assemble");
        assert_eq!(sig(&["ls", "."]), "ls .");
    }

    #[test]
    fn the_learning_key_includes_both_the_label_and_command() {
        let first = argv(&["vitest", "fixture-1"]);
        let second = argv(&["vitest", "fixture-2"]);
        assert_ne!(
            signature(&first, Some("gate tests")),
            signature(&second, Some("gate tests"))
        );
        assert_ne!(
            signature(&first, Some("gate tests")),
            signature(&first, Some("browser tests"))
        );
        assert_ne!(signature(&first, None), signature(&second, None));
    }

    /// Every string a client sends is made printable and bounded, so it cannot forge a
    /// log line, break the TSV status file or fill the terminal.
    #[test]
    fn client_text_is_printable_and_bounded() {
        assert_eq!(sanitize("a\tb\nc\x1b[31md\u{2028}é", 80), "a?b?c?[31md??");
        assert_eq!(sanitize(&"x".repeat(500), 80).len(), 80);
        assert_eq!(sanitize("  padded  ", 80), "padded");

        let line = format!("REQUEST\tv1\t{}\tsig\tstray", "L".repeat(500));
        let Ok(Message::Request {
            label, signature, ..
        }) = Message::parse(&line)
        else {
            panic!("{line}");
        };
        assert_eq!((label.len(), signature.as_str()), (MAX_LABEL, "sig"));
    }

    #[test]
    fn messages_round_trip_and_bad_ones_are_refused() {
        for m in [
            Message::Request {
                label: "tests".into(),
                signature: "bun run test".into(),
                first_attempt_us: Some(123),
            },
            Message::Started(4242),
            Message::Done(-1, None),
            Message::Done(
                0,
                Some(Completion {
                    elapsed_ms: 5,
                    cpu_ns: 2_000_000,
                    peak_bytes: 4096,
                }),
            ),
        ] {
            assert_eq!(Message::parse(m.line().trim_end()), Ok(m));
        }
        for r in [
            Reply::Queued {
                position: 3,
                need: 1 << 30,
                reason: WaitReason::Unspecified,
            },
            Reply::Go,
            Reply::Error("no".into()),
        ] {
            assert_eq!(Reply::parse(r.line().trim_end()), Some(r));
        }
        let version = Message::parse("REQUEST\tv2\tl\ts").unwrap_err();
        assert!(version.contains("version"), "{version}");
        for bad in [
            "",
            "REQUEST",
            "REQUEST\tv1\tonly-a-label",
            "STARTED\tx",
            "HELLO",
        ] {
            assert!(Message::parse(bad).is_err(), "{bad:?}");
        }
        assert_eq!(Reply::parse("QUEUED\t1"), None);
    }

    #[test]
    fn queued_reasons_round_trip_and_old_daemons_are_readable() {
        for reason in [
            WaitReason::Unspecified,
            WaitReason::Tight,
            WaitReason::Frozen,
            WaitReason::Behind,
            WaitReason::Measuring,
            WaitReason::Cap {
                used: 40 * (1 << 30),
                reserved: 8 * (1 << 30),
                cap: 48 * (1 << 30),
            },
            WaitReason::Cpu {
                committed: 18000,
                budget: 24000,
            },
        ] {
            let reply = Reply::Queued {
                position: 1,
                need: 1 << 30,
                reason,
            };
            assert_eq!(Reply::parse(reply.line().trim_end()), Some(reply));
        }
        assert_eq!(
            Reply::parse("QUEUED\t2\t1073741824"),
            Some(Reply::Queued {
                position: 2,
                need: 1 << 30,
                reason: WaitReason::Unspecified,
            })
        );
        for bad in ["QUEUED\t1\t1\tcap", "QUEUED\t1\t1\tcap\t2\tx\t4"] {
            assert_eq!(Reply::parse(bad), None);
        }
    }

    /// The README promises these two numbers; a running daemon never lets a client reach its
    /// silence limit because five lines in a row would have to be lost.
    #[test]
    fn the_keep_alive_and_the_silence_limit_are_what_the_readme_says() {
        assert_eq!(SILENCE, Duration::from_secs(30));
        assert_eq!(KEEP_ALIVE, Duration::from_secs(5));
    }

    #[test]
    fn lines_are_taken_one_at_a_time_and_a_partial_line_waits() {
        let mut buf = b"GO\r\nQUEUED\t1\t2\nPAR".to_vec();
        assert_eq!(take_line(&mut buf).as_deref(), Some("GO"));
        assert_eq!(take_line(&mut buf).as_deref(), Some("QUEUED\t1\t2"));
        assert_eq!(take_line(&mut buf), None);
        assert_eq!(buf, b"PAR");
    }
}
