//! The line protocol between `membrake exec` and the daemon's queue, and the text rules both
//! ends share. One message per line, fields separated by a tab, version-tagged:
//!
//! ```text
//! client -> daemon   REQUEST  v1  <label>  <signature>
//!                    STARTED  <pgid>
//!                    DONE     <exit code>
//! daemon -> client   QUEUED   <position>  <need in bytes>    (whenever either changes)
//!                    GO
//!                    ERROR    <text>
//! ```
//!
//! Nothing a client sends is trusted: the daemon sanitizes every string and bounds every line.

use std::ffi::{OsStr, OsString};
use std::path::Path;

/// Where the daemon listens and `membrake exec` connects, unless told otherwise.
pub const DEFAULT_SOCKET: &str = "/var/run/membrake.sock";
/// The protocol version a request names; a daemon refuses any other.
pub const VERSION: &str = "v1";
/// The longest line either end accepts.
pub const MAX_LINE: usize = 1024;
pub const MAX_LABEL: usize = 80;
pub const MAX_SIGNATURE: usize = 80;

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

/// What kind of command this is, for the learned sizes: the program's name plus up to two of
/// the arguments after it that are not flags, each with its directories stripped. `bun run
/// test` and `bun run build` are different jobs; `/opt/bin/tsc -p apps/web --noEmit` is `tsc
/// web`.
pub fn signature(argv: &[OsString]) -> String {
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
    sanitize(&words.join(" "), MAX_SIGNATURE)
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
    },
    /// The command is running as this process group.
    Started(i32),
    /// The command has ended with this exit code.
    Done(i32),
}

impl Message {
    pub fn line(&self) -> String {
        match self {
            Message::Request { label, signature } => {
                format!("REQUEST\t{VERSION}\t{label}\t{signature}\n")
            }
            Message::Started(pgid) => format!("STARTED\t{pgid}\n"),
            Message::Done(code) => format!("DONE\t{code}\n"),
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
                })
            }
            Some("STARTED") => Ok(Message::Started(number(fields.next())?)),
            Some("DONE") => Ok(Message::Done(number(fields.next())?)),
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
    },
    /// Start the command now.
    Go,
    Error(String),
}

impl Reply {
    pub fn line(&self) -> String {
        match self {
            Reply::Queued { position, need } => format!("QUEUED\t{position}\t{need}\n"),
            Reply::Go => "GO\n".to_string(),
            Reply::Error(text) => format!("ERROR\t{}\n", sanitize(text, MAX_LINE / 2)),
        }
    }

    pub fn parse(line: &str) -> Option<Reply> {
        let mut fields = line.split('\t');
        match fields.next()? {
            "QUEUED" => Some(Reply::Queued {
                position: fields.next()?.parse().ok()?,
                need: fields.next()?.parse().ok()?,
            }),
            "GO" => Some(Reply::Go),
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
        let sig = |words: &[&str]| signature(&argv(words));
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

    /// Every string a client sends is made printable and bounded, so it cannot forge a
    /// log line, break the TSV status file or fill the terminal.
    #[test]
    fn client_text_is_printable_and_bounded() {
        assert_eq!(sanitize("a\tb\nc\x1b[31md\u{2028}é", 80), "a?b?c?[31md??");
        assert_eq!(sanitize(&"x".repeat(500), 80).len(), 80);
        assert_eq!(sanitize("  padded  ", 80), "padded");

        let line = format!("REQUEST\tv1\t{}\tsig\tstray", "L".repeat(500));
        let Ok(Message::Request { label, signature }) = Message::parse(&line) else {
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
            },
            Message::Started(4242),
            Message::Done(-1),
        ] {
            assert_eq!(Message::parse(m.line().trim_end()), Ok(m));
        }
        for r in [
            Reply::Queued {
                position: 3,
                need: 1 << 30,
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
    fn lines_are_taken_one_at_a_time_and_a_partial_line_waits() {
        let mut buf = b"GO\r\nQUEUED\t1\t2\nPAR".to_vec();
        assert_eq!(take_line(&mut buf).as_deref(), Some("GO"));
        assert_eq!(take_line(&mut buf).as_deref(), Some("QUEUED\t1\t2"));
        assert_eq!(take_line(&mut buf), None);
        assert_eq!(buf, b"PAR");
    }
}
