use clap::{Args, Parser, Subcommand};
use membrake::engine::Policy;
use membrake::exec;
use membrake::jobs::{self, Rules};
use membrake::recorder;
use membrake::runner::{self, Location, RunOptions};
use membrake::source::{Live, Scripted};
use membrake::wire::DEFAULT_SOCKET;
use std::ffi::OsString;
use std::fs::File;
use std::io::{BufReader, Write};
use std::path::PathBuf;
use std::process::ExitCode;

const GIB: f64 = (1u64 << 30) as f64;
const LOG_DIR: &str = "/var/log/membrake";
const DRY_RUN_LOG_DIR: &str = "/var/log/membrake/dry-run";

/// A memory brake for macOS.
///
/// When many heavy jobs (type-checkers, test runners, builds) together outgrow physical
/// memory, macOS does not kill them: it compresses, swaps and finally freezes. membrake
/// watches what the kernel actually runs out of (compressor occupancy and room for swap),
/// freezes (SIGSTOP) the newest eligible job when memory gets tight, resumes frozen jobs
/// when it recovers, and kills the biggest eligible job only at the cliff.
///
/// A job is a process group. Never touched: application, XPC and extension bundles, system
/// processes, groups led by a uid below 500 or mixing users, the agent CLIs (claude, codex),
/// terminal multiplexers and terminals, and membrake's own group.
#[derive(Parser)]
#[command(name = "membrake", version, after_help = EXAMPLES)]
struct Cli {
    #[command(subcommand)]
    command: Command,
}

const EXAMPLES: &str = "\
Examples:
  membrake status                          live signals, frozen jobs, next candidates
  membrake run --dry-run                   log every decision, send no signals
  sudo membrake run --protect-user ci      what the LaunchDaemon runs
  membrake exec -- bun run test            start it when there is memory for it
  membrake replay /var/log/membrake/samples.tsv";

#[derive(Subcommand)]
enum Command {
    /// Watch memory and freeze, resume and kill jobs (what the LaunchDaemon runs)
    ///
    /// Samples once a second. Frozen jobs are recorded in the state directory first, and are
    /// resumed when membrake starts and when it receives SIGTERM, SIGINT or SIGHUP. Run as
    /// root to act on other users' jobs; only one instance may run at a time.
    ///
    /// Also serves the queue: `membrake exec` asks for its turn on the socket (--socket) and
    /// the daemon starts each command when there is memory for it.
    Run(RunArgs),
    /// Show the live signals, the frozen jobs and the next candidates
    ///
    /// Needs no privileges; it lists what the calling user can see.
    Status(StatusArgs),
    /// Feed a recorded samples.tsv through the engine and print what it would do
    ///
    /// Nothing is signalled. The candidate jobs are the ones recorded in the file.
    Replay(ReplayArgs),
    /// Run a command when there is memory for it
    ///
    /// Asks the daemon (`membrake run`) for a turn, waits for the go-ahead, runs the command
    /// below interactive apps (nice 10, utility QoS) in a process group of its own, and exits
    /// with its exit code (128 plus the signal if it died of one). The daemon starts a command
    /// when the memory in use plus what it is expected to need stays under a cap it learns,
    /// never while memory is tight or a job is frozen. If the daemon cannot be reached the
    /// command runs at once. Inside a command that was admitted, it runs directly.
    Exec(ExecArgs),
}

/// Policy knobs shared by every command.
#[derive(Args)]
struct PolicyArgs {
    /// Never freeze a job smaller than this
    #[arg(long, value_name = "GB", default_value_t = 1.0)]
    min_job_gb: f64,
    /// Minimum time between freezes, between resumes, and after a kill
    #[arg(long, value_name = "SECONDS", default_value_t = 10.0)]
    settle_seconds: f64,
    /// Kill a job frozen longer than this while memory is still tight
    #[arg(long, value_name = "MINUTES", default_value_t = 15.0)]
    max_freeze_minutes: f64,
}

/// Which groups are eligible, and in what order.
#[derive(Args)]
struct RuleArgs {
    /// This user's jobs are never frozen and are killed only when no other eligible job is
    /// left, e.g. a CI runner account. Ranking only: a uid below 500 also needs
    /// --include-user (repeatable)
    #[arg(long, value_name = "NAME")]
    protect_user: Vec<String>,
    /// Treat this user's jobs as eligible even though its uid is below 500 (repeatable)
    #[arg(long, value_name = "NAME")]
    include_user: Vec<String>,
    /// Jobs matching this POSIX extended regex rank first for freezing and killing; matched
    /// against process names, executable paths and the leader's command line (repeatable)
    #[arg(long, value_name = "REGEX")]
    prefer: Vec<String>,
    /// Jobs matching this regex are never touched. Added to the built-in list, which always
    /// protects claude, codex, Claude Code's install directory, tmux, herdr and common
    /// terminals (repeatable)
    #[arg(long, value_name = "REGEX")]
    never: Vec<String>,
}

#[derive(Args)]
struct RunArgs {
    #[command(flatten)]
    policy: PolicyArgs,
    #[command(flatten)]
    rules: RuleArgs,
    /// Log every decision but send no signals, run no hooks and touch no state; logs go to
    /// the `dry-run` subdirectory of the default log directory so they never mix with the
    /// live daemon's
    #[arg(long)]
    dry_run: bool,
    /// Seconds between SIGTERM and SIGKILL when a kill is not an emergency
    #[arg(long, value_name = "SECONDS", default_value_t = 5.0)]
    term_seconds: f64,
    /// Where frozen jobs are recorded so a restart can resume them
    #[arg(long, value_name = "DIR", default_value = "/var/db/membrake")]
    state_dir: PathBuf,
    /// Where samples.tsv and events.log are written [default: /var/log/membrake, or
    /// /var/log/membrake/dry-run with --dry-run]
    #[arg(long, value_name = "DIR")]
    log_dir: Option<PathBuf>,
    /// Run `/bin/sh -c CMD` on every freeze, resume and kill, with MEMBRAKE_EVENT,
    /// MEMBRAKE_PGID, MEMBRAKE_BYTES, MEMBRAKE_COMMAND and MEMBRAKE_UID set
    #[arg(long, value_name = "CMD")]
    on_event: Option<String>,
    /// Where `membrake exec` asks for its turn; any local user may [default:
    /// /var/run/membrake.sock]
    // The default is applied in `execute`, not here: a simulated run has none, so that it can
    // never reach for the live daemon's socket.
    #[arg(long, value_name = "PATH")]
    socket: Option<PathBuf>,
    /// TEST ONLY, hidden from `--help`. Take the readings from this file (the samples.tsv
    /// format, one row per tick; see `source::Scripted`) instead of the kernel. The process
    /// table is still scanned and real groups are still signalled, so it also needs
    /// `--simulate-only`, explicit `--state-dir` and `--log-dir` (never the live daemon's
    /// defaults), and refuses to run as root.
    #[arg(
        long,
        value_name = "FILE",
        hide = true,
        conflicts_with = "dry_run",
        requires_all = ["simulate_only", "state_dir", "log_dir"]
    )]
    simulate: Option<PathBuf>,
    /// TEST ONLY, hidden. With `--simulate`: only groups whose leader's command line matches
    /// this regex are eligible, so simulated readings reach the processes a test spawned
    /// and no others.
    #[arg(long, value_name = "REGEX", hide = true, requires = "simulate")]
    simulate_only: Option<String>,
}

#[derive(Args)]
struct ExecArgs {
    /// What the queue shows for this command [default: the command and its first two plain
    /// arguments]
    #[arg(long, value_name = "LABEL")]
    label: Option<String>,
    /// The daemon's socket
    #[arg(long, value_name = "PATH", default_value = DEFAULT_SOCKET)]
    socket: PathBuf,
    /// The command and its arguments, after `--`
    #[arg(last = true, required = true, value_name = "COMMAND")]
    command: Vec<OsString>,
}

#[derive(Args)]
struct StatusArgs {
    #[command(flatten)]
    policy: PolicyArgs,
    #[command(flatten)]
    rules: RuleArgs,
    /// Where frozen jobs are recorded
    #[arg(long, value_name = "DIR", default_value = "/var/db/membrake")]
    state_dir: PathBuf,
}

#[derive(Args)]
struct ReplayArgs {
    #[command(flatten)]
    policy: PolicyArgs,
    /// A samples.tsv written by `membrake run`
    file: PathBuf,
}

fn policy(args: &PolicyArgs) -> Result<Policy, String> {
    for (name, value, min) in [
        ("--min-job-gb", args.min_job_gb, 0.0),
        ("--settle-seconds", args.settle_seconds, 0.0),
        ("--max-freeze-minutes", args.max_freeze_minutes, 0.0),
    ] {
        if !value.is_finite() || value < min {
            return Err(format!("{name} must be a non-negative number"));
        }
    }
    Ok(Policy {
        min_job_bytes: (args.min_job_gb * GIB) as u64,
        settle: args.settle_seconds,
        max_freeze: args.max_freeze_minutes * 60.0,
        ..Policy::default()
    })
}

fn rules(args: &RuleArgs) -> Result<Rules, String> {
    let uids = |names: &[String]| {
        names
            .iter()
            .map(|n| jobs::uid_of(n))
            .collect::<Result<Vec<_>, _>>()
    };
    Rules::new(
        &args.never,
        &args.prefer,
        uids(&args.protect_user)?,
        uids(&args.include_user)?,
        runner::own_pgid(),
    )
}

fn execute(cli: Cli) -> Result<ExitCode, String> {
    let done: Result<(), String> = match cli.command {
        Command::Run(a) => {
            let RunArgs {
                policy: policy_args,
                rules: rule_args,
                dry_run,
                term_seconds,
                state_dir,
                log_dir,
                on_event,
                socket,
                simulate,
                simulate_only,
            } = a;
            // Only where the files are is needed to recover the jobs a previous run froze, so
            // that comes first. The rest is checked after the recovery: a flag that no longer
            // parses, or an account that has been deleted since the daemon was configured,
            // must not leave work frozen.
            let location = Location {
                state_dir,
                log_dir: log_dir.unwrap_or_else(|| {
                    PathBuf::from(if dry_run { DRY_RUN_LOG_DIR } else { LOG_DIR })
                }),
                dry_run,
                // A simulated run never reaches for the live daemon's socket.
                socket: socket
                    .or_else(|| simulate.is_none().then(|| PathBuf::from(DEFAULT_SOCKET))),
            };
            let configure = move || -> Result<RunOptions, String> {
                if !term_seconds.is_finite() || term_seconds < 0.0 {
                    return Err("--term-seconds must be a non-negative number".into());
                }
                let mut rules = rules(&rule_args)?;
                if let Some(only) = &simulate_only {
                    rules = rules.only_matching(only)?;
                }
                Ok(RunOptions {
                    policy: policy(&policy_args)?,
                    rules,
                    term_seconds,
                    on_event,
                })
            };
            match simulate {
                Some(script) => runner::run(location, move || {
                    Ok((configure()?, Scripted::open(&script)?))
                }),
                None => runner::run(location, move || Ok((configure()?, Live::new()))),
            }
        }
        Command::Status(a) => runner::status(&policy(&a.policy)?, &rules(&a.rules)?, &a.state_dir),
        Command::Replay(a) => {
            let file = File::open(&a.file).map_err(|e| format!("{}: {e}", a.file.display()))?;
            use std::io::IsTerminal;
            let color = std::io::stdout().is_terminal() && std::env::var_os("NO_COLOR").is_none();
            recorder::replay_with(
                BufReader::new(file),
                policy(&a.policy)?,
                &mut std::io::stdout(),
                color,
            )
            .map(|_| ())
            .map_err(|e| format!("{}: {e}", a.file.display()))
        }
        Command::Exec(a) => {
            return Ok(ExitCode::from(exec::run(exec::Args {
                label: a.label,
                socket: a.socket,
                command: a.command,
            }) as u8));
        }
    };
    done.map(|()| ExitCode::SUCCESS)
}

fn main() -> ExitCode {
    match execute(Cli::parse()) {
        Ok(code) => code,
        Err(e) => {
            // Not eprintln!: it panics when stderr is closed.
            let _ = writeln!(std::io::stderr(), "membrake: {e}");
            ExitCode::FAILURE
        }
    }
}
