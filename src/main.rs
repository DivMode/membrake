use clap::{Args, Parser, Subcommand};
use membrake::engine::Policy;
use membrake::jobs::{self, Rules};
use membrake::recorder;
use membrake::runner::{self, RunOptions};
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
  membrake replay /var/log/membrake/samples.tsv";

#[derive(Subcommand)]
enum Command {
    /// Watch memory and freeze, resume and kill jobs (what the LaunchDaemon runs)
    ///
    /// Samples once a second. Frozen jobs are recorded in the state directory first, and are
    /// resumed when membrake starts and when it receives SIGTERM, SIGINT or SIGHUP. Run as
    /// root to act on other users' jobs; only one instance may run at a time.
    Run(RunArgs),
    /// Show the live signals, the frozen jobs and the next candidates
    ///
    /// Needs no privileges; it lists what the calling user can see.
    Status(StatusArgs),
    /// Feed a recorded samples.tsv through the engine and print what it would do
    ///
    /// Nothing is signalled. The candidate jobs are the ones recorded in the file.
    Replay(ReplayArgs),
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
    /// left, e.g. a CI runner account (repeatable)
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

fn execute(cli: Cli) -> Result<(), String> {
    match cli.command {
        Command::Run(a) => {
            if !a.term_seconds.is_finite() || a.term_seconds < 0.0 {
                return Err("--term-seconds must be a non-negative number".into());
            }
            runner::run(RunOptions {
                policy: policy(&a.policy)?,
                rules: rules(&a.rules)?,
                term_seconds: a.term_seconds,
                state_dir: a.state_dir,
                log_dir: a.log_dir.unwrap_or_else(|| {
                    PathBuf::from(if a.dry_run { DRY_RUN_LOG_DIR } else { LOG_DIR })
                }),
                on_event: a.on_event,
                dry_run: a.dry_run,
            })
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
    }
}

fn main() -> ExitCode {
    match execute(Cli::parse()) {
        Ok(()) => ExitCode::SUCCESS,
        Err(e) => {
            // Not eprintln!: it panics when stderr is closed.
            let _ = writeln!(std::io::stderr(), "membrake: {e}");
            ExitCode::FAILURE
        }
    }
}
