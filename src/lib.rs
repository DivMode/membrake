//! membrake: a memory brake for macOS.
//!
//! Module split: `signals` reads what the kernel runs out of, `jobs` finds and vets process
//! groups, `engine` is the pure policy, `act` signals groups and keeps the crash-safety state
//! file, `recorder` writes the samples and event logs and replays them, `runner` is the loop,
//! and `source` is where the loop gets its readings and its clock (the kernel, or a script
//! in tests).
//!
//! The queue (`membrake exec`) is admission control in front of the same machine: `queue` is
//! the pure scheduler, `sizes` the learned peak per kind of command, `wire` the line protocol
//! and its text rules, `broker` the daemon's side of the socket, and `exec` the client.

pub mod act;
pub mod broker;
pub mod engine;
pub mod exec;
pub mod jobs;
pub mod queue;
pub mod recorder;
pub mod runner;
pub mod signals;
pub mod sizes;
pub mod source;
pub mod wire;

#[cfg(test)]
mod testutil;
