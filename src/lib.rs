//! membrake: a memory brake for macOS.
//!
//! Module split: `signals` reads what the kernel runs out of, `jobs` finds and vets process
//! groups, `engine` is the pure policy, `act` signals groups and keeps the crash-safety state
//! file, `recorder` writes the samples and event logs and replays them, `runner` is the loop,
//! and `source` is where the loop gets its readings and its clock (the kernel, or a script
//! in tests).

pub mod act;
pub mod engine;
pub mod jobs;
pub mod recorder;
pub mod runner;
pub mod signals;
pub mod source;

#[cfg(test)]
mod testutil;
