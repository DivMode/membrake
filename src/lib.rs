//! membrake: a memory brake for macOS.
//!
//! Module split: `signals` reads what the kernel runs out of, `jobs` finds and vets process
//! groups, `engine` is the pure policy, `act` signals groups and keeps the crash-safety state
//! file, `recorder` writes the samples and event logs and replays them, `runner` is the loop.

pub mod act;
pub mod engine;
pub mod jobs;
pub mod recorder;
pub mod runner;
pub mod signals;

#[cfg(test)]
mod testutil;
