//! What each kind of command turned out to need: peak memory and CPU demand per command
//! signature (see `wire::signature`), so the queue can admit the next `bun run test` knowing
//! how big the last ones were.
//!
//! The maximum memory and CPU peaks from the five most recent observations are retained.
//! The table also keeps the longest successful runtime for backfill estimates.

use std::collections::HashMap;

/// Signatures kept; past this the least recently used is dropped.
pub const MAX_SIGNATURES: usize = 1000;
const RECENT: usize = 5;

#[derive(Clone)]
struct Peaks<T>([Option<T>; RECENT]);

impl<T: Copy + Ord> Peaks<T> {
    fn load(latest: Option<T>, recent: Option<&str>) -> Self
    where
        T: std::str::FromStr,
    {
        let mut peaks = Self([None; RECENT]);
        if let Some(recent) = recent {
            for value in recent.split(',').filter_map(|v| v.parse::<T>().ok()) {
                peaks.record(value);
            }
        } else if let Some(latest) = latest {
            peaks.record(latest);
        }
        peaks
    }

    fn record(&mut self, value: T) {
        self.0.rotate_left(1);
        self.0[RECENT - 1] = Some(value);
    }

    fn max(&self) -> Option<T> {
        self.0.iter().copied().flatten().max()
    }

    fn append(&self, line: &mut String)
    where
        T: std::fmt::Display,
    {
        use std::fmt::Write as _;
        line.push('\t');
        for (i, value) in self.0.iter().flatten().enumerate() {
            if i > 0 {
                line.push(',');
            }
            let _ = write!(line, "{value}");
        }
    }
}

#[derive(Clone, Copy, Default)]
pub struct Demand {
    pub bytes: Option<u64>,
    pub millicores: Option<u32>,
    pub runtime_ms: Option<u64>,
}

#[derive(Clone)]
struct Entry {
    memory: Peaks<u64>,
    seen: u32,
    cpu: Peaks<u32>,
    runtime_ms: Option<u64>,
    /// When it was last observed, as a count of the observations since the table began.
    stamp: u64,
}

#[derive(Clone, Default)]
pub struct Sizes {
    entries: HashMap<String, Entry>,
    clock: u64,
}

impl Sizes {
    /// Reads the text of a file. Lines that are not an entry are skipped; their number is
    /// returned so the daemon can say so.
    pub fn parse(text: &str) -> (Sizes, usize) {
        let mut sizes = Sizes::default();
        let mut skipped = 0;
        for line in text.lines().filter(|l| !l.is_empty()) {
            let mut f = line.split('\t');
            let entry = (|| {
                let signature = f.next().filter(|s| !s.is_empty())?;
                let peak = f.next()?.parse::<u64>().ok()?;
                let seen = f.next()?.parse::<u32>().ok()?;
                let millicores = f.next().and_then(|v| v.parse().ok());
                let runtime_ms = f.next().and_then(|v| v.parse().ok()).filter(|&ms| ms > 0);
                let memory = Peaks::load((peak > 0).then_some(peak), f.next());
                let cpu = Peaks::load(millicores, f.next());
                (memory.max().is_some() || cpu.max().is_some() || runtime_ms.is_some())
                    .then_some((signature, memory, seen, cpu, runtime_ms))
            })();
            match entry {
                Some((signature, memory, seen, cpu, runtime_ms)) => {
                    sizes.clock += 1;
                    let stamp = sizes.clock;
                    sizes.entries.insert(
                        signature.to_string(),
                        Entry {
                            memory,
                            seen,
                            cpu,
                            runtime_ms,
                            stamp,
                        },
                    );
                }
                None => skipped += 1,
            }
        }
        sizes.evict();
        (sizes, skipped)
    }

    /// The file's text, least recently used first.
    pub fn render(&self) -> String {
        let mut rows: Vec<(&String, &Entry)> = self.entries.iter().collect();
        rows.sort_by_key(|(_, e)| e.stamp);
        rows.into_iter()
            .map(|(s, e)| {
                use std::fmt::Write as _;
                let mut line = format!("{s}\t{}\t{}\t", e.memory.max().unwrap_or(0), e.seen);
                if let Some(cpu) = e.cpu.max() {
                    let _ = write!(line, "{cpu}");
                }
                line.push('\t');
                if let Some(ms) = e.runtime_ms {
                    let _ = write!(line, "{ms}");
                }
                e.memory.append(&mut line);
                e.cpu.append(&mut line);
                line.push('\n');
                line
            })
            .collect()
    }

    /// The expected peak of a command of this signature, if one has been seen.
    pub fn get(&self, signature: &str) -> Option<u64> {
        self.demand(signature).bytes
    }

    pub fn demand(&self, signature: &str) -> Demand {
        self.entries
            .get(signature)
            .or_else(|| self.entries.get(crate::wire::legacy_signature(signature)?))
            .map_or(Demand::default(), |e| Demand {
                bytes: e.memory.max(),
                millicores: e.cpu.max(),
                runtime_ms: e.runtime_ms,
            })
    }

    /// Folds in one observed peak. A peak of zero (the command ended before it was ever
    /// measured) says nothing and is ignored.
    pub fn record(&mut self, signature: &str, peak: u64) {
        self.record_usage(signature, peak, None, None);
    }

    pub(crate) fn record_usage(
        &mut self,
        signature: &str,
        peak: u64,
        millicores: Option<u32>,
        runtime_ms: Option<u64>,
    ) {
        if (peak == 0 && millicores.is_none() && runtime_ms.is_none()) || signature.is_empty() {
            return;
        }
        self.clock += 1;
        let stamp = self.clock;
        match self.entries.get_mut(signature) {
            Some(e) => {
                if peak > 0 {
                    e.memory.record(peak);
                }
                if let Some(cpu) = millicores {
                    e.cpu.record(cpu);
                }
                e.seen = e.seen.saturating_add(1);
                e.runtime_ms = e.runtime_ms.max(runtime_ms);
                e.stamp = stamp;
            }
            None => {
                self.entries.insert(
                    signature.to_string(),
                    Entry {
                        memory: Peaks::load((peak > 0).then_some(peak), None),
                        seen: 1,
                        cpu: Peaks::load(millicores, None),
                        runtime_ms,
                        stamp,
                    },
                );
                self.evict();
            }
        }
    }

    fn evict(&mut self) {
        while self.entries.len() > MAX_SIGNATURES {
            let Some(oldest) = self
                .entries
                .iter()
                .min_by_key(|(_, e)| e.stamp)
                .map(|(s, _)| s.clone())
            else {
                return;
            };
            self.entries.remove(&oldest);
        }
    }

    pub fn len(&self) -> usize {
        self.entries.len()
    }

    pub fn is_empty(&self) -> bool {
        self.entries.is_empty()
    }
}

#[cfg(test)]
mod tests {
    use super::*;

    const GIB: u64 = 1 << 30;

    #[test]
    fn the_largest_of_five_recent_peaks_survives_restart_then_ages_out() {
        let mut sizes = Sizes::default();
        sizes.record_usage("tsc", 20 * GIB, Some(8000), None);
        for _ in 0..4 {
            sizes.record_usage("tsc", 10 * GIB, Some(1000), None);
        }
        let demand = sizes.demand("tsc");
        assert_eq!(
            (demand.bytes, demand.millicores),
            (Some(20 * GIB), Some(8000))
        );
        let (mut sizes, skipped) = Sizes::parse(&sizes.render());
        assert_eq!(skipped, 0);
        assert_eq!(sizes.get("tsc"), Some(20 * GIB));
        sizes.record_usage("tsc", 10 * GIB, Some(1000), None);
        let demand = sizes.demand("tsc");
        assert_eq!(
            (demand.bytes, demand.millicores),
            (Some(10 * GIB), Some(1000))
        );
    }

    #[test]
    fn review3_a_labeled_request_uses_legacy_command_data_until_it_has_its_own() {
        let (mut sizes, skipped) = Sizes::parse("cargo build\t21474836480\t1\t8000\n");
        assert_eq!(skipped, 0);
        let argv = ["cargo".into(), "build".into()];
        let key = crate::wire::signature(&argv, Some("my build"));
        assert_eq!(sizes.get(&key), Some(20 * GIB));
        assert_eq!(sizes.demand(&key).millicores, Some(8000));
        sizes.record(&key, 24 * GIB);
        assert_eq!(sizes.get(&key), Some(24 * GIB));
        assert_eq!(sizes.get("cargo build"), Some(20 * GIB));
    }

    /// 1000 signatures are kept, the ones used longest ago dropped first, and a signature
    /// observed again counts as used.
    #[test]
    fn the_least_recently_used_signature_is_dropped_past_a_thousand() {
        let mut sizes = Sizes::default();
        for n in 0..MAX_SIGNATURES {
            sizes.record(&format!("cmd {n}"), GIB);
        }
        sizes.record("cmd 0", GIB); // used again: no longer the oldest
        sizes.record("one more", GIB);
        assert_eq!(sizes.len(), MAX_SIGNATURES);
        assert!(sizes.get("cmd 0").is_some(), "recently used, kept");
        assert!(sizes.get("cmd 1").is_none(), "least recently used, dropped");
        assert!(sizes.get("one more").is_some());
    }

    /// The file keeps the order of use, a damaged line costs only itself, and a table
    /// longer than the cap is cut at the least recently used end.
    #[test]
    fn the_file_round_trips_and_a_damaged_line_costs_only_itself() {
        let mut sizes = Sizes::default();
        sizes.record("a", GIB);
        sizes.record_usage("b b", 2 * GIB, Some(6200), Some(750));
        sizes.record("a", 3 * GIB);
        let text = sizes.render();
        assert_eq!(text.lines().next().unwrap().split('\t').next(), Some("b b"));
        let (again, skipped) = Sizes::parse(&text);
        assert_eq!(
            (skipped, again.get("a"), again.get("b b")),
            (0, sizes.get("a"), sizes.get("b b"))
        );
        assert_eq!(again.render(), text);
        assert_eq!(again.demand("b b").millicores, Some(6200));
        assert_eq!(
            again.demand("a").millicores,
            None,
            "legacy memory-only entry"
        );

        let (damaged, skipped) =
            Sizes::parse("good\t5\t1\nnot a row\nzero\t0\t1\n\tmissing\t1\nalso good\t6\t2\n");
        assert_eq!((skipped, damaged.len()), (3, 2));

        let long: String = (0..MAX_SIGNATURES + 5)
            .map(|n| format!("c{n}\t1\t1\n"))
            .collect();
        let (cut, _) = Sizes::parse(&long);
        assert_eq!(cut.len(), MAX_SIGNATURES);
        assert!(cut.get("c0").is_none() && cut.get("c1004").is_some());
    }
}
