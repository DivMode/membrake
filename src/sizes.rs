//! What each kind of command turned out to need: the learned peak memory per command
//! signature (see `wire::signature`), so the queue can admit the next `bun run test` knowing
//! how big the last ones were.
//!
//! Each peak is an exponentially weighted moving average of the peaks observed, and the table
//! keeps the signatures used most recently. Pure data and text: the daemon reads and writes
//! the file (`sizes.tsv`: signature, average peak in bytes, observations; least recently used
//! first).

use std::collections::HashMap;

/// Signatures kept; past this the least recently used is dropped.
pub const MAX_SIGNATURES: usize = 1000;
/// Weight of the newest observation.
const ALPHA: f64 = 0.3;

#[derive(Clone)]
struct Entry {
    /// Average peak, bytes.
    peak: u64,
    seen: u32,
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
            let mut f = line.splitn(3, '\t');
            let entry = (|| {
                let signature = f.next().filter(|s| !s.is_empty())?;
                let peak = f.next()?.parse::<u64>().ok().filter(|&p| p > 0)?;
                let seen = f.next()?.parse::<u32>().ok()?;
                Some((signature, peak, seen))
            })();
            match entry {
                Some((signature, peak, seen)) => {
                    sizes.clock += 1;
                    let stamp = sizes.clock;
                    sizes
                        .entries
                        .insert(signature.to_string(), Entry { peak, seen, stamp });
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
            .map(|(s, e)| format!("{s}\t{}\t{}\n", e.peak, e.seen))
            .collect()
    }

    /// The expected peak of a command of this signature, if one has been seen.
    pub fn get(&self, signature: &str) -> Option<u64> {
        self.entries.get(signature).map(|e| e.peak)
    }

    /// Folds in one observed peak. A peak of zero (the command ended before it was ever
    /// measured) says nothing and is ignored.
    pub fn record(&mut self, signature: &str, peak: u64) {
        if peak == 0 || signature.is_empty() {
            return;
        }
        self.clock += 1;
        let stamp = self.clock;
        match self.entries.get_mut(signature) {
            Some(e) => {
                let blended = ALPHA * peak as f64 + (1.0 - ALPHA) * e.peak as f64;
                e.peak = blended.round() as u64;
                e.seen = e.seen.saturating_add(1);
                e.stamp = stamp;
            }
            None => {
                self.entries.insert(
                    signature.to_string(),
                    Entry {
                        peak,
                        seen: 1,
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

    /// The first observation is the size; later ones move it 30% of the way, so one odd run
    /// neither erases what was learned nor goes unnoticed.
    #[test]
    fn the_size_is_a_weighted_average_of_the_peaks() {
        let mut sizes = Sizes::default();
        assert_eq!(sizes.get("tsc"), None);
        sizes.record("tsc", 10 * GIB);
        assert_eq!(sizes.get("tsc"), Some(10 * GIB));
        sizes.record("tsc", 20 * GIB);
        assert_eq!(sizes.get("tsc"), Some(13 * GIB));
        sizes.record("tsc", 0);
        assert_eq!(
            sizes.get("tsc"),
            Some(13 * GIB),
            "an unmeasured run says nothing"
        );
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
        sizes.record("b b", 2 * GIB);
        sizes.record("a", 3 * GIB);
        let text = sizes.render();
        assert_eq!(text.lines().next().unwrap().split('\t').next(), Some("b b"));
        let (again, skipped) = Sizes::parse(&text);
        assert_eq!(
            (skipped, again.get("a"), again.get("b b")),
            (0, sizes.get("a"), sizes.get("b b"))
        );
        assert_eq!(again.render(), text);

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
