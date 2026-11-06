//! The policy: a pure function of the samples, the job list and the clock. It does no I/O, so
//! every scenario is tested with synthetic samples.
//!
//! Every trigger is an arm with a leaky bucket of strikes: a tripping sample adds one, a
//! clean sample drains half, and the arm fires at its threshold. Requiring continuous
//! pressure fails when the signal flickers; resetting on one clean sample is how guards have
//! watched a Mac freeze. Buckets are capped at their threshold so a long episode cannot
//! leave an arm primed for minutes after memory recovers.
//!
//! Two tiers. The freeze tier is cheap and reversible: SIGSTOP the newest eligible job.
//! The kill tier is for the cliff: kill the biggest. Of the kill tier's arms, the two
//! emergency ones (compressor >= 92%, swap headroom under 2 GB) kill at once, with no SIGTERM
//! grace: the Mac has seconds left. The others mean the cliff is near, not here: they freeze
//! every running eligible job at once, whatever settle period is running, and the kill that
//! follows waits for it. A job frozen past the limit while memory stays tight is killed
//! whatever else the sample did. Every arm except critical pressure
//! counts only while memory is tight: the kernel frees empty compressor segments lazily, so
//! occupancy can sit high for hours after a crunch with RAM free again, and a nearly full
//! disk says nothing about a machine that is not swapping. Resume ignores the compressor for
//! the same reason and waits only for memory to stop being tight.
//!
//! Time is the sample's `time`, awake uptime: monotonic and stopped while the Mac sleeps.

use crate::jobs::Job;
use crate::signals::{KERNEL_LIMIT, Pressure, Signals};

const GIB: u64 = 1 << 30;
/// Readings kept for the fill rate: ~10 s at one sample per second, with slack.
const HISTORY: usize = 16;
/// The fill rate is measured over about this long.
const RATE_WINDOW: f64 = 10.0;

#[derive(Clone, Debug)]
pub struct Policy {
    /// Jobs below this are never frozen: they cannot be what breaks the machine.
    pub min_job_bytes: u64,
    /// Minimum time between two freezes, two resumes, or after a kill before the strike
    /// arms may fire again.
    pub settle: f64,
    /// A job frozen longer than this while memory is still tight is killed.
    pub max_freeze: f64,
    /// A gap between samples longer than this is a stall, never a rate.
    pub stall_gap: f64,
    /// `kern.memorystatus_level` at or below this (percent) means memory is tight.
    pub tight_level: i32,

    pub freeze_occupancy: f64,
    pub freeze_eta: f64,
    pub freeze_eta_min_occupancy: f64,
    pub freeze_strikes: f64,

    /// Resume when memory has not been tight for this many consecutive samples.
    pub resume_samples: u32,

    pub kill_occupancy_now: f64,
    pub kill_occupancy: f64,
    pub kill_eta: f64,
    pub kill_eta_min_occupancy: f64,
    pub kill_headroom_now: u64,
    pub kill_headroom: u64,
    pub kill_pressure_strikes: f64,
    pub kill_strikes: f64,
}

impl Default for Policy {
    fn default() -> Policy {
        Policy {
            min_job_bytes: GIB,
            settle: 10.0,
            max_freeze: 15.0 * 60.0,
            stall_gap: 5.0,
            tight_level: 40,
            freeze_occupancy: 0.60,
            freeze_eta: 300.0,
            freeze_eta_min_occupancy: 0.30,
            freeze_strikes: 2.0,
            resume_samples: 5,
            kill_occupancy_now: 0.92,
            kill_occupancy: 0.85,
            kill_eta: 60.0,
            kill_eta_min_occupancy: 0.50,
            kill_headroom_now: 2 * GIB,
            kill_headroom: 4 * GIB,
            kill_pressure_strikes: 10.0,
            kill_strikes: 2.0,
        }
    }
}

#[derive(Clone, Debug, PartialEq)]
pub enum Action {
    /// The previous sample was too long ago for a rate to mean anything.
    Stall {
        gap: f64,
    },
    /// Something worth a log line that is not an action on a job.
    Warn {
        text: String,
    },
    Freeze {
        job: Job,
        reason: String,
    },
    Resume {
        job: Job,
    },
    /// `immediate` means an emergency arm fired: the Mac has seconds left, so SIGSTOP is
    /// followed by SIGKILL with no SIGTERM and no grace period.
    Kill {
        job: Job,
        immediate: bool,
        reason: String,
    },
}

/// What one sample led to.
#[derive(Clone, Debug, PartialEq)]
pub struct Step {
    pub occupancy: f64,
    /// Seconds until occupancy reaches the kernel's 98%, infinite when not rising.
    pub eta: f64,
    pub tight: bool,
    pub actions: Vec<Action>,
}

#[derive(Clone, Debug)]
pub struct Frozen {
    pub job: Job,
    pub since: f64,
}

#[derive(Clone, Copy, PartialEq, Eq)]
enum Tier {
    Freeze,
    Kill,
}

#[derive(Clone, Copy)]
enum Arm {
    FreezeOccupancy,
    FreezeEta,
    FreezeHeadroom,
    KillOccupancyNow,
    KillHeadroomNow,
    KillOccupancy,
    KillEta,
    KillHeadroom,
    KillPressure,
}

const ARMS: [Arm; 9] = [
    Arm::FreezeOccupancy,
    Arm::FreezeEta,
    Arm::FreezeHeadroom,
    Arm::KillOccupancyNow,
    Arm::KillHeadroomNow,
    Arm::KillOccupancy,
    Arm::KillEta,
    Arm::KillHeadroom,
    Arm::KillPressure,
];

/// The readings an arm looks at.
struct View {
    occupancy: f64,
    eta: f64,
    tight: bool,
    headroom: u64,
    ram: u64,
    pressure: Pressure,
}

impl Arm {
    fn tier(self) -> Tier {
        match self {
            Arm::FreezeOccupancy | Arm::FreezeEta | Arm::FreezeHeadroom => Tier::Freeze,
            _ => Tier::Kill,
        }
    }

    fn needed(self, p: &Policy) -> f64 {
        match self {
            Arm::FreezeOccupancy | Arm::FreezeEta | Arm::FreezeHeadroom => p.freeze_strikes,
            Arm::KillOccupancyNow | Arm::KillHeadroomNow => 1.0,
            Arm::KillOccupancy | Arm::KillEta | Arm::KillHeadroom => p.kill_strikes,
            Arm::KillPressure => p.kill_pressure_strikes,
        }
    }

    fn immediate(self) -> bool {
        matches!(self, Arm::KillOccupancyNow | Arm::KillHeadroomNow)
    }

    fn tripped(self, v: &View, p: &Policy) -> bool {
        match self {
            Arm::FreezeOccupancy => v.tight && v.occupancy >= p.freeze_occupancy,
            Arm::FreezeEta => {
                v.tight && v.eta < p.freeze_eta && v.occupancy >= p.freeze_eta_min_occupancy
            }
            Arm::FreezeHeadroom => v.tight && v.headroom < v.ram,
            Arm::KillOccupancyNow => v.tight && v.occupancy >= p.kill_occupancy_now,
            Arm::KillHeadroomNow => v.tight && v.headroom < p.kill_headroom_now,
            Arm::KillOccupancy => v.tight && v.occupancy >= p.kill_occupancy,
            // A fast fill from nearly empty is common and harmless; believe it from half full.
            Arm::KillEta => {
                v.tight && v.eta < p.kill_eta && v.occupancy >= p.kill_eta_min_occupancy
            }
            Arm::KillHeadroom => v.tight && v.headroom < p.kill_headroom,
            Arm::KillPressure => v.pressure == Pressure::Critical,
        }
    }

    fn label(self, p: &Policy) -> String {
        let pct = |f: f64| (f * 100.0).round();
        let gb = |b: u64| b as f64 / GIB as f64;
        match self {
            Arm::FreezeOccupancy => format!("compressor >= {}%", pct(p.freeze_occupancy)),
            Arm::FreezeEta => format!("full in < {}s", p.freeze_eta),
            Arm::FreezeHeadroom => "swap headroom < RAM".to_string(),
            Arm::KillOccupancyNow => format!("compressor >= {}%", pct(p.kill_occupancy_now)),
            Arm::KillHeadroomNow => format!("swap headroom < {}GB", gb(p.kill_headroom_now)),
            Arm::KillOccupancy => format!("compressor >= {}%", pct(p.kill_occupancy)),
            Arm::KillEta => format!("full in < {}s", p.kill_eta),
            Arm::KillHeadroom => format!("swap headroom < {}GB", gb(p.kill_headroom)),
            Arm::KillPressure => "critical pressure".to_string(),
        }
    }
}

/// Recent (time, occupancy) readings in a fixed array, so the steady loop never allocates.
struct History {
    buf: [(f64, f64); HISTORY],
    len: usize,
}

impl History {
    fn new() -> History {
        History {
            buf: [(0.0, 0.0); HISTORY],
            len: 0,
        }
    }

    fn clear(&mut self) {
        self.len = 0;
    }

    fn push(&mut self, time: f64, occupancy: f64) {
        let stale = self.buf[..self.len]
            .iter()
            .take_while(|(t, _)| time - t > RATE_WINDOW + 1.0)
            .count();
        self.buf.copy_within(stale..self.len, 0);
        self.len -= stale;
        if self.len == HISTORY {
            self.buf.copy_within(1.., 0);
            self.len -= 1;
        }
        self.buf[self.len] = (time, occupancy);
        self.len += 1;
    }

    /// Seconds until the kernel's limit at the rate over the window; infinite when the
    /// history is shorter than half a window or occupancy is not rising.
    fn eta(&self, time: f64, occupancy: f64) -> f64 {
        let Some(&(t0, o0)) = self.buf[..self.len]
            .iter()
            .find(|(t, _)| time - t <= RATE_WINDOW + 1.0)
        else {
            return f64::INFINITY;
        };
        let span = time - t0;
        if span < RATE_WINDOW / 2.0 {
            return f64::INFINITY;
        }
        let rate = (occupancy - o0) / span;
        if rate > 0.0 {
            ((KERNEL_LIMIT - occupancy) / rate).max(0.0)
        } else {
            f64::INFINITY
        }
    }
}

pub struct Engine {
    policy: Policy,
    history: History,
    last_time: Option<f64>,
    strikes: [f64; ARMS.len()],
    /// No freeze, resume or non-immediate kill before this.
    quiet_until: f64,
    calm_streak: u32,
    frozen: Vec<Frozen>,
    warned_no_victim: bool,
}

fn same(a: &Job, b: &Job) -> bool {
    a.pgid == b.pgid && a.start_us == b.start_us
}

/// The job list of one step: the process table is scanned the first time it is asked for, and
/// never if nothing needs it.
struct Candidates<F> {
    scan: Option<F>,
    list: Vec<Job>,
}

impl<F: FnOnce() -> Vec<Job>> Candidates<F> {
    fn new(scan: F) -> Candidates<F> {
        Candidates {
            scan: Some(scan),
            list: Vec::new(),
        }
    }

    fn get(&mut self) -> &[Job] {
        if let Some(scan) = self.scan.take() {
            self.list = scan();
        }
        &self.list
    }
}

impl Engine {
    pub fn new(policy: Policy) -> Engine {
        Engine {
            policy,
            history: History::new(),
            last_time: None,
            strikes: [0.0; ARMS.len()],
            quiet_until: f64::NEG_INFINITY,
            calm_streak: 0,
            frozen: Vec::new(),
            warned_no_victim: false,
        }
    }

    pub fn policy(&self) -> &Policy {
        &self.policy
    }

    /// Jobs this engine has frozen and not yet resumed or killed, oldest first.
    pub fn frozen(&self) -> &[Frozen] {
        &self.frozen
    }

    /// The runner could not freeze this job, or it is no longer frozen (it exited or someone
    /// resumed it by hand).
    pub fn forget(&mut self, pgid: i32) {
        self.frozen.retain(|f| f.job.pgid != pgid);
    }

    /// The fail-safe resumed the frozen jobs: keep only those `keep` says are still frozen
    /// (their resume did not go through), so they stay tracked and are tried again.
    pub fn retain_frozen(&mut self, mut keep: impl FnMut(i32) -> bool) {
        self.frozen.retain(|f| keep(f.job.pgid));
    }

    /// Call after anything that blocked the loop on purpose (a kill waiting for exit), so
    /// the gap is not reported as a stall and the pre-kill fill rate is not reused.
    pub fn resync(&mut self) {
        self.last_time = None;
        self.history.clear();
    }

    /// Feeds one reading through the policy. `jobs` is called at most once, and only when
    /// an action needs a victim, so a calm machine never scans its process table.
    pub fn step(&mut self, s: &Signals, jobs: impl FnOnce() -> Vec<Job>) -> Step {
        let mut actions = Vec::new();
        let now = s.time;
        if let Some(prev) = self.last_time {
            let gap = now - prev;
            if !(gap > 0.0 && gap <= self.policy.stall_gap) {
                actions.push(Action::Stall { gap });
                self.history.clear();
            }
        }
        self.last_time = Some(now);

        let occupancy = s.occupancy();
        let eta = self.history.eta(now, occupancy);
        self.history.push(now, occupancy);
        let tight = s.pressure > Pressure::Normal || s.level <= self.policy.tight_level;
        let view = View {
            occupancy,
            eta,
            tight,
            headroom: s.headroom,
            ram: s.ram,
            pressure: s.pressure,
        };

        for (arm, strikes) in ARMS.iter().zip(self.strikes.iter_mut()) {
            *strikes = if arm.tripped(&view, &self.policy) {
                (*strikes + 1.0).min(arm.needed(&self.policy))
            } else {
                (*strikes - 0.5).max(0.0)
            };
        }
        self.calm_streak = if tight { 0 } else { self.calm_streak + 1 };

        let quiet = now < self.quiet_until;
        // Kill-tier arms at their threshold, settle period or not: the emergency ones kill at
        // once and the others freeze at once; only the kill that follows a freeze waits.
        let kill_arms = self.fired(Tier::Kill, false);
        let freeze_arms = self.fired(Tier::Freeze, quiet);
        // Frozen past the limit while memory is still tight. Checked on its own, not behind
        // the kill arms, and not behind the settle period either: every freeze starts a new
        // period, so jobs arriving every few seconds under urgent pressure would keep it
        // running for ever and the overdue job would never be reached.
        let expired = tight
            .then(|| {
                self.frozen
                    .iter()
                    .find(|f| now - f.since > self.policy.max_freeze)
            })
            .flatten()
            .map(|f| f.job.pgid);
        let mut candidates = Candidates::new(jobs);

        if kill_arms.iter().any(|a| a.immediate()) {
            let reason = self.reasons(&kill_arms);
            self.kill(now, &mut candidates, None, true, reason, &mut actions);
        } else {
            let urgent = !kill_arms.is_empty();
            let reason = if urgent {
                self.reasons(&kill_arms)
            } else {
                String::new()
            };
            let froze = urgent && self.freeze_running(now, &mut candidates, &reason, &mut actions);
            if let Some(pgid) = expired {
                let reason = format!(
                    "frozen for more than {} minutes while memory is still tight",
                    (self.policy.max_freeze / 60.0).round()
                );
                self.kill(
                    now,
                    &mut candidates,
                    Some(pgid),
                    false,
                    reason,
                    &mut actions,
                );
            } else if urgent {
                // Nothing is left to freeze and the settle period is over: now kill.
                if !quiet && !froze {
                    self.kill(now, &mut candidates, None, false, reason, &mut actions);
                }
            } else if !freeze_arms.is_empty() {
                let reason = self.reasons(&freeze_arms);
                self.freeze(now, &mut candidates, reason, &mut actions);
            } else if !quiet
                && self.calm_streak >= self.policy.resume_samples
                && !self.frozen.is_empty()
            {
                // The job frozen first goes first; among jobs frozen together, the oldest
                // job, so the newest (the likeliest runaway) comes back last.
                let next = (0..self.frozen.len())
                    .min_by(|&a, &b| {
                        let (a, b) = (&self.frozen[a], &self.frozen[b]);
                        a.since
                            .total_cmp(&b.since)
                            .then(a.job.start_us.cmp(&b.job.start_us))
                    })
                    .unwrap_or(0);
                let job = self.frozen.remove(next).job;
                self.quiet_until = now + self.policy.settle;
                actions.push(Action::Resume { job });
            } else if self.strikes.iter().all(|&n| n == 0.0) {
                self.warned_no_victim = false;
            }
        }
        Step {
            occupancy,
            eta,
            tight,
            actions,
        }
    }

    /// Arms of `tier` at their threshold. While `quiet`, only the immediate arms count.
    fn fired(&self, tier: Tier, quiet: bool) -> Vec<Arm> {
        ARMS.iter()
            .zip(&self.strikes)
            .filter(|(a, n)| {
                a.tier() == tier && **n >= a.needed(&self.policy) && (a.immediate() || !quiet)
            })
            .map(|(a, _)| *a)
            .collect()
    }

    fn reasons(&self, arms: &[Arm]) -> String {
        arms.iter()
            .map(|a| a.label(&self.policy))
            .collect::<Vec<_>>()
            .join(", ")
    }

    /// The newest running job of at least the minimum size, preferred jobs first.
    fn freeze<F: FnOnce() -> Vec<Job>>(
        &mut self,
        now: f64,
        candidates: &mut Candidates<F>,
        reason: String,
        actions: &mut Vec<Action>,
    ) {
        let victim = candidates
            .get()
            .iter()
            .filter(|j| self.freezable(j))
            .max_by_key(|j| (j.prefer, j.start_us, j.bytes))
            .cloned();
        let Some(job) = victim else {
            self.no_victim("freeze", &reason, actions);
            return;
        };
        self.warned_no_victim = false;
        self.frozen.push(Frozen {
            job: job.clone(),
            since: now,
        });
        self.quiet_until = now + self.policy.settle;
        self.history.clear();
        actions.push(Action::Freeze { job, reason });
    }

    /// A running job this engine may freeze.
    fn freezable(&self, j: &Job) -> bool {
        !j.protected
            && !j.stopped
            && j.bytes >= self.policy.min_job_bytes
            && !self.frozen.iter().any(|f| same(&f.job, j))
    }

    /// A non-immediate kill arm fired: the machine is heading for the cliff, but not there
    /// yet. Freezing is instant and loses no work, so freeze every running eligible job at
    /// once, even in the middle of an earlier settle period, and kill only when a kill arm
    /// fires again with nothing left to freeze. Returns whether there was anything to freeze.
    fn freeze_running<F: FnOnce() -> Vec<Job>>(
        &mut self,
        now: f64,
        candidates: &mut Candidates<F>,
        reason: &str,
        actions: &mut Vec<Action>,
    ) -> bool {
        let running: Vec<Job> = candidates
            .get()
            .iter()
            .filter(|j| self.freezable(j))
            .cloned()
            .collect();
        if running.is_empty() {
            return false;
        }
        let reason = format!("{reason}; freeze all first");
        for job in running {
            self.frozen.push(Frozen {
                job: job.clone(),
                since: now,
            });
            actions.push(Action::Freeze {
                job,
                reason: reason.clone(),
            });
        }
        self.warned_no_victim = false;
        self.strikes = [0.0; ARMS.len()];
        self.history.clear();
        self.quiet_until = now + self.policy.settle;
        true
    }

    /// The biggest job, protected users' jobs last, preferred jobs first, jobs already
    /// frozen before running ones. `only` restricts the choice to one frozen job.
    fn kill<F: FnOnce() -> Vec<Job>>(
        &mut self,
        now: f64,
        candidates: &mut Candidates<F>,
        only: Option<i32>,
        immediate: bool,
        reason: String,
        actions: &mut Vec<Action>,
    ) {
        let mut pool: Vec<(Job, bool)> = Vec::new();
        if let Some(pgid) = only {
            pool.extend(
                self.frozen
                    .iter()
                    .filter(|f| f.job.pgid == pgid)
                    .map(|f| (f.job.clone(), true)),
            );
        } else {
            for job in candidates.get() {
                let frozen = self.frozen.iter().any(|f| same(&f.job, job));
                if frozen || job.bytes >= self.policy.min_job_bytes {
                    pool.push((job.clone(), frozen));
                }
            }
            // A frozen job the scan no longer lists (it shrank, it was not scanned, or it has
            // since been excluded) stays a candidate here; the runner re-checks the full
            // rules on the group itself before it signals anything.
            for f in &self.frozen {
                if !pool.iter().any(|(j, _)| same(j, &f.job)) {
                    pool.push((f.job.clone(), true));
                }
            }
        }
        let victim = pool
            .into_iter()
            .max_by_key(|(j, frozen)| (!j.protected, j.prefer, *frozen, j.bytes))
            .map(|(j, _)| j);
        let Some(job) = victim else {
            self.no_victim("kill", &reason, actions);
            return;
        };
        self.warned_no_victim = false;
        self.frozen.retain(|f| !same(&f.job, &job));
        self.strikes = [0.0; ARMS.len()];
        self.history.clear();
        self.quiet_until = now + self.policy.settle;
        actions.push(Action::Kill {
            job,
            immediate,
            reason,
        });
    }

    fn no_victim(&mut self, what: &str, reason: &str, actions: &mut Vec<Action>) {
        if !self.warned_no_victim {
            self.warned_no_victim = true;
            actions.push(Action::Warn {
                text: format!(
                    "{reason}, but there is no eligible job of at least {:.1}GB to {what}",
                    self.policy.min_job_bytes as f64 / GIB as f64
                ),
            });
        }
    }
}

#[cfg(test)]
mod tests {
    use super::*;

    const RAM: u64 = 96 * GIB;

    fn sample(time: f64, occupancy: f64, level: i32) -> Signals {
        Signals {
            time,
            wall: 1.7e9 + time,
            segments: occupancy,
            pages: 0.0,
            swap_used: 0,
            headroom: 500 * GIB,
            ram: RAM,
            level,
            pressure: Pressure::Normal,
        }
    }

    fn job(pgid: i32, start: u64, gb: u64) -> Job {
        Job {
            pgid,
            leader_pid: pgid,
            start_us: start,
            uid: 501,
            bytes: gb * GIB,
            procs: 1,
            prefer: false,
            protected: false,
            stopped: false,
            command: format!("job-{pgid}"),
        }
    }

    /// Runs `signals` and collects (time, action) pairs.
    fn drive(engine: &mut Engine, signals: &[Signals], jobs: &[Job]) -> Vec<(f64, Action)> {
        let mut out = Vec::new();
        for s in signals {
            for a in engine.step(s, || jobs.to_vec()).actions {
                out.push((s.time, a));
            }
        }
        out
    }

    fn steady(from: f64, seconds: u32, occupancy: f64, level: i32) -> Vec<Signals> {
        (0..seconds)
            .map(|i| sample(from + f64::from(i), occupancy, level))
            .collect()
    }

    fn freezes(actions: &[(f64, Action)]) -> Vec<(f64, i32)> {
        actions
            .iter()
            .filter_map(|(t, a)| match a {
                Action::Freeze { job, .. } => Some((*t, job.pgid)),
                _ => None,
            })
            .collect()
    }

    fn kills(actions: &[(f64, Action)]) -> Vec<(f64, i32, bool)> {
        actions
            .iter()
            .filter_map(|(t, a)| match a {
                Action::Kill { job, immediate, .. } => Some((*t, job.pgid, *immediate)),
                _ => None,
            })
            .collect()
    }

    fn resumes(actions: &[(f64, Action)]) -> Vec<(f64, i32)> {
        actions
            .iter()
            .filter_map(|(t, a)| match a {
                Action::Resume { job } => Some((*t, job.pgid)),
                _ => None,
            })
            .collect()
    }

    fn three_jobs() -> Vec<Job> {
        vec![job(10, 100, 8), job(11, 200, 4), job(12, 300, 6)]
    }

    /// The headline behaviour: a slow fill crosses the freeze threshold and the newest job is
    /// frozen, then the next newest one settle period later, never an already frozen job.
    #[test]
    fn gradual_fill_freezes_newest_jobs_one_per_settle_period() {
        let mut e = Engine::new(Policy::default());
        // 0.001/s is too slow for the ETA arm (it needs under 300 s to full), so the
        // occupancy arm is what fires, at 0.60.
        let mut signals = steady(0.0, 20, 0.30, 35);
        signals.extend(
            (0..=330).map(|i| sample(20.0 + f64::from(i), 0.30 + 0.001 * f64::from(i), 35)),
        );
        signals.extend(steady(351.0, 30, 0.63, 35));
        let actions = drive(&mut e, &signals, &three_jobs());

        let f = freezes(&actions);
        assert_eq!(f.iter().map(|x| x.1).collect::<Vec<_>>(), vec![12, 11, 10]);
        // Occupancy first reaches 0.60 at t=320; two strikes means the next sample.
        assert!(
            (320.0..=322.0).contains(&f[0].0),
            "first freeze at {}",
            f[0].0
        );
        assert!(f[1].0 - f[0].0 >= 10.0 && f[2].0 - f[1].0 >= 10.0, "{f:?}");
        assert!(kills(&actions).is_empty());
        assert!(
            matches!(&actions.iter().find(|(_, a)| matches!(a, Action::Freeze { .. })).unwrap().1,
            Action::Freeze { reason, .. } if reason.contains("compressor >= 60%"))
        );
    }

    /// A fast fill is caught by the ETA arm long before 60%.
    #[test]
    fn fast_fill_freezes_on_time_to_full() {
        let mut e = Engine::new(Policy::default());
        let signals: Vec<Signals> = (0..40)
            .map(|i| sample(f64::from(i), (0.20 + 0.01 * f64::from(i)).min(0.45), 35))
            .collect();
        let actions = drive(&mut e, &signals, &three_jobs());
        let f = freezes(&actions);
        assert!(!f.is_empty());
        // Occupancy is 0.30 at t=10 and never gets near 0.60; two strikes later it freezes.
        assert!(f[0].0 <= 12.0, "froze at {}", f[0].0);
        let (_, first) = actions
            .iter()
            .find(|(_, a)| matches!(a, Action::Freeze { .. }))
            .unwrap();
        assert!(matches!(first, Action::Freeze { reason, .. } if reason.contains("full in")));
    }

    #[test]
    fn calm_machine_takes_no_action() {
        let mut e = Engine::new(Policy::default());
        assert!(drive(&mut e, &steady(0.0, 600, 0.10, 80), &three_jobs()).is_empty());
    }

    /// After a crunch, empty compressor segments linger; with RAM free again that must not
    /// freeze or kill anything. The same readings kill the moment memory is tight.
    #[test]
    fn gate_blocks_compressor_arms_while_memory_is_free() {
        let mut e = Engine::new(Policy::default());
        assert!(drive(&mut e, &steady(0.0, 120, 0.95, 80), &three_jobs()).is_empty());
        let actions = drive(&mut e, &steady(120.0, 1, 0.95, 30), &three_jobs());
        assert_eq!(kills(&actions), vec![(120.0, 10, true)]);
    }

    /// Resume needs memory not tight for five consecutive samples, whatever the compressor
    /// says (its segments stay full for hours after a crunch); then the job frozen first goes
    /// first, one per settle period.
    #[test]
    fn resume_needs_five_untight_samples_whatever_the_compressor_says() {
        let mut e = Engine::new(Policy::default());
        let jobs = three_jobs();
        let frozen = freezes(&drive(&mut e, &steady(0.0, 14, 0.65, 35), &jobs));
        assert_eq!(frozen.iter().map(|x| x.1).collect::<Vec<_>>(), vec![12, 11]);

        // Tight, even with low occupancy: no resume.
        assert!(resumes(&drive(&mut e, &steady(14.0, 10, 0.45, 30), &jobs)).is_empty());
        // Four calm samples, then a tight one resets the streak.
        let mut flicker = steady(24.0, 4, 0.90, 80);
        flicker.extend(steady(28.0, 1, 0.45, 30));
        assert!(resumes(&drive(&mut e, &flicker, &jobs)).is_empty());
        // Not tight with the compressor still at 90%: the fifth consecutive sample resumes
        // the first-frozen job, the next one a settle period later.
        let r = resumes(&drive(&mut e, &steady(29.0, 30, 0.90, 80), &jobs));
        assert_eq!(r.iter().map(|x| x.1).collect::<Vec<_>>(), vec![12, 11]);
        assert_eq!(r[0].0, 33.0);
        assert!(r[1].0 - r[0].0 >= 10.0, "{r:?}");
        assert!(e.frozen().is_empty());
    }

    /// Kill prefers a job that is already frozen, even a smaller one; with nothing frozen it
    /// takes the biggest.
    #[test]
    fn kill_prefers_frozen_job_over_a_bigger_running_one() {
        let jobs = vec![job(10, 100, 30), job(11, 300, 2)];
        let mut e = Engine::new(Policy::default());
        let f = freezes(&drive(&mut e, &steady(0.0, 3, 0.65, 35), &jobs));
        assert_eq!(f.iter().map(|x| x.1).collect::<Vec<_>>(), vec![11]);
        let k = kills(&drive(&mut e, &steady(3.0, 1, 0.93, 30), &jobs));
        assert_eq!(k.iter().map(|x| x.1).collect::<Vec<_>>(), vec![11]);
        assert!(e.frozen().is_empty());

        let mut fresh = Engine::new(Policy::default());
        let k = kills(&drive(&mut fresh, &steady(0.0, 1, 0.93, 30), &jobs));
        assert_eq!(k.iter().map(|x| x.1).collect::<Vec<_>>(), vec![10]);
    }

    /// Emergency arms kill at once and skip the SIGTERM grace.
    #[test]
    fn immediate_arms_kill_on_the_first_sample() {
        let jobs = three_jobs();
        let mut e = Engine::new(Policy::default());
        assert_eq!(
            kills(&drive(&mut e, &steady(0.0, 1, 0.93, 30), &jobs)),
            vec![(0.0, 10, true)]
        );

        // Swap headroom under 2 GB is an emergency once memory is tight.
        let mut e = Engine::new(Policy::default());
        let mut s = sample(0.0, 0.1, 30);
        s.headroom = GIB;
        assert_eq!(kills(&drive(&mut e, &[s], &jobs)), vec![(0.0, 10, true)]);
    }

    /// A two-strike kill arm means the cliff is near, not here: freeze every running job at
    /// once (no work lost), and kill only when the arm fires again after the settle period
    /// with nothing left to freeze, the biggest frozen job first, with the SIGTERM grace.
    #[test]
    fn urgent_arms_freeze_everything_before_killing() {
        let jobs = three_jobs();
        let mut e = Engine::new(Policy::default());
        let actions = drive(&mut e, &steady(0.0, 12, 0.86, 30), &jobs);
        let f = freezes(&actions);
        assert!(f.iter().all(|x| x.0 == 1.0));
        let mut frozen: Vec<i32> = f.iter().map(|x| x.1).collect();
        frozen.sort_unstable();
        assert_eq!(frozen, vec![10, 11, 12]);
        assert_eq!(kills(&actions), vec![(11.0, 10, false)]);
    }

    /// The cliff comes into view while an earlier freeze's settle period is still running:
    /// the jobs still running are frozen at once, not when the period ends. Only the kill
    /// that follows waits for the settle period.
    #[test]
    fn an_urgent_arm_freezes_the_rest_at_once_even_during_an_earlier_settle_period() {
        let jobs = three_jobs();
        let mut e = Engine::new(Policy::default());
        // 65% for two samples freezes the newest job (12) and starts a settle period that
        // runs to t=11. From t=2 the compressor is at 86%, which is two strikes at t=3.
        let mut signals = steady(0.0, 2, 0.65, 35);
        signals.extend(steady(2.0, 12, 0.86, 35));
        let actions = drive(&mut e, &signals, &jobs);

        let mut f = freezes(&actions);
        f.sort_by(|a, b| a.partial_cmp(b).unwrap());
        assert_eq!(f, vec![(1.0, 12), (3.0, 10), (3.0, 11)], "{actions:?}");
        // The kill waits a settle period from that freeze, and takes the biggest.
        assert_eq!(kills(&actions), vec![(13.0, 10, false)]);
    }

    /// Runs 1000 s of urgent pressure with the one job, 10, frozen at t=1, and a new job
    /// arriving every `every` seconds from t=2 (the engine freezes each at once); returns
    /// (second, group, reason) of every kill.
    fn urgent_pressure_with_newcomers(every: u32) -> Vec<(u32, i32, String)> {
        let mut e = Engine::new(Policy::default());
        let jobs_at = |t: u32, gone: &[i32]| -> Vec<Job> {
            let mut jobs = vec![job(10, 100, 8)];
            if t >= 2 {
                jobs.extend(
                    (0..=(t - 2) / every)
                        .map(|k| job(100 + i32::try_from(k).unwrap(), 1000 + u64::from(k), 4)),
                );
            }
            jobs.retain(|j| !gone.contains(&j.pgid));
            jobs
        };
        let mut gone: Vec<i32> = Vec::new();
        let mut killed: Vec<(u32, i32, String)> = Vec::new();
        for t in 0..1000 {
            // Two tight samples at 65% freeze the only job, 10; then 86% for good.
            let s = if t < 2 {
                sample(f64::from(t), 0.65, 35)
            } else {
                sample(f64::from(t), 0.86, 35)
            };
            let jobs = jobs_at(t, &gone);
            for action in e.step(&s, || jobs).actions {
                if let Action::Kill { job, reason, .. } = action {
                    gone.push(job.pgid);
                    killed.push((t, job.pgid, reason));
                }
            }
        }
        killed
    }

    fn first_overdue(killed: &[(u32, i32, String)]) -> (u32, i32) {
        killed
            .iter()
            .find(|(_, _, reason)| reason.contains("frozen for more than 15 minutes"))
            .map(|&(t, pgid, _)| (t, pgid))
            .unwrap_or_else(|| panic!("an overdue job is killed: {killed:?}"))
    }

    /// Urgent arms that keep firing must not hide a job frozen past the limit: new jobs keep
    /// arriving, one per settle period, and each time the arm freezes the newcomer. The job
    /// that has been frozen for more than 15 minutes is still killed, as soon as it is overdue.
    #[test]
    fn a_job_frozen_too_long_is_killed_even_while_urgent_arms_keep_firing() {
        let killed = urgent_pressure_with_newcomers(10);
        // Frozen at t=1: overdue after t=901.
        assert_eq!(first_overdue(&killed), (902, 10), "{killed:?}");
    }

    /// Every freeze starts a settle period, so newcomers arriving more often than the period
    /// is long keep it running for ever. They must not postpone the expiry of the job that is
    /// overdue: it is killed on the first sample it is, settle period or not.
    #[test]
    fn newcomers_every_two_seconds_cannot_postpone_the_expiry_of_a_frozen_job() {
        let killed = urgent_pressure_with_newcomers(2);
        assert_eq!(first_overdue(&killed), (902, 10), "{killed:?}");
    }

    /// Jobs frozen together come back oldest first, so the newest, the likeliest runaway,
    /// is resumed last.
    #[test]
    fn jobs_frozen_together_resume_oldest_first() {
        // Listed newest first, so list order alone would resume the wrong way round.
        let jobs = vec![job(30, 300, 2), job(20, 200, 2), job(10, 100, 2)];
        let mut e = Engine::new(Policy::default());
        let mut signals = steady(0.0, 2, 0.86, 30);
        signals.extend(steady(2.0, 60, 0.30, 80));
        let actions = drive(&mut e, &signals, &jobs);
        assert_eq!(freezes(&actions).len(), 3);
        assert_eq!(
            resumes(&actions).iter().map(|x| x.1).collect::<Vec<_>>(),
            vec![10, 20, 30]
        );
    }

    /// A nearly full disk says nothing about a Mac that is not short of memory: no kill while
    /// memory is free, and the same readings kill at once when it gets tight.
    #[test]
    fn calm_memory_with_tiny_headroom_does_not_kill() {
        let jobs = three_jobs();
        let mut e = Engine::new(Policy::default());
        let calm: Vec<Signals> = (0..120)
            .map(|i| {
                let mut s = sample(f64::from(i), 0.10, 80);
                s.headroom = GIB;
                s
            })
            .collect();
        assert!(drive(&mut e, &calm, &jobs).is_empty());
        let mut tight = sample(120.0, 0.10, 30);
        tight.headroom = GIB;
        assert_eq!(
            kills(&drive(&mut e, &[tight], &jobs)),
            vec![(120.0, 10, true)]
        );
    }

    #[test]
    fn critical_pressure_kills_after_ten_strikes() {
        let jobs = three_jobs();
        let mut e = Engine::new(Policy::default());
        let signals: Vec<Signals> = (0..21)
            .map(|i| {
                let mut s = sample(f64::from(i), 0.1, 80);
                s.pressure = Pressure::Critical;
                s
            })
            .collect();
        let actions = drive(&mut e, &signals, &jobs);
        // Ten strikes freeze every job; ten more, after the settle period, kill one.
        assert!(freezes(&actions).iter().all(|x| x.0 == 9.0));
        assert_eq!(freezes(&actions).len(), 3);
        let k = kills(&actions);
        assert_eq!(k.len(), 1);
        assert_eq!(k[0].0, 19.0);
    }

    /// One kill, then the strike arms stay quiet for the settle period while an emergency arm
    /// may still fire.
    #[test]
    fn one_kill_then_settle_before_strike_arms_fire_again() {
        let jobs = vec![job(10, 100, 8), job(11, 200, 6), job(12, 300, 4)];
        let mut e = Engine::new(Policy::default());
        let k = kills(&drive(&mut e, &steady(0.0, 21, 0.86, 30), &jobs));
        // Everything is frozen at 1; the first kill comes a settle period later, and no strike
        // arm kills again within the next settle period. (The engine is handed the same jobs
        // each time, so after 21 s it would see the victim running again; the runner removes
        // it in real use.)
        assert_eq!(k.iter().map(|x| x.0).collect::<Vec<_>>(), vec![11.0]);
        assert!(k.iter().all(|x| !x.2));

        let mut e = Engine::new(Policy::default());
        let mut signals = steady(0.0, 2, 0.86, 30);
        signals.extend(steady(2.0, 3, 0.93, 30));
        let k = kills(&drive(&mut e, &signals, &jobs));
        assert_eq!(
            k.iter().map(|x| (x.0, x.2)).collect::<Vec<_>>(),
            vec![(2.0, true), (3.0, true), (4.0, true)]
        );
    }

    /// Protected users' jobs are never frozen and are killed only when nothing else is left.
    #[test]
    fn protected_jobs_are_never_frozen_and_killed_last() {
        let mut protected = job(20, 900, 40);
        protected.protected = true;
        let both = vec![protected.clone(), job(11, 200, 5), job(12, 100, 3)];

        let mut e = Engine::new(Policy::default());
        let f = freezes(&drive(&mut e, &steady(0.0, 30, 0.65, 35), &both));
        assert_eq!(f.iter().map(|x| x.1).collect::<Vec<_>>(), vec![11, 12]);

        let mut e = Engine::new(Policy::default());
        let k = kills(&drive(&mut e, &steady(0.0, 1, 0.93, 30), &both));
        assert_eq!(k.iter().map(|x| x.1).collect::<Vec<_>>(), vec![11]);

        let only = vec![protected];
        let mut e = Engine::new(Policy::default());
        let actions = drive(&mut e, &steady(0.0, 3, 0.65, 35), &only);
        assert!(freezes(&actions).is_empty());
        assert!(
            actions
                .iter()
                .any(|(_, a)| matches!(a, Action::Warn { .. }))
        );
        let k = kills(&drive(&mut e, &steady(3.0, 1, 0.93, 30), &only));
        assert_eq!(k.iter().map(|x| x.1).collect::<Vec<_>>(), vec![20]);
    }

    /// `--prefer` jobs go first for freezing and for killing, even when older or smaller.
    #[test]
    fn preferred_jobs_rank_first() {
        let mut preferred = job(10, 100, 3);
        preferred.prefer = true;
        let jobs = vec![preferred, job(11, 200, 8), job(12, 300, 6)];
        let mut e = Engine::new(Policy::default());
        let f = freezes(&drive(&mut e, &steady(0.0, 3, 0.65, 35), &jobs));
        assert_eq!(f.iter().map(|x| x.1).collect::<Vec<_>>(), vec![10]);
        let mut e = Engine::new(Policy::default());
        let k = kills(&drive(&mut e, &steady(0.0, 1, 0.93, 30), &jobs));
        assert_eq!(k.iter().map(|x| x.1).collect::<Vec<_>>(), vec![10]);
    }

    /// Jobs under the minimum size are not frozen; a user-suspended job is not "running".
    #[test]
    fn small_and_already_stopped_jobs_are_not_frozen() {
        let mut stopped = job(13, 400, 9);
        stopped.stopped = true;
        let mut small = job(14, 500, 1);
        small.bytes = GIB / 2;
        let jobs = vec![small, stopped, job(10, 100, 8)];
        let mut e = Engine::new(Policy::default());
        let f = freezes(&drive(&mut e, &steady(0.0, 3, 0.65, 35), &jobs));
        assert_eq!(f.iter().map(|x| x.1).collect::<Vec<_>>(), vec![10]);
    }

    /// A job frozen past the limit while memory stays tight is killed; not before.
    #[test]
    fn job_frozen_too_long_while_tight_is_killed() {
        let jobs = vec![job(10, 100, 8)];
        let mut e = Engine::new(Policy::default());
        let f = freezes(&drive(&mut e, &steady(0.0, 3, 0.65, 35), &jobs));
        let frozen_at = f[0].0;
        // Still tight, but occupancy no longer trips any freeze or kill arm.
        let held = drive(&mut e, &steady(3.0, 895, 0.40, 35), &jobs);
        assert!(held.is_empty(), "{held:?}");
        let later = drive(&mut e, &steady(898.0, 10, 0.40, 35), &jobs);
        let k = kills(&later);
        assert_eq!(k.len(), 1);
        assert!(k[0].0 > frozen_at + 900.0 && !k[0].2);
        assert!(matches!(&later[0].1, Action::Kill { reason, .. } if reason.contains("frozen")));
        assert!(e.frozen().is_empty());
    }

    /// A gap between samples is a stall: reported, never read as a fill rate.
    #[test]
    fn stall_is_not_read_as_a_rate() {
        let jobs = three_jobs();
        let mut e = Engine::new(Policy::default());
        let mut signals = steady(0.0, 10, 0.20, 35);
        // The next reading is 6 s later (a stall) and the compressor is at 0.60. Read as a
        // rate over the last 11 s that is 0.036/s: 10 s to full, which would kill.
        signals.push(sample(15.0, 0.60, 35));
        signals.push(sample(16.0, 0.61, 35));
        let mut seen = Vec::new();
        let mut etas = Vec::new();
        for s in &signals {
            let step = e.step(s, || jobs.clone());
            etas.push(step.eta);
            seen.extend(step.actions.into_iter().map(|a| (s.time, a)));
        }
        assert!(
            seen.iter().any(|(t, a)| *t == 15.0
                && matches!(a, Action::Stall { gap } if (*gap - 6.0).abs() < 1e-9))
        );
        assert!(etas[10].is_infinite() && etas[11].is_infinite(), "{etas:?}");
        assert!(
            kills(&seen).is_empty(),
            "the kill-ETA arm must not see a rate: {seen:?}"
        );
    }

    /// The earlyoom failure: starting while swap is already in use must not be read as
    /// growth. Here the machine starts tight with 40 GB already swapped.
    #[test]
    fn starting_under_existing_swap_does_not_fire() {
        let mut e = Engine::new(Policy::default());
        let signals: Vec<Signals> = (0..60)
            .map(|i| {
                let mut s = sample(f64::from(i), 0.45, 30);
                s.swap_used = 40 * GIB;
                s.headroom = 200 * GIB;
                s.pressure = Pressure::Warn;
                s
            })
            .collect();
        assert!(drive(&mut e, &signals, &three_jobs()).is_empty());
    }

    /// A flickering signal accumulates: trip, clean, trip, clean, trip fires on the fifth
    /// sample (1, 0.5, 1.5, 1.0, 2.0), where reset-on-clean never would.
    #[test]
    fn flickering_signal_accumulates_strikes() {
        let mut e = Engine::new(Policy::default());
        let signals: Vec<Signals> = (0..9)
            .map(|i| {
                let mut s = sample(f64::from(i), 0.20, 35);
                s.headroom = if i % 2 == 0 { 50 * GIB } else { 500 * GIB };
                s
            })
            .collect();
        let f = freezes(&drive(&mut e, &signals, &three_jobs()));
        assert_eq!(f[0].0, 4.0);
    }

    /// A long episode must not leave the bucket primed: after memory has been fine for a
    /// while, one stray tripping sample is not enough.
    #[test]
    fn buckets_are_capped_so_a_long_episode_drains_quickly() {
        let jobs: Vec<Job> = (0..20)
            .map(|i| job(100 + i, 100 + u64::try_from(i).unwrap(), 4))
            .collect();
        let mut e = Engine::new(Policy::default());
        let mut signals = steady(0.0, 100, 0.65, 35);
        signals.extend(steady(100.0, 12, 0.20, 35));
        signals.push(sample(112.0, 0.65, 35));
        signals.push(sample(113.0, 0.20, 35));
        let f = freezes(&drive(&mut e, &signals, &jobs));
        assert!(f.iter().all(|x| x.0 < 100.0), "froze after recovery: {f:?}");
    }
}
