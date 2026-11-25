//! The policy: a pure function of the samples, the job list and the clock. It does no I/O, so
//! every scenario is tested with synthetic samples.
//!
//! Measured pressure must persist before freezing or an ordinary kill. The two emergency
//! thresholds bypass the settle period. Recovery counts calm readings and restarts the
//! count when a measured freeze arm fires.

use crate::jobs::Job;
use crate::queue::{Reservation, ReservationLimit};
use crate::signals::{Pressure, Signals};

const GIB: u64 = 1 << 30;
#[derive(Clone, Debug)]
pub struct Policy {
    /// Jobs below this are never frozen: they cannot be what breaks the machine.
    pub min_job_bytes: u64,
    /// Minimum time between two freezes, two resumes, or after a kill before the strike
    /// arms may fire again.
    pub settle: f64,
    /// At sustained freeze pressure, an overdue frozen job can bypass the settle period.
    pub max_freeze: f64,
    /// Report gaps longer than this as a stall.
    pub stall_gap: f64,
    /// `kern.memorystatus_level` at or below this (percent) means memory is tight.
    pub tight_level: i32,

    pub freeze_occupancy: f64,
    pub freeze_strikes: f64,

    /// Resume after this many calm samples without sustained freeze pressure.
    pub resume_samples: u32,

    pub kill_occupancy_now: f64,
    pub kill_occupancy: f64,
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
            freeze_strikes: 2.0,
            resume_samples: 5,
            kill_occupancy_now: 0.92,
            kill_occupancy: 0.85,
            kill_headroom_now: 2 * GIB,
            kill_headroom: 4 * GIB,
            kill_pressure_strikes: 10.0,
            kill_strikes: 2.0,
        }
    }
}

#[derive(Clone, Debug, PartialEq)]
pub enum Action {
    /// The previous sample was too long ago.
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
        threshold: KillThreshold,
    },
}

#[derive(Clone, Debug, PartialEq)]
pub enum KillThreshold {
    Overdue,
    Arms(Vec<Arm>),
}

impl KillThreshold {
    pub(crate) fn still_tripped(&self, s: &Signals, policy: &Policy) -> bool {
        let view = View::new(s, policy);
        match self {
            Self::Overdue => view.freeze_pressure(policy),
            Self::Arms(arms) => arms.iter().any(|arm| arm.tripped(&view, policy)),
        }
    }
}

/// What one sample led to.
#[derive(Clone, Debug, PartialEq)]
pub struct Step {
    pub occupancy: f64,
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

#[derive(Clone, Copy, Debug, PartialEq)]
pub enum Arm {
    FreezeOccupancy,
    FreezeHeadroom,
    KillOccupancyNow,
    KillHeadroomNow,
    KillOccupancy,
    KillHeadroom,
    KillPressure,
}

const ARMS: [Arm; 7] = [
    Arm::FreezeOccupancy,
    Arm::FreezeHeadroom,
    Arm::KillOccupancyNow,
    Arm::KillHeadroomNow,
    Arm::KillOccupancy,
    Arm::KillHeadroom,
    Arm::KillPressure,
];

/// The readings an arm looks at.
struct View {
    occupancy: f64,
    tight: bool,
    headroom: u64,
    ram: u64,
    pressure: Pressure,
}

impl View {
    fn new(s: &Signals, policy: &Policy) -> Self {
        Self {
            occupancy: s.occupancy(),
            tight: s.pressure > Pressure::Normal || s.level <= policy.tight_level,
            headroom: s.headroom,
            ram: s.ram,
            pressure: s.pressure,
        }
    }

    fn freeze_pressure(&self, policy: &Policy) -> bool {
        Arm::FreezeOccupancy.tripped(self, policy) || Arm::FreezeHeadroom.tripped(self, policy)
    }
}

impl Arm {
    fn tier(self) -> Tier {
        match self {
            Arm::FreezeOccupancy | Arm::FreezeHeadroom => Tier::Freeze,
            _ => Tier::Kill,
        }
    }

    fn needed(self, p: &Policy) -> f64 {
        match self {
            Arm::FreezeOccupancy | Arm::FreezeHeadroom => p.freeze_strikes,
            Arm::KillOccupancyNow | Arm::KillHeadroomNow => 1.0,
            Arm::KillOccupancy | Arm::KillHeadroom => p.kill_strikes,
            Arm::KillPressure => p.kill_pressure_strikes,
        }
    }

    fn immediate(self) -> bool {
        matches!(self, Arm::KillOccupancyNow | Arm::KillHeadroomNow)
    }

    fn tripped(self, v: &View, p: &Policy) -> bool {
        match self {
            Arm::FreezeOccupancy => v.tight && v.occupancy >= p.freeze_occupancy,
            Arm::FreezeHeadroom => v.tight && v.headroom < v.ram,
            Arm::KillOccupancyNow => v.tight && v.occupancy >= p.kill_occupancy_now,
            Arm::KillHeadroomNow => v.tight && v.headroom < p.kill_headroom_now,
            Arm::KillOccupancy => v.tight && v.occupancy >= p.kill_occupancy,
            Arm::KillHeadroom => v.tight && v.headroom < p.kill_headroom,
            Arm::KillPressure => v.pressure == Pressure::Critical,
        }
    }

    fn label(self, p: &Policy) -> String {
        let pct = |f: f64| (f * 100.0).round();
        let gb = |b: u64| b as f64 / GIB as f64;
        match self {
            Arm::FreezeOccupancy => format!("compressor >= {}%", pct(p.freeze_occupancy)),
            Arm::FreezeHeadroom => "swap headroom < RAM".to_string(),
            Arm::KillOccupancyNow => format!("compressor >= {}%", pct(p.kill_occupancy_now)),
            Arm::KillHeadroomNow => format!("swap headroom < {}GB", gb(p.kill_headroom_now)),
            Arm::KillOccupancy => format!("compressor >= {}%", pct(p.kill_occupancy)),
            Arm::KillHeadroom => format!("swap headroom < {}GB", gb(p.kill_headroom)),
            Arm::KillPressure => "critical pressure".to_string(),
        }
    }
}

pub struct Engine {
    policy: Policy,
    last_time: Option<f64>,
    strikes: [f64; ARMS.len()],
    /// Settle period for normal freezing and non-immediate killing.
    quiet_until: f64,
    calm_streak: u32,
    resume_after: f64,
    frozen: Vec<Frozen>,
    warned_no_victim: bool,
}

fn same(a: &Job, b: &Job) -> bool {
    a.pgid == b.pgid && a.start_us == b.start_us
}

fn rank(reservations: &[Reservation], job: &Job) -> (u8, u64) {
    match reservations
        .iter()
        .find(|r| r.pgid == job.pgid && r.pgid == job.leader_pid && r.start_us == job.start_us)
    {
        None => (2, job.bytes),
        Some(r) => match r.limit {
            ReservationLimit::Learned(need) if job.bytes > need => (2, job.bytes - need),
            ReservationLimit::Cold(need) if job.bytes > need => (1, job.bytes),
            _ => (0, 0),
        },
    }
}

impl Engine {
    pub fn new(policy: Policy) -> Engine {
        Engine {
            policy,
            last_time: None,
            strikes: [0.0; ARMS.len()],
            quiet_until: f64::NEG_INFINITY,
            calm_streak: 0,
            resume_after: f64::NEG_INFINITY,
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

    /// A deliberate pause, such as waiting for a kill, is not a sampling stall.
    pub fn resync(&mut self) {
        self.last_time = None;
    }

    /// Scans jobs at most once, and only when an action could need a victim.
    pub fn step(&mut self, s: &Signals, jobs: impl FnOnce() -> Vec<Job>) -> Step {
        self.step_with_admissions(s, &[], jobs)
    }

    pub(crate) fn step_with_admissions(
        &mut self,
        s: &Signals,
        reservations: &[Reservation],
        jobs: impl FnOnce() -> Vec<Job>,
    ) -> Step {
        self.step_with_candidates(s, || (jobs(), reservations))
    }

    pub(crate) fn step_with_candidates<'a>(
        &mut self,
        s: &Signals,
        candidates: impl FnOnce() -> (Vec<Job>, &'a [Reservation]),
    ) -> Step {
        let mut actions = Vec::new();
        let now = s.time;
        if let Some(prev) = self.last_time {
            let gap = now - prev;
            if !(gap > 0.0 && gap <= self.policy.stall_gap) {
                actions.push(Action::Stall { gap });
            }
        }
        self.last_time = Some(now);

        let view = View::new(s, &self.policy);

        for (arm, strikes) in ARMS.iter().zip(self.strikes.iter_mut()) {
            *strikes = if arm.tripped(&view, &self.policy) {
                (*strikes + 1.0).min(arm.needed(&self.policy))
            } else {
                (*strikes - 0.5).max(0.0)
            };
        }
        let pressure = view.freeze_pressure(&self.policy) || s.pressure == Pressure::Critical;
        let quiet = now < self.quiet_until;
        let kill = self.fired(Tier::Kill, &view);
        let freeze = self.fired(Tier::Freeze, &view);
        let sustained_freeze = !freeze.is_empty();
        if sustained_freeze {
            self.calm_streak = 0;
        } else if !pressure {
            self.calm_streak = self.calm_streak.saturating_add(1);
        }
        let overdue = sustained_freeze
            && self
                .frozen
                .iter()
                .any(|f| now - f.since > self.policy.max_freeze);

        if !kill.is_empty() || (!quiet && sustained_freeze) || overdue {
            let (mut pool, reservations) = candidates();
            for f in &self.frozen {
                if !pool.iter().any(|j| same(j, &f.job)) {
                    pool.push(f.job.clone());
                }
            }
            pool.retain(|j| j.bytes >= self.policy.min_job_bytes || self.holds(j));
            pool.retain(|j| !j.protected);
            let expired_job = |job: &Job| {
                self.frozen
                    .iter()
                    .any(|f| same(&f.job, job) && now - f.since > self.policy.max_freeze)
            };
            pool.sort_unstable_by_key(|j| {
                let (class, excess) = rank(reservations, j);
                (
                    class,
                    excess,
                    overdue && expired_job(j),
                    j.prefer,
                    j.start_us,
                    j.bytes,
                )
            });
            let victim = pool.last();
            let immediate = kill.iter().any(|a| a.immediate());
            let urgent = !kill.is_empty();
            let highest = victim.map(|j| rank(reservations, j).0);
            let ordinary_kill = highest != Some(0) || s.pressure == Pressure::Critical;
            let reason = self.reasons(if urgent { &kill } else { &freeze });
            if immediate {
                self.kill(
                    now,
                    victim,
                    true,
                    reason,
                    KillThreshold::Arms(kill),
                    &mut actions,
                );
            } else if overdue {
                let reason = format!("measured pressure persists; {}", self.overdue_reason());
                self.kill(
                    now,
                    victim,
                    false,
                    reason,
                    KillThreshold::Overdue,
                    &mut actions,
                );
            } else if urgent
                && !quiet
                && ordinary_kill
                && pool
                    .iter()
                    .any(|j| self.holds(j) && Some(rank(reservations, j).0) == highest)
            {
                self.kill(
                    now,
                    victim,
                    false,
                    reason,
                    KillThreshold::Arms(kill),
                    &mut actions,
                );
            } else if urgent || !quiet {
                let freeze_class = if urgent {
                    highest
                } else {
                    pool.iter()
                        .rev()
                        .find(|j| self.freezable(j))
                        .map(|j| rank(reservations, j).0)
                };
                let eligible = pool.iter().rev().filter(|j| {
                    let class = rank(reservations, j).0;
                    class > 0 && Some(class) == freeze_class && self.freezable(j)
                });
                let count = if urgent && reservations.is_empty() {
                    usize::MAX
                } else {
                    1
                };
                let mut froze = false;
                for job in eligible.take(count).cloned().collect::<Vec<_>>() {
                    froze = true;
                    self.frozen.push(Frozen {
                        job: job.clone(),
                        since: now,
                    });
                    actions.push(Action::Freeze {
                        job,
                        reason: reason.clone(),
                    });
                }
                if froze {
                    self.warned_no_victim = false;
                    self.quiet_until = now + self.policy.settle;
                } else if urgent && !quiet && ordinary_kill {
                    self.kill(
                        now,
                        victim,
                        false,
                        reason,
                        KillThreshold::Arms(kill),
                        &mut actions,
                    );
                } else if !urgent
                    && !pool
                        .iter()
                        .any(|j| self.holds(j) && rank(reservations, j).0 > 0)
                {
                    self.no_victim("freeze", &reason, &mut actions);
                }
            }
        } else if !pressure
            && self.calm_streak >= self.policy.resume_samples
            && now >= self.resume_after
            && !self.frozen.is_empty()
        {
            let next = self
                .frozen
                .iter()
                .enumerate()
                .min_by(|(_, a), (_, b)| {
                    a.since
                        .total_cmp(&b.since)
                        .then(a.job.start_us.cmp(&b.job.start_us))
                })
                .map_or(0, |(index, _)| index);
            let job = self.frozen.remove(next).job;
            self.calm_streak = 0;
            self.resume_after = now + self.policy.settle;
            self.strikes = [0.0; ARMS.len()];
            self.quiet_until = now + self.policy.settle;
            actions.push(Action::Resume { job });
        } else if self.strikes.iter().all(|&n| n == 0.0) {
            self.warned_no_victim = false;
        }
        Step {
            occupancy: view.occupancy,
            tight: view.tight
                && (pressure
                    || s.pressure > Pressure::Normal
                    || s.level <= self.policy.tight_level - 2),
            actions,
        }
    }

    fn fired(&self, tier: Tier, view: &View) -> Vec<Arm> {
        ARMS.iter()
            .zip(&self.strikes)
            .filter(|(a, n)| {
                a.tier() == tier && **n >= a.needed(&self.policy) && a.tripped(view, &self.policy)
            })
            .map(|(a, _)| *a)
            .collect()
    }

    /// Why a job frozen past the limit is killed.
    fn overdue_reason(&self) -> String {
        format!(
            "frozen for more than {} minutes",
            (self.policy.max_freeze / 60.0).round()
        )
    }

    fn reasons(&self, arms: &[Arm]) -> String {
        arms.iter()
            .map(|a| a.label(&self.policy))
            .collect::<Vec<_>>()
            .join(", ")
    }

    fn holds(&self, job: &Job) -> bool {
        self.frozen.iter().any(|f| same(&f.job, job))
    }

    fn freezable(&self, job: &Job) -> bool {
        !job.protected && !job.stopped && !self.holds(job)
    }

    fn kill(
        &mut self,
        now: f64,
        job: Option<&Job>,
        immediate: bool,
        reason: String,
        threshold: KillThreshold,
        actions: &mut Vec<Action>,
    ) {
        let Some(job) = job.cloned() else {
            self.no_victim("kill", &reason, actions);
            return;
        };
        self.warned_no_victim = false;
        self.frozen.retain(|f| !same(&f.job, &job));
        self.strikes = [0.0; ARMS.len()];
        self.quiet_until = now + self.policy.settle;
        actions.push(Action::Kill {
            job,
            immediate,
            reason,
            threshold,
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

    /// The measured threshold freezes groups by footprint, at most one per settle period.
    #[test]
    fn gradual_fill_freezes_largest_jobs_one_per_settle_period() {
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
        assert_eq!(f.iter().map(|x| x.1).collect::<Vec<_>>(), vec![10, 12, 11]);
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

    #[test]
    fn mixed_growth_at_flat_or_falling_levels_acts_only_at_measured_thresholds() {
        let jobs = [job(10, 100, 8), job(11, 200, 9)];
        let reservations = [Reservation {
            pgid: 10,
            start_us: 100,
            limit: ReservationLimit::Learned(12 * GIB),
        }];
        for start_level in [35, 43] {
            let mut e = Engine::new(Policy::default());
            for i in 0..=10 {
                let mut reading = sample(
                    f64::from(i),
                    0.40 + 0.018 * f64::from(i),
                    start_level - (start_level - 35) * i / 10,
                );
                reading.pressure = Pressure::Warn;
                let step = e.step_with_admissions(&reading, &reservations, || jobs.to_vec());
                assert!(step.actions.is_empty(), "{step:?}");
            }
            for (second, expected) in [(11, None), (12, Some(11))] {
                let reading = sample(f64::from(second), 0.65, 35);
                let step = e.step_with_admissions(&reading, &reservations, || jobs.to_vec());
                assert_eq!(
                    step.actions.iter().find_map(|action| match action {
                        Action::Freeze { job, .. } => Some(job.pgid),
                        _ => None,
                    }),
                    expected
                );
            }
            let step =
                e.step_with_admissions(&sample(13.0, 0.93, 35), &reservations, || jobs.to_vec());
            assert!(
                matches!(step.actions.as_slice(), [Action::Kill { job, .. }] if job.pgid == 11)
            );
        }
    }

    #[test]
    fn over_reservation_jobs_rank_ahead_of_newer_and_frozen_healthy_jobs() {
        let runaway = job(10, 100, 9);
        let healthy = job(11, 200, 8);
        let both = || vec![runaway.clone(), healthy.clone()];
        let reservations = [
            Reservation {
                pgid: 10,
                start_us: 100,
                limit: ReservationLimit::Learned(4 * GIB),
            },
            Reservation {
                pgid: 11,
                start_us: 200,
                limit: ReservationLimit::Learned(12 * GIB),
            },
        ];
        for occupancy in [0.65, 0.86] {
            let mut e = Engine::new(Policy::default());
            e.step_with_admissions(&sample(0.0, occupancy, 35), &reservations, both);
            let step = e.step_with_admissions(&sample(1.0, occupancy, 35), &reservations, both);
            assert!(
                matches!(step.actions.as_slice(), [Action::Freeze { job, .. }] if job.pgid == 10)
            );
        }
        let mut e = Engine::new(Policy::default());
        drive(
            &mut e,
            &steady(0.0, 2, 0.65, 35),
            std::slice::from_ref(&healthy),
        );
        let step = e.step_with_admissions(&sample(2.0, 0.93, 35), &reservations, both);
        assert!(matches!(step.actions.as_slice(), [Action::Kill { job, .. }] if job.pgid == 10));
    }

    #[test]
    fn frozen_work_and_admission_recover_at_level_39_without_pressure() {
        let mut e = Engine::new(Policy::default());
        let jobs = three_jobs();
        assert_eq!(
            freezes(&drive(&mut e, &steady(0.0, 2, 0.65, 39), &jobs)).len(),
            1
        );
        let mut resumed = false;
        for s in steady(2.0, 15, 0.55, 39) {
            let step = e.step(&s, || jobs.clone());
            resumed |= step
                .actions
                .iter()
                .any(|a| matches!(a, Action::Resume { .. }));
            if resumed {
                assert!(!step.tight);
            }
        }
        assert!(resumed);
    }

    #[test]
    fn lint_growing_into_its_reservation_does_not_freeze() {
        use crate::queue::{Sample, Scheduler};
        use crate::sizes::Demand;

        let mut e = Engine::new(Policy::default());
        let mut queue = Scheduler::new();
        let id = queue.enqueue(1, "lint model split".into(), "bun run lint".into(), 0.0);
        let demand = |_: &str| Demand {
            bytes: Some((19.4 * GIB as f64) as u64),
            ..Demand::default()
        };
        let mut admitted = Vec::new();
        // samples.tsv, 2026-11-25 07:22:49–07:23:02 UTC. At the last two
        // readings the kernel warns, but swap is flat and over 200 GiB are available
        // for more swap. The lint footprint at the freeze is 16.3 of its 19.4 GiB.
        let readings = [
            (0.4613, 62),
            (0.4613, 62),
            (0.4613, 62),
            (0.4596, 62),
            (0.4666, 61),
            (0.4804, 59),
            (0.4980, 57),
            (0.4848, 55),
            (0.5008, 53),
            (0.5181, 51),
            (0.5358, 49),
            (0.5539, 46),
            (0.5508, 46),
            (0.5570, 46),
        ];
        for (i, (occupancy, level)) in readings.into_iter().enumerate() {
            let mut s = sample(i as f64, occupancy, level);
            s.ram = 96 * GIB;
            s.headroom = 220 * GIB;
            s.swap_used = 38 * GIB;
            if i >= 12 {
                s.pressure = Pressure::Warn;
            }
            // Only the final group footprint was logged. Model the intervening footprint
            // from the observed drop in free memory, anchored at that final 16.3 GiB.
            let footprint = f64::from(62 - level) / 16.0 * 16.3 * GIB as f64;
            queue.observe(id, footprint as u64, s.time);
            let reservations = [Reservation {
                pgid: 87149,
                start_us: 100,
                limit: ReservationLimit::Learned((19.4 * GIB as f64) as u64),
            }];
            let step = e.step_with_admissions(&s, &reservations, || vec![job(87149, 100, 16)]);
            queue.step(
                &Sample {
                    time: s.time,
                    wall: s.wall,
                    level: s.level,
                    ram: s.ram,
                    occupancy: step.occupancy,
                    tight: step.tight,
                    frozen: false,
                },
                &demand,
                &mut admitted,
            );
            if i == 0 {
                assert_eq!(admitted, [id]);
            }
            assert!(step.actions.is_empty(), "sample {i}: {:?}", step.actions);
        }
    }

    #[test]
    fn frozen_runaway_keeps_healthy_jobs_out_of_the_victim_pool() {
        let mut e = Engine::new(Policy::default());
        let jobs = [job(10, 100, 6), job(11, 200, 8), job(12, 300, 12)];
        let reservations = [
            Reservation {
                pgid: 10,
                start_us: 100,
                limit: ReservationLimit::Learned(3 * GIB),
            },
            Reservation {
                pgid: 11,
                start_us: 200,
                limit: ReservationLimit::Learned(7 * GIB),
            },
            Reservation {
                pgid: 12,
                start_us: 300,
                limit: ReservationLimit::Learned(15 * GIB),
            },
        ];
        let actions: Vec<_> = steady(0.0, 8, 0.86, 35)
            .iter()
            .flat_map(|s| {
                e.step_with_admissions(s, &reservations, || jobs.to_vec())
                    .actions
                    .into_iter()
                    .map(|action| (s.time, action))
            })
            .collect();
        assert_eq!(freezes(&actions), [(1.0, 10), (2.0, 11)]);
        assert!(kills(&actions).is_empty());
    }

    #[test]
    fn healthy_jobs_are_left_running_until_the_cliff_and_newest_is_first() {
        let mut e = Engine::new(Policy::default());
        let jobs = [job(10, 100, 10), job(11, 200, 8)];
        let reservations = [
            Reservation {
                pgid: 10,
                start_us: 100,
                limit: ReservationLimit::Learned(12 * GIB),
            },
            Reservation {
                pgid: 11,
                start_us: 200,
                limit: ReservationLimit::Learned(12 * GIB),
            },
        ];
        for s in steady(0.0, 30, 0.86, 35) {
            assert!(
                e.step_with_admissions(&s, &reservations, || jobs.to_vec())
                    .actions
                    .is_empty()
            );
        }
        let step = e.step_with_admissions(&sample(30.0, 0.93, 35), &reservations, || jobs.to_vec());
        assert!(
            matches!(step.actions.as_slice(), [Action::Kill { job, immediate: true, .. }] if job.pgid == 11)
        );
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

    #[test]
    fn sustained_freeze_pressure_restarts_the_five_sample_recovery() {
        let mut e = Engine::new(Policy::default());
        let jobs = [job(10, 100, 8)];
        assert_eq!(
            freezes(&drive(&mut e, &steady(0.0, 2, 0.65, 35), &jobs)),
            [(1.0, 10)]
        );
        assert!(resumes(&drive(&mut e, &steady(2.0, 4, 0.55, 35), &jobs)).is_empty());
        assert!(resumes(&drive(&mut e, &steady(6.0, 2, 0.65, 35), &jobs)).is_empty());
        let recovered = drive(&mut e, &steady(8.0, 5, 0.55, 35), &jobs);
        assert_eq!(resumes(&recovered), [(12.0, 10)]);
        assert!(e.frozen().is_empty());
    }

    #[test]
    fn a_frozen_smaller_group_does_not_displace_a_larger_runaway_at_the_cliff() {
        let jobs = vec![job(10, 100, 30), job(11, 300, 2)];
        let mut e = Engine::new(Policy::default());
        let f = freezes(&drive(&mut e, &steady(0.0, 2, 0.65, 35), &jobs[1..]));
        assert_eq!(f, [(1.0, 11)]);
        let k = kills(&drive(&mut e, &steady(2.0, 1, 0.93, 30), &jobs));
        assert_eq!(k, [(2.0, 10, true)]);
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
        // 65% for two samples freezes the largest job (10) and starts a settle period that
        // runs to t=11. From t=2 the compressor is at 86%, which is two strikes at t=3.
        let mut signals = steady(0.0, 2, 0.65, 35);
        signals.extend(steady(2.0, 12, 0.86, 35));
        let actions = drive(&mut e, &signals, &jobs);

        let mut f = freezes(&actions);
        f.sort_by(|a, b| a.partial_cmp(b).unwrap());
        assert_eq!(f, vec![(1.0, 10), (3.0, 11), (3.0, 12)], "{actions:?}");
        // The kill waits a settle period from that freeze, and takes the biggest.
        assert_eq!(kills(&actions), vec![(13.0, 10, false)]);
    }

    #[test]
    fn overdue_healthy_work_cannot_displace_a_runaway_or_unprofiled_candidate() {
        for profiled in [false, true] {
            let mut e = Engine::new(Policy::default());
            let healthy = job(11, 200, 8);
            let runaway = job(10, 100, 6);
            e.frozen.push(Frozen {
                job: healthy.clone(),
                since: 1.0,
            });
            let mut reservations = vec![Reservation {
                pgid: 11,
                start_us: 200,
                limit: ReservationLimit::Learned(12 * GIB),
            }];
            if profiled {
                reservations.push(Reservation {
                    pgid: 10,
                    start_us: 100,
                    limit: ReservationLimit::Learned(4 * GIB),
                });
            }
            let s = sample(902.0, 0.93, 35);
            let step = e.step_with_admissions(&s, &reservations, || vec![healthy.clone(), runaway]);
            assert_eq!(
                kills(
                    &step
                        .actions
                        .into_iter()
                        .map(|a| (s.time, a))
                        .collect::<Vec<_>>()
                ),
                [(902.0, 10, true)]
            );
            let step = e.step_with_admissions(&sample(903.0, 0.65, 35), &reservations, || {
                vec![healthy.clone()]
            });
            assert!(step.actions.is_empty());
            let step =
                e.step_with_admissions(&sample(904.0, 0.65, 35), &reservations, || vec![healthy]);
            assert!(
                matches!(step.actions.as_slice(), [Action::Kill { job, reason, .. }]
                if job.pgid == 11 && reason.contains("frozen for more than"))
            );
        }
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

    /// Protected users' jobs are exempt from freezing and killing.
    #[test]
    fn protected_jobs_are_never_frozen_or_killed() {
        let mut protected = job(20, 900, 40);
        protected.protected = true;
        let both = vec![protected.clone(), job(11, 200, 5), job(12, 100, 3)];

        let mut e = Engine::new(Policy::default());
        let f = freezes(&drive(&mut e, &steady(0.0, 30, 0.65, 35), &both));
        assert_eq!(f.iter().map(|x| x.1).collect::<Vec<_>>(), vec![11, 12]);

        let mut e = Engine::new(Policy::default());
        let k = kills(&drive(&mut e, &steady(0.0, 1, 0.93, 30), &both));
        assert_eq!(k.iter().map(|x| x.1).collect::<Vec<_>>(), vec![11]);

        let reservations = [Reservation {
            pgid: protected.pgid,
            start_us: protected.start_us,
            limit: ReservationLimit::Learned(GIB),
        }];
        let mut e = Engine::new(Policy::default());
        let step = e.step_with_admissions(&sample(0.0, 0.93, 30), &reservations, || both.clone());
        assert!(matches!(step.actions.as_slice(), [Action::Kill { job, .. }] if job.pgid == 11));

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
        assert!(k.is_empty());
    }

    /// Preference breaks ties between equally large groups.
    #[test]
    fn preference_breaks_equal_excess_ties() {
        let mut preferred = job(10, 100, 8);
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

    #[test]
    fn an_overdue_group_resumes_if_pressure_falls_below_the_freeze_tier() {
        let jobs = [job(10, 100, 8)];
        let policy = Policy {
            max_freeze: 4.0,
            ..Policy::default()
        };
        let mut e = Engine::new(policy);
        assert_eq!(
            freezes(&drive(&mut e, &steady(0.0, 2, 0.65, 35), &jobs)),
            [(1.0, 10)]
        );
        let recovered = drive(&mut e, &steady(2.0, 5, 0.40, 35), &jobs);
        assert!(kills(&recovered).is_empty());
        assert_eq!(resumes(&recovered), [(6.0, 10)]);
    }

    /// A sampling gap produces a stall event, with no action below the measured thresholds.
    #[test]
    fn a_sampling_gap_is_reported() {
        let jobs = three_jobs();
        let mut e = Engine::new(Policy::default());
        let mut signals = steady(0.0, 10, 0.20, 35);
        // The next reading is 6 s later (a stall) and the compressor is at 0.58. Read as a
        // rate over the last 11 s that is 0.035/s: about 11 s to full.
        signals.push(sample(15.0, 0.58, 35));
        signals.push(sample(16.0, 0.59, 35));
        let mut seen = Vec::new();
        for s in &signals {
            let step = e.step(s, || jobs.clone());
            seen.extend(step.actions.into_iter().map(|a| (s.time, a)));
        }
        assert!(
            seen.iter().any(|(t, a)| *t == 15.0
                && matches!(a, Action::Stall { gap } if (*gap - 6.0).abs() < 1e-9))
        );
        assert!(
            freezes(&seen).is_empty() && kills(&seen).is_empty(),
            "both measurements are below the freeze threshold: {seen:?}"
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

    #[test]
    fn review3_unqueued_runaway_outranks_a_cold_job_and_protection_is_absolute() {
        let jobs = [job(10, 100, 30), job(11, 200, 8)];
        let reservations = [Reservation {
            pgid: 11,
            start_us: 200,
            limit: ReservationLimit::Cold(6 * GIB),
        }];
        let mut e = Engine::new(Policy::default());
        let step = e.step_with_admissions(&sample(0.0, 0.93, 35), &reservations, || jobs.to_vec());
        assert!(matches!(step.actions.as_slice(), [Action::Kill { job, .. }] if job.pgid == 10));
        let mut e = Engine::new(Policy::default());
        e.step_with_admissions(&sample(0.0, 0.65, 35), &reservations, || jobs.to_vec());
        let step = e.step_with_admissions(&sample(1.0, 0.65, 35), &reservations, || jobs.to_vec());
        assert!(matches!(step.actions.as_slice(), [Action::Freeze { job, .. }] if job.pgid == 10));
        let mut protected = jobs[0].clone();
        protected.protected = true;
        let mut e = Engine::new(Policy::default());
        let step = e.step_with_admissions(&sample(2.0, 0.93, 35), &reservations, || {
            vec![protected, jobs[1].clone()]
        });
        assert!(matches!(step.actions.as_slice(), [Action::Kill { job, .. }] if job.pgid == 11));
    }
    #[test]
    fn review4_early_freeze_spares_healthy_cold_jobs_and_warns_once() {
        let mut e = Engine::new(Policy::default());
        let cold = job(10, 100, 5);
        let reservations = [Reservation {
            pgid: 10,
            start_us: 100,
            limit: ReservationLimit::Cold(8 * GIB),
        }];
        for (i, s) in steady(0.0, 15, 0.65, 35).iter().enumerate() {
            let step = e.step_with_admissions(s, &reservations, || vec![cold.clone()]);
            if i == 1 {
                assert!(
                    matches!(step.actions.as_slice(), [Action::Warn { .. }]),
                    "{step:?}"
                );
            } else {
                assert!(step.actions.is_empty(), "{step:?}");
            }
        }
        let runaway = job(10, 100, 9);
        let step = e.step_with_admissions(&sample(15.0, 0.65, 35), &reservations, || vec![runaway]);
        assert!(matches!(step.actions.as_slice(), [Action::Freeze { job, .. }] if job.pgid == 10));
    }

    #[test]
    fn early_freeze_reaches_a_cold_runaway_beside_a_frozen_unqueued_group() {
        let mut e = Engine::new(Policy::default());
        let unqueued = job(10, 100, 30);
        let mut cold = job(11, 200, 5);
        let reservations = [Reservation {
            pgid: cold.pgid,
            start_us: cold.start_us,
            limit: ReservationLimit::Cold(6 * GIB),
        }];
        let first = (0..=1)
            .flat_map(|t| {
                e.step_with_admissions(&sample(f64::from(t), 0.65, 35), &reservations, || {
                    vec![unqueued.clone(), cold.clone()]
                })
                .actions
            })
            .collect::<Vec<_>>();
        assert!(matches!(first.as_slice(), [Action::Freeze { job, .. }] if job.pgid == 10));

        cold.bytes = 8 * GIB;
        let actions = steady(2.0, 10, 0.65, 35)
            .iter()
            .flat_map(|s| {
                e.step_with_admissions(s, &reservations, || vec![unqueued.clone(), cold.clone()])
                    .actions
                    .into_iter()
                    .map(|action| (s.time, action))
            })
            .collect::<Vec<_>>();
        assert_eq!(freezes(&actions), [(11.0, 11)]);
        assert!(kills(&actions).is_empty());
    }

    #[test]
    fn review4_one_rebound_does_not_kill_an_overdue_group() {
        let mut e = Engine::new(Policy {
            max_freeze: 4.0,
            ..Policy::default()
        });
        let jobs = [job(10, 100, 8)];
        assert_eq!(
            freezes(&drive(&mut e, &steady(0.0, 2, 0.65, 35), &jobs)),
            [(1.0, 10)]
        );
        assert!(drive(&mut e, &steady(2.0, 4, 0.59, 35), &jobs).is_empty());
        let step = e.step(&sample(6.0, 0.61, 35), || jobs.to_vec());
        assert!(step.actions.is_empty(), "{step:?}");
        let step = e.step(&sample(7.0, 0.61, 35), || jobs.to_vec());
        assert!(
            matches!(step.actions.as_slice(), [Action::Kill { job, immediate: false, .. }] if job.pgid == 10)
        );
    }

    #[test]
    fn review4_one_rebound_does_not_erase_recovery_samples() {
        let mut e = Engine::new(Policy::default());
        let jobs = [job(10, 100, 8)];
        drive(&mut e, &steady(0.0, 2, 0.65, 35), &jobs);
        assert!(drive(&mut e, &steady(2.0, 4, 0.59, 35), &jobs).is_empty());
        assert!(
            e.step(&sample(6.0, 0.61, 35), || jobs.to_vec())
                .actions
                .is_empty()
        );
        let step = e.step(&sample(7.0, 0.59, 35), || jobs.to_vec());
        assert!(
            matches!(step.actions.as_slice(), [Action::Resume { job }] if job.pgid == 10),
            "{step:?}"
        );
    }

    #[test]
    fn review3_persistent_freeze_pressure_can_kill_an_overdue_group() {
        let mut e = Engine::new(Policy::default());
        let jobs = [job(10, 100, 8)];
        let actions = drive(&mut e, &steady(0.0, 903, 0.65, 35), &jobs);
        assert_eq!(kills(&actions), [(902.0, 10, false)]);
    }
}
