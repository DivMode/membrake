//! Memory and CPU admission from measured background use and whole-job reservations.
//! The oldest request has priority. EASY backfill uses learned completion times to
//! admit smaller requests that can finish before the head needs their resources.

use std::collections::VecDeque;

use crate::engine::Policy;
use crate::sizes::Demand;

/// Memory is counted in bytes; this is the unit of the defaults.
pub const GIB: u64 = 1 << 30;
/// Memory in use is averaged over this long when deciding whether a command fits (seconds).
pub const USED_WINDOW: f64 = 5.0;
/// The cap is learned from the memory in use averaged over this long (seconds).
pub const CAP_WINDOW: f64 = 15.0;
/// The cap is probed upwards after this long without pressure (seconds).
pub const PROBE_AFTER: f64 = 60.0;
/// The cap starts here, as a percentage of RAM.
const START_PERCENT: u64 = 70;
/// Each probe raises the cap by this, as a percentage of RAM...
const PROBE_PERCENT: u64 = 5;
/// ...and never above this. Live stress runs (#26) showed probes toward 90% letting two
/// 38 GB reservations run together on a 96 GB host whose other work sat in swap.
const PROBE_MAX_PERCENT: u64 = 75;
/// An oversized head may start beside others while everything fits under this.
const MAX_PERCENT: u64 = 90;
/// Readings kept: more than `CAP_WINDOW` holds at one a second.
const HISTORY: usize = 64;
const FRESH_SAMPLE_SECONDS: f64 = 2.5;
const EXCLUSIVE_WAIT_SECONDS: f64 = 600.0;
const CPU_WINDOW_SECONDS: f64 = 10.0;
const COLD_PLATEAU_SECONDS: f64 = 30.0;
/// Readings a plateau needs inside its window (the sampler runs once a second), so a gap in
/// sampling cannot pass for a flat peak.
const COLD_PLATEAU_SAMPLES: usize = 20;
/// Set the soft admission budget below the machine's logical core count by this share.
pub const DEFAULT_CPU_HEADROOM_PERCENT: u32 = 20;
pub(crate) const MILLICORES: u32 = 1000;

pub type Id = u64;

#[derive(Clone, Copy)]
pub(crate) struct Reservation {
    pub pgid: i32,
    pub start_us: u64,
    pub limit: ReservationLimit,
}

#[derive(Clone, Copy)]
pub(crate) enum ReservationLimit {
    Learned(u64),
    Cold(u64),
}

/// One reading, as the queue sees it.
#[derive(Clone, Copy, Debug)]
pub struct Sample {
    /// Engine time, seconds.
    pub time: f64,
    /// Seconds since the Unix epoch, for what is shown to people.
    pub wall: f64,
    pub ram: u64,
    /// `kern.memorystatus_level`: the percent of memory the kernel counts as available.
    pub level: i32,
    /// Compressor occupancy as a fraction of the closer limit.
    pub occupancy: f64,
    /// The engine's own test for tight memory.
    pub tight: bool,
    /// The engine holds at least one frozen job.
    pub frozen: bool,
}

impl Sample {
    /// Memory in use: what the kernel does not count as available.
    pub fn used(&self) -> u64 {
        let in_use = 100 - i128::from(self.level.clamp(0, 100));
        (i128::from(self.ram) * in_use / 100) as u64
    }
}

fn percent(ram: u64, percent: u64) -> u64 {
    (u128::from(ram) * u128::from(percent) / 100) as u64
}

/// Recent (time, memory in use) readings in a fixed array, so a step never allocates.
#[derive(Clone, Debug)]
struct History {
    buf: [(f64, u64); HISTORY],
    len: usize,
}

impl History {
    fn new() -> History {
        History {
            buf: [(0.0, 0); HISTORY],
            len: 0,
        }
    }

    fn push(&mut self, time: f64, used: u64) {
        // A clock that went backwards starts the history over rather than mixing two timelines.
        if self.buf[..self.len].last().is_some_and(|&(t, _)| time < t) {
            self.len = 0;
        }
        if self.len == HISTORY {
            self.buf.copy_within(1.., 0);
            self.len -= 1;
        }
        self.buf[self.len] = (time, used);
        self.len += 1;
    }

    /// The mean of the readings of the last `window` seconds; the newest is always among them.
    fn average(&self, now: f64, window: f64) -> u64 {
        let (sum, count) = self.buf[..self.len]
            .iter()
            .filter(|&&(t, _)| now - t < window)
            .fold((0u128, 0u128), |(sum, count), &(_, used)| {
                (sum + u128::from(used), count + 1)
            });
        sum.checked_div(count).unwrap_or(0) as u64
    }
}

/// A command waiting for its turn.
#[derive(Clone, Debug)]
pub struct Waiting {
    pub id: Id,
    /// Who asked: the agent the client runs under, else the client's parent.
    pub session: i32,
    pub label: String,
    pub signature: String,
    /// Wall-clock seconds when it asked.
    pub since: f64,
}

#[derive(Clone, Copy)]
struct Estimate {
    demand: Demand,
    need: u64,
    measuring: bool,
}

#[derive(Clone, Debug)]
struct Footprint {
    time: f64,
    bytes: u64,
    peak: u64,
}

/// A command that has been told to start, until it ends.
#[derive(Clone, Debug)]
pub struct Running {
    pub id: Id,
    pub session: i32,
    pub label: String,
    pub signature: String,
    /// Wall-clock seconds when it was admitted.
    pub since: f64,
    /// Engine seconds when it was admitted.
    pub admitted: f64,
    /// How long it waited, seconds.
    pub waited: f64,
    /// Reservation at admission, also the limit used to identify unexpected growth.
    pub need: u64,
    cpu_need: u32,
    cold: bool,
    exclusive: Option<ExclusiveAdmission>,
    backfill_for: Option<Id>,
    runtime_ms: Option<u64>,
    /// Its process group, once the client has said and the daemon has checked.
    pub pgid: Option<i32>,
    /// When the leader of that group started (microseconds since the epoch): with the pgid,
    /// the group's identity for as long as it is measured.
    pub leader_start_us: u64,
    /// The most memory its group has been seen to use.
    pub peak: u64,
    /// Recent footprint samples, including the last one at or before the window's start.
    footprints: VecDeque<Footprint>,
    cpu: Cpu,
}

#[derive(Clone, Debug)]
struct Cpu {
    last: Option<(f64, u64, bool)>,
    peak: Option<u32>,
    sampled_seconds: f64,
    /// Candidates for the recent maximum, in time order and decreasing demand.
    recent: VecDeque<(f64, u32)>,
}

impl Default for Cpu {
    fn default() -> Self {
        Self {
            last: None,
            peak: None,
            sampled_seconds: 0.0,
            // The live sampler runs once a second. Reserve a full window with room for jitter.
            recent: VecDeque::with_capacity(16),
        }
    }
}

impl Cpu {
    fn observe(&mut self, now: f64, nanos: u64, stopped: bool, machine_cpu: u32) {
        if let Some((then, previous, was_stopped)) = self.last {
            let elapsed = now - then;
            if elapsed <= 0.0 {
                return;
            }
            if !stopped && !was_stopped && nanos >= previous {
                // Age the window only on a running reading: a frozen group keeps its
                // commitment until it has been measured running again.
                while self
                    .recent
                    .front()
                    .is_some_and(|&(end, _)| now - end >= CPU_WINDOW_SECONDS)
                {
                    self.recent.pop_front();
                }
                let used = ((nanos - previous) as f64 / (1e6 * elapsed)).ceil() as u32;
                let used = used.min(machine_cpu);
                self.peak = Some(self.peak.unwrap_or(0).max(used));
                self.sampled_seconds += elapsed;
                // A newer reading stays in the window longer than any smaller or equal one.
                while self.recent.back().is_some_and(|&(_, older)| older <= used) {
                    self.recent.pop_back();
                }
                self.recent.push_back((now, used));
            }
            self.last = Some((now, nanos.max(previous), stopped));
        } else {
            self.last = Some((now, nanos, stopped));
        }
    }
}

fn with_margin(bytes: u64) -> u64 {
    bytes.saturating_add(bytes.div_ceil(4))
}

impl Running {
    pub(crate) fn measured_cpu(&self) -> Option<u32> {
        self.cpu.peak
    }

    fn reservation(&self) -> u64 {
        self.need.max(with_margin(self.peak))
    }

    fn has_plateaued(&self) -> bool {
        self.footprints.len() >= COLD_PLATEAU_SAMPLES
            && self
                .footprints
                .back()
                .zip(self.footprints.front())
                .is_some_and(|(latest, baseline)| {
                    // One twentieth is the maximum 5% increase from the window's starting peak.
                    latest.time - baseline.time >= COLD_PLATEAU_SECONDS
                        && latest.peak - baseline.peak <= baseline.peak / 20
                })
    }

    fn cores(&self, budget: u32) -> u32 {
        if self.cpu.sampled_seconds < CPU_WINDOW_SECONDS {
            return self.cpu_need;
        }
        self.cpu
            .recent
            .front()
            .map_or(0, |&(_, used)| used)
            .clamp(MILLICORES, budget)
    }

    pub(crate) fn limit(&self) -> ReservationLimit {
        if self.cold {
            ReservationLimit::Cold(self.need)
        } else {
            ReservationLimit::Learned(self.need)
        }
    }

    fn expected_end(&self, now: f64) -> Option<f64> {
        let end = self.admitted + self.runtime_ms? as f64 / 1000.0;
        (end > now).then_some(end)
    }
}

/// What `Scheduler::release` took out.
#[derive(Debug)]
pub enum Released {
    Waiting(Waiting),
    Running(Running),
}

#[derive(Clone, Copy, Debug, PartialEq, Eq)]
pub(crate) enum ExclusiveAdmission {
    Alone,
    Reservations,
    TimedOut,
}

impl ExclusiveAdmission {
    pub(crate) fn reason(self) -> &'static str {
        match self {
            Self::Alone => "no other jobs running",
            Self::Reservations => "background and reservations fit under the maximum cap",
            Self::TimedOut => "held the turn for 10 minutes and memory pressure is calm",
        }
    }
}

/// Why a request is still waiting, as of the latest admission decision.
#[derive(Clone, Copy, Debug, Default, PartialEq, Eq)]
pub enum WaitReason {
    /// No sample yet, or a reply from a daemon that does not give reasons.
    #[default]
    Unspecified,
    Tight,
    Frozen,
    Measuring,
    Cap {
        used: u64,
        reserved: u64,
        cap: u64,
    },
    Cpu {
        committed: u32,
        budget: u32,
    },
    Behind,
}

impl std::fmt::Display for WaitReason {
    fn fmt(&self, f: &mut std::fmt::Formatter<'_>) -> std::fmt::Result {
        let gib = |bytes| bytes as f64 / GIB as f64;
        let cores = |millicores| f64::from(millicores) / f64::from(MILLICORES);
        match *self {
            Self::Unspecified => Ok(()),
            Self::Tight => f.write_str("memory is tight"),
            Self::Frozen => f.write_str("a job is frozen"),
            Self::Measuring => f.write_str("first run of this command is still being measured"),
            Self::Behind => f.write_str("behind another request"),
            Self::Cap {
                used,
                reserved,
                cap,
            } => write!(
                f,
                "memory over the cap ({:.1} GB in use + {:.1} GB reserved of a {:.1} GB cap)",
                gib(used),
                gib(reserved),
                gib(cap)
            ),
            Self::Cpu { committed, budget } => write!(
                f,
                "CPU budget ({:.1} cores committed, {:.1} core budget)",
                cores(committed),
                cores(budget)
            ),
        }
    }
}

/// A request waiting for its turn, as it is shown to the world.
#[derive(Default)]
pub struct Place {
    pub id: Id,
    /// Counts from 1 in request order.
    pub position: usize,
    pub need: u64,
    pub reason: WaitReason,
    pub session: i32,
    pub since: f64,
    pub label: String,
}

/// A command that has been told to start, as it is shown to the world.
#[derive(Default)]
pub struct Admitted {
    pub id: Id,
    pub session: i32,
    pub since: f64,
    /// 0 until the client has said which group it runs as.
    pub pgid: i32,
    pub peak: u64,
    /// How long it waited, seconds, and what it was expected to need when it was admitted.
    pub waited: f64,
    pub need: u64,
    pub label: String,
    pub(crate) exclusive: Option<ExclusiveAdmission>,
}

/// Where the queue stands, copied out of the scheduler so that it can be read, shown and
/// worked out from without holding whatever the scheduler is shared behind. Its buffers are
/// reused: refilling it allocates only when the queue has grown.
#[derive(Default)]
pub struct Snapshot {
    pub ram: u64,
    /// 0 before the first reading.
    pub cap: u64,
    pub used: u64,
    /// In request order.
    pub waiting: Vec<Place>,
    pub running: Vec<Admitted>,
}

/// Makes `rows` exactly `len` long, keeping the rows (and what they own) it already has.
fn refit<T: Default>(rows: &mut Vec<T>, len: usize) {
    rows.truncate(len);
    while rows.len() < len {
        rows.push(T::default());
    }
}

pub struct Scheduler {
    waiting: Vec<Waiting>,
    running: Vec<Running>,
    history: History,
    background: History,
    /// None until the first reading says how much RAM there is.
    cap: Option<u64>,
    /// The latest reading trips the engine's compressor freeze arm.
    freeze_pressure: bool,
    ram: u64,
    /// Background memory averaged over the last `USED_WINDOW`.
    used: u64,
    cpu_budget: u32,
    /// Engine time of the latest admission decision, and the last sample's stop conditions.
    now: f64,
    tight: bool,
    frozen: bool,
    /// The last time memory was tight or the cap was probed: what the 60 s count runs from.
    quiet_since: f64,
    next_id: Id,
    machine_cpu: u32,
    /// The head holding the turn and when it took it, even if the cap is probed up.
    exclusive_turn: Option<(Id, f64)>,
    sample_time: f64,
    measured_background: u64,
}

impl Default for Scheduler {
    fn default() -> Scheduler {
        Scheduler::new()
    }
}

impl Scheduler {
    pub fn new() -> Scheduler {
        Self::with_cpu_headroom(DEFAULT_CPU_HEADROOM_PERCENT)
    }

    pub(crate) fn with_cpu_headroom(percent: u32) -> Scheduler {
        let cores = std::thread::available_parallelism().map_or(1, |n| n.get() as u32);
        let budget = cores * 100u32.saturating_sub(percent) * (MILLICORES / 100);
        Self::with_cpu_capacity(budget.max(MILLICORES), cores * MILLICORES)
    }

    #[cfg(test)]
    pub(crate) fn with_cpu_budget(cpu_budget: u32) -> Scheduler {
        Self::with_cpu_capacity(cpu_budget, cpu_budget)
    }

    fn with_cpu_capacity(cpu_budget: u32, machine_cpu: u32) -> Scheduler {
        Scheduler {
            waiting: Vec::new(),
            running: Vec::new(),
            history: History::new(),
            background: History::new(),
            cap: None,
            freeze_pressure: false,
            ram: 0,
            used: 0,
            cpu_budget,
            now: 0.0,
            tight: false,
            frozen: false,
            quiet_since: 0.0,
            next_id: 0,
            machine_cpu,
            exclusive_turn: None,
            sample_time: 0.0,
            measured_background: 0,
        }
    }

    /// The memory the commands may use together, as learned; None before the first reading.
    pub fn cap(&self) -> Option<u64> {
        self.cap
    }

    /// Memory in use over the last 5 s, as of the last step.
    pub fn used(&self) -> u64 {
        self.used
    }

    pub fn ram(&self) -> u64 {
        self.ram
    }

    pub fn waiting(&self) -> &[Waiting] {
        &self.waiting
    }

    pub fn running(&self) -> &[Running] {
        &self.running
    }

    /// Order by the first connection attempt, with receipt order breaking ties.
    pub fn enqueue(&mut self, session: i32, label: String, signature: String, wall: f64) -> Id {
        self.next_id += 1;
        let at = self.waiting.partition_point(|w| w.since <= wall);
        self.waiting.insert(
            at,
            Waiting {
                id: self.next_id,
                session,
                label,
                signature,
                since: wall,
            },
        );
        self.next_id
    }

    pub(crate) fn sample_time(&self) -> f64 {
        self.sample_time
    }

    /// The request ends: it stops waiting, or it stops running. None if it is not known.
    pub fn release(&mut self, id: Id) -> Option<Released> {
        if let Some(at) = self.waiting.iter().position(|w| w.id == id) {
            if self.exclusive_turn.is_some_and(|(head, _)| head == id) {
                self.exclusive_turn = None;
            }
            return Some(Released::Waiting(self.waiting.remove(at)));
        }
        let at = self.running.iter().position(|r| r.id == id)?;
        Some(Released::Running(self.running.remove(at)))
    }

    /// The admitted command runs as this process group, whose leader started at
    /// `leader_start_us`. False if `id` is not admitted.
    pub fn started(&mut self, id: Id, pgid: i32, leader_start_us: u64) -> bool {
        let Some(job) = self.running.iter_mut().find(|r| r.id == id) else {
            return false;
        };
        job.pgid = Some(pgid);
        job.leader_start_us = leader_start_us;
        true
    }

    /// The admitted command's group is using `bytes` at engine time `now`, the sample's own
    /// time (the scheduler's clock has not moved to it yet when the broker measures).
    pub fn observe(&mut self, id: Id, bytes: u64, now: f64) {
        if bytes == 0 {
            return;
        }
        if let Some(job) = self.running.iter_mut().find(|r| r.id == id) {
            while job
                .footprints
                .get(1)
                .is_some_and(|sample| now - sample.time >= COLD_PLATEAU_SECONDS)
            {
                job.footprints.pop_front();
            }
            job.peak = job.peak.max(bytes);
            job.footprints.push_back(Footprint {
                time: now,
                bytes,
                peak: job.peak,
            });
        }
    }

    /// The group was stopped when measured (frozen by the brake, or suspended): a stopped
    /// group's flat footprint says nothing about whether it has finished growing, so its
    /// plateau window starts again from this reading. The reading itself stays: it is this
    /// tick's attributed memory.
    pub(crate) fn restart_plateau(&mut self, id: Id) {
        if let Some(job) = self.running.iter_mut().find(|r| r.id == id) {
            let keep = job.footprints.len().saturating_sub(1);
            job.footprints.drain(..keep);
        }
    }

    pub(crate) fn observe_cpu(&mut self, id: Id, nanos: u64, stopped: bool, now: f64) {
        if let Some(job) = self.running.iter_mut().find(|r| r.id == id) {
            job.cpu.observe(now, nanos, stopped, self.machine_cpu);
        }
    }

    fn estimate(&self, job: &Waiting, learned: &dyn Fn(&str) -> Demand) -> Estimate {
        let demand = learned(&job.signature);
        let mut need = demand
            .bytes
            .map_or_else(|| (4 * GIB).max(self.cap.unwrap_or(0) / 8), with_margin);
        let mut measuring = false;
        if !demand.has_profile() {
            let mut plateaued = false;
            let mut peak = 0;
            for sibling in self.running.iter().filter(|r| r.signature == job.signature) {
                measuring = true;
                peak = peak.max(sibling.peak);
                plateaued |= sibling.has_plateaued();
            }
            if plateaued {
                measuring = false;
                need = need.max(with_margin(peak));
            }
        }
        Estimate {
            demand,
            need,
            measuring,
        }
    }

    /// Siblings waiting for a profile or a peak plateau yield their turn.
    fn next_for_admission(&self, learned: &dyn Fn(&str) -> Demand) -> Option<(usize, Estimate)> {
        self.waiting.iter().enumerate().find_map(|(at, job)| {
            let estimate = self.estimate(job, learned);
            (!estimate.measuring).then_some((at, estimate))
        })
    }

    fn cpu_need(&self, demand: Demand) -> u32 {
        demand
            .millicores
            .map_or(2 * MILLICORES, |cpu| cpu.max(MILLICORES))
            .min(self.cpu_budget)
    }

    fn cpu_committed(&self) -> u32 {
        self.running
            .iter()
            .fold(0, |sum, job| sum.saturating_add(job.cores(self.cpu_budget)))
    }

    fn reserved(&self) -> u64 {
        self.running
            .iter()
            .fold(0, |sum, job| sum.saturating_add(job.reservation()))
    }

    /// Unknown and overdue jobs promise no release. A known completion only creates a
    /// backfill window if the head can start with every still-committed job present.
    fn earliest_start(&self, head_need: u64) -> Option<f64> {
        let mut completions: Vec<_> = self
            .running
            .iter()
            .filter_map(|job| job.expected_end(self.now).map(|end| (end, job)))
            .collect();
        completions.sort_unstable_by(|a, b| a.0.total_cmp(&b.0));

        let mut memory = self.reserved();
        let mut cpu = self.cpu_committed();
        let mut left = self.running.len();
        for (end, job) in completions {
            left -= 1;
            memory = memory.saturating_sub(job.reservation());
            cpu = cpu.saturating_sub(job.cores(self.cpu_budget));
            if left == 0 || self.fits(head_need, memory, cpu) {
                return Some(end);
            }
        }
        None
    }

    fn exclusive(&self, need: u64) -> bool {
        need >= self.cap.unwrap_or(u64::MAX)
    }

    fn exclusive_admission(
        &self,
        need: u64,
        reserved: u64,
        since: f64,
    ) -> Option<ExclusiveAdmission> {
        let max = percent(self.ram, MAX_PERCENT);
        if self.running.is_empty() {
            Some(ExclusiveAdmission::Alone)
        } else if self.now - self.sample_time > FRESH_SAMPLE_SECONDS {
            None
        } else if !self.freeze_pressure
            && self
                .measured_background
                .saturating_add(reserved)
                .saturating_add(need)
                < max
        {
            // A cap that fell back under freeze-tier pressure must not start a waiting job
            // beside other jobs until the pressure clears.
            Some(ExclusiveAdmission::Reservations)
        } else if self.now - since >= EXCLUSIVE_WAIT_SECONDS {
            Some(ExclusiveAdmission::TimedOut)
        } else {
            None
        }
    }

    /// Without a completion estimate, assume every incumbent that can coexist with
    /// the head might still be running when it starts. Backfills must also fit now.
    fn fits_spare(&self, head_id: Id, head_need: u64, candidate: Estimate) -> bool {
        let mut memory = head_need.saturating_add(candidate.need);
        let mut cpu = self.cpu_need(candidate.demand);
        for job in self.running.iter().filter(|job| {
            job.backfill_for == Some(head_id)
                || self.fits(head_need, job.reservation(), job.cores(self.cpu_budget))
        }) {
            memory = memory.saturating_add(job.reservation());
            cpu = cpu.saturating_add(job.cores(self.cpu_budget));
        }
        self.fits(memory, 0, cpu)
    }

    fn fits(&self, need: u64, reserved: u64, committed: u32) -> bool {
        committed < self.cpu_budget && self.fits_memory(need, reserved)
    }

    fn fits_memory(&self, need: u64, reserved: u64) -> bool {
        self.used.saturating_add(reserved).saturating_add(need) < self.cap.unwrap_or(0)
    }

    /// Copies the queue in reservation order and each admitted job's current reservation.
    pub fn snapshot(&self, learned: &dyn Fn(&str) -> Demand, out: &mut Snapshot) {
        out.ram = self.ram;
        out.cap = self.cap.unwrap_or(0);
        out.used = self.used;
        refit(&mut out.waiting, self.waiting.len());
        let reserved = self.reserved();
        let committed = self.cpu_committed();
        let mut head_seen = false;
        for (index, (w, place)) in self.waiting.iter().zip(&mut out.waiting).enumerate() {
            let position = index + 1;
            place.id = w.id;
            place.position = position;
            let estimate = self.estimate(w, learned);
            place.need = estimate.need;
            let behind = head_seen;
            head_seen |= !estimate.measuring;
            place.reason = if estimate.measuring {
                WaitReason::Measuring
            } else if behind {
                WaitReason::Behind
            } else if self.tight {
                WaitReason::Tight
            } else if self.frozen {
                WaitReason::Frozen
            } else if !self.exclusive(estimate.need)
                && self.exclusive_turn.is_none()
                && committed >= self.cpu_budget
            {
                WaitReason::Cpu {
                    committed,
                    budget: self.cpu_budget,
                }
            } else if self.cap.is_some() && !self.fits_memory(place.need, reserved) {
                WaitReason::Cap {
                    used: self.used,
                    reserved,
                    cap: out.cap,
                }
            } else {
                WaitReason::Unspecified
            };
            place.session = w.session;
            place.since = w.since;
            place.label.clone_from(&w.label);
        }

        refit(&mut out.running, self.running.len());
        for (row, r) in out.running.iter_mut().zip(&self.running) {
            row.id = r.id;
            row.session = r.session;
            row.since = r.since;
            row.pgid = r.pgid.unwrap_or(0);
            row.peak = r.peak;
            row.waited = r.waited;
            row.need = r.reservation();
            row.label.clone_from(&r.label);
            row.exclusive = r.exclusive;
        }
    }

    /// Takes one reading and admits every request that fits. Clears `out`, then appends the
    /// admitted ids in order. Allocation-free when nobody is waiting.
    pub fn step(&mut self, s: &Sample, learned: &dyn Fn(&str) -> Demand, out: &mut Vec<Id>) {
        self.step_at(s, s.time, &Policy::default(), learned, out);
    }

    pub(crate) fn step_at(
        &mut self,
        s: &Sample,
        decision_time: f64,
        policy: &Policy,
        learned: &dyn Fn(&str) -> Demand,
        out: &mut Vec<Id>,
    ) {
        let now = s.time;
        self.sample_time = now;
        let measured_memory = s.used();
        self.ram = s.ram;
        self.tight = s.tight;
        self.frozen = s.frozen;
        self.freeze_pressure = policy.freeze_occupancy_tripped(s.tight, s.occupancy);
        if self.cap.is_none() {
            self.cap = Some(percent(s.ram, START_PERCENT));
            self.quiet_since = now;
        }
        let attributable = self
            .running
            .iter()
            .filter_map(|job| job.footprints.back())
            .filter(|sample| sample.time == now)
            .fold(0u64, |sum, sample| sum.saturating_add(sample.bytes));
        self.measured_background = measured_memory.saturating_sub(attributable);
        self.history.push(now, measured_memory);
        self.background.push(now, self.measured_background);
        self.used = self.background.average(now, USED_WINDOW);
        if s.tight {
            // Tight readings, including freeze-tier pressure, replace any probed cap.
            self.cap = Some(self.history.average(now, CAP_WINDOW));
            self.quiet_since = now;
        }

        // Held back by the cap and nothing else, for a minute of quiet: try a little more.
        if !s.tight
            && !s.frozen
            && !self.freeze_pressure
            && !self.running.is_empty()
            && now - self.quiet_since >= PROBE_AFTER
            && self
                .next_for_admission(learned)
                .is_some_and(|(_, estimate)| {
                    self.cpu_committed() < self.cpu_budget
                        && !self.fits_memory(estimate.need, self.reserved())
                })
        {
            let max = percent(s.ram, PROBE_MAX_PERCENT);
            if let Some(cap) = self.cap.as_mut().filter(|cap| **cap < max) {
                *cap = (*cap + percent(s.ram, PROBE_PERCENT)).min(max);
            }
            self.quiet_since = now;
        }
        self.admit_now(decision_time, s.wall, learned, out);
    }

    /// Uses the latest sample's memory and stop conditions, without changing history or the
    /// learned cap. Before the first sample, nothing starts. Shares the admission rule with
    /// `step`; `out` is cleared even when nothing can start.
    pub fn admit_now(
        &mut self,
        now: f64,
        wall: f64,
        learned: &dyn Fn(&str) -> Demand,
        out: &mut Vec<Id>,
    ) {
        out.clear();
        self.now = now;
        if self.cap.is_none() || self.running.iter().any(|job| job.exclusive.is_some()) {
            return;
        }
        let mut reserved = self.reserved();
        let mut committed = self.cpu_committed();
        while let Some((head_at, estimate)) = self.next_for_admission(learned) {
            let head = &self.waiting[head_at];
            self.exclusive_turn = self
                .exclusive_turn
                .filter(|(id, _)| *id == head.id)
                .or_else(|| self.exclusive(estimate.need).then_some((head.id, now)));
            if self.tight || self.frozen {
                break;
            }
            let exclusive = match self.exclusive_turn {
                Some((_, since)) => {
                    let Some(rule) = self.exclusive_admission(estimate.need, reserved, since)
                    else {
                        break;
                    };
                    Some(rule)
                }
                None => None,
            };
            let fits = |need| self.fits(need, reserved, committed);
            let (at, estimate, backfill_for) =
                if self.running.is_empty() || exclusive.is_some() || fits(estimate.need) {
                    (head_at, estimate, None)
                } else {
                    let start = self.earliest_start(estimate.need);
                    let Some(backfill) = self
                        .waiting
                        .iter()
                        .enumerate()
                        .skip(head_at + 1)
                        .find_map(|(at, job)| {
                            let candidate = self.estimate(job, learned);
                            if candidate.measuring
                                || self.exclusive(candidate.need)
                                || !fits(candidate.need)
                            {
                                return None;
                            }
                            let can_backfill = match start {
                                Some(start) => candidate.demand.runtime_ms.is_some_and(|ms| {
                                    let runtime = ms as f64 / 1000.0;
                                    now + runtime + (runtime * 0.25).max(0.25) <= start
                                }),
                                None => self.fits_spare(head.id, estimate.need, candidate),
                            };
                            can_backfill.then_some((at, candidate, Some(head.id)))
                        })
                    else {
                        break;
                    };
                    backfill
                };
            let Estimate { need, demand, .. } = estimate;
            let cpu_need = self.cpu_need(demand);
            let w = self.waiting.remove(at);
            self.running.push(Running {
                id: w.id,
                session: w.session,
                label: w.label,
                signature: w.signature,
                since: wall,
                admitted: now,
                waited: (wall - w.since).max(0.0),
                need,
                cpu_need,
                cold: demand.bytes.is_none(),
                exclusive,
                backfill_for,
                runtime_ms: demand.runtime_ms,
                pgid: None,
                leader_start_us: 0,
                peak: 0,
                // Room for the baseline and a full window of one-second live samples.
                footprints: VecDeque::with_capacity(32),
                cpu: Cpu::default(),
            });
            out.push(w.id);
            reserved = reserved.saturating_add(need);
            committed = committed.saturating_add(cpu_need);
            if exclusive.is_some() {
                self.exclusive_turn = None;
                break;
            }
        }
    }
}

#[cfg(test)]
mod tests {
    use super::*;

    /// 100 GiB, so that a level of `100 - n` is `n` GiB in use and the initial cap is 70 GiB.
    const RAM: u64 = 100 * GIB;

    /// A reading at `level` (percent available), tight as the engine counts it (level 40 or
    /// less).
    fn at(time: f64, level: i32) -> Sample {
        Sample {
            time,
            wall: 1.7e9 + time,
            ram: RAM,
            level,
            occupancy: 0.0,
            tight: level <= 40,
            frozen: false,
        }
    }

    /// 20 GiB in use.
    fn calm(time: f64) -> Sample {
        at(time, 80)
    }

    // Isolate memory and fairness in the older scenarios by assuming one core per job.
    fn memory(bytes: Option<u64>) -> Demand {
        Demand {
            bytes: bytes.or(Some(4 * GIB / 5)),
            millicores: Some(MILLICORES),
            runtime_ms: None,
        }
    }

    fn unknown(_: &str) -> Demand {
        memory(None)
    }

    /// Asks for a command named `name` for `session`; its signature is its name.
    fn ask(q: &mut Scheduler, session: i32, name: &str) -> Id {
        q.enqueue(session, name.into(), name.into(), 0.0)
    }

    fn names(q: &Scheduler) -> Vec<&str> {
        q.running().iter().map(|r| r.label.as_str()).collect()
    }

    /// Existing single-admission scenarios also check that no extra request starts.
    fn step(q: &mut Scheduler, s: &Sample, learned: &dyn Fn(&str) -> Demand) -> Option<Id> {
        let mut out = Vec::new();
        q.step(s, learned, &mut out);
        assert!(out.len() <= 1, "unexpected extra admissions: {out:?}");
        out.first().copied()
    }

    /// Readings every second from `from` to `to` inclusive at `level`; the last result.
    fn run(
        q: &mut Scheduler,
        from: u32,
        to: u32,
        level: i32,
        learned: &dyn Fn(&str) -> Demand,
    ) -> Option<Id> {
        (from..=to)
            .map(|t| step(q, &at(f64::from(t), level), learned))
            .last()
            .flatten()
    }

    #[test]
    fn a_command_is_admitted_when_there_is_room() {
        let mut q = Scheduler::with_cpu_budget(24 * MILLICORES);
        let a = ask(&mut q, 1, "a");
        assert_eq!(step(&mut q, &calm(0.0), &unknown), Some(a));
        assert_eq!(names(&q), ["a"]);
        assert!(q.waiting().is_empty());
        assert_eq!(q.cap(), Some(70 * GIB), "the cap starts at 70% of RAM");
        assert_eq!(
            q.running()[0].need,
            GIB,
            "and a stranger is expected to need 1 GiB"
        );
    }

    #[test]
    fn an_unmeasured_idle_watch_leaves_room_for_new_signatures() {
        let mut q = Scheduler::with_cpu_budget(4 * MILLICORES);
        let mut out = Vec::new();
        let unknown = |_: &str| Demand::default();
        let watch = ask(&mut q, 1, "watch");
        q.step(&calm(0.0), &unknown, &mut out);
        assert_eq!(out, [watch]);
        q.observe_cpu(watch, 0, false, 1.0);
        q.observe_cpu(watch, 0, false, 2.0);
        let short = ask(&mut q, 2, "short-a");
        let other = ask(&mut q, 3, "short-b");
        let ready = |s: &str| {
            if s == "watch" {
                Demand::default()
            } else {
                Demand {
                    bytes: Some(GIB),
                    millicores: Some(2000),
                    runtime_ms: None,
                }
            }
        };
        q.step(&calm(2.0), &ready, &mut out);
        assert_eq!(out, [short], "the idle watch keeps its bounded reservation");
        q.release(short);
        q.step(&calm(3.0), &ready, &mut out);
        assert_eq!(out, [other]);
    }

    #[test]
    fn the_oldest_waiter_is_not_overtaken_without_a_completion_estimate() {
        let mut q = Scheduler::with_cpu_budget(3 * MILLICORES);
        let mut out = Vec::new();
        let learned = |s: &str| Demand {
            bytes: Some(GIB),
            millicores: Some(if s == "waiter" { 3 } else { 1 } * MILLICORES),
            runtime_ms: None,
        };
        ask(&mut q, 1, "watch");
        let first = ask(&mut q, 2, "first");
        let second = ask(&mut q, 3, "second");
        q.step(&calm(0.0), &learned, &mut out);
        assert_eq!(out.len(), 3);
        let waiter = ask(&mut q, 1, "waiter");
        q.step(&calm(31.0), &learned, &mut out);
        let newcomer = ask(&mut q, 4, "newcomer");
        q.admit_now(31.0, 31.0, &learned, &mut out);
        assert!(out.is_empty());
        q.release(first).unwrap();
        q.step(&calm(32.0), &learned, &mut out);
        assert_eq!(out, [waiter]);
        assert_eq!(q.waiting()[0].id, newcomer);
        q.release(second).unwrap();
        assert_eq!(step(&mut q, &calm(33.0), &learned), None);
    }

    #[test]
    fn review4_peak_cpu_job_starts_beside_a_long_running_watch() {
        let mut q = Scheduler::with_cpu_capacity(24000, 30000);
        let learned = |name: &str| Demand {
            bytes: Some(GIB),
            millicores: Some(match name {
                "watch" => 2000,
                "build" => 30000,
                _ => 1000,
            }),
            runtime_ms: None,
        };
        let watch = ask(&mut q, 1, "watch");
        assert_eq!(step(&mut q, &calm(0.0), &learned), Some(watch));
        assert!(q.started(watch, 123, 7));
        let build = ask(&mut q, 2, "build");
        let small = ask(&mut q, 3, "small");
        assert_eq!(
            step(&mut q, &calm(3600.0), &learned),
            Some(build),
            "two committed cores leave the 30-core request free to start"
        );
        assert_eq!(q.cpu_committed(), 26000);
        assert_eq!(q.waiting()[0].id, small);
        q.release(build);
        assert_eq!(step(&mut q, &calm(3601.0), &learned), Some(small));
        assert_eq!(names(&q), ["watch", "small"]);
    }

    #[test]
    fn oversized_memory_head_times_out_on_a_fresh_calm_reading_even_without_fit() {
        let learned = |name: &str| {
            memory(Some(match name {
                "head" => 58 * GIB,
                "watch" => 20 * GIB,
                _ => GIB,
            }))
        };
        let mut q = Scheduler::with_cpu_capacity(24000, 30000);
        let watch = ask(&mut q, 1, "watch");
        assert_eq!(step(&mut q, &calm(0.0), &learned), Some(watch));
        assert!(q.started(watch, 123, 7));
        let head = ask(&mut q, 2, "head");
        let small = ask(&mut q, 3, "small");
        q.observe(watch, 2 * GIB, 1.0);
        assert_eq!(
            step(&mut q, &at(1.0, 88), &learned),
            None,
            "10 + 25 + 72.5 exceeds 90"
        );
        assert_eq!(
            q.waiting().iter().map(|w| w.id).collect::<Vec<_>>(),
            [head, small]
        );
        q.observe(watch, 2 * GIB, 600.0);
        assert_eq!(
            step(&mut q, &at(600.0, 88), &learned),
            None,
            "one second before the timeout"
        );
        q.admit_now(603.0, 603.0, &learned, &mut Vec::new());
        assert_eq!(
            q.waiting()[0].id,
            head,
            "a stale reading cannot use the timeout"
        );
        assert_eq!(
            step(&mut q, &at(604.0, 35), &learned),
            None,
            "tight memory still blocks admission"
        );
        let frozen = Sample {
            frozen: true,
            ..calm(605.0)
        };
        assert_eq!(
            step(&mut q, &frozen, &learned),
            None,
            "a frozen job still blocks admission"
        );
        q.observe(watch, 2 * GIB, 606.0);
        assert_eq!(
            step(&mut q, &calm(606.0), &learned),
            Some(head),
            "the first fresh calm reading admits the head even though 20 + 72.5 exceeds 90"
        );
        assert_eq!(q.running()[1].exclusive, Some(ExclusiveAdmission::TimedOut));
        assert_eq!(
            step(&mut q, &calm(607.0), &learned),
            None,
            "the younger request waits through the exclusive run"
        );
        q.release(head);
        assert_eq!(
            step(&mut q, &at(612.0, 90), &learned),
            Some(small),
            "the younger request starts once it fits the cap learned under pressure"
        );
    }

    #[test]
    fn review4_oversized_memory_head_can_start_early_when_reservations_fit() {
        let learned = |name: &str| memory(Some(if name == "head" { 58 } else { 2 } * GIB));
        let mut q = Scheduler::with_cpu_capacity(24000, 30000);
        let watch = ask(&mut q, 1, "watch");
        assert_eq!(step(&mut q, &calm(0.0), &learned), Some(watch));
        assert!(q.started(watch, 123, 7));
        let head = ask(&mut q, 2, "head");
        q.observe(watch, 2 * GIB, 1.0);
        assert_eq!(
            step(&mut q, &at(1.0, 85), &learned),
            Some(head),
            "13 + 2.5 + 72.5 fits under 90"
        );
        assert_eq!(
            q.running()[1].exclusive,
            Some(ExclusiveAdmission::Reservations)
        );
    }

    #[test]
    fn review4_cold_jobs_start_together_and_reserve_their_observed_peak() {
        let mut q = Scheduler::with_cpu_budget(24000);
        let cold = |_: &str| Demand::default();
        let first = ask(&mut q, 1, "cold-a");
        let second = ask(&mut q, 2, "cold-b");
        let mut out = Vec::new();
        q.step(&calm(0.0), &cold, &mut out);
        assert_eq!(out, [first, second], "both cold defaults fit the cap");
        let default = 70 * GIB / 8;
        assert_eq!(q.reserved(), 2 * default);
        q.observe(first, 8 * GIB, 1.0);
        assert_eq!(q.reserved(), 10 * GIB + default);
    }

    #[test]
    fn a_cold_sibling_waits_through_two_minutes_of_peak_growth() {
        let mut q = Scheduler::with_cpu_budget(24_000);
        let learned = |_: &str| Demand::default();
        let first = ask(&mut q, 1, "build");
        assert_eq!(step(&mut q, &calm(0.0), &learned), Some(first));
        let sibling = ask(&mut q, 2, "build");

        for t in 1..=120_u32 {
            let time = f64::from(t);
            q.observe(first, u64::from(t) * (GIB / 4), time);
            assert_eq!(step(&mut q, &at(time, 60), &learned), None, "at {t} s");
        }
        let mut view = Snapshot::default();
        q.snapshot(&learned, &mut view);
        assert_eq!(view.waiting[0].id, sibling);
        assert_eq!(view.waiting[0].reason, WaitReason::Measuring);
    }

    #[test]
    fn a_cold_sibling_starts_after_thirty_seconds_of_stable_peak() {
        let mut q = Scheduler::with_cpu_budget(24_000);
        let learned = |_: &str| Demand::default();
        let sample = |time| Sample {
            ram: 2 * RAM,
            ..calm(time)
        };
        let first = ask(&mut q, 1, "watch");
        assert_eq!(step(&mut q, &sample(0.0), &learned), Some(first));
        let sibling = ask(&mut q, 2, "watch");
        let peak = 16 * GIB;

        for t in 1..40 {
            let time = f64::from(t);
            q.observe(first, if t == 10 { peak } else { 2 * GIB }, time);
            assert_eq!(step(&mut q, &sample(time), &learned), None, "at {t} s");
        }

        let mut out = Vec::new();
        q.admit_now(40.0, 40.0, &learned, &mut out);
        assert!(
            out.is_empty(),
            "socket time alone does not complete measurement"
        );

        q.observe(first, 2 * GIB, 40.0);
        assert_eq!(
            step(&mut q, &sample(40.0), &learned),
            Some(sibling),
            "the first run's peak has been stable for 30 s"
        );
        assert_eq!(q.running()[1].need, 20 * GIB, "125% of the 16 GiB peak");

        let third = ask(&mut q, 3, "watch");
        q.observe(first, peak + peak / 20, 41.0);
        q.observe(sibling, 18 * GIB, 41.0);
        assert_eq!(step(&mut q, &sample(41.0), &learned), Some(third));
        assert_eq!(q.running()[1].reservation(), 45 * GIB / 2);
        assert_eq!(
            q.running()[2].need,
            45 * GIB / 2,
            "125% of the largest peak, even when it belongs to a newer sibling"
        );
    }

    /// A frozen instance's flat footprint is not a plateau: after the brake resumes it, the
    /// sibling waits for 30 s of running readings.
    #[test]
    fn a_frozen_instance_does_not_release_its_cold_sibling() {
        let mut q = Scheduler::with_cpu_budget(24_000);
        let learned = |_: &str| Demand::default();
        let sample = |time| Sample {
            ram: 2 * RAM,
            ..calm(time)
        };
        let first = ask(&mut q, 1, "huge");
        assert_eq!(step(&mut q, &sample(0.0), &learned), Some(first));
        let sibling = ask(&mut q, 2, "huge");

        for t in 1..=10_u32 {
            q.observe(first, u64::from(t) * 2 * GIB, f64::from(t));
            assert_eq!(step(&mut q, &sample(f64::from(t)), &learned), None);
        }
        // Frozen at 20 GiB for 40 s: every reading is flat, and stopped.
        for t in 11..=50_u32 {
            q.observe(first, 20 * GIB, f64::from(t));
            q.restart_plateau(first);
            assert_eq!(
                step(&mut q, &sample(f64::from(t)), &learned),
                None,
                "frozen at {t} s"
            );
        }
        // Resumed and flat: 30 s of running readings are needed.
        for t in 51..80_u32 {
            q.observe(first, 20 * GIB, f64::from(t));
            assert_eq!(
                step(&mut q, &sample(f64::from(t)), &learned),
                None,
                "resumed for {} s",
                t - 50
            );
        }
        q.observe(first, 20 * GIB, 80.0);
        assert_eq!(step(&mut q, &sample(80.0), &learned), Some(sibling));
    }

    #[test]
    fn backfill_skips_a_cold_sibling_and_starts_another_signature() {
        let mut q = Scheduler::with_cpu_budget(24_000);
        let learned = |signature: &str| match signature {
            "incumbent" => memory(Some(21 * GIB)),
            "head" => memory(Some(24 * GIB)),
            _ => Demand::default(),
        };
        let first = ask(&mut q, 1, "cold-a");
        let incumbent = ask(&mut q, 2, "incumbent");
        let mut out = Vec::new();
        q.step(&calm(0.0), &learned, &mut out);
        assert_eq!(out, [first, incumbent]);

        let head = ask(&mut q, 3, "head");
        let sibling = ask(&mut q, 1, "cold-a");
        let other = ask(&mut q, 4, "cold-b");
        // Either cold request fits now and beside the head after the incumbent exits.
        q.step(&calm(1.0), &learned, &mut out);
        assert_eq!(out, [other]);
        assert_eq!(q.waiting()[0].id, head);
        assert_eq!(q.waiting()[1].id, sibling);
    }

    #[test]
    fn review4_cpu_learning_uses_the_full_interval_and_clamps_to_the_machine() {
        let mut q = Scheduler::with_cpu_capacity(24000, 30000);
        let build = ask(&mut q, 1, "build");
        assert_eq!(step(&mut q, &calm(0.0), &unknown), Some(build));
        q.observe_cpu(build, 0, false, 1.0);
        q.observe_cpu(build, 48_000_000_000, false, 7.0);
        assert_eq!(
            q.running()[0].measured_cpu(),
            Some(8000),
            "48 CPU-seconds over six seconds"
        );
        q.observe_cpu(build, 88_000_000_000, false, 8.0);
        assert_eq!(
            q.running()[0].measured_cpu(),
            Some(30000),
            "the peak cannot exceed 30 cores"
        );
    }

    #[test]
    fn an_idle_watch_releases_cpu_budget_after_its_burst_ages_out() {
        let mut q = Scheduler::with_cpu_capacity(24 * MILLICORES, 30 * MILLICORES);
        let demand = |_: &str| Demand::default();
        let watch = ask(&mut q, 1, "watch");
        assert_eq!(step(&mut q, &calm(0.0), &demand), Some(watch));
        for t in 0..10 {
            q.observe_cpu(watch, 0, false, f64::from(t));
        }
        assert_eq!(q.cpu_committed(), 2 * MILLICORES);

        let build = ask(&mut q, 2, "build");
        for t in 10..20 {
            q.observe_cpu(watch, 25_000_000_000, false, f64::from(t));
            assert_eq!(step(&mut q, &calm(f64::from(t)), &demand), None);
        }
        let mut status = Snapshot::default();
        q.snapshot(&demand, &mut status);
        assert_eq!(
            status.waiting[0].reason,
            WaitReason::Cpu {
                committed: 24 * MILLICORES,
                budget: 24 * MILLICORES,
            }
        );

        q.observe_cpu(watch, 25_000_000_000, false, 20.0);
        assert_eq!(
            step(&mut q, &calm(20.0), &demand),
            Some(build),
            "an idle watch releases the CPU budget while it is still running"
        );
        assert_eq!(q.cpu_committed(), 3 * MILLICORES);
        assert_eq!(q.running()[0].measured_cpu(), Some(25 * MILLICORES));
    }

    #[test]
    fn a_frozen_build_keeps_its_cpu_commitment_until_it_runs_again() {
        let mut q = Scheduler::with_cpu_capacity(24 * MILLICORES, 30 * MILLICORES);
        let demand = |_: &str| Demand::default();
        let build = ask(&mut q, 1, "build");
        assert_eq!(step(&mut q, &calm(0.0), &demand), Some(build));
        for t in 0..=11u32 {
            q.observe_cpu(build, u64::from(t) * 25_000_000_000, false, f64::from(t));
        }
        assert_eq!(q.cpu_committed(), 24 * MILLICORES);

        let stopped_at = 11 * 25_000_000_000;
        for t in 12..=30u32 {
            q.observe_cpu(build, stopped_at, true, f64::from(t));
            assert_eq!(q.cpu_committed(), 24 * MILLICORES, "frozen at {t} s");
        }
        // The thaw tick's interval spans the stop, so it cannot measure demand.
        q.observe_cpu(build, stopped_at, false, 31.0);
        assert_eq!(
            q.cpu_committed(),
            24 * MILLICORES,
            "resumed, not yet measured"
        );
        q.observe_cpu(build, stopped_at + 25_000_000_000, false, 32.0);
        assert_eq!(q.cpu_committed(), 24 * MILLICORES, "measured busy again");
    }

    #[test]
    fn cpu_learning_keeps_an_eight_core_burst_after_eight_idle_seconds() {
        let mut q = Scheduler::with_cpu_budget(12 * MILLICORES);
        let mut out = Vec::new();
        let learned = |_: &str| memory(None);
        let first = ask(&mut q, 1, "burst");
        q.step(&calm(0.0), &learned, &mut out);
        for i in 0..=10 {
            q.observe_cpu(
                first,
                (i as u64).saturating_sub(8) * 8_000_000_000,
                false,
                f64::from(i),
            );
        }
        let mut sizes = crate::sizes::Sizes::default();
        sizes.record_usage("burst", GIB, q.running()[0].measured_cpu(), None);
        assert_eq!(sizes.demand("burst").millicores, Some(8000));
        let second = ask(&mut q, 2, "burst");
        let third = ask(&mut q, 3, "burst");
        q.step(&calm(10.0), &|s| sizes.demand(s), &mut out);
        assert_eq!(
            out,
            [second],
            "another job can start while eight cores are committed"
        );
        assert_eq!(
            q.waiting()[0].id,
            third,
            "the two bursts now commit sixteen cores"
        );
        q.release(first).unwrap();
        q.step(&calm(11.0), &|s| sizes.demand(s), &mut out);
        assert_eq!(out, [third]);
    }

    #[test]
    fn cpu_heavy_jobs_wait_and_start_in_arrival_order_as_cores_free_up() {
        let mut q = Scheduler::with_cpu_budget(24 * MILLICORES);
        let mut sizes = crate::sizes::Sizes::default();
        sizes.record_usage("test", GIB, Some(12 * MILLICORES), None);
        let learned = |signature: &str| sizes.demand(signature);
        let a1 = ask(&mut q, 1, "test");
        let a2 = ask(&mut q, 1, "test");
        let b1 = ask(&mut q, 2, "test");
        let b2 = ask(&mut q, 2, "test");
        let mut out = Vec::new();
        q.step(&calm(0.0), &learned, &mut out);
        assert_eq!(out, [a1, a2]);

        let mut view = Snapshot::default();
        q.snapshot(&learned, &mut view);
        assert_eq!((view.waiting[0].id, view.waiting[1].id), (b1, b2));
        assert_eq!(
            view.waiting[0].reason,
            WaitReason::Cpu {
                committed: 24 * MILLICORES,
                budget: 24 * MILLICORES,
            }
        );
        assert_eq!(view.waiting[1].reason, WaitReason::Behind);

        assert!(matches!(q.release(a1), Some(Released::Running(_))));
        q.step(&calm(1.0), &learned, &mut out);
        assert_eq!(out, [b1]);
        assert!(matches!(q.release(a2), Some(Released::Running(_))));
        q.step(&calm(2.0), &learned, &mut out);
        assert_eq!(out, [b2]);
        assert!(q.waiting().is_empty());
    }

    #[test]
    fn cpu_learning_uses_active_elapsed_time_and_ignores_counter_rebounds() {
        let mut cpu = Cpu::default();
        cpu.observe(0.0, 1_000_000_000, false, 30000);
        cpu.observe(1.0, 5_000_000_000, false, 30000);
        cpu.observe(2.0, 3_000_000_000, false, 30000); // worker exited before being reaped
        cpu.observe(3.0, 7_000_000_000, false, 30000);
        cpu.observe(4.0, 7_000_000_000, true, 30000);
        cpu.observe(24.0, 7_000_000_000, true, 30000);
        cpu.observe(25.0, 7_000_000_000, false, 30000);
        cpu.observe(26.0, 9_000_000_000, false, 30000);
        assert_eq!(cpu.peak, Some(4000));
    }

    /// At 58 GiB background plus 1 GiB reserved, another 11 GiB reaches the cap and waits.
    #[test]
    fn a_command_waits_when_background_plus_reservations_reaches_the_cap() {
        let learned = |s: &str| {
            memory(match s {
                "edge" => Some(44 * GIB / 5),
                "fits" => Some(8 * GIB),
                _ => None,
            })
        };
        let mut q = Scheduler::with_cpu_budget(24 * MILLICORES);
        let first = ask(&mut q, 1, "first");
        assert!(step(&mut q, &calm(0.0), &learned).is_some());
        q.observe(first, GIB, 0.0);
        run(&mut q, 1, 10, 42, &learned);
        assert_eq!(q.used(), 58 * GIB);

        let edge = ask(&mut q, 2, "edge");
        assert_eq!(run(&mut q, 11, 20, 42, &learned), None);
        assert_eq!(q.waiting().len(), 1, "58 + 1 + 11 equals 70");

        q.release(edge);
        let fits = ask(&mut q, 2, "fits");
        assert_eq!(
            step(&mut q, &at(21.0, 42), &learned),
            Some(fits),
            "58 + 1 + 10 is under 70"
        );
    }

    /// Memory the engine calls tight, or a job it holds frozen, stops every admission, however
    /// little is in use.
    #[test]
    fn nothing_is_admitted_while_memory_is_tight_or_a_job_is_frozen() {
        let mut q = Scheduler::with_cpu_budget(24 * MILLICORES);
        let a = ask(&mut q, 1, "a");
        assert_eq!(step(&mut q, &at(0.0, 35), &unknown), None, "tight");
        let frozen = Sample {
            frozen: true,
            ..calm(1.0)
        };
        assert_eq!(step(&mut q, &frozen, &unknown), None, "frozen");
        let pressure = Sample {
            tight: true,
            ..calm(2.0)
        };
        assert_eq!(
            step(&mut q, &pressure, &unknown),
            None,
            "tight by pressure alone"
        );
        assert_eq!(
            step(&mut q, &calm(3.0), &unknown),
            Some(a),
            "calm and thawed"
        );
    }

    /// The memory in use is the average of the last 5 s, not the last reading: one calm
    /// reading after a long busy spell does not let a big command in, five do.
    #[test]
    fn the_memory_in_use_is_averaged_over_five_seconds() {
        let big = |s: &str| memory((s == "big").then_some(20 * GIB));
        let mut q = Scheduler::with_cpu_budget(24 * MILLICORES);
        ask(&mut q, 1, "first");
        assert!(step(&mut q, &calm(0.0), &big).is_some());
        run(&mut q, 1, 10, 45, &big); // 55 GiB in use
        let id = ask(&mut q, 2, "big");
        // 20 GiB in use now, but 4 x 55 + 20 over 5 s is 48, and 48 + 25 does not fit under 70.
        assert_eq!(step(&mut q, &at(11.0, 80), &big), None);
        assert_eq!(q.used(), 48 * GIB);
        // Two readings: (3 x 55 + 2 x 20) / 5 = 41 GiB, and 41 + 25 fits.
        assert_eq!(step(&mut q, &at(12.0, 80), &big), Some(id));
        assert_eq!(q.used(), 41 * GIB);
    }

    #[test]
    fn two_strangers_that_fit_start_in_the_same_step() {
        let mut q = Scheduler::with_cpu_budget(24 * MILLICORES);
        let (a, b) = (ask(&mut q, 1, "a"), ask(&mut q, 2, "b"));
        let mut out = Vec::new();
        q.step(&calm(0.0), &unknown, &mut out);
        assert_eq!(out, [a, b]);
        assert_eq!(names(&q), ["a", "b"]);
        assert!(
            q.running()
                .iter()
                .all(|r| r.need == GIB && r.admitted == 0.0)
        );
    }

    #[test]
    fn the_entire_memory_commitment_remains_when_footprint_falls_or_is_missing() {
        let mut q = Scheduler::with_cpu_budget(24 * MILLICORES);
        let need = |_: &str| memory(Some(18 * GIB));
        let ids: Vec<_> = (1..=3).map(|s| ask(&mut q, s, "build")).collect();
        let mut out = Vec::new();
        q.step(&calm(0.0), &need, &mut out);
        assert_eq!(out, ids[..2]);
        for t in 30..=40 {
            let time = f64::from(t);
            for &id in &ids[..2] {
                q.observe(id, 20 * GIB, time);
            }
            // 59 GiB measured, 40 attributable to admitted jobs: 19 background.
            q.step(&at(time, 41), &need, &mut out);
            assert!(out.is_empty());
        }
        assert_eq!(q.used(), 19 * GIB);
        for t in 41..=50 {
            let time = f64::from(t);
            q.observe(ids[0], GIB, time);
            q.step(&calm(time), &need, &mut out);
            assert!(out.is_empty());
        }
        assert_eq!(q.reserved(), 50 * GIB);
        q.release(ids[0]);
        assert_eq!(step(&mut q, &calm(51.0), &need), Some(ids[2]));
    }

    #[test]
    fn reservation_grows_with_the_peak_even_after_current_use_falls() {
        let mut q = Scheduler::with_cpu_budget(24 * MILLICORES);
        let id = ask(&mut q, 1, "build");
        step(&mut q, &calm(0.0), &unknown);
        assert_eq!(q.reserved(), GIB);
        q.observe(id, 2 * GIB, 1.0);
        q.observe(id, 0, 2.0);
        assert_eq!(q.reserved(), with_margin(2 * GIB));
        q.release(id);
        assert_eq!(q.reserved(), 0);
    }

    #[test]
    fn easy_backfills_only_jobs_finishing_by_the_heads_earliest_start() {
        let mut q = Scheduler::with_cpu_budget(4 * MILLICORES);
        let learned = |s: &str| {
            let (gib, cores, seconds) = match s {
                "watch" => (8, 2, None),
                "blocker" => (16, 1, Some(10)),
                "head" => (24, 4, Some(2)),
                "short" => (1, 3, Some(2)),
                "long" => (1, 1, Some(20)),
                _ => (1, 1, None),
            };
            Demand {
                bytes: Some(gib * GIB),
                millicores: Some(cores * MILLICORES),
                runtime_ms: seconds.map(|s| s * 1000),
            }
        };
        ask(&mut q, 1, "watch");
        let blocker = ask(&mut q, 1, "blocker");
        let mut out = Vec::new();
        q.step(&calm(0.0), &learned, &mut out);
        let head = ask(&mut q, 1, "head");
        ask(&mut q, 2, "long");
        ask(&mut q, 3, "unknown");
        for i in [1, 3, 5, 7] {
            let short = ask(&mut q, 4, "short");
            q.step(&calm(f64::from(i)), &learned, &mut out);
            assert_eq!(out, [short]);
            q.release(short);
            assert_eq!(q.waiting()[0].id, head);
        }
        ask(&mut q, 4, "short");
        q.step(&calm(9.0), &learned, &mut out);
        assert!(out.is_empty(), "two seconds would delay the head");
        q.release(blocker);
        q.step(&calm(10.0), &learned, &mut out);
        assert_eq!(out, [head]);
    }

    #[test]
    fn review3_backfill_can_use_spare_capacity_without_an_incumbent_estimate() {
        let mut q = Scheduler::with_cpu_budget(4000);
        let learned = |s: &str| Demand {
            millicores: Some(match s {
                "watch" => 3000,
                "head" => 4000,
                _ => 1000,
            }),
            bytes: Some(
                match s {
                    "watch" => 24,
                    "head" => 20,
                    _ => 1,
                } * GIB,
            ),
            runtime_ms: None,
        };
        let watch = ask(&mut q, 1, "watch");
        step(&mut q, &calm(0.0), &learned);
        let head = ask(&mut q, 2, "head");
        let small = ask(&mut q, 3, "small");
        assert_eq!(step(&mut q, &calm(1.0), &learned), Some(small));
        q.release(watch);
        assert_eq!(step(&mut q, &calm(2.0), &learned), Some(head));
    }

    #[test]
    fn a_backfill_cannot_block_a_head_that_could_run_after_a_partial_release() {
        let mut q = Scheduler::with_cpu_budget(4000);
        let learned = |name: &str| {
            memory(Some(
                match name {
                    "watch" => 20,
                    "blocker" => 8,
                    "head" => 16,
                    _ => 4,
                } * GIB,
            ))
        };
        ask(&mut q, 1, "watch");
        let blocker = ask(&mut q, 1, "blocker");
        q.step(&calm(0.0), &learned, &mut Vec::new());
        let head = ask(&mut q, 2, "head");
        ask(&mut q, 3, "small");
        assert_eq!(step(&mut q, &calm(1.0), &learned), None);
        q.release(blocker);
        assert_eq!(step(&mut q, &calm(2.0), &learned), Some(head));
    }

    #[test]
    fn missed_cpu_readings_use_the_elapsed_time_and_keep_the_default_commitment() {
        let mut q = Scheduler::with_cpu_budget(12 * MILLICORES);
        let demand = |_: &str| Demand::default();
        let id = ask(&mut q, 1, "burst");
        step(&mut q, &calm(0.0), &demand);
        q.observe_cpu(id, 0, false, 0.0);
        q.observe_cpu(id, 0, false, 1.0);
        q.observe_cpu(id, 5_600_000_000, false, 8.0);
        q.observe_cpu(id, 5_600_000_000, false, 9.0);
        assert_eq!(q.running()[0].measured_cpu(), Some(800));
        assert_eq!(q.cpu_committed(), 2000);
    }

    #[test]
    fn arrivals_use_the_last_sample_without_learning_or_probing() {
        let mut q = Scheduler::with_cpu_budget(24 * MILLICORES);
        let first = ask(&mut q, 1, "first");
        let mut out = vec![999];
        q.admit_now(0.0, 0.0, &unknown, &mut out);
        assert!(out.is_empty(), "no admission before the first sample");
        q.step(&calm(0.0), &unknown, &mut out);
        assert_eq!(out, [first]);
        let second = ask(&mut q, 2, "second");
        q.admit_now(0.1, 0.1, &unknown, &mut out);
        assert_eq!(out, [second], "no sample between request and admission");
        assert_eq!(q.running()[1].admitted, 0.1);
        ask(&mut q, 3, "large");
        let large = |_: &str| memory(Some(60 * GIB));
        q.admit_now(PROBE_AFTER + 1.0, 61.0, &large, &mut out);
        assert!(out.is_empty());
        assert_eq!(q.cap(), Some(70 * GIB), "arrival never probes the cap");
        assert_eq!(q.used(), 20 * GIB);
        assert_eq!(q.history.len, 1, "arrival does not add readings");
        assert_eq!(q.quiet_since, 0.0);
    }

    #[test]
    fn arrivals_obey_the_last_samples_tight_and_frozen_conditions() {
        for (tight, frozen) in [(true, false), (false, true)] {
            let mut q = Scheduler::with_cpu_budget(24 * MILLICORES);
            let mut out = Vec::new();
            q.step(
                &Sample {
                    tight,
                    frozen,
                    ..at(0.0, 35)
                },
                &unknown,
                &mut out,
            );
            ask(&mut q, 1, "first");
            ask(&mut q, 2, "second");
            q.admit_now(1.0, 1.0, &unknown, &mut out);
            assert!(out.is_empty());
            assert_eq!(q.waiting().len(), 2);
            q.step(&calm(2.0), &unknown, &mut out);
            assert_eq!(
                out.len(),
                2,
                "the next calm sample clears the stop condition"
            );
        }
    }

    #[test]
    fn the_head_explains_its_wait_and_keeps_its_share() {
        let mut q = Scheduler::with_cpu_budget(24 * MILLICORES);
        let first = ask(&mut q, 1, "large");
        let head = ask(&mut q, 2, "head");
        let behind = ask(&mut q, 3, "small");
        let need = |s: &str| {
            memory(Some(match s {
                "large" => 30 * GIB,
                "head" => 39 * GIB,
                _ => 2 * GIB,
            }))
        };
        let mut out = Vec::new();
        q.step(&calm(0.0), &need, &mut out);
        assert_eq!(out, [first], "the small request cannot pass the head");
        let mut view = Snapshot::default();
        q.snapshot(&need, &mut view);
        assert_eq!((view.waiting[0].id, view.waiting[1].id), (head, behind));
        assert_eq!(
            view.waiting[0].reason,
            WaitReason::Cap {
                used: 20 * GIB,
                reserved: 30 * GIB * 5 / 4,
                cap: 70 * GIB,
            }
        );
        assert_eq!(view.waiting[1].reason, WaitReason::Behind);
        for (tight, frozen, reason) in [
            (true, false, WaitReason::Tight),
            (false, true, WaitReason::Frozen),
        ] {
            q.step(
                &Sample {
                    tight,
                    frozen,
                    ..calm(1.0)
                },
                &need,
                &mut out,
            );
            assert!(out.is_empty());
            q.snapshot(&need, &mut view);
            assert_eq!(view.waiting[0].reason, reason);
            assert_eq!(view.waiting[1].reason, WaitReason::Behind);
        }
    }

    #[test]
    fn cap_probe_raises_the_cap_below_freeze_occupancy() {
        let mut q = Scheduler::with_cpu_budget(24 * MILLICORES);
        let need = |_: &str| memory(Some(30 * GIB));
        let mut out = Vec::new();
        q.step(&calm(0.0), &need, &mut out);
        let first = ask(&mut q, 1, "first");
        q.step(&calm(PROBE_AFTER - 1.0), &need, &mut out);
        assert_eq!(out, [first]);
        ask(&mut q, 2, "second");
        let s = Sample {
            occupancy: Policy::default().freeze_occupancy - 0.01,
            ..calm(PROBE_AFTER)
        };
        q.step(&s, &need, &mut out);
        assert!(
            out.is_empty(),
            "20 used + 30 reserved + 30 needed exceeds even the probe"
        );
        assert_eq!(
            q.cap(),
            Some(75 * GIB),
            "only the reservation holds the head back"
        );
    }

    #[test]
    fn cap_probe_raises_the_cap_when_occupancy_stays_high_but_memory_is_calm() {
        let mut q = Scheduler::with_cpu_budget(24 * MILLICORES);
        let need = |_: &str| memory(Some(30 * GIB));
        let quiet = |time| Sample {
            occupancy: 0.75,
            ..at(time, 90)
        };
        let first = ask(&mut q, 1, "first");
        assert_eq!(step(&mut q, &quiet(0.0), &need), Some(first));
        let head = ask(&mut q, 2, "second");
        assert_eq!(step(&mut q, &quiet(PROBE_AFTER - 1.0), &need), None);
        assert_eq!(q.cap(), Some(70 * GIB));
        assert_eq!(step(&mut q, &quiet(PROBE_AFTER), &need), None);
        assert_eq!(q.waiting()[0].id, head);
        assert_eq!(q.cap(), Some(75 * GIB), "the blocked head triggers a probe");
    }

    #[test]
    fn cap_probe_gives_back_its_increase_under_freeze_pressure() {
        for occupancy in [0.75, Policy::default().freeze_occupancy] {
            let mut q = Scheduler::with_cpu_budget(24 * MILLICORES);
            // Even after pressure clears, 10 + 42.5 + 42.5 exceeds the exclusive ceiling.
            let need = |_: &str| memory(Some(34 * GIB));
            let reading = |time, level| Sample {
                occupancy,
                ..at(time, level)
            };
            let first = ask(&mut q, 1, "first");
            assert_eq!(step(&mut q, &calm(0.0), &need), Some(first));
            ask(&mut q, 2, "second");
            assert_eq!(run(&mut q, 1, 60, 90, &need), None);
            assert_eq!(q.cap(), Some(75 * GIB), "probed in calm memory");
            assert_eq!(step(&mut q, &reading(61.0, 90), &need), None);
            assert_eq!(q.cap(), Some(75 * GIB), "occupancy alone keeps the probe");

            assert_eq!(step(&mut q, &reading(62.0, 35), &need), None);
            assert_eq!(
                q.cap(),
                Some((14 * 10 + 65) * GIB / 15),
                "tight memory gives back the probe and learns the last 15 s of use"
            );
            for time in 63..=122 {
                assert_eq!(step(&mut q, &reading(f64::from(time), 35), &need), None);
                if time >= 76 {
                    assert_eq!(q.cap(), Some(65 * GIB), "no probe under sustained pressure");
                }
            }
            assert_eq!(step(&mut q, &reading(123.0, 90), &need), None);
            assert_eq!(
                q.cap(),
                Some(65 * GIB),
                "quiet time restarts after pressure"
            );
        }
    }

    #[test]
    fn an_oversized_head_can_start_beside_others_when_pressure_clears() {
        let learned = |s: &str| match s {
            "small" => memory(Some(8 * GIB)),
            _ => memory(Some(56 * GIB)),
        };
        // 5 GiB in use: 5 + 10 reserved + 70 needed fits under the 90% ceiling.
        let quiet = |time, occupancy| Sample {
            occupancy,
            ..at(time, 95)
        };
        let mut q = Scheduler::with_cpu_budget(24 * MILLICORES);
        let small = ask(&mut q, 1, "small");
        assert_eq!(step(&mut q, &quiet(0.0, 0.0), &learned), Some(small));
        let big = ask(&mut q, 2, "big");
        // The OS can report warning pressure while the memory level remains high.
        assert_eq!(
            step(
                &mut q,
                &Sample {
                    tight: true,
                    ..quiet(1.0, 0.75)
                },
                &learned
            ),
            None,
            "pressure holds the head even though the reservations fit"
        );
        assert_eq!(
            step(&mut q, &quiet(2.0, 0.75), &learned),
            Some(big),
            "the reservations fit once pressure clears at the same occupancy"
        );
        assert_eq!(
            q.running()[1].exclusive,
            Some(ExclusiveAdmission::Reservations)
        );
    }

    /// The ten-minute fallback bounds the wait when reservations exceed 90% of RAM.
    #[test]
    fn an_oversized_head_waits_at_most_ten_minutes_at_freeze_occupancy() {
        let learned = |s: &str| match s {
            "small" => memory(Some(8 * GIB)),
            _ => memory(Some(61 * GIB)),
        };
        let quiet = |time, occupancy| Sample {
            occupancy,
            ..at(time, 95)
        };
        let freeze = Policy::default().freeze_occupancy;
        let mut q = Scheduler::with_cpu_budget(24 * MILLICORES);
        let small = ask(&mut q, 1, "small");
        assert_eq!(step(&mut q, &quiet(0.0, 0.0), &learned), Some(small));
        let big = ask(&mut q, 2, "big");
        assert_eq!(
            step(&mut q, &quiet(1.0, freeze), &learned),
            None,
            "5 used + 10 reserved + 76.25 needed exceeds the 90% ceiling"
        );
        let waited = 1.0 + EXCLUSIVE_WAIT_SECONDS;
        assert_eq!(
            step(&mut q, &quiet(waited - 1.0, freeze), &learned),
            None,
            "still inside the ten minutes"
        );
        assert_eq!(
            step(&mut q, &quiet(waited, freeze), &learned),
            Some(big),
            "the fallback ends the wait"
        );
        assert_eq!(q.running()[1].exclusive, Some(ExclusiveAdmission::TimedOut));
    }

    /// With nothing running the next command starts although it does not fit (a machine that
    /// is busy with other things never gets quieter), but never while memory is tight, and
    /// once it runs the next one has to fit.
    #[test]
    fn the_next_command_starts_when_nothing_runs_even_if_it_does_not_fit() {
        let big = |_: &str| memory(Some(30 * GIB));
        let mut q = Scheduler::with_cpu_budget(24 * MILLICORES);
        let (a, b) = (ask(&mut q, 1, "a"), ask(&mut q, 1, "b"));
        assert_eq!(step(&mut q, &at(0.0, 35), &big), None, "never while tight");
        // 55 + 30 is over the cap, which the tight reading set to 65.
        assert_eq!(
            step(&mut q, &at(1.0, 45), &big),
            Some(a),
            "alone: forward progress"
        );
        assert_eq!(run(&mut q, 2, 20, 45, &big), None, "now it has to fit");
        assert_eq!(q.waiting().len(), 1);
        q.release(a);
        assert_eq!(step(&mut q, &at(21.0, 45), &big), Some(b), "alone again");
    }

    /// The cap is what was in use over the last 15 s whenever a reading is tight: here ten
    /// calm seconds at 20 GiB and then 75 GiB, and later only the 75 GiB.
    #[test]
    fn the_cap_is_learned_from_a_tight_episode() {
        let mut q = Scheduler::with_cpu_budget(24 * MILLICORES);
        run(&mut q, 0, 9, 80, &unknown);
        assert_eq!(q.cap(), Some(70 * GIB));
        step(&mut q, &at(10.0, 25), &unknown);
        assert_eq!(q.cap(), Some(25 * GIB), "(10 x 20 + 75) / 11");
        step(&mut q, &at(11.0, 25), &unknown);
        assert_eq!(q.cap(), Some(350 * GIB / 12), "(10 x 20 + 2 x 75) / 12");
        run(&mut q, 12, 40, 25, &unknown);
        assert_eq!(
            q.cap(),
            Some(75 * GIB),
            "the calm seconds have left the window"
        );
    }

    /// Blocked only by the cap, the cap rises by 5% of RAM after 60 s without pressure, and
    /// again every 60 s; the request that fits at last is admitted.
    #[test]
    fn the_cap_is_probed_upwards_while_a_request_is_blocked_by_it() {
        let need = |s: &str| memory((s == "b").then_some(15 * GIB));
        let mut q = Scheduler::with_cpu_budget(24 * MILLICORES);
        ask(&mut q, 1, "a");
        assert!(step(&mut q, &calm(0.0), &need).is_some());
        run(&mut q, 1, 10, 45, &need); // 55 GiB in use, the 5 s average settled
        let b = ask(&mut q, 2, "b");
        // 55 + 18.75 does not fit under 70.
        for t in 11..=59 {
            assert_eq!(step(&mut q, &at(f64::from(t), 45), &need), None, "t = {t}");
            assert_eq!(q.cap(), Some(70 * GIB), "t = {t}");
        }
        assert_eq!(
            step(&mut q, &at(60.0, 45), &need),
            Some(b),
            "75 GiB: 55 + 18.75 fits"
        );
        assert_eq!(q.cap(), Some(75 * GIB));
    }

    #[test]
    fn probing_stops_at_75_percent_of_ram_and_waits_for_a_blocked_request() {
        let huge = |_: &str| memory(Some(95 * GIB));
        let mut q = Scheduler::with_cpu_budget(24 * MILLICORES);
        ask(&mut q, 1, "a");
        assert!(step(&mut q, &calm(0.0), &huge).is_some());
        assert_eq!(q.running()[0].exclusive, Some(ExclusiveAdmission::Alone));
        // Nobody waiting: nothing is blocked, so the cap stays where it is.
        run(&mut q, 1, 200, 45, &huge);
        assert_eq!(q.cap(), Some(70 * GIB));

        ask(&mut q, 2, "b");
        let mut caps = Vec::new();
        for t in 201..=600 {
            assert_eq!(step(&mut q, &at(f64::from(t), 45), &huge), None);
            caps.push(q.cap().unwrap() / GIB);
        }
        assert_eq!(caps.iter().max(), Some(&75), "never above 75% of RAM");
        assert_eq!(caps.last(), Some(&75));
        assert!(caps.iter().all(|&cap| cap == 70 || cap == 75));
    }

    /// Pressure restarts the 60 s: the next probe is 60 s after the last tight reading, from
    /// the cap that reading learned.
    #[test]
    fn a_tight_reading_restarts_the_minute_before_the_next_probe() {
        let need = |_: &str| memory(Some(40 * GIB));
        let mut q = Scheduler::with_cpu_budget(24 * MILLICORES);
        ask(&mut q, 1, "a");
        assert!(step(&mut q, &calm(0.0), &need).is_some());
        ask(&mut q, 2, "b");
        run(&mut q, 1, 99, 45, &need);
        assert_eq!(q.cap(), Some(75 * GIB), "probed once, at 60 s");
        step(&mut q, &at(100.0, 35), &need);
        let learned = q.cap().unwrap();
        assert_eq!(learned, (14 * 55 + 65) * GIB / 15, "the last 15 s of use");
        run(&mut q, 101, 159, 45, &need);
        assert_eq!(
            q.cap(),
            Some(learned),
            "no probe 59 s after the tight reading"
        );
        step(&mut q, &at(160.0, 45), &need);
        assert_eq!(q.cap(), Some(learned + 5 * GIB));
    }

    #[test]
    fn what_was_learned_about_a_command_replaces_the_default_need() {
        let mut q = Scheduler::with_cpu_budget(24 * MILLICORES);
        ask(&mut q, 1, "first");
        assert!(step(&mut q, &calm(0.0), &unknown).is_some());
        run(&mut q, 1, 10, 55, &unknown); // 45 GiB in use, the 5 s average settled
        ask(&mut q, 2, "bun run test");
        let need = |q: &Scheduler, learned: &dyn Fn(&str) -> Demand| {
            let mut view = Snapshot::default();
            q.snapshot(learned, &mut view);
            view.waiting[0].need
        };
        assert_eq!(need(&q, &unknown), GIB);
        let learned = |s: &str| memory((s == "bun run test").then_some(30 * GIB));
        assert_eq!(need(&q, &learned), 30 * GIB * 5 / 4);

        // 45 + 30 is over the 70 GiB cap; 45 + the default 1 is not.
        assert_eq!(run(&mut q, 11, 20, 55, &learned), None);
        assert_eq!(
            q.waiting().len(),
            1,
            "held back by what it was seen to need"
        );
        assert!(
            step(&mut q, &at(21.0, 55), &unknown).is_some(),
            "admitted with the default"
        );
    }

    #[test]
    fn cold_reservations_have_a_four_gib_floor_even_on_small_machines() {
        let mut q = Scheduler::with_cpu_budget(24000);
        let cold = |_: &str| Demand::default();
        let a = ask(&mut q, 1, "cold a");
        let b = ask(&mut q, 1, "cold b");
        let small = Sample {
            ram: 4 * GIB,
            ..calm(0.0)
        };
        assert_eq!(step(&mut q, &small, &cold), Some(a));
        assert_eq!(q.running()[0].need, 4 * GIB);
        assert_eq!(q.waiting()[0].id, b);
        let mut view = Snapshot::default();
        q.snapshot(&cold, &mut view);
        assert_eq!(view.waiting[0].need, 4 * GIB);
    }

    #[test]
    fn reconnection_keeps_the_original_request_order_across_sessions() {
        let mut q = Scheduler::with_cpu_budget(24 * MILLICORES);
        let new = q.enqueue(2, "new".into(), "new".into(), 30.0);
        let retry = q.enqueue(1, "old".into(), "old".into(), 10.0);
        let mut view = Snapshot::default();
        q.snapshot(&unknown, &mut view);
        assert_eq!(
            view.waiting.iter().map(|w| w.id).collect::<Vec<_>>(),
            [retry, new]
        );
        let mut out = Vec::new();
        q.step(&calm(0.0), &unknown, &mut out);
        assert_eq!(out, [retry, new]);
    }

    /// Within one session the order never changes, whoever else comes and goes, and the
    /// positions told to the clients follow it.
    #[test]
    fn requests_of_one_session_stay_in_arrival_order() {
        let mut q = Scheduler::with_cpu_budget(24 * MILLICORES);
        let ids: Vec<Id> = ["a", "b", "c", "d"]
            .iter()
            .map(|n| ask(&mut q, 7, n))
            .collect();
        let positions = |q: &Scheduler| {
            let mut view = Snapshot::default();
            q.snapshot(&unknown, &mut view);
            view.waiting
                .iter()
                .map(|w| (w.id, w.position))
                .collect::<Vec<_>>()
        };
        assert_eq!(
            positions(&q),
            [(ids[0], 1), (ids[1], 2), (ids[2], 3), (ids[3], 4)]
        );
        // A request that goes away (its client was killed) closes the gap and moves nobody
        // past anybody.
        assert!(matches!(q.release(ids[1]), Some(Released::Waiting(_))));
        assert_eq!(positions(&q), [(ids[0], 1), (ids[2], 2), (ids[3], 3)]);
        let mut out = Vec::new();
        q.step(&calm(0.0), &unknown, &mut out);
        assert_eq!(out, [ids[0], ids[2], ids[3]]);
    }

    #[test]
    fn a_running_command_is_measured_and_released() {
        let mut q = Scheduler::with_cpu_budget(24 * MILLICORES);
        let a = ask(&mut q, 1, "a");
        assert!(!q.started(a, 4242, 7), "not admitted yet");
        assert_eq!(step(&mut q, &calm(0.0), &unknown), Some(a));
        assert!(q.started(a, 4242, 7));
        q.observe(a, 3 * GIB, 1.0);
        q.observe(a, 2 * GIB, 2.0);
        let Some(Released::Running(job)) = q.release(a) else {
            panic!("it was running");
        };
        assert_eq!(
            (job.pgid, job.leader_start_us, job.peak),
            (Some(4242), 7, 3 * GIB)
        );
        assert!(q.release(a).is_none(), "released once");
        assert!(q.running().is_empty());
    }

    #[test]
    fn review3_a_single_busy_interval_is_learned_as_the_peak() {
        let demand = |_: &str| memory(None);
        let mut q = Scheduler::with_cpu_budget(12000);
        let id = ask(&mut q, 1, "burst");
        step(&mut q, &calm(0.0), &demand);
        q.observe_cpu(id, 0, false, 0.0);
        for i in 1..=20 {
            q.observe_cpu(id, 8_000_000_000, false, f64::from(i));
        }
        assert_eq!(q.running()[0].measured_cpu(), Some(8000));
    }
    #[test]
    fn review3_late_backfill_uses_the_arrival_clock_and_margin() {
        let mut q = Scheduler::with_cpu_budget(4000);
        let learned = |s: &str| Demand {
            bytes: Some(
                match s {
                    "watch" => 8,
                    "blocker" => 16,
                    "head" => 24,
                    _ => 1,
                } * GIB,
            ),
            millicores: Some(if s == "watch" || s == "head" {
                2000
            } else {
                1000
            }),
            runtime_ms: match s {
                "blocker" => Some(10000),
                "late" => Some(500),
                _ => None,
            },
        };
        ask(&mut q, 1, "watch");
        ask(&mut q, 1, "blocker");
        let mut out = Vec::new();
        q.step(&calm(0.0), &learned, &mut out);
        ask(&mut q, 1, "head");
        ask(&mut q, 1, "late");
        q.admit_now(9.4, 9.4, &learned, &mut out);
        assert!(out.is_empty());
    }
}
