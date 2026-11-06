//! Admission control: which waiting command may start now. A pure function of the memory
//! readings and the requests, with no I/O, so every rule is tested with synthetic samples (as
//! `engine` is).
//!
//! The design is Buck2's local-action scheduler (`scheduler.rs`, Meta Platforms, Apache-2.0;
//! see THIRD_PARTY_NOTICES.md): start one more job only when the memory in use, averaged over
//! the last 5 s, plus what that job is expected to need stays under a cap; space the starts at
//! least 3 s apart, because a started job takes seconds to show in the numbers; learn the cap
//! from the memory in use, averaged over the last 15 s, whenever the machine was under
//! pressure; and expect `min(1 GiB, cap / 5)` of a job nothing is known about. Buck2 leaves a
//! cap that was learned too low as a FIXME, and it stays low for good. Here it is probed
//! upwards: after 60 s without pressure, while a request is held back only by the cap, the cap
//! rises by 5% of RAM, and no further than 90% of RAM.
//!
//! The queue and the freeze engine are one ordered system: nothing is admitted while memory is
//! tight or while the engine holds a frozen job (Buck2: a new scene waits while anything is
//! suspended). A command that does fit is started only after the queue has taken the rest of
//! its rules into account:
//!
//! - Requests are served in turns between sessions (an agent and everything it runs): the
//!   next one is the oldest request of the session with the fewest commands running, so one
//!   agent's backlog cannot hold up another's single command. Within a session it is first
//!   come, first served.
//! - When the next one does not fit, nothing behind it starts.
//! - If no admitted command is running and nothing is frozen, the next one starts even if it
//!   does not fit, or it would wait for a machine that never gets any quieter. Never while
//!   memory is tight.

/// Memory is counted in bytes; this is the unit of the defaults.
pub const GIB: u64 = 1 << 30;
/// The least time between two admissions (seconds).
pub const SPACING: f64 = 3.0;
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
/// ...and never above this.
const MAX_PERCENT: u64 = 90;
/// Readings kept: more than `CAP_WINDOW` holds at one a second.
const HISTORY: usize = 64;

pub type Id = u64;

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

/// A command that has been told to start, until it ends.
#[derive(Clone, Debug)]
pub struct Running {
    pub id: Id,
    pub session: i32,
    pub label: String,
    pub signature: String,
    /// Wall-clock seconds when it was admitted.
    pub since: f64,
    /// How long it waited, seconds.
    pub waited: f64,
    /// What it was expected to need when it was admitted.
    pub need: u64,
    /// Its process group, once the client has said.
    pub pgid: Option<i32>,
    /// The most memory its group has been seen to use.
    pub peak: u64,
}

/// What `Scheduler::release` took out.
#[derive(Debug)]
pub enum Released {
    Waiting(Waiting),
    Running(Running),
}

/// Where a request stands.
#[derive(Clone, Copy, Debug, PartialEq, Eq)]
pub enum Standing {
    /// `position` counts from 1, in the order the requests will be admitted if nothing changes.
    Waiting {
        position: usize,
        need: u64,
    },
    Admitted,
}

pub struct Scheduler {
    waiting: Vec<Waiting>,
    running: Vec<Running>,
    history: History,
    /// None until the first reading says how much RAM there is.
    cap: Option<u64>,
    ram: u64,
    /// Memory in use over the last `USED_WINDOW`, as of the last step.
    used: u64,
    last_admission: f64,
    /// The last time memory was tight or the cap was probed: what the 60 s count runs from.
    quiet_since: f64,
    next_id: Id,
}

impl Default for Scheduler {
    fn default() -> Scheduler {
        Scheduler::new()
    }
}

impl Scheduler {
    pub fn new() -> Scheduler {
        Scheduler {
            waiting: Vec::new(),
            running: Vec::new(),
            history: History::new(),
            cap: None,
            ram: 0,
            used: 0,
            last_admission: f64::NEG_INFINITY,
            quiet_since: 0.0,
            next_id: 0,
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

    /// A new request goes to the back of the line.
    pub fn enqueue(&mut self, session: i32, label: String, signature: String, wall: f64) -> Id {
        self.next_id += 1;
        self.waiting.push(Waiting {
            id: self.next_id,
            session,
            label,
            signature,
            since: wall,
        });
        self.next_id
    }

    /// The request ends: it stops waiting, or it stops running. None if it is not known.
    pub fn release(&mut self, id: Id) -> Option<Released> {
        if let Some(at) = self.waiting.iter().position(|w| w.id == id) {
            return Some(Released::Waiting(self.waiting.remove(at)));
        }
        let at = self.running.iter().position(|r| r.id == id)?;
        Some(Released::Running(self.running.remove(at)))
    }

    /// The admitted command runs as this process group. False if `id` is not admitted.
    pub fn started(&mut self, id: Id, pgid: i32) -> bool {
        let Some(job) = self.running.iter_mut().find(|r| r.id == id) else {
            return false;
        };
        job.pgid = Some(pgid);
        true
    }

    /// The admitted command's group is using `bytes` right now.
    pub fn observe(&mut self, id: Id, bytes: u64) {
        if let Some(job) = self.running.iter_mut().find(|r| r.id == id) {
            job.peak = job.peak.max(bytes);
        }
    }

    fn running_of(&self, session: i32) -> usize {
        self.running.iter().filter(|r| r.session == session).count()
    }

    /// Where in `waiting` the request that goes next is: the oldest of the session with the
    /// fewest commands running.
    fn next(&self) -> Option<usize> {
        self.waiting
            .iter()
            .enumerate()
            .min_by_key(|(_, w)| (self.running_of(w.session), w.id))
            .map(|(at, _)| at)
    }

    /// What a command of this signature is expected to need: what was learned of it, or else
    /// `min(1 GiB, cap / 5)`.
    fn need(&self, signature: &str, learned: &dyn Fn(&str) -> Option<u64>) -> u64 {
        learned(signature).unwrap_or_else(|| self.cap.map_or(GIB, |cap| GIB.min(cap / 5)))
    }

    fn fits(&self, at: usize, learned: &dyn Fn(&str) -> Option<u64>) -> bool {
        let need = self.need(&self.waiting[at].signature, learned);
        self.used.saturating_add(need) < self.cap.unwrap_or(0)
    }

    /// Where every request stands: the waiting ones in the order they will be admitted if
    /// nothing changes (each session's turn comes round in turn, as `next` picks them), then
    /// the admitted ones.
    pub fn standings(&self, learned: &dyn Fn(&str) -> Option<u64>) -> Vec<(Id, Standing)> {
        // Each session with a request waiting, and how many of its commands count as running
        // by the time its turn comes.
        let mut turns: Vec<(i32, usize)> = Vec::new();
        for w in &self.waiting {
            if !turns.iter().any(|&(session, _)| session == w.session) {
                turns.push((w.session, self.running_of(w.session)));
            }
        }
        let turn = |turns: &[(i32, usize)], session: i32| {
            turns
                .iter()
                .find(|&&(s, _)| s == session)
                .map_or(0, |&(_, running)| running)
        };
        let mut left: Vec<&Waiting> = self.waiting.iter().collect();
        let mut out = Vec::with_capacity(self.waiting.len() + self.running.len());
        while let Some(at) =
            (0..left.len()).min_by_key(|&at| (turn(&turns, left[at].session), left[at].id))
        {
            let w = left.remove(at);
            if let Some(entry) = turns.iter_mut().find(|t| t.0 == w.session) {
                entry.1 += 1;
            }
            let position = out.len() + 1;
            let need = self.need(&w.signature, learned);
            out.push((w.id, Standing::Waiting { position, need }));
        }
        out.extend(self.running.iter().map(|r| (r.id, Standing::Admitted)));
        out
    }

    /// Takes one reading and decides whether the next request starts now. At most one starts
    /// per reading. Allocation-free when nobody is waiting.
    pub fn step(&mut self, s: &Sample, learned: &dyn Fn(&str) -> Option<u64>) -> Option<Id> {
        let now = s.time;
        self.ram = s.ram;
        if self.cap.is_none() {
            self.cap = Some(percent(s.ram, START_PERCENT));
            self.quiet_since = now;
        }
        self.history.push(now, s.used());
        self.used = self.history.average(now, USED_WINDOW);
        if s.tight {
            // What was in use when memory ran short is what the machine can hold.
            self.cap = Some(self.history.average(now, CAP_WINDOW));
            self.quiet_since = now;
        }

        let at = self.next()?;
        // Nothing admitted is running: the request goes next however little room there is.
        // (A frozen job stops every admission below, and a tight reading too.)
        let alone = self.running.is_empty();
        // Held back by the cap and nothing else, for a minute of quiet: try a little more.
        if !s.tight
            && !s.frozen
            && !alone
            && now - self.quiet_since >= PROBE_AFTER
            && !self.fits(at, learned)
        {
            let max = percent(s.ram, MAX_PERCENT);
            if let Some(cap) = self.cap.as_mut().filter(|cap| **cap < max) {
                *cap = (*cap + percent(s.ram, PROBE_PERCENT)).min(max);
            }
            self.quiet_since = now;
        }
        if s.tight || s.frozen || now - self.last_admission < SPACING {
            return None;
        }
        if !alone && !self.fits(at, learned) {
            return None;
        }

        let need = self.need(&self.waiting[at].signature, learned);
        let w = self.waiting.remove(at);
        self.last_admission = now;
        self.running.push(Running {
            id: w.id,
            session: w.session,
            label: w.label,
            signature: w.signature,
            since: s.wall,
            waited: s.wall - w.since,
            need,
            pgid: None,
            peak: 0,
        });
        Some(w.id)
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
            tight: level <= 40,
            frozen: false,
        }
    }

    /// 20 GiB in use.
    fn calm(time: f64) -> Sample {
        at(time, 80)
    }

    fn unknown(_: &str) -> Option<u64> {
        None
    }

    /// Asks for a command named `name` for `session`; its signature is its name.
    fn ask(q: &mut Scheduler, session: i32, name: &str) -> Id {
        q.enqueue(session, name.into(), name.into(), 0.0)
    }

    fn names(q: &Scheduler) -> Vec<&str> {
        q.running().iter().map(|r| r.label.as_str()).collect()
    }

    /// Readings every second from `from` to `to` inclusive at `level`; the last result.
    fn run(
        q: &mut Scheduler,
        from: u32,
        to: u32,
        level: i32,
        learned: &dyn Fn(&str) -> Option<u64>,
    ) -> Option<Id> {
        (from..=to)
            .map(|t| q.step(&at(f64::from(t), level), learned))
            .last()
            .flatten()
    }

    #[test]
    fn a_command_is_admitted_when_there_is_room() {
        let mut q = Scheduler::new();
        let a = ask(&mut q, 1, "a");
        assert_eq!(q.step(&calm(0.0), &unknown), Some(a));
        assert_eq!(names(&q), ["a"]);
        assert!(q.waiting().is_empty());
        assert_eq!(q.cap(), Some(70 * GIB), "the cap starts at 70% of RAM");
        assert_eq!(
            q.running()[0].need,
            GIB,
            "and a stranger is expected to need 1 GiB"
        );
    }

    /// `used + need < cap` is the rule, so equality waits. With another command already
    /// running (so that forward progress does not apply) a command that needs 15 GiB at 55 GiB
    /// in use waits for the 70 GiB cap, and one that needs 14 GiB does not.
    #[test]
    fn a_command_waits_when_used_plus_need_reaches_the_cap() {
        let learned = |s: &str| match s {
            "edge" => Some(15 * GIB),
            "fits" => Some(14 * GIB),
            _ => None,
        };
        let mut q = Scheduler::new();
        ask(&mut q, 1, "first");
        assert!(q.step(&calm(0.0), &learned).is_some());
        run(&mut q, 1, 10, 45, &learned); // 55 GiB in use, the 5 s average settled
        assert_eq!(q.used(), 55 * GIB);

        let edge = ask(&mut q, 2, "edge");
        assert_eq!(run(&mut q, 11, 20, 45, &learned), None);
        assert_eq!(q.waiting().len(), 1, "55 + 15 = 70 is not under 70");

        q.release(edge);
        let fits = ask(&mut q, 2, "fits");
        assert_eq!(
            q.step(&at(21.0, 45), &learned),
            Some(fits),
            "55 + 14 = 69 is"
        );
    }

    /// Memory the engine calls tight, or a job it holds frozen, stops every admission, however
    /// little is in use.
    #[test]
    fn nothing_is_admitted_while_memory_is_tight_or_a_job_is_frozen() {
        let mut q = Scheduler::new();
        let a = ask(&mut q, 1, "a");
        assert_eq!(q.step(&at(0.0, 35), &unknown), None, "tight");
        let frozen = Sample {
            frozen: true,
            ..calm(1.0)
        };
        assert_eq!(q.step(&frozen, &unknown), None, "frozen");
        let pressure = Sample {
            tight: true,
            ..calm(2.0)
        };
        assert_eq!(q.step(&pressure, &unknown), None, "tight by pressure alone");
        assert_eq!(q.step(&calm(3.0), &unknown), Some(a), "calm and thawed");
    }

    /// The memory in use is the average of the last 5 s, not the last reading: one calm
    /// reading after a long busy spell does not let a big command in, five do.
    #[test]
    fn the_memory_in_use_is_averaged_over_five_seconds() {
        let big = |s: &str| (s == "big").then_some(25 * GIB);
        let mut q = Scheduler::new();
        ask(&mut q, 1, "first");
        assert!(q.step(&calm(0.0), &big).is_some());
        run(&mut q, 1, 10, 45, &big); // 55 GiB in use
        let id = ask(&mut q, 2, "big");
        // 20 GiB in use now, but 4 x 55 + 20 over 5 s is 48, and 48 + 25 does not fit under 70.
        assert_eq!(q.step(&at(11.0, 80), &big), None);
        assert_eq!(q.used(), 48 * GIB);
        // Two readings: (3 x 55 + 2 x 20) / 5 = 41 GiB, and 41 + 25 fits.
        assert_eq!(q.step(&at(12.0, 80), &big), Some(id));
        assert_eq!(q.used(), 41 * GIB);
    }

    #[test]
    fn admissions_are_at_least_three_seconds_apart() {
        let mut q = Scheduler::new();
        let (a, b) = (ask(&mut q, 1, "a"), ask(&mut q, 1, "b"));
        assert_eq!(q.step(&calm(0.0), &unknown), Some(a));
        assert_eq!(q.step(&calm(1.0), &unknown), None);
        assert_eq!(q.step(&calm(2.9), &unknown), None);
        assert_eq!(q.step(&calm(3.0), &unknown), Some(b));
    }

    /// With nothing running the next command starts although it does not fit (a machine that
    /// is busy with other things never gets quieter), but never while memory is tight, and
    /// once it runs the next one has to fit.
    #[test]
    fn the_next_command_starts_when_nothing_runs_even_if_it_does_not_fit() {
        let big = |_: &str| Some(30 * GIB);
        let mut q = Scheduler::new();
        let (a, b) = (ask(&mut q, 1, "a"), ask(&mut q, 1, "b"));
        assert_eq!(q.step(&at(0.0, 35), &big), None, "never while tight");
        // 55 + 30 is over the cap, which the tight reading set to 65.
        assert_eq!(
            q.step(&at(1.0, 45), &big),
            Some(a),
            "alone: forward progress"
        );
        assert_eq!(run(&mut q, 2, 20, 45, &big), None, "now it has to fit");
        assert_eq!(q.waiting().len(), 1);
        q.release(a);
        assert_eq!(q.step(&at(21.0, 45), &big), Some(b), "alone again");
    }

    /// The cap is what was in use over the last 15 s whenever a reading is tight: here ten
    /// calm seconds at 20 GiB and then 75 GiB, and later only the 75 GiB.
    #[test]
    fn the_cap_is_learned_from_a_tight_episode() {
        let mut q = Scheduler::new();
        run(&mut q, 0, 9, 80, &unknown);
        assert_eq!(q.cap(), Some(70 * GIB));
        q.step(&at(10.0, 25), &unknown);
        assert_eq!(q.cap(), Some(25 * GIB), "(10 x 20 + 75) / 11");
        q.step(&at(11.0, 25), &unknown);
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
        let need = |s: &str| (s == "b").then_some(20 * GIB);
        let mut q = Scheduler::new();
        ask(&mut q, 1, "a");
        assert!(q.step(&calm(0.0), &need).is_some());
        run(&mut q, 1, 10, 45, &need); // 55 GiB in use, the 5 s average settled
        let b = ask(&mut q, 2, "b");
        // 55 + 20 = 75 does not fit under 70, nor (equal) under 75.
        for t in 11..=119 {
            assert_eq!(q.step(&at(f64::from(t), 45), &need), None, "t = {t}");
            let cap = match t {
                0..=59 => 70,
                _ => 75,
            };
            assert_eq!(q.cap(), Some(cap * GIB), "t = {t}");
        }
        assert_eq!(
            q.step(&at(120.0, 45), &need),
            Some(b),
            "80 GiB: 55 + 20 fits"
        );
        assert_eq!(q.cap(), Some(80 * GIB));
    }

    #[test]
    fn probing_stops_at_90_percent_of_ram_and_waits_for_a_blocked_request() {
        let huge = |_: &str| Some(95 * GIB);
        let mut q = Scheduler::new();
        ask(&mut q, 1, "a");
        assert!(q.step(&calm(0.0), &huge).is_some());
        // Nobody waiting: nothing is blocked, so the cap stays where it is.
        run(&mut q, 1, 200, 45, &huge);
        assert_eq!(q.cap(), Some(70 * GIB));

        ask(&mut q, 2, "b");
        let mut caps = Vec::new();
        for t in 201..=600 {
            assert_eq!(q.step(&at(f64::from(t), 45), &huge), None);
            caps.push(q.cap().unwrap() / GIB);
        }
        assert_eq!(caps.iter().max(), Some(&90), "never above 90% of RAM");
        assert_eq!(caps.last(), Some(&90));
        assert!(caps.contains(&75) && caps.contains(&80) && caps.contains(&85));
    }

    /// Pressure restarts the 60 s: the next probe is 60 s after the last tight reading, from
    /// the cap that reading learned.
    #[test]
    fn a_tight_reading_restarts_the_minute_before_the_next_probe() {
        let need = |_: &str| Some(40 * GIB);
        let mut q = Scheduler::new();
        ask(&mut q, 1, "a");
        assert!(q.step(&calm(0.0), &need).is_some());
        ask(&mut q, 2, "b");
        run(&mut q, 1, 99, 45, &need);
        assert_eq!(q.cap(), Some(75 * GIB), "probed once, at 60 s");
        q.step(&at(100.0, 35), &need);
        let learned = q.cap().unwrap();
        assert_eq!(learned, (14 * 55 + 65) * GIB / 15, "the last 15 s of use");
        run(&mut q, 101, 159, 45, &need);
        assert_eq!(
            q.cap(),
            Some(learned),
            "no probe 59 s after the tight reading"
        );
        q.step(&at(160.0, 45), &need);
        assert_eq!(q.cap(), Some(learned + 5 * GIB));
    }

    #[test]
    fn what_was_learned_about_a_command_replaces_the_default_need() {
        let mut q = Scheduler::new();
        ask(&mut q, 1, "first");
        assert!(q.step(&calm(0.0), &unknown).is_some());
        run(&mut q, 1, 10, 55, &unknown); // 45 GiB in use, the 5 s average settled
        ask(&mut q, 2, "bun run test");
        let need = |q: &Scheduler, learned: &dyn Fn(&str) -> Option<u64>| match q.standings(learned)
            [0]
        .1
        {
            Standing::Waiting { need, .. } => need,
            Standing::Admitted => panic!("still waiting"),
        };
        assert_eq!(need(&q, &unknown), GIB);
        let learned = |s: &str| (s == "bun run test").then_some(30 * GIB);
        assert_eq!(need(&q, &learned), 30 * GIB);

        // 45 + 30 is over the 70 GiB cap; 45 + the default 1 is not.
        assert_eq!(run(&mut q, 11, 20, 55, &learned), None);
        assert_eq!(
            q.waiting().len(),
            1,
            "held back by what it was seen to need"
        );
        assert!(
            q.step(&at(21.0, 55), &unknown).is_some(),
            "admitted with the default"
        );
    }

    /// A stranger is expected to need a fifth of the cap on a small machine, not a whole GiB.
    #[test]
    fn the_default_need_is_a_fifth_of_the_cap_on_a_small_machine() {
        let mut q = Scheduler::new();
        ask(&mut q, 1, "a");
        let small = Sample {
            ram: 4 * GIB,
            ..calm(0.0)
        };
        let _ = q.step(&small, &unknown);
        let cap = q.cap().unwrap();
        assert_eq!(cap, 4 * GIB * 70 / 100);
        ask(&mut q, 2, "b");
        let (_, standing) = q.standings(&unknown)[0];
        assert_eq!(
            standing,
            Standing::Waiting {
                position: 1,
                need: cap / 5
            }
        );
        assert!(cap / 5 < GIB);
    }

    /// Sessions take turns: agent 1 asks three times before agent 2 asks once, and agent 2's
    /// command is second, not fourth.
    #[test]
    fn sessions_take_turns() {
        let mut q = Scheduler::new();
        for name in ["a1", "a2", "a3"] {
            ask(&mut q, 1, name);
        }
        ask(&mut q, 2, "b1");
        let order = |q: &Scheduler| -> Vec<String> {
            q.standings(&unknown)
                .into_iter()
                .filter_map(|(id, _)| q.waiting().iter().find(|w| w.id == id))
                .map(|w| w.label.clone())
                .collect()
        };
        assert_eq!(order(&q), ["a1", "b1", "a2", "a3"]);
        for (t, expect) in [(0.0, "a1"), (3.0, "b1"), (6.0, "a2"), (9.0, "a3")] {
            let id = q.step(&calm(t), &unknown).expect("admitted");
            assert_eq!(q.running().last().unwrap().id, id);
            assert_eq!(q.running().last().unwrap().label, expect, "t = {t}");
        }
    }

    /// A session that already has a command running goes behind one that has none, however
    /// much older its request is; and ties go to the older request.
    #[test]
    fn the_session_with_fewer_commands_running_goes_first_and_ties_go_to_the_older() {
        let mut q = Scheduler::new();
        ask(&mut q, 1, "running");
        assert!(q.step(&calm(0.0), &unknown).is_some());
        ask(&mut q, 1, "old");
        ask(&mut q, 2, "new");
        ask(&mut q, 3, "newest");
        assert_eq!(
            q.step(&calm(3.0), &unknown).map(|_| names(&q)[1]),
            Some("new")
        );
        // Sessions 1 and 2 have one running each, session 3 none.
        assert_eq!(
            q.step(&calm(6.0), &unknown).map(|_| names(&q)[2]),
            Some("newest")
        );
        // Now every session has one: the older request, "old", is next.
        assert_eq!(
            q.step(&calm(9.0), &unknown).map(|_| names(&q)[3]),
            Some("old")
        );
    }

    /// Within one session the order never changes, whoever else comes and goes, and the
    /// positions told to the clients follow it.
    #[test]
    fn requests_of_one_session_stay_in_arrival_order() {
        let mut q = Scheduler::new();
        let ids: Vec<Id> = ["a", "b", "c", "d"]
            .iter()
            .map(|n| ask(&mut q, 7, n))
            .collect();
        let positions = |q: &Scheduler| {
            q.standings(&unknown)
                .into_iter()
                .map(|(id, st)| match st {
                    Standing::Waiting { position, .. } => (id, position),
                    Standing::Admitted => (id, 0),
                })
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
        for (t, id) in [(0.0, ids[0]), (3.0, ids[2]), (6.0, ids[3])] {
            assert_eq!(q.step(&calm(t), &unknown), Some(id));
        }
    }

    #[test]
    fn a_running_command_is_measured_and_released() {
        let mut q = Scheduler::new();
        let a = ask(&mut q, 1, "a");
        assert!(!q.started(a, 4242), "not admitted yet");
        assert_eq!(q.step(&calm(0.0), &unknown), Some(a));
        assert!(q.started(a, 4242));
        q.observe(a, 3 * GIB);
        q.observe(a, 2 * GIB);
        let Some(Released::Running(job)) = q.release(a) else {
            panic!("it was running");
        };
        assert_eq!((job.pgid, job.peak), (Some(4242), 3 * GIB));
        assert!(q.release(a).is_none(), "released once");
        assert!(q.running().is_empty());
    }
}
