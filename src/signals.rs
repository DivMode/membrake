//! What the kernel runs out of, read straight from sysctl and statfs.
//!
//! Nothing here spawns a process or allocates, so the sampling loop is safe to run once a
//! second on a machine that is short of memory.

use std::ffi::CStr;
use std::fmt;
use std::mem::MaybeUninit;
use std::time::{SystemTime, UNIX_EPOCH};

/// The kernel declares the compressor exhausted at 98% of either limit.
pub const KERNEL_LIMIT: f64 = 0.98;

/// The volume that holds the swap files.
const SWAP_VOLUME: &CStr = c"/System/Volumes/VM";

/// `kern.memorystatus_vm_pressure_level`. It ignores swap and can read normal while the
/// machine thrashes, so the engine uses it only as a backstop.
#[derive(Clone, Copy, Debug, PartialEq, Eq, PartialOrd, Ord)]
pub enum Pressure {
    Normal = 1,
    Warn = 2,
    Critical = 4,
}

impl Pressure {
    pub fn from_raw(raw: u64) -> Pressure {
        match raw {
            4.. => Pressure::Critical,
            2..=3 => Pressure::Warn,
            _ => Pressure::Normal,
        }
    }
}

/// One raw reading. The derived values (fill rate, tightness) belong to the engine.
#[derive(Clone, Copy, Debug, PartialEq)]
pub struct Signals {
    /// Engine time: seconds of awake uptime (`CLOCK_UPTIME_RAW`). It never steps and does not
    /// advance while the Mac sleeps, so fill rates, deadlines and the maximum freeze time
    /// count only time the machine was running.
    pub time: f64,
    /// Seconds since the Unix epoch, for log lines and the recorder only.
    pub wall: f64,
    /// Compressor segments in use over the segment limit, 0..1.
    pub segments: f64,
    /// Compressed pages over the compressed-page limit, 0..1.
    pub pages: f64,
    pub swap_used: u64,
    /// Room left for swap: free bytes on the swap volume plus unused swapfile space.
    pub headroom: u64,
    pub ram: u64,
    /// `kern.memorystatus_level`: percent of memory the kernel counts as available.
    pub level: i32,
    pub pressure: Pressure,
}

impl Signals {
    /// Distance to the cliff: whichever compressor limit is closer.
    pub fn occupancy(&self) -> f64 {
        self.segments.max(self.pages)
    }
}

#[derive(Debug)]
pub struct ReadError {
    pub what: &'static str,
    pub source: std::io::Error,
}

impl fmt::Display for ReadError {
    fn fmt(&self, f: &mut fmt::Formatter<'_>) -> fmt::Result {
        write!(f, "{}: {}", self.what, self.source)
    }
}

impl std::error::Error for ReadError {}

/// Wall-clock seconds since the Unix epoch. Only for timestamps; it can step.
pub fn wall() -> f64 {
    SystemTime::now()
        .duration_since(UNIX_EPOCH)
        .map_or(0.0, |d| d.as_secs_f64())
}

/// Seconds of awake uptime: monotonic, and stopped while the machine sleeps.
pub fn uptime() -> f64 {
    let mut ts = libc::timespec {
        tv_sec: 0,
        tv_nsec: 0,
    };
    // SAFETY: `ts` is a valid timespec.
    unsafe { libc::clock_gettime(libc::CLOCK_UPTIME_RAW, &mut ts) };
    ts.tv_sec as f64 + ts.tv_nsec as f64 * 1e-9
}

/// Reads a 4- or 8-byte integer sysctl. A 4-byte value lands in the low half of the zeroed
/// buffer on a little-endian machine, which is every Mac this runs on.
fn sysctl_u64(name: &'static CStr) -> Result<u64, ReadError> {
    let mut value: u64 = 0;
    let mut len = size_of::<u64>();
    // SAFETY: `value` is a valid 8-byte buffer and `len` says so.
    let rc = unsafe {
        libc::sysctlbyname(
            name.as_ptr(),
            (&raw mut value).cast(),
            &mut len,
            std::ptr::null_mut(),
            0,
        )
    };
    if rc != 0 {
        return Err(ReadError {
            what: name.to_str().unwrap_or("sysctl"),
            source: std::io::Error::last_os_error(),
        });
    }
    Ok(value)
}

fn ratio(used: &'static CStr, limit: &'static CStr) -> Result<f64, ReadError> {
    let limit = sysctl_u64(limit)?;
    Ok(if limit == 0 {
        0.0
    } else {
        sysctl_u64(used)? as f64 / limit as f64
    })
}

fn swap_usage() -> Result<libc::xsw_usage, ReadError> {
    let mut usage = MaybeUninit::<libc::xsw_usage>::zeroed();
    let mut len = size_of::<libc::xsw_usage>();
    // SAFETY: the buffer is a zeroed `xsw_usage` of exactly `len` bytes.
    let rc = unsafe {
        libc::sysctlbyname(
            c"vm.swapusage".as_ptr(),
            usage.as_mut_ptr().cast(),
            &mut len,
            std::ptr::null_mut(),
            0,
        )
    };
    if rc != 0 {
        return Err(ReadError {
            what: "vm.swapusage",
            source: std::io::Error::last_os_error(),
        });
    }
    // SAFETY: the kernel filled it, and zero is a valid bit pattern for every field.
    Ok(unsafe { usage.assume_init() })
}

fn free_bytes(path: &CStr) -> Result<u64, ReadError> {
    let mut st = MaybeUninit::<libc::statfs>::zeroed();
    // SAFETY: `path` is NUL-terminated and `st` is a valid `statfs` buffer.
    if unsafe { libc::statfs(path.as_ptr(), st.as_mut_ptr()) } != 0 {
        return Err(ReadError {
            what: "statfs of the swap volume",
            source: std::io::Error::last_os_error(),
        });
    }
    // SAFETY: statfs succeeded.
    let st = unsafe { st.assume_init() };
    Ok(st.f_bavail * u64::from(st.f_bsize))
}

/// Takes one reading. Fails, naming the call, if the kernel refuses any of them.
pub fn read() -> Result<Signals, ReadError> {
    let swap = swap_usage()?;
    Ok(Signals {
        time: uptime(),
        wall: wall(),
        segments: ratio(
            c"vm.compressor.segment.total",
            c"vm.compressor.segment.limit",
        )?,
        pages: ratio(
            c"vm.compressor.pages_compressed",
            c"vm.compressor.pages_compressed_limit",
        )?,
        swap_used: swap.xsu_used,
        headroom: free_bytes(SWAP_VOLUME)? + swap.xsu_avail,
        ram: sysctl_u64(c"hw.memsize")?,
        level: i32::try_from(sysctl_u64(c"kern.memorystatus_level")? & 0xFFFF_FFFF).unwrap_or(0),
        pressure: Pressure::from_raw(sysctl_u64(c"kern.memorystatus_vm_pressure_level")?),
    })
}

#[cfg(test)]
mod tests {
    use super::*;

    /// Catches a wrong sysctl name or width: every value must come back plausible on the
    /// machine running the tests.
    #[test]
    fn live_reading_is_plausible() {
        let s = read().expect("all signal sysctls exist on macOS");
        assert!(s.ram >= 1 << 30, "ram {}", s.ram);
        assert!(
            (0.0..=1.0).contains(&s.occupancy()),
            "occupancy {}",
            s.occupancy()
        );
        assert!((0..=100).contains(&s.level), "level {}", s.level);
        assert!(s.headroom > 0);
        assert!(s.wall > 1.7e9);
        assert!(s.time > 0.0 && s.time < s.wall);
    }
}
