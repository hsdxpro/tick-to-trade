//! Pinning a thread to a core, because an unpinned latency-critical thread
//! measures the scheduler.
//!
//! A migrated thread arrives on a core whose caches hold nothing of its
//! working set: the book, the ring's cache lines, the parser's tables all
//! have to be pulled across again. That shows up exactly where it hurts, in
//! the tail — and the tail is what a trading system is judged on.
//!
//! Pinning alone is half the job. The other half is telling the OS to leave
//! the core alone (`isolcpus` and `nohz_full` on Linux), which is a boot
//! argument rather than code and is stated in the README instead of pretended
//! at here.

/// Pins the calling thread to `core`. Returns whether the OS agreed.
///
/// A failure is reported rather than fatal: a container with a restricted
/// CPU mask is a legitimate deployment, and it should run slower, not refuse
/// to start.
#[must_use]
pub fn pin_to(core: usize) -> bool {
    platform::pin_to(core)
}

/// Cores the process may actually use.
#[must_use]
pub fn available() -> usize {
    std::thread::available_parallelism().map_or(1, std::num::NonZero::get)
}

#[cfg(target_os = "linux")]
mod platform {
    pub fn pin_to(core: usize) -> bool {
        // SAFETY: `set` is zeroed then written only through the kernel's own
        // macro-equivalent bit arithmetic, and its size is passed explicitly.
        unsafe {
            let mut set: libc::cpu_set_t = std::mem::zeroed();
            libc::CPU_SET(core, &mut set);
            libc::sched_setaffinity(0, std::mem::size_of::<libc::cpu_set_t>(), &raw const set) == 0
        }
    }
}

#[cfg(windows)]
mod platform {
    // Declared rather than pulled from a crate: two symbols do not justify a
    // dependency, and the signatures are stable ABI.
    unsafe extern "system" {
        fn GetCurrentThread() -> isize;
        fn SetThreadAffinityMask(thread: isize, mask: usize) -> usize;
    }

    pub fn pin_to(core: usize) -> bool {
        if core >= usize::BITS as usize {
            return false; // beyond one processor group; needs the group API
        }
        // SAFETY: both calls take only integers and return only integers.
        unsafe { SetThreadAffinityMask(GetCurrentThread(), 1_usize << core) != 0 }
    }
}

#[cfg(not(any(target_os = "linux", windows)))]
mod platform {
    pub fn pin_to(_core: usize) -> bool {
        false
    }
}

#[cfg(test)]
mod tests {
    use super::*;

    #[test]
    fn pinning_reports_honestly_and_does_not_wedge_the_thread() {
        let pinned = pin_to(0);
        // Whatever the answer, the thread must still run and still be able to
        // observe a core count.
        assert!(available() >= 1);
        if pinned {
            // Re-pinning to the same core is idempotent, not an error.
            assert!(pin_to(0));
        }
    }

    #[test]
    fn an_impossible_core_is_refused_rather_than_silently_ignored() {
        assert!(!pin_to(usize::BITS as usize + 1));
    }
}
