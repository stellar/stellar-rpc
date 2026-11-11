//! A global allocator that counts the heap the process holds through Rust, and
//! ends the process when an allocation would take the count past a limit. It
//! writes one line to stderr and exits with the code the process chose, so
//! the preflight worker that passed its limit ends, and nothing else does.
//!
//! Each block counts as the largest chunk glibc can hand out for it: its size
//! plus an 8-byte header, rounded up to 16 bytes and to at least 32, plus the
//! 16 bytes glibc may leave on it rather than split off. A block glibc maps on
//! its own, from 128 KiB by default, can take up to a page more.
//!
//! It counts only in a process that starts it counting, which only a
//! preflight worker does. Elsewhere, as in the daemon, it passes every
//! allocation straight to the system allocator: a count that every thread
//! updates would make the daemon's threads wait on each other.
//!
//! It lives in this crate so that both static libraries linked into
//! stellar-rpc carry it. Each defines the same allocator symbols, and the
//! linker keeps one.

use std::alloc::{GlobalAlloc, Layout, System};
use std::sync::atomic::{AtomicBool, AtomicI32, AtomicUsize, Ordering::Relaxed};

#[global_allocator]
static ALLOCATOR: LimitedAllocator = LimitedAllocator::new();

/// Starts counting the heap the process holds. An allocation past the limit
/// then ends the process with `exit_code`. The count misses what the process
/// held already, so a process calls this before it allocates.
pub fn start_counting(exit_code: i32) {
    ALLOCATOR.start_counting(exit_code);
}

/// Ends the process when an allocation would take the count past `bytes`.
/// Only counted allocations end it.
pub fn set_limit(bytes: usize) {
    ALLOCATOR.set_limit(bytes);
}

/// The most heap the process may hold, or `None` without a limit.
#[must_use]
pub fn limit() -> Option<usize> {
    ALLOCATOR.limit()
}

/// The most heap the process has held since the last `reset_peak`.
#[must_use]
pub fn peak() -> usize {
    ALLOCATOR.peak()
}

/// Restarts the peak from the heap the process holds now.
pub fn reset_peak() {
    ALLOCATOR.reset_peak();
}

/// The largest chunk glibc can hand out for a block of `size` bytes.
fn block_bytes(size: usize) -> usize {
    (size + 8).next_multiple_of(16).max(32) + 16
}

struct LimitedAllocator {
    counting: AtomicBool,
    exit_code: AtomicI32,
    live: AtomicUsize,
    peak: AtomicUsize,
    limit: AtomicUsize,
}

impl LimitedAllocator {
    const fn new() -> Self {
        Self {
            counting: AtomicBool::new(false),
            exit_code: AtomicI32::new(0),
            live: AtomicUsize::new(0),
            peak: AtomicUsize::new(0),
            limit: AtomicUsize::new(usize::MAX),
        }
    }

    fn start_counting(&self, exit_code: i32) {
        self.exit_code.store(exit_code, Relaxed);
        self.counting.store(true, Relaxed);
    }

    fn set_limit(&self, bytes: usize) {
        self.limit.store(bytes, Relaxed);
    }

    fn limit(&self) -> Option<usize> {
        Some(self.limit.load(Relaxed)).filter(|&limit| limit != usize::MAX)
    }

    fn live(&self) -> usize {
        self.live.load(Relaxed)
    }

    fn peak(&self) -> usize {
        self.peak.load(Relaxed)
    }

    fn reset_peak(&self) {
        self.peak.store(self.live(), Relaxed);
    }

    /// Adds `bytes` to the count, or returns false and leaves the count as it
    /// was when that would take it past the limit.
    fn reserve(&self, bytes: usize) -> bool {
        if !self.counting.load(Relaxed) {
            return true;
        }
        let live = self.live.fetch_add(bytes, Relaxed).wrapping_add(bytes);
        if live > self.limit.load(Relaxed) {
            self.live.fetch_sub(bytes, Relaxed);
            return false;
        }
        self.peak.fetch_max(live, Relaxed);
        true
    }

    fn release(&self, bytes: usize) {
        if !self.counting.load(Relaxed) {
            return;
        }
        // A block allocated before counting started was never counted, so the
        // count stops at zero.
        let mut live = self.live.load(Relaxed);
        while let Err(now) =
            self.live
                .compare_exchange_weak(live, live.saturating_sub(bytes), Relaxed, Relaxed)
        {
            live = now;
        }
    }

    /// Ends the process, writing a fixed line without allocating.
    fn exit(&self) -> ! {
        const LINE: &[u8] = b"the simulation passed its memory limit\n";
        unsafe {
            libc::write(2, LINE.as_ptr().cast(), LINE.len());
            libc::_exit(self.exit_code.load(Relaxed))
        }
    }
}

unsafe impl GlobalAlloc for LimitedAllocator {
    unsafe fn alloc(&self, layout: Layout) -> *mut u8 {
        let bytes = block_bytes(layout.size());
        if !self.reserve(bytes) {
            self.exit();
        }
        let ptr = unsafe { System.alloc(layout) };
        if ptr.is_null() {
            self.release(bytes);
        }
        ptr
    }

    unsafe fn alloc_zeroed(&self, layout: Layout) -> *mut u8 {
        let bytes = block_bytes(layout.size());
        if !self.reserve(bytes) {
            self.exit();
        }
        let ptr = unsafe { System.alloc_zeroed(layout) };
        if ptr.is_null() {
            self.release(bytes);
        }
        ptr
    }

    unsafe fn dealloc(&self, ptr: *mut u8, layout: Layout) {
        unsafe { System.dealloc(ptr, layout) };
        self.release(block_bytes(layout.size()));
    }

    unsafe fn realloc(&self, ptr: *mut u8, layout: Layout, new_size: usize) -> *mut u8 {
        let (old_bytes, new_bytes) = (block_bytes(layout.size()), block_bytes(new_size));
        if new_bytes > old_bytes && !self.reserve(new_bytes - old_bytes) {
            self.exit();
        }
        let new_ptr = unsafe { System.realloc(ptr, layout, new_size) };
        if new_ptr.is_null() {
            if new_bytes > old_bytes {
                self.release(new_bytes - old_bytes);
            }
        } else if new_bytes < old_bytes {
            self.release(old_bytes - new_bytes);
        }
        new_ptr
    }
}

#[cfg(test)]
mod tests {
    use super::*;

    fn layout(size: usize) -> Layout {
        Layout::from_size_align(size, 8).unwrap()
    }

    fn counting() -> LimitedAllocator {
        let heap = LimitedAllocator::new();
        heap.start_counting(3);
        heap
    }

    #[test]
    fn counts_the_largest_chunk_glibc_can_hand_out_for_a_block() {
        // The smallest chunk, 32 bytes, plus 16.
        assert_eq!((block_bytes(1), block_bytes(24)), (48, 48));
        // 25 bytes and a header need a 48-byte chunk.
        assert_eq!(block_bytes(25), 64);
        for size in 1_usize..=8192 {
            assert!(block_bytes(size) >= size + 8 + 16, "{size}");
            assert_eq!(block_bytes(size) % 16, 0, "{size}");
        }
    }

    #[test]
    fn counts_live_blocks_and_their_peak() {
        let heap = counting();
        let (a_bytes, b_bytes) = (block_bytes(100), block_bytes(50));
        unsafe {
            let a = heap.alloc(layout(100));
            let b = heap.alloc(layout(50));
            assert_eq!(heap.live(), a_bytes + b_bytes);
            heap.dealloc(a, layout(100));
            assert_eq!((heap.live(), heap.peak()), (b_bytes, a_bytes + b_bytes));
            heap.reset_peak();
            assert_eq!(heap.peak(), b_bytes);
            heap.dealloc(b, layout(50));
            assert_eq!((heap.live(), heap.peak()), (0, b_bytes));
        }
    }

    #[test]
    fn refuses_reservations_past_the_limit() {
        let heap = counting();
        assert_eq!(heap.limit(), None);
        heap.set_limit(100);
        assert_eq!(heap.limit(), Some(100));
        assert!(heap.reserve(60));
        assert!(!heap.reserve(41));
        assert_eq!((heap.live(), heap.peak()), (60, 60));
        assert!(heap.reserve(40));
        assert_eq!((heap.live(), heap.peak()), (100, 100));
    }

    #[test]
    fn realloc_counts_growth_and_shrinkage() {
        let heap = counting();
        unsafe {
            let a = heap.alloc(layout(10));
            a.write_bytes(7, 10);
            let a = heap.realloc(a, layout(10), 80);
            assert!(!a.is_null());
            assert_eq!(*a.add(9), 7);
            assert_eq!(heap.live(), block_bytes(80));
            let a = heap.realloc(a, layout(80), 20);
            assert!(!a.is_null());
            assert_eq!(*a.add(9), 7);
            assert_eq!(
                (heap.live(), heap.peak()),
                (block_bytes(20), block_bytes(80))
            );
            heap.dealloc(a, layout(20));
        }
        assert_eq!(heap.live(), 0);
    }

    #[test]
    fn alloc_zeroed_counts_zeroed_blocks() {
        let heap = counting();
        unsafe {
            let a = heap.alloc_zeroed(layout(64));
            assert!(std::slice::from_raw_parts(a, 64).iter().all(|&b| b == 0));
            assert_eq!(heap.live(), block_bytes(64));
            heap.dealloc(a, layout(64));
        }
        assert_eq!(heap.live(), 0);
    }

    #[test]
    fn counts_and_refuses_nothing_until_counting_starts() {
        let heap = LimitedAllocator::new();
        heap.set_limit(10);
        assert!(heap.reserve(100));
        assert_eq!((heap.live(), heap.peak()), (0, 0));
        heap.start_counting(3);
        assert!(heap.reserve(5));
        assert_eq!(heap.live(), 5);
        // Releasing a block that was never counted leaves the count at zero
        // rather than below it.
        heap.release(100);
        assert_eq!(heap.live(), 0);
    }
}
