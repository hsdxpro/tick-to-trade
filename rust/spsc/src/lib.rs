//! A single-producer single-consumer ring, the way the pipeline actually uses
//! one: two pinned threads, one direction, no waiting.
//!
//! # Why unsafe is here, and what answers for it
//!
//! A wait-free SPSC ring cannot be written in safe Rust: two threads hold
//! references into one buffer at once, and the proof that they never hold the
//! *same* slot is a protocol, not a type. The protocol is small enough to state
//! completely — it is the two invariants below — and it is checked by [`loom`],
//! which model-checks every reachable interleaving of the atomics rather than
//! hoping a stress test stumbles onto the bad one. `cargo test --release`
//! covers behaviour; `RUSTFLAGS="--cfg loom" cargo test` covers orderings.
//!
//! # The protocol
//!
//! Indices increase forever and are masked only at slot access, so full and
//! empty are never ambiguous: `head - tail == capacity` is full, `head == tail`
//! is empty.
//!
//! - The producer owns `head`. Only it writes `head`, so it reads its own plain
//!   copy and never pays for an atomic load of it.
//! - The consumer owns `tail`, symmetrically.
//! - A slot is handed over by **publishing the index after touching the slot**:
//!   the producer writes the value, then stores `head + 1` with `Release`; the
//!   consumer loads `head` with `Acquire` before reading the value. The pair
//!   orders the slot write before the slot read. Hand-back goes the same way
//!   through `tail`.
//!
//! # Why each side caches the other's index
//!
//! The hot path costs one atomic load of the *other* side's index per
//! operation — and even that is usually avoided. Each side keeps a stale copy
//! of the other's index and refreshes it only when the ring *looks* full or
//! empty. Between refreshes, producer and consumer touch disjoint cache lines
//! entirely: no coherence traffic, which is where a naive ring loses most of
//! its throughput. The fields are cache-line padded for the same reason —
//! `head` and `tail` on one line would make every publish invalidate the
//! other core's line even though neither reads the other's field.

#[cfg(loom)]
use loom::cell::UnsafeCell;
#[cfg(loom)]
use loom::sync::atomic::{AtomicUsize, Ordering};

#[cfg(not(loom))]
use core::cell::UnsafeCell;
#[cfg(not(loom))]
use core::sync::atomic::{AtomicUsize, Ordering};

use std::mem::MaybeUninit;
use std::sync::Arc;

/// Pads to a cache line, so two fields never share one.
///
/// 64 bytes on every x86-64 and most aarch64 parts. On the few parts where the
/// destructive interference size is 128 (some Apple and Neoverse cores), 64
/// still halves the false sharing rather than eliminating it; the constant is
/// one place to change.
#[repr(align(64))]
#[derive(Debug, Default)]
struct CachePadded<T>(T);

struct Shared<T> {
    /// Next index the producer will write. Written by the producer only.
    head: CachePadded<AtomicUsize>,
    /// Next index the consumer will read. Written by the consumer only.
    tail: CachePadded<AtomicUsize>,
    buffer: Box<[UnsafeCell<MaybeUninit<T>>]>,
    /// `capacity - 1`; capacity is a power of two so masking replaces modulo.
    mask: usize,
}

// SAFETY: the protocol above is exactly the argument. Distinct slots are
// handed across threads by the Release/Acquire pairs, and no slot is
// reachable by both sides at once, so sharing `Shared` is sound whenever the
// items themselves may cross threads.
unsafe impl<T: Send> Send for Shared<T> {}
// SAFETY: as above -- the two halves never touch the same slot concurrently.
unsafe impl<T: Send> Sync for Shared<T> {}

impl<T> std::fmt::Debug for Shared<T> {
    fn fmt(&self, f: &mut std::fmt::Formatter<'_>) -> std::fmt::Result {
        f.debug_struct("Shared")
            .field("capacity", &(self.mask + 1))
            .finish_non_exhaustive()
    }
}

/// The sending half. `Send`, not `Sync`: exactly one thread may hold it.
#[derive(Debug)]
pub struct Producer<T> {
    shared: Arc<Shared<T>>,
    /// The producer's own copy of `head`. It is the only writer, so this is
    /// always exact and costs nothing to read.
    head: usize,
    /// Stale copy of the consumer's `tail`, refreshed only when the ring looks
    /// full. Staleness is always conservative: the real tail can only have
    /// moved forward, so acting on the stale value never overwrites.
    cached_tail: usize,
}

/// The receiving half. `Send`, not `Sync`.
#[derive(Debug)]
pub struct Consumer<T> {
    shared: Arc<Shared<T>>,
    tail: usize,
    /// Stale copy of the producer's `head`, refreshed only when the ring looks
    /// empty. Conservative for the symmetric reason: it can only lag, so
    /// acting on it never reads an unwritten slot.
    cached_head: usize,
}

/// Creates a ring holding at most `capacity` items.
///
/// `capacity` is rounded up to a power of two so the index mask replaces a
/// modulo on the hot path.
///
/// # Panics
/// If `capacity` is zero.
#[must_use]
pub fn channel<T>(capacity: usize) -> (Producer<T>, Consumer<T>) {
    assert!(
        capacity > 0,
        "a zero-capacity ring can never accept an item"
    );
    let capacity = capacity.next_power_of_two();

    #[cfg(not(loom))]
    let buffer = (0..capacity)
        .map(|_| UnsafeCell::new(MaybeUninit::uninit()))
        .collect();
    #[cfg(loom)]
    let buffer = (0..capacity)
        .map(|_| UnsafeCell::new(MaybeUninit::uninit()))
        .collect();

    let shared = Arc::new(Shared {
        head: CachePadded(AtomicUsize::new(0)),
        tail: CachePadded(AtomicUsize::new(0)),
        buffer,
        mask: capacity - 1,
    });
    (
        Producer {
            shared: Arc::clone(&shared),
            head: 0,
            cached_tail: 0,
        },
        Consumer {
            shared,
            tail: 0,
            cached_head: 0,
        },
    )
}

impl<T> Producer<T> {
    /// Hands `value` to the consumer, or returns it if the ring is full.
    ///
    /// Wait-free: one slot write, one `Release` store, and at worst one
    /// `Acquire` load when the cached tail has gone stale.
    ///
    /// # Errors
    /// Returns `Err(value)` when the ring is full, so the caller decides what
    /// full means — drop, spin, or count it. A queue that blocks would decide
    /// for them, on the hot path.
    pub fn try_push(&mut self, value: T) -> Result<(), T> {
        let capacity = self.shared.mask + 1;
        if self.head - self.cached_tail == capacity {
            // Looks full. The truth can only be equal or better, because tail
            // only moves forward. Acquire pairs with the consumer's Release
            // store of tail: everything the consumer did to vacate the slot
            // happened before this load says it is vacant.
            self.cached_tail = self.shared.tail.0.load(Ordering::Acquire);
            if self.head - self.cached_tail == capacity {
                return Err(value);
            }
        }

        let slot = &self.shared.buffer[self.head & self.shared.mask];
        // SAFETY: the fullness check above proved the consumer cannot reach
        // this slot until the Release store below publishes it, so the write
        // is unaliased.
        #[cfg(loom)]
        slot.with_mut(|p| unsafe { (*p).write(value) });
        #[cfg(not(loom))]
        unsafe {
            (*slot.get()).write(value);
        }

        self.head += 1;
        // Release publishes the slot write above to the consumer's Acquire.
        self.shared.head.0.store(self.head, Ordering::Release);
        Ok(())
    }

    /// Items currently in the ring, as an estimate: exact for the producer's
    /// own writes, stale by at most the consumer's progress since the last
    /// refresh.
    #[must_use]
    pub fn len(&self) -> usize {
        self.head - self.shared.tail.0.load(Ordering::Acquire)
    }

    #[must_use]
    pub fn is_empty(&self) -> bool {
        self.len() == 0
    }
}

impl<T> Consumer<T> {
    /// Takes the oldest item, or `None` if the ring is empty.
    ///
    /// Wait-free, with the same cost shape as [`Producer::try_push`].
    pub fn try_pop(&mut self) -> Option<T> {
        if self.cached_head == self.tail {
            // Looks empty. Acquire pairs with the producer's Release store of
            // head: everything the producer wrote into the slot happened
            // before this load says the slot is filled.
            self.cached_head = self.shared.head.0.load(Ordering::Acquire);
            if self.cached_head == self.tail {
                return None;
            }
        }

        let slot = &self.shared.buffer[self.tail & self.shared.mask];
        // SAFETY: the emptiness check proved the producer published this slot
        // -- so it is initialized -- and cannot touch it again until the
        // Release store below hands it back.
        #[cfg(loom)]
        let value = slot.with_mut(|p| unsafe { (*p).assume_init_read() });
        #[cfg(not(loom))]
        let value = unsafe { (*slot.get()).assume_init_read() };

        self.tail += 1;
        // Release hands the vacated slot back to the producer's Acquire.
        self.shared.tail.0.store(self.tail, Ordering::Release);
        Some(value)
    }

    /// Items currently readable. Exact for what the consumer has taken, stale
    /// by at most the producer's progress since the last refresh.
    #[must_use]
    pub fn len(&self) -> usize {
        self.shared.head.0.load(Ordering::Acquire) - self.tail
    }

    #[must_use]
    pub fn is_empty(&self) -> bool {
        self.len() == 0
    }
}

/// Drops whatever was pushed and never popped.
///
/// On the consumer's side of the fence: by the time both halves are gone, the
/// last `head` store happened-before this thread observed the halves dropped,
/// so reading the occupied range is ordered. Items must be dropped exactly
/// once, and the test suite counts.
impl<T> Drop for Shared<T> {
    fn drop(&mut self) {
        #[cfg(loom)]
        let (head, tail) = (
            self.head.0.load(Ordering::Relaxed),
            self.tail.0.load(Ordering::Relaxed),
        );
        #[cfg(not(loom))]
        let (head, tail) = (
            self.head.0.load(Ordering::Relaxed),
            self.tail.0.load(Ordering::Relaxed),
        );
        for index in tail..head {
            let slot = &mut self.buffer[index & self.mask];
            // SAFETY: this drop runs on the last owner, so no other thread
            // exists, and `tail..head` is precisely the initialized,
            // unconsumed range.
            #[cfg(loom)]
            slot.with_mut(|p| unsafe { (*p).assume_init_drop() });
            #[cfg(not(loom))]
            unsafe {
                (*slot.get()).assume_init_drop();
            }
        }
    }
}

#[cfg(all(test, not(loom)))]
mod tests {
    use super::*;
    use std::sync::atomic::{AtomicUsize as StdAtomicUsize, Ordering as StdOrdering};

    #[test]
    fn values_cross_in_order() {
        let (mut tx, mut rx) = channel(4);
        for i in 0..4 {
            tx.try_push(i).unwrap();
        }
        for i in 0..4 {
            assert_eq!(rx.try_pop(), Some(i));
        }
        assert_eq!(rx.try_pop(), None);
    }

    #[test]
    fn full_returns_the_value_instead_of_losing_it() {
        let (mut tx, mut rx) = channel(2);
        tx.try_push(1).unwrap();
        tx.try_push(2).unwrap();
        assert_eq!(tx.try_push(3), Err(3));
        assert_eq!(rx.try_pop(), Some(1));
        tx.try_push(3).unwrap();
        assert_eq!(rx.try_pop(), Some(2));
        assert_eq!(rx.try_pop(), Some(3));
    }

    #[test]
    fn capacity_rounds_up_to_a_power_of_two() {
        let (mut tx, _rx) = channel(3);
        for i in 0..4 {
            tx.try_push(i).unwrap();
        }
        assert_eq!(tx.try_push(4), Err(4));
    }

    #[test]
    fn wraps_correctly_across_many_generations() {
        // Indices mask into 4 slots but count to 40,000: every slot is reused
        // ten thousand times, which is what catches a masking mistake that one
        // pass over the buffer cannot.
        let (mut tx, mut rx) = channel(4);
        for i in 0_u64..40_000 {
            tx.try_push(i).unwrap();
            assert_eq!(rx.try_pop(), Some(i));
        }
    }

    #[test]
    fn cross_thread_stream_arrives_complete_and_ordered() {
        const COUNT: u64 = 1_000_000;
        let (mut tx, mut rx) = channel(1024);
        let producer = std::thread::spawn(move || {
            for i in 0..COUNT {
                let mut item = i;
                loop {
                    match tx.try_push(item) {
                        Ok(()) => break,
                        Err(back) => {
                            item = back;
                            std::hint::spin_loop();
                        }
                    }
                }
            }
        });
        let mut expected = 0;
        while expected < COUNT {
            if let Some(got) = rx.try_pop() {
                assert_eq!(got, expected, "reordered or duplicated in flight");
                expected += 1;
            } else {
                std::hint::spin_loop();
            }
        }
        producer.join().unwrap();
        assert_eq!(rx.try_pop(), None);
    }

    #[test]
    fn undelivered_items_drop_exactly_once() {
        static DROPS: StdAtomicUsize = StdAtomicUsize::new(0);
        #[derive(Debug)]
        struct Counted;
        impl Drop for Counted {
            fn drop(&mut self) {
                DROPS.fetch_add(1, StdOrdering::SeqCst);
            }
        }

        let (mut tx, mut rx) = channel(8);
        for _ in 0..5 {
            tx.try_push(Counted).unwrap();
        }
        drop(rx.try_pop().unwrap()); // one delivered and dropped by the caller
        drop(tx);
        drop(rx); // four still in the ring when the last half goes
        assert_eq!(
            DROPS.load(StdOrdering::SeqCst),
            5,
            "an in-flight item leaked or double-dropped"
        );
    }
}

/// Every reachable interleaving of the atomics, not a sample of them.
///
/// Run with `RUSTFLAGS="--cfg loom" cargo test -p t2t-spsc --release`. The
/// scenarios are small — loom's state space is exponential in operations — but
/// they are chosen to force every protocol edge: wraparound, the full check,
/// the empty check, and the handoff in both directions.
#[cfg(all(test, loom))]
mod loom_tests {
    use super::*;

    #[test]
    fn handoff_is_ordered_across_wraparound() {
        loom::model(|| {
            // Capacity 2, 4 items: the ring wraps mid-scenario, so the model
            // covers both the fresh-slot and reused-slot handoffs.
            let (mut tx, mut rx) = channel(2);
            let producer = loom::thread::spawn(move || {
                let mut sent = 0_u32;
                while sent < 4 {
                    if tx.try_push(sent).is_ok() {
                        sent += 1;
                    } else {
                        loom::thread::yield_now();
                    }
                }
            });
            let mut expected = 0_u32;
            while expected < 4 {
                match rx.try_pop() {
                    Some(got) => {
                        assert_eq!(got, expected);
                        expected += 1;
                    }
                    None => loom::thread::yield_now(),
                }
            }
            producer.join().unwrap();
        });
    }

    #[test]
    fn a_full_ring_never_overwrites() {
        loom::model(|| {
            let (mut tx, mut rx) = channel(1);
            tx.try_push(1_u32).unwrap();
            let producer = loom::thread::spawn(move || {
                // Either outcome is legal; overwriting is not, and loom would
                // catch it as a data race on the occupied slot.
                let _ = tx.try_push(2);
                tx
            });
            let first = rx.try_pop();
            assert!(first.is_none() || first == Some(1));
            let _ = producer.join().unwrap();
        });
    }

    #[test]
    fn drop_never_races_the_last_operation() {
        loom::model(|| {
            let (mut tx, rx) = channel(2);
            tx.try_push(Box::new(7_u64)).unwrap();
            let consumer = loom::thread::spawn(move || drop(rx));
            drop(tx);
            consumer.join().unwrap();
        });
    }
}
