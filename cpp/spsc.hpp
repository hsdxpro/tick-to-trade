#pragma once

// A single-producer single-consumer ring: two pinned threads, one direction,
// no waiting. The mirror of the Rust implementation, protocol-identical, so
// the two can be benchmarked against each other and reviewed side by side.
//
// The protocol, stated completely:
//
//   * Indices increase forever and are masked only at slot access, so full and
//     empty are never ambiguous: head - tail == capacity is full, head == tail
//     is empty.
//   * The producer owns head; the consumer owns tail. Each side reads its own
//     index as a plain member -- it is the only writer -- and publishes it
//     atomically for the other side.
//   * A slot is handed over by publishing the index AFTER touching the slot:
//     the producer constructs the value, then stores head + 1 with release;
//     the consumer loads head with acquire before reading the value. The pair
//     orders the slot write before the slot read. Hand-back is symmetric
//     through tail.
//
// Each side caches the other's index and refreshes only when the ring looks
// full or empty, so between refreshes the two threads touch disjoint cache
// lines and generate no coherence traffic. The atomics are cache-line aligned
// for the same reason: head and tail on one line would make every publish
// invalidate the other core's copy of a field it never reads.
//
// Verification is split by tool: the unit tests cover behaviour, and the same
// tests compiled with -fsanitize=thread cover the orderings -- TSAN instruments
// the acquire/release edges, so weakening one to relaxed is reported as a data
// race on the slot rather than passing quietly. The Rust twin goes further
// with loom, which explores every interleaving; agreement between the two
// implementations under their respective checkers is the point of having two.

#include <atomic>
#include <bit>
#include <cassert>
#include <cstddef>
#include <memory>
#include <new>
#include <optional>
#include <utility>

namespace t2t {

// 64 on every mainstream x86-64 part and most aarch64. Some standard libraries
// define std::hardware_destructive_interference_size, but its value is allowed
// to differ between translation units compiled with different flags, which is
// a subtle ODR hazard; a named constant is one place to change and cannot
// drift.
inline constexpr std::size_t kCacheLine = 64;

template <typename T>
class SpscQueue final {
public:
    /// Capacity is rounded up to a power of two so a mask replaces modulo on
    /// the hot path.
    explicit SpscQueue(std::size_t capacity)
        : mask_(std::bit_ceil(capacity < 2 ? std::size_t{2} : capacity) - 1),
          slots_(static_cast<Slot*>(::operator new[](
              (mask_ + 1) * sizeof(Slot), std::align_val_t{alignof(Slot)}))) {
        assert(capacity > 0 && "a zero-capacity ring can never accept an item");
    }

    ~SpscQueue() {
        // Only the occupied range holds constructed objects.
        const auto head = head_.value.load(std::memory_order_relaxed);
        for (auto tail = tail_.value.load(std::memory_order_relaxed); tail != head; ++tail) {
            slots_[tail & mask_].destroy();
        }
        ::operator delete[](slots_, std::align_val_t{alignof(Slot)});
    }

    SpscQueue(const SpscQueue&) = delete;
    SpscQueue& operator=(const SpscQueue&) = delete;
    SpscQueue(SpscQueue&&) = delete;
    SpscQueue& operator=(SpscQueue&&) = delete;

    /// Hands `value` to the consumer. Returns false -- leaving `value` intact --
    /// when the ring is full, so the caller decides what full means: drop,
    /// spin, or count it.
    ///
    /// Wait-free: one construct, one release store, and at worst one acquire
    /// load when the cached tail has gone stale. Called by the producer thread
    /// only; that exclusivity is the S in SPSC and is asserted by the checker
    /// builds rather than a runtime branch.
    [[nodiscard]] bool try_push(T&& value) {
        const auto head = producer_head_;
        if (head - producer_cached_tail_ == mask_ + 1) {
            // Looks full. The truth can only be better: tail only moves
            // forward. Acquire pairs with the consumer's release store of
            // tail, ordering the consumer's read of the slot before this
            // thread's reuse of it.
            producer_cached_tail_ = tail_.value.load(std::memory_order_acquire);
            if (head - producer_cached_tail_ == mask_ + 1) {
                return false;
            }
        }
        slots_[head & mask_].construct(std::move(value));
        producer_head_ = head + 1;
        // Release publishes the construction above to the consumer's acquire.
        head_.value.store(head + 1, std::memory_order_release);
        return true;
    }

    /// Takes the oldest item, or nothing if the ring is empty. Wait-free, with
    /// the same cost shape as try_push. Consumer thread only.
    [[nodiscard]] std::optional<T> try_pop() {
        const auto tail = consumer_tail_;
        if (consumer_cached_head_ == tail) {
            // Looks empty. Acquire pairs with the producer's release store of
            // head: the construction happened before this load says it did.
            consumer_cached_head_ = head_.value.load(std::memory_order_acquire);
            if (consumer_cached_head_ == tail) {
                return std::nullopt;
            }
        }
        auto& slot = slots_[tail & mask_];
        std::optional<T> value{std::move(slot.ref())};
        slot.destroy();
        consumer_tail_ = tail + 1;
        // Release hands the vacated slot back to the producer's acquire.
        tail_.value.store(tail + 1, std::memory_order_release);
        return value;
    }

    /// An estimate: exact for the calling side's own progress, stale by at
    /// most the other side's progress since its last publish.
    [[nodiscard]] std::size_t size() const {
        const auto head = head_.value.load(std::memory_order_acquire);
        const auto tail = tail_.value.load(std::memory_order_acquire);
        return head - tail;
    }

    [[nodiscard]] std::size_t capacity() const { return mask_ + 1; }

private:
    /// Storage for one T whose lifetime is managed by hand, because slots are
    /// constructed by one thread and destroyed by another and neither event
    /// coincides with the ring's own lifetime.
    struct Slot {
        alignas(T) std::byte storage[sizeof(T)];

        void construct(T&& value) { ::new (storage) T(std::move(value)); }
        T& ref() { return *std::launder(reinterpret_cast<T*>(storage)); }
        void destroy() { ref().~T(); }
    };

    /// Aligned so the two indices, and the producer's and consumer's private
    /// fields, each live on their own line.
    struct alignas(kCacheLine) PaddedIndex {
        std::atomic<std::size_t> value{0};
    };

    const std::size_t mask_;
    Slot* const slots_;

    PaddedIndex head_;
    PaddedIndex tail_;

    // The producer's private line: its exact head, and its stale view of tail.
    alignas(kCacheLine) std::size_t producer_head_{0};
    std::size_t producer_cached_tail_{0};

    // The consumer's private line, symmetric.
    alignas(kCacheLine) std::size_t consumer_tail_{0};
    std::size_t consumer_cached_head_{0};
};

} // namespace t2t
