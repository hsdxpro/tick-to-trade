#pragma once

// Book maintenance: the normalized event stream in, best-bid-offer and depth
// out. The C++ twin of rust/book -- same structures, same semantics, and the
// same rule enforced by the same benchmarks: a custom structure that cannot
// beat the standard library on this workload has no reason to exist.
//
// The two structures, and why they are custom:
//
//   * Ladder -- a dense quantity array over a window of the instrument's
//     price grid with an occupancy bitmap above it. Prices live on a grid, so
//     a price is an index and an update is a store; re-finding the touch after
//     it empties is a first-set-bit walk. The window follows the market and
//     the index is a reciprocal multiply rather than a divide. The Rust module
//     documents the sorted-array design this replaced and the benchmark that
//     killed it.
//   * OrderMap -- order ID to order, open addressing, linear probing,
//     power-of-two table, backward-shift deletion so churn never leaves
//     tombstones. ID zero is the reserved empty marker, a documented contract
//     of order-by-order feeds (ITCH references start at 1).

#include "../feed/feed.hpp"

#include <algorithm>
#include <bit>
#include <cassert>
#include <cstdint>
#include <optional>
#include <utility>
#include <vector>

namespace t2t::book {

using feed::Event;
using feed::Kind;
using feed::Side;

/// An instrument's price grid: the tick size, and how wide a window of it to
/// keep resident.
///
/// No floor. The window finds its own place from the first price it sees, so
/// one structure serves a penny-tick equity and a cent-tick perpetual without
/// either being sized for a year of price history.
struct Band {
    std::int64_t tick;
    std::size_t ticks;
};

/// An order as the L3 book holds it.
struct Order {
    std::uint16_t symbol{0};
    Side side{Side::Bid};
    std::int64_t price{0};
    std::int64_t qty{0};

    friend bool operator==(const Order&, const Order&) = default;
};

class Ladder final {
public:
    static constexpr std::size_t kEmpty = static_cast<std::size_t>(-1);

    static Ladder bids(const Band& band) { return Ladder(band, false); }
    static Ladder asks(const Band& band) { return Ladder(band, true); }

    /// Adds signed quantity at a price; a level reaching zero is removed.
    void add(std::int64_t price, std::int64_t delta) {
        const auto index = locate(price);
        if (index == kEmpty) {
            return;
        }
        const auto was = qty_[index];
        const auto now = std::max<std::int64_t>(was + delta, 0);
        qty_[index] = now;
        transition(index, was, now);
    }

    /// Sets the absolute quantity at a price; zero removes (L2 feeds).
    void set(std::int64_t price, std::int64_t qty) {
        const auto index = locate(price);
        if (index == kEmpty) {
            return;
        }
        const auto was = qty_[index];
        const auto now = std::max<std::int64_t>(qty, 0);
        qty_[index] = now;
        transition(index, was, now);
    }

    /// The touch: best price and its quantity.
    [[nodiscard]] std::optional<std::pair<std::int64_t, std::int64_t>> best() const {
        if (best_ == kEmpty) {
            return std::nullopt;
        }
        return {{price_at(best_), qty_[best_]}};
    }

    [[nodiscard]] std::size_t depth() const { return len_; }

    /// Windows shifted, prices refused for being off-grid, and levels dropped
    /// for falling a whole window behind the market. Counted rather than
    /// logged: an operator watching any of the three climb learns something,
    /// and the hot path pays an increment.
    [[nodiscard]] std::uint64_t rebases() const { return rebases_; }
    [[nodiscard]] std::uint64_t off_grid() const { return off_grid_; }
    [[nodiscard]] std::uint64_t evicted() const { return evicted_; }

    /// Visits live levels from the touch outward.
    template <typename Visit>
    void for_each_from_touch(Visit&& visit) const {
        for (auto at = best_; at != kEmpty; at = next_worse(at)) {
            visit(price_at(at), qty_[at]);
        }
    }

private:
    /// The scale is picked from the width, and the width then bounds the tick.
    ///
    /// `shift = 63 - ceil(log2(ticks))` is the largest scale at which the
    /// product in `index_of` provably cannot overflow 64 bits. That choice
    /// costs a ceiling on `ticks * tick`, asserted here rather than left to be
    /// discovered: at the 4,096-tick default it permits ticks up to about
    /// 5.5e11, six orders of magnitude past the coarsest tick any venue quotes
    /// in fixed point.
    Ladder(const Band& band, bool descending)
        : qty_(band.ticks, 0),
          occupied_((band.ticks + 63) / 64, 0),
          tick_(band.tick),
          shift_(63 - static_cast<unsigned>(std::bit_width(band.ticks - 1))),
          reciprocal_(ceil_div(1ULL << shift_, static_cast<std::uint64_t>(band.tick))),
          span_(static_cast<std::uint64_t>(band.ticks) * static_cast<std::uint64_t>(band.tick)),
          descending_(descending) {
        // Four levels is what makes the quarter-window headroom at least one
        // tick, which is what guarantees a rebase always moves the base.
        assert(band.tick > 0 && band.ticks >= 4
               && "a grid needs a positive tick and at least four levels");
        assert(static_cast<std::uint64_t>(band.ticks)
                       <= (1ULL << shift_) / static_cast<std::uint64_t>(band.tick)
               && "ticks * tick must fit the reciprocal's scale");
    }

    static constexpr std::uint64_t ceil_div(std::uint64_t a, std::uint64_t b) {
        return a / b + (a % b != 0 ? 1 : 0);
    }

    [[nodiscard]] std::int64_t price_at(std::size_t index) const {
        return base_ + static_cast<std::int64_t>(index) * tick_;
    }

    /// `(price - base) / tick`, as a multiply.
    ///
    /// With `m = ceil(2^s / tick)`, `tick * m` lies in `[2^s, 2^s + tick)`. For
    /// an on-grid offset `k * tick` the product is `k * (tick * m)`, which lies
    /// in `[k*2^s, k*2^s + offset)`, so the shift recovers `k` exactly whenever
    /// `offset < 2^s` -- guaranteed by the span bound the constructor asserts.
    ///
    /// An off-grid offset needs no separate argument: whatever index comes
    /// back, `index * tick` is a multiple of the tick and the offset is not, so
    /// the equality check in `index_at` cannot pass.
    [[nodiscard]] std::size_t index_of(std::uint64_t offset) const {
        return static_cast<std::size_t>((offset * reciprocal_) >> shift_);
    }

    /// Distance from the window's base, as an unsigned value.
    ///
    /// The wrap is the point: one unsigned comparison against the span decides
    /// *both* "below the window" and "above it", and the unplaced base of
    /// `INT64_MIN` lands far above any span, so a fresh ladder takes the same
    /// branch a breached one does with no flag to test.
    [[nodiscard]] std::uint64_t offset_of(std::int64_t price) const {
        return static_cast<std::uint64_t>(price) - static_cast<std::uint64_t>(base_);
    }

    /// Where `price` sits, shifting the window if it falls outside.
    ///
    /// The offset is computed once. Routing this through `locate_placed` read
    /// `base_` and recomputed the span a second time on every message, to
    /// re-answer a question this branch had just answered.
    [[nodiscard]] std::size_t locate(std::int64_t price) {
        auto offset = offset_of(price);
        if (offset >= span_) {
            rebase(price);
            offset = offset_of(price);
        }
        return index_at(offset);
    }

    /// The index for an offset already known to be inside the window.
    [[nodiscard]] std::size_t index_at(std::uint64_t offset) {
        const auto index = index_of(offset);
        if (static_cast<std::uint64_t>(index) * static_cast<std::uint64_t>(tick_) != offset) {
            ++off_grid_;
            return kEmpty;
        }
        return index;
    }

    /// Shifts the window onto `price`, carrying live levels across.
    ///
    /// Out of line: it runs when the market has walked past an edge, and the
    /// hot path should not carry its instructions. The shift is the minimum
    /// that admits the price plus a quarter-window of headroom -- centring
    /// would discard half the existing coverage every time, and no headroom
    /// would rebase on every oscillation across the boundary.
    void rebase(std::int64_t price) {
        const auto unplaced = base_ == kUnplaced;
        const auto width = static_cast<std::int64_t>(qty_.size());
        const auto reach = width * tick_;
        const auto headroom = (width / 4) * tick_;
        const auto target = unplaced       ? price - reach / 2
                            : price < base_ ? price - headroom
                                            : price - reach + headroom;
        // Snap to the existing grid so on-grid prices stay on it.
        const auto shifted =
            unplaced ? target : base_ + ((target - base_) / tick_) * tick_;
        if (len_ > 0) {
            // The base moves by a whole number of ticks, so every level moves
            // by the same index delta. That makes the carry one bulk move
            // rather than a per-level replay: this used to walk each live
            // level and re-place it, O(live) with a scratch buffer, and is now
            // O(width) at copy speed with no scratch at all.
            shift_levels(static_cast<std::ptrdiff_t>((shifted - base_) / tick_));
        }
        base_ = shifted;
        ++rebases_;
    }

    /// Moves every held level down by `delta` indices (up when negative),
    /// dropping the levels the move pushes off either end.
    ///
    /// Dropping them is correct, not lossy bookkeeping: a level a whole window
    /// away from where the market now trades is stale depth, and holding it
    /// would mean sizing for an instrument's lifetime range.
    void shift_levels(std::ptrdiff_t delta) {
        assert(delta != 0 && "a rebase that moves nothing is a bug upstream");
        const auto width = qty_.size();
        const auto d = static_cast<std::size_t>(delta < 0 ? -delta : delta);
        if (d >= width) {
            // The window jumped clear of everything it held.
            evicted_ += len_;
            std::fill(qty_.begin(), qty_.end(), 0);
            std::fill(occupied_.begin(), occupied_.end(), 0);
            len_ = 0;
            best_ = kEmpty;
            return;
        }
        std::uint64_t dropped = 0;
        if (delta > 0) {
            // Window moved up: indices fall by `d`, the lowest `d` fall out.
            dropped = take_bits(0, d);
            std::copy(qty_.begin() + static_cast<std::ptrdiff_t>(d), qty_.end(), qty_.begin());
            std::fill(qty_.end() - static_cast<std::ptrdiff_t>(d), qty_.end(), 0);
            shift_bits_down(d);
        } else {
            // Window moved down: indices rise by `d`, the highest `d` fall out.
            // Their bits are cleared before the shift so nothing real is ever
            // pushed into the slack above `width` in the top word.
            dropped = take_bits(width - d, width);
            std::copy_backward(qty_.begin(), qty_.end() - static_cast<std::ptrdiff_t>(d), qty_.end());
            std::fill(qty_.begin(), qty_.begin() + static_cast<std::ptrdiff_t>(d), 0);
            shift_bits_up(d);
        }
        evicted_ += dropped;
        len_ -= static_cast<std::size_t>(dropped);
        best_ = find_best();
    }

    /// Zeroes bits `[from, to)` and returns how many were set.
    std::uint64_t take_bits(std::size_t from, std::size_t to) {
        std::uint64_t taken = 0;
        auto at = from;
        while (at < to) {
            const auto word = at / 64;
            const auto hi = std::min((word + 1) * 64, to);
            const auto low_mask = ~0ULL << (at % 64);
            const auto bits = hi - word * 64;
            const auto mask = bits == 64 ? low_mask : ((1ULL << bits) - 1) & low_mask;
            taken += static_cast<std::uint64_t>(std::popcount(occupied_[word] & mask));
            occupied_[word] &= ~mask;
            at = hi;
        }
        return taken;
    }

    /// Moves every set bit `d` positions toward index zero.
    void shift_bits_down(std::size_t d) {
        const auto words = occupied_.size();
        const auto jump = d / 64;
        const auto bits = static_cast<unsigned>(d % 64);
        for (std::size_t word = 0; word < words; ++word) {
            const auto low = word + jump < words ? occupied_[word + jump] : 0;
            const auto high = word + jump + 1 < words ? occupied_[word + jump + 1] : 0;
            occupied_[word] = bits == 0 ? low : (low >> bits) | (high << (64 - bits));
        }
    }

    /// Moves every set bit `d` positions away from index zero. Bits that
    /// would land at or beyond `width` must already be cleared.
    void shift_bits_up(std::size_t d) {
        const auto words = occupied_.size();
        const auto jump = d / 64;
        const auto bits = static_cast<unsigned>(d % 64);
        for (std::size_t word = words; word-- > 0;) {
            const auto high = word >= jump ? occupied_[word - jump] : 0;
            const auto low = word > jump ? occupied_[word - jump - 1] : 0;
            occupied_[word] = bits == 0 ? high : (high << bits) | (low >> (64 - bits));
        }
    }

    /// Re-finds the touch by scanning the bitmap from the best end.
    [[nodiscard]] std::size_t find_best() const {
        if (descending_) {
            for (std::size_t word = 0; word < occupied_.size(); ++word) {
                if (occupied_[word] != 0) {
                    return word * 64 + static_cast<std::size_t>(std::countr_zero(occupied_[word]));
                }
            }
        } else {
            for (std::size_t word = occupied_.size(); word-- > 0;) {
                if (occupied_[word] != 0) {
                    return word * 64 + 63 - static_cast<std::size_t>(std::countl_zero(occupied_[word]));
                }
            }
        }
        return kEmpty;
    }

    void occupy(std::size_t index) {
        occupied_[index / 64] |= 1ULL << (index % 64);
        ++len_;
        if (best_ == kEmpty || better(index, best_)) {
            best_ = index;
        }
    }

    [[nodiscard]] bool better(std::size_t a, std::size_t b) const {
        return descending_ ? a < b : a > b;
    }

    void transition(std::size_t index, std::int64_t was, std::int64_t now) {
        if ((was > 0) == (now > 0)) {
            return; // quantity changed, occupancy did not
        }
        if (now > 0) {
            occupy(index);
        } else {
            occupied_[index / 64] &= ~(1ULL << (index % 64));
            --len_;
            if (index == best_) {
                best_ = next_worse(index);
            }
        }
    }

    /// The nearest occupied index on the worse side of `from`, or kEmpty.
    /// One masked word, then whole words: the bitmap makes the emptiness
    /// between levels free to skip.
    [[nodiscard]] std::size_t next_worse(std::size_t from) const {
        auto word_index = from / 64;
        const auto bit = from % 64;
        if (descending_) {
            // Asks: worse is higher. `2ULL << 63` is zero, which masks all.
            auto word = occupied_[word_index] & ~((2ULL << bit) - 1ULL);
            for (;;) {
                if (word != 0) {
                    return word_index * 64 + static_cast<std::size_t>(std::countr_zero(word));
                }
                if (++word_index == occupied_.size()) {
                    return kEmpty;
                }
                word = occupied_[word_index];
            }
        }
        // Bids: worse is lower.
        auto word = occupied_[word_index] & ((1ULL << bit) - 1ULL);
        for (;;) {
            if (word != 0) {
                return word_index * 64 + 63 - static_cast<std::size_t>(std::countl_zero(word));
            }
            if (word_index-- == 0) {
                return kEmpty;
            }
            word = occupied_[word_index];
        }
    }

    std::vector<std::int64_t> qty_;
    std::vector<std::uint64_t> occupied_;
    /// Price at index zero, `kUnplaced` until the first price decides where
    /// the window sits. Moves when the market leaves it.
    static constexpr std::int64_t kUnplaced = INT64_MIN;
    std::int64_t base_{kUnplaced};
    std::int64_t tick_;
    unsigned shift_;
    std::uint64_t reciprocal_;
    /// Price the window spans. `ticks * tick`, and neither changes, so it is
    /// stored rather than recomputed on a path that runs per message.
    std::uint64_t span_;
    std::size_t best_{kEmpty};
    bool descending_;
    std::size_t len_{0};
    std::uint64_t rebases_{0};
    std::uint64_t off_grid_{0};
    std::uint64_t evicted_{0};
};

/// One table slot: the key beside the order it addresses, in one allocation.
///
/// This started as parallel `keys_` and `values_` vectors, which is the shape
/// that reads well and the wrong one for the access pattern. Every successful
/// lookup wants the value the instant the key matches, and two allocations put
/// those on two cache lines -- so the common case paid two misses where one
/// would do. Interleaved, the line that answers the key question carries the
/// answer with it. Worth 7% on the isolation benchmark.
struct Slot {
    std::uint64_t key{0};
    Order order{};
};

class OrderMap final {
public:
    explicit OrderMap(std::size_t capacity) {
        auto size = std::bit_ceil(std::max<std::size_t>(capacity, 16)) * 2;
        slots_.assign(size, Slot{});
        mask_ = size - 1;
    }

    void insert(std::uint64_t key, const Order& value) {
        assert(key != 0 && "order ID zero is the reserved empty marker");
        if ((len_ + 1) * 4 > slots_.size() * 3) {
            grow();
        }
        auto at = slot(key);
        for (;;) {
            const auto held = slots_[at].key;
            if (held == 0 || held == key) {
                len_ += held == 0 ? 1 : 0;
                slots_[at] = Slot{key, value};
                return;
            }
            at = (at + 1) & mask_;
        }
    }

    [[nodiscard]] Order* find(std::uint64_t key) {
        for (auto at = slot(key);; at = (at + 1) & mask_) {
            if (slots_[at].key == 0) {
                return nullptr;
            }
            if (slots_[at].key == key) {
                return &slots_[at].order;
            }
        }
    }

    /// Removes and returns the order, closing the probe gap by shifting
    /// followers back so lookups never wade through tombstones.
    std::optional<Order> remove(std::uint64_t key) {
        auto at = slot(key);
        for (;; at = (at + 1) & mask_) {
            if (slots_[at].key == 0) {
                return std::nullopt;
            }
            if (slots_[at].key == key) {
                break;
            }
        }
        const auto removed = slots_[at].order;
        --len_;
        auto hole = at;
        for (auto probe = (at + 1) & mask_; slots_[probe].key != 0; probe = (probe + 1) & mask_) {
            const auto home = slot(slots_[probe].key);
            // Ring-safe "does this entry belong at or before the hole".
            if (((hole - home) & mask_) <= ((probe - home) & mask_)) {
                slots_[hole] = slots_[probe];
                hole = probe;
            }
        }
        slots_[hole].key = 0;
        return removed;
    }

    [[nodiscard]] std::size_t size() const { return len_; }

private:
    /// Fibonacci multiplicative mix: one multiply, one shift. Enough for
    /// order references, which are not adversarial keys.
    [[nodiscard]] std::size_t slot(std::uint64_t key) const {
        return static_cast<std::size_t>((key * 0x9e37'79b9'7f4a'7c15ULL) >> 32) & mask_;
    }

    void grow() {
        auto old = std::move(slots_);
        slots_.assign(old.size() * 2, Slot{});
        mask_ = slots_.size() - 1;
        len_ = 0;
        for (const auto& entry : old) {
            if (entry.key != 0) {
                insert(entry.key, entry.order);
            }
        }
    }

    std::vector<Slot> slots_;
    std::size_t mask_{0};
    std::size_t len_{0};
};

/// One symbol's book, and the application of the normalized stream to it.
struct SymbolBook {
    OrderMap orders;
    Ladder bids;
    Ladder asks;

    explicit SymbolBook(const Band& band)
        : orders(4'096), bids(Ladder::bids(band)), asks(Ladder::asks(band)) {}

    Ladder& side(Side s) { return s == Side::Bid ? bids : asks; }
};

class Books final {
public:
    Books(std::size_t symbol_count, const Band& band) {
        symbols_.reserve(symbol_count);
        for (std::size_t i = 0; i < symbol_count; ++i) {
            symbols_.emplace_back(band);
        }
    }

    [[nodiscard]] const SymbolBook& symbol(std::uint16_t index) const {
        return symbols_[index];
    }

    [[nodiscard]] std::uint64_t unknown_orders() const { return unknown_orders_; }

    void apply(const Event& event) {
        auto& book = symbols_[event.symbol];
        switch (event.kind) {
            case Kind::Add:
                book.orders.insert(event.order_id,
                                   {event.symbol, event.side, event.price, event.qty});
                book.side(event.side).add(event.price, event.qty);
                break;
            case Kind::Execute:
            case Kind::Cancel: {
                auto* order = book.orders.find(event.order_id);
                if (order == nullptr) {
                    ++unknown_orders_;
                    return;
                }
                const auto taken = std::min(event.qty, order->qty);
                order->qty -= taken;
                const auto side = order->side;
                const auto price = order->price;
                if (order->qty == 0) {
                    book.orders.remove(event.order_id);
                }
                book.side(side).add(price, -taken);
                break;
            }
            case Kind::Delete: {
                const auto order = book.orders.remove(event.order_id);
                if (!order) {
                    ++unknown_orders_;
                    return;
                }
                book.side(order->side).add(order->price, -order->qty);
                break;
            }
            case Kind::Replace: {
                const auto old = book.orders.remove(event.order_id);
                if (!old) {
                    ++unknown_orders_;
                    return;
                }
                book.side(old->side).add(old->price, -old->qty);
                book.orders.insert(event.aux, {event.symbol, old->side, event.price, event.qty});
                book.side(old->side).add(event.price, event.qty);
                break;
            }
            case Kind::Level:
                book.side(event.side).set(event.price, event.qty);
                break;
            case Kind::Trade:
                break; // prints inform, they do not move the book
        }
    }

private:
    std::vector<SymbolBook> symbols_;
    std::uint64_t unknown_orders_{0};
};

} // namespace t2t::book
