#pragma once

// Book maintenance: the normalized event stream in, best-bid-offer and depth
// out. The C++ twin of rust/book -- same structures, same semantics, and the
// same rule enforced by the same benchmarks: a custom structure that cannot
// beat the standard library on this workload has no reason to exist.
//
// The two structures, and why they are custom:
//
//   * Ladder -- a dense quantity array over the instrument's price band with
//     an occupancy bitmap above it. A feed handler knows the band and tick
//     size (venues publish both), so a price is an index and an update is a
//     store; re-finding the touch after it empties is a first-set-bit walk.
//     The Rust module documents the sorted-array design this replaced and
//     the benchmark that killed it.
//   * OrderMap -- order ID to order, open addressing, linear probing,
//     power-of-two table, backward-shift deletion so churn never leaves
//     tombstones. ID zero is the reserved empty marker, a documented contract
//     of order-by-order feeds (ITCH references start at 1).

#include "../feed/feed.hpp"

#include <bit>
#include <cassert>
#include <cstdint>
#include <optional>
#include <vector>

namespace t2t::book {

using feed::Event;
using feed::Kind;
using feed::Side;

/// An instrument's price band, in fixed-point price units.
struct Band {
    std::int64_t floor;
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
        const auto index = this->index(price);
        const auto was = qty_[index];
        const auto now = std::max<std::int64_t>(was + delta, 0);
        qty_[index] = now;
        transition(index, was, now);
    }

    /// Sets the absolute quantity at a price; zero removes (L2 feeds).
    void set(std::int64_t price, std::int64_t qty) {
        const auto index = this->index(price);
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
        return {{floor_ + static_cast<std::int64_t>(best_) * tick_, qty_[best_]}};
    }

    [[nodiscard]] std::size_t depth() const { return len_; }

    /// Visits live levels from the touch outward.
    template <typename Visit>
    void for_each_from_touch(Visit&& visit) const {
        for (auto at = best_; at != kEmpty; at = next_worse(at)) {
            visit(floor_ + static_cast<std::int64_t>(at) * tick_, qty_[at]);
        }
    }

private:
    Ladder(const Band& band, bool descending)
        : qty_(band.ticks, 0),
          occupied_((band.ticks + 63) / 64, 0),
          floor_(band.floor),
          tick_(band.tick),
          descending_(descending) {
        assert(band.tick > 0 && band.ticks > 0);
    }

    /// A price the band cannot address is a configuration error; refusing
    /// beats silently folding it into the nearest level.
    [[nodiscard]] std::size_t index(std::int64_t price) const {
        const auto offset = price - floor_;
        const auto index = offset / tick_;
        assert(offset >= 0 && offset % tick_ == 0
               && static_cast<std::size_t>(index) < qty_.size()
               && "price outside the configured band or off-tick");
        return static_cast<std::size_t>(index);
    }

    [[nodiscard]] bool better(std::size_t a, std::size_t b) const {
        return descending_ ? a < b : a > b;
    }

    void transition(std::size_t index, std::int64_t was, std::int64_t now) {
        if ((was > 0) == (now > 0)) {
            return; // quantity changed, occupancy did not
        }
        if (now > 0) {
            occupied_[index / 64] |= 1ULL << (index % 64);
            ++len_;
            if (best_ == kEmpty || better(index, best_)) {
                best_ = index;
            }
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
    std::int64_t floor_;
    std::int64_t tick_;
    std::size_t best_{kEmpty};
    bool descending_;
    std::size_t len_{0};
};

class OrderMap final {
public:
    explicit OrderMap(std::size_t capacity) {
        auto size = std::bit_ceil(std::max<std::size_t>(capacity, 16)) * 2;
        keys_.assign(size, 0);
        values_.assign(size, Order{});
        mask_ = size - 1;
    }

    void insert(std::uint64_t key, const Order& value) {
        assert(key != 0 && "order ID zero is the reserved empty marker");
        if ((len_ + 1) * 4 > keys_.size() * 3) {
            grow();
        }
        auto at = slot(key);
        for (;;) {
            if (keys_[at] == 0 || keys_[at] == key) {
                len_ += keys_[at] == 0 ? 1 : 0;
                keys_[at] = key;
                values_[at] = value;
                return;
            }
            at = (at + 1) & mask_;
        }
    }

    [[nodiscard]] Order* find(std::uint64_t key) {
        for (auto at = slot(key);; at = (at + 1) & mask_) {
            if (keys_[at] == 0) {
                return nullptr;
            }
            if (keys_[at] == key) {
                return &values_[at];
            }
        }
    }

    /// Removes and returns the order, closing the probe gap by shifting
    /// followers back so lookups never wade through tombstones.
    std::optional<Order> remove(std::uint64_t key) {
        auto at = slot(key);
        for (;; at = (at + 1) & mask_) {
            if (keys_[at] == 0) {
                return std::nullopt;
            }
            if (keys_[at] == key) {
                break;
            }
        }
        const auto removed = values_[at];
        --len_;
        auto hole = at;
        for (auto probe = (at + 1) & mask_; keys_[probe] != 0; probe = (probe + 1) & mask_) {
            const auto home = slot(keys_[probe]);
            // Ring-safe "does this entry belong at or before the hole".
            if (((hole - home) & mask_) <= ((probe - home) & mask_)) {
                keys_[hole] = keys_[probe];
                values_[hole] = values_[probe];
                hole = probe;
            }
        }
        keys_[hole] = 0;
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
        auto old_keys = std::move(keys_);
        auto old_values = std::move(values_);
        keys_.assign(old_keys.size() * 2, 0);
        values_.assign(old_values.size() * 2, Order{});
        mask_ = keys_.size() - 1;
        len_ = 0;
        for (std::size_t i = 0; i < old_keys.size(); ++i) {
            if (old_keys[i] != 0) {
                insert(old_keys[i], old_values[i]);
            }
        }
    }

    std::vector<std::uint64_t> keys_;
    std::vector<Order> values_;
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
