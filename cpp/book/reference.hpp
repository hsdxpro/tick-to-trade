#pragma once

// The same books on the standard library: std::map ladders, std::unordered_map
// orders. The differential reference in the tests, the baseline in the bench.

#include "book.hpp"

#include <map>
#include <unordered_map>

namespace t2t::book {

struct ReferenceSymbol {
    std::unordered_map<std::uint64_t, Order> orders;
    std::map<std::int64_t, std::int64_t> bids;
    std::map<std::int64_t, std::int64_t> asks;

    void add(Side side, std::int64_t price, std::int64_t delta) {
        auto& levels = side == Side::Bid ? bids : asks;
        auto& slot = levels[price];
        slot += delta;
        if (slot <= 0) {
            levels.erase(price);
        }
    }

    void set(Side side, std::int64_t price, std::int64_t qty) {
        auto& levels = side == Side::Bid ? bids : asks;
        if (qty <= 0) {
            levels.erase(price);
        } else {
            levels[price] = qty;
        }
    }

    [[nodiscard]] std::optional<std::pair<std::int64_t, std::int64_t>> best_bid() const {
        if (bids.empty()) return std::nullopt;
        return {*bids.rbegin()};
    }

    [[nodiscard]] std::optional<std::pair<std::int64_t, std::int64_t>> best_ask() const {
        if (asks.empty()) return std::nullopt;
        return {*asks.begin()};
    }
};

struct ReferenceBooks {
    std::vector<ReferenceSymbol> symbols;
    std::uint64_t unknown_orders{0};

    explicit ReferenceBooks(std::size_t symbol_count) : symbols(symbol_count) {}

    void apply(const Event& event) {
        auto& book = symbols[event.symbol];
        switch (event.kind) {
            case Kind::Add:
                book.orders[event.order_id] = {event.symbol, event.side, event.price, event.qty};
                book.add(event.side, event.price, event.qty);
                break;
            case Kind::Execute:
            case Kind::Cancel: {
                const auto it = book.orders.find(event.order_id);
                if (it == book.orders.end()) {
                    ++unknown_orders;
                    return;
                }
                const auto taken = std::min(event.qty, it->second.qty);
                it->second.qty -= taken;
                const auto side = it->second.side;
                const auto price = it->second.price;
                if (it->second.qty == 0) {
                    book.orders.erase(it);
                }
                book.add(side, price, -taken);
                break;
            }
            case Kind::Delete: {
                const auto it = book.orders.find(event.order_id);
                if (it == book.orders.end()) {
                    ++unknown_orders;
                    return;
                }
                const auto order = it->second;
                book.orders.erase(it);
                book.add(order.side, order.price, -order.qty);
                break;
            }
            case Kind::Replace: {
                const auto it = book.orders.find(event.order_id);
                if (it == book.orders.end()) {
                    ++unknown_orders;
                    return;
                }
                const auto old = it->second;
                book.orders.erase(it);
                book.add(old.side, old.price, -old.qty);
                book.orders[event.aux] = {event.symbol, old.side, event.price, event.qty};
                book.add(old.side, event.price, event.qty);
                break;
            }
            case Kind::Level:
                book.set(event.side, event.price, event.qty);
                break;
            case Kind::Trade:
                break;
        }
    }
};

} // namespace t2t::book
