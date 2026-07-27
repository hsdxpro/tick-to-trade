#pragma once

// Generators: a byte stream and the events it must decode to, from one seed.
// Transliterated from the Rust generators call-for-call -- same RNG, same
// call order, same formatting -- so both languages produce byte-identical
// streams and the benchmarks compare parsers rather than workloads. The
// differential tests in each language check their parser against their own
// generator; a cross-language test checks the streams themselves match.

#include "feed.hpp"

#include <cstdio>
#include <string>
#include <vector>

namespace t2t::feed::synth {

inline constexpr std::string_view kTradfi[] = {"AAPL", "MSFT", "NVDA", "TSLA"};
inline constexpr std::string_view kCrypto[] = {"BTCUSDT", "ETHUSDT", "SOLUSDT"};

struct Generated {
    std::vector<std::byte> bytes;
    std::vector<Event> events;
};

inline void push_bytes(std::vector<std::byte>& out, const void* data, std::size_t len) {
    const auto* p = static_cast<const std::byte*>(data);
    out.insert(out.end(), p, p + len);
}

inline void push_be16(std::vector<std::byte>& out, std::uint16_t v) {
    const std::uint8_t b[2] = {static_cast<std::uint8_t>(v >> 8), static_cast<std::uint8_t>(v)};
    push_bytes(out, b, 2);
}

inline void push_be32(std::vector<std::byte>& out, std::uint32_t v) {
    const std::uint8_t b[4] = {
        static_cast<std::uint8_t>(v >> 24), static_cast<std::uint8_t>(v >> 16),
        static_cast<std::uint8_t>(v >> 8), static_cast<std::uint8_t>(v)};
    push_bytes(out, b, 4);
}

inline void push_be64(std::vector<std::byte>& out, std::uint64_t v) {
    push_be32(out, static_cast<std::uint32_t>(v >> 32));
    push_be32(out, static_cast<std::uint32_t>(v));
}

inline void push_zeros(std::vector<std::byte>& out, std::size_t n) {
    out.insert(out.end(), n, std::byte{0});
}

inline void push_stock(std::vector<std::byte>& out, std::uint16_t symbol) {
    char field[8] = {' ', ' ', ' ', ' ', ' ', ' ', ' ', ' '};
    const auto name = kTradfi[symbol];
    for (std::size_t i = 0; i < name.size(); ++i) field[i] = name[i];
    push_bytes(out, field, 8);
}

// ------------------------------------------------------------------ ITCH

/// One step of a symbol's mid-price walk, and a price near it -- the same
/// clustered shape as the Rust generator, call-for-call, because the
/// workload's distribution is part of what the benchmarks compare.
inline std::uint32_t walk_price(std::int64_t& mid, Rng& rng) {
    mid += (static_cast<std::int64_t>(rng.below(3)) - 1) * 100;
    mid = std::max<std::int64_t>(1'000'000, std::min<std::int64_t>(1'500'000, mid));
    const auto offset = (static_cast<std::int64_t>(rng.below(65)) - 32) * 100;
    return static_cast<std::uint32_t>(mid + offset);
}

inline Generated itch(std::size_t count, Rng& rng) {
    Generated out;
    std::vector<std::pair<std::uint64_t, std::uint16_t>> live;
    std::uint64_t next_order = 1;
    std::int64_t mids[4] = {1'250'000, 1'250'000, 1'250'000, 1'250'000};

    auto frame = [&out](const std::vector<std::byte>& body) {
        push_be16(out.bytes, static_cast<std::uint16_t>(body.size()));
        out.bytes.insert(out.bytes.end(), body.begin(), body.end());
    };

    for (std::size_t n = 0; n < count; ++n) {
        const auto symbol = static_cast<std::uint16_t>(rng.below(4));
        const auto roll = rng.below(100);
        if (roll < 40 || live.empty()) {
            const auto order = next_order++;
            const Side side = rng.below(2) == 0 ? Side::Bid : Side::Ask;
            const auto shares = static_cast<std::uint32_t>(1 + rng.below(5'000));
            const auto price = walk_price(mids[symbol], rng);
            live.emplace_back(order, symbol);
            std::vector<std::byte> body;
            body.push_back(static_cast<std::byte>('A'));
            push_be16(body, symbol);
            push_zeros(body, 8);
            push_be64(body, order);
            body.push_back(static_cast<std::byte>(side == Side::Bid ? 'B' : 'S'));
            push_be32(body, shares);
            push_stock(body, symbol);
            push_be32(body, price);
            frame(body);
            out.events.push_back({Kind::Add, side, symbol,
                                  std::int64_t{price} * (kPriceScale / 10'000),
                                  std::int64_t{shares} * kQtyScale, order, 0});
        } else {
            const auto pick = static_cast<std::size_t>(rng.below(live.size()));
            const auto [order, order_symbol] = live[pick];
            if (roll <= 59) {
                const auto shares = static_cast<std::uint32_t>(1 + rng.below(500));
                const auto match_no = rng.next();
                std::vector<std::byte> body;
                body.push_back(static_cast<std::byte>('E'));
                push_be16(body, order_symbol);
                push_zeros(body, 8);
                push_be64(body, order);
                push_be32(body, shares);
                push_be64(body, match_no);
                frame(body);
                out.events.push_back({Kind::Execute, Side::Bid, order_symbol, 0,
                                      std::int64_t{shares} * kQtyScale, order, match_no});
            } else if (roll <= 74) {
                const auto shares = static_cast<std::uint32_t>(1 + rng.below(500));
                std::vector<std::byte> body;
                body.push_back(static_cast<std::byte>('X'));
                push_be16(body, order_symbol);
                push_zeros(body, 8);
                push_be64(body, order);
                push_be32(body, shares);
                frame(body);
                out.events.push_back({Kind::Cancel, Side::Bid, order_symbol, 0,
                                      std::int64_t{shares} * kQtyScale, order, 0});
            } else if (roll <= 89) {
                live[pick] = live.back();
                live.pop_back();
                std::vector<std::byte> body;
                body.push_back(static_cast<std::byte>('D'));
                push_be16(body, order_symbol);
                push_zeros(body, 8);
                push_be64(body, order);
                frame(body);
                out.events.push_back({Kind::Delete, Side::Bid, order_symbol, 0, 0, order, 0});
            } else if (roll <= 95) {
                const auto replacement = next_order++;
                live[pick] = {replacement, order_symbol};
                const auto shares = static_cast<std::uint32_t>(1 + rng.below(5'000));
                const auto price = walk_price(mids[order_symbol], rng);
                std::vector<std::byte> body;
                body.push_back(static_cast<std::byte>('U'));
                push_be16(body, order_symbol);
                push_zeros(body, 8);
                push_be64(body, order);
                push_be64(body, replacement);
                push_be32(body, shares);
                push_be32(body, price);
                frame(body);
                out.events.push_back({Kind::Replace, Side::Bid, order_symbol,
                                      std::int64_t{price} * (kPriceScale / 10'000),
                                      std::int64_t{shares} * kQtyScale, order, replacement});
            } else {
                const Side side = rng.below(2) == 0 ? Side::Bid : Side::Ask;
                const auto shares = static_cast<std::uint32_t>(1 + rng.below(500));
                const auto price = walk_price(mids[order_symbol], rng);
                const auto match_no = rng.next();
                std::vector<std::byte> body;
                body.push_back(static_cast<std::byte>('P'));
                push_be16(body, order_symbol);
                push_zeros(body, 8);
                push_be64(body, order);
                body.push_back(static_cast<std::byte>(side == Side::Bid ? 'B' : 'S'));
                push_be32(body, shares);
                push_stock(body, order_symbol);
                push_be32(body, price);
                push_be64(body, match_no);
                frame(body);
                out.events.push_back({Kind::Trade, side, order_symbol,
                                      std::int64_t{price} * (kPriceScale / 10'000),
                                      std::int64_t{shares} * kQtyScale, order, match_no});
            }
        }
    }
    return out;
}

// ------------------------------------------------------------------- FIX

inline void push_fix(std::string& out, std::uint32_t tag, std::string_view value) {
    out += std::to_string(tag);
    out += '=';
    out += value;
    out += '\x01';
}

inline Generated fix(std::size_t count, Rng& rng) {
    Generated out;
    for (std::size_t sequence = 0; sequence < count; ++sequence) {
        const auto symbol = static_cast<std::uint16_t>(rng.below(4));
        const auto entries = 1 + rng.below(3);
        std::string body;
        push_fix(body, 35, "X");
        push_fix(body, 49, "VENUE");
        push_fix(body, 56, "HANDLER");
        push_fix(body, 34, std::to_string(sequence + 1));
        push_fix(body, 55, kTradfi[symbol]);
        push_fix(body, 268, std::to_string(entries));
        for (std::uint64_t e = 0; e < entries; ++e) {
            const bool is_trade = rng.below(5) == 0;
            const Side side = rng.below(2) == 0 ? Side::Bid : Side::Ask;
            const auto price_cents = static_cast<std::int64_t>(10'000 + rng.below(90'000));
            const auto qty_whole = static_cast<std::int64_t>(1 + rng.below(9'999));
            push_fix(body, 269, is_trade ? "2" : (side == Side::Bid ? "0" : "1"));
            char price_text[32];
            std::snprintf(price_text, sizeof price_text, "%lld.%02lld",
                          static_cast<long long>(price_cents / 100),
                          static_cast<long long>(price_cents % 100));
            push_fix(body, 270, price_text);
            push_fix(body, 271, std::to_string(qty_whole));
            out.events.push_back({is_trade ? Kind::Trade : Kind::Level,
                                  is_trade ? Side::Bid : side, symbol,
                                  price_cents * (kPriceScale / 100), qty_whole * kQtyScale, 0, 0});
        }
        std::string message;
        push_fix(message, 8, "FIX.4.4");
        push_fix(message, 9, std::to_string(body.size()));
        message += body;
        std::uint32_t sum = 0;
        for (const char c : message) sum += static_cast<std::uint8_t>(c);
        char checksum[8];
        std::snprintf(checksum, sizeof checksum, "%03u", sum % 256);
        push_fix(message, 10, checksum);
        push_bytes(out.bytes, message.data(), message.size());
    }
    return out;
}

// ------------------------------------------------------------------ JSON

/// A fixed-point value as the decimal string an exchange sends, trailing
/// zeros trimmed the way real feeds trim them.
inline std::string scaled(std::int64_t value) {
    const auto whole = value / kPriceScale;
    const auto frac = value % kPriceScale;
    if (frac == 0) {
        return std::to_string(whole);
    }
    char digits[16];
    std::snprintf(digits, sizeof digits, "%08lld", static_cast<long long>(frac));
    std::string tail{digits};
    while (!tail.empty() && tail.back() == '0') tail.pop_back();
    return std::to_string(whole) + "." + tail;
}

inline Generated json(std::size_t count, Rng& rng) {
    Generated out;
    for (std::size_t n = 0; n < count; ++n) {
        const auto symbol = static_cast<std::uint16_t>(rng.below(3));
        const std::string name{kCrypto[symbol]};
        std::string line;
        if (rng.below(2) == 0) {
            const auto price = static_cast<std::int64_t>(1'000'000'000 + rng.below(4'000'000'000ULL));
            const auto qty = static_cast<std::int64_t>(1'000'000 + rng.below(500'000'000));
            const auto trade_id = rng.below(1ULL << 40);
            const bool maker_is_buyer = rng.below(2) == 0;
            line = "{\"e\":\"trade\",\"E\":1700000000000,\"s\":\"" + name + "\",\"t\":"
                 + std::to_string(trade_id) + ",\"p\":\"" + scaled(price) + "\",\"q\":\""
                 + scaled(qty) + "\",\"m\":" + (maker_is_buyer ? "true" : "false") + "}\n";
            out.events.push_back({Kind::Trade, maker_is_buyer ? Side::Ask : Side::Bid, symbol,
                                  price, qty, 0, trade_id});
        } else {
            const auto bids = 1 + rng.below(3);
            const auto asks = 1 + rng.below(3);
            line = "{\"e\":\"depthUpdate\",\"E\":1700000000000,\"s\":\"" + name
                 + "\",\"U\":1,\"u\":2,\"b\":[";
            for (std::uint64_t i = 0; i < bids; ++i) {
                const auto price = static_cast<std::int64_t>(1'000'000'000 + rng.below(4'000'000'000ULL));
                const auto qty = static_cast<std::int64_t>(rng.below(500'000'000));
                if (i > 0) line += ',';
                line += "[\"" + scaled(price) + "\",\"" + scaled(qty) + "\"]";
                out.events.push_back({Kind::Level, Side::Bid, symbol, price, qty, 0, 0});
            }
            line += "],\"a\":[";
            for (std::uint64_t i = 0; i < asks; ++i) {
                const auto price = static_cast<std::int64_t>(1'000'000'000 + rng.below(4'000'000'000ULL));
                const auto qty = static_cast<std::int64_t>(rng.below(500'000'000));
                if (i > 0) line += ',';
                line += "[\"" + scaled(price) + "\",\"" + scaled(qty) + "\"]";
                out.events.push_back({Kind::Level, Side::Ask, symbol, price, qty, 0, 0});
            }
            line += "]}\n";
        }
        push_bytes(out.bytes, line.data(), line.size());
    }
    return out;
}

} // namespace t2t::feed::synth
