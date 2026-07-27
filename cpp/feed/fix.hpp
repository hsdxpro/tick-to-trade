#pragma once

// FIX 4.4 tag=value, market data subset: one left-to-right pass that never
// re-reads a byte, integers and decimals parsed inline into fixed-point, the
// checksum verified because it is the only integrity the protocol offers,
// and BodyLength used to frame so partial-buffer detection is O(1).
//
// Running out of bytes mid-pair is NeedMore -- the routine stream case; a
// byte that cannot belong to a pair is Malformed. Keeping those apart is
// what keeps the framing free of guesses about truncated headers.

#include "feed.hpp"

namespace t2t::feed {

inline constexpr std::uint8_t kSoh = 0x01;

template <typename Sink>
class Fix final {
public:
    explicit Fix(std::span<const std::string_view> symbols) : symbols_(symbols) {}

    Outcome parse(Bytes bytes, Sink& sink) const {
        std::size_t pos = 0;
        for (;;) {
            if (pos == bytes.size()) {
                return {pos, {}};
            }
            std::size_t cursor = pos;
            Pair header{};
            // 8=FIX.4.4, then 9=length, which frames the rest.
            switch (pair(bytes, cursor, header)) {
                case PairEnd::Ok: break;
                case PairEnd::Truncated: return {pos, {}};
                case PairEnd::Bad: return {pos, Error::Malformed};
            }
            if (header.tag != 8) return {pos, Error::Malformed};
            switch (pair(bytes, cursor, header)) {
                case PairEnd::Ok: break;
                case PairEnd::Truncated: return {pos, {}};
                case PairEnd::Bad: return {pos, Error::Malformed};
            }
            if (header.tag != 9) return {pos, Error::Malformed};
            std::int64_t body_length = 0;
            if (!integer(bytes, header, body_length)) return {pos, Error::Malformed};

            const std::size_t end = cursor + static_cast<std::size_t>(body_length) + 7;
            if (end > bytes.size()) {
                return {pos, {}};
            }
            // Trailer: "10=xxx" + SOH, checksum over everything before it.
            if (at(bytes, end - 7) != '1' || at(bytes, end - 6) != '0'
                || at(bytes, end - 5) != '=' || at(bytes, end - 1) != kSoh) {
                return {pos, Error::Malformed};
            }
            std::uint32_t sum = 0;
            for (std::size_t i = pos; i < end - 7; ++i) {
                sum += at(bytes, i);
            }
            const std::uint32_t claimed = (at(bytes, end - 4) - '0') * 100U
                                        + (at(bytes, end - 3) - '0') * 10U
                                        + (at(bytes, end - 2) - '0');
            if (sum % 256 != claimed) {
                return {pos, Error::Malformed};
            }
            if (auto error = body(bytes, cursor, end - 7, sink)) {
                return {pos, *error};
            }
            pos = end;
        }
    }

private:
    struct Pair {
        std::uint32_t tag{0};
        std::size_t value_from{0};
        std::size_t value_len{0};
    };

    enum class PairEnd : std::uint8_t { Ok, Truncated, Bad };

    static PairEnd pair(Bytes bytes, std::size_t& cursor, Pair& out) {
        std::uint32_t tag = 0;
        std::size_t i = cursor;
        for (;;) {
            if (i == bytes.size()) return PairEnd::Truncated;
            const auto byte = at(bytes, i);
            if (byte >= '0' && byte <= '9') {
                tag = tag * 10 + (byte - '0');
                ++i;
            } else if (byte == '=' && i > cursor) {
                break;
            } else {
                return PairEnd::Bad;
            }
        }
        ++i;
        const std::size_t value_from = i;
        for (;;) {
            if (i == bytes.size()) return PairEnd::Truncated;
            if (at(bytes, i) == kSoh) break;
            ++i;
        }
        out = {tag, value_from, i - value_from};
        cursor = i + 1;
        return PairEnd::Ok;
    }

    static bool integer(Bytes bytes, const Pair& pair, std::int64_t& out) {
        if (pair.value_len == 0) return false;
        std::int64_t value = 0;
        for (std::size_t i = 0; i < pair.value_len; ++i) {
            const auto byte = at(bytes, pair.value_from + i);
            if (byte < '0' || byte > '9') return false;
            value = value * 10 + (byte - '0');
        }
        out = value;
        return true;
    }

    /// Decimal into fixed-point during the scan; no float anywhere.
    static bool decimal(Bytes bytes, const Pair& pair, std::int64_t scale, std::int64_t& out) {
        std::int64_t whole = 0;
        std::size_t i = 0;
        bool any = false;
        for (; i < pair.value_len; ++i) {
            const auto byte = at(bytes, pair.value_from + i);
            if (byte == '.') break;
            if (byte < '0' || byte > '9') return false;
            whole = whole * 10 + (byte - '0');
            any = true;
        }
        if (!any) return false;
        std::int64_t value = whole * scale;
        if (i < pair.value_len) {
            ++i; // the dot
            std::int64_t worth = scale;
            for (; i < pair.value_len; ++i) {
                const auto byte = at(bytes, pair.value_from + i);
                if (byte < '0' || byte > '9') return false;
                worth /= 10;
                if (worth == 0) return false; // more precision than the scale holds
                value += (byte - '0') * worth;
            }
        }
        out = value;
        return true;
    }

    std::optional<Error> body(Bytes bytes, std::size_t cursor, std::size_t end, Sink& sink) const {
        std::optional<std::uint16_t> symbol{};
        Event entry{};
        entry.kind = Kind::Level;
        bool entry_open = false;

        while (cursor < end) {
            Pair p{};
            // The frame is fully buffered, so truncation here means the
            // structure overran its own BodyLength.
            if (pair(bytes, cursor, p) != PairEnd::Ok) return Error::Malformed;
            switch (p.tag) {
                case 55: {
                    const std::string_view name{
                        reinterpret_cast<const char*>(bytes.data() + p.value_from), p.value_len};
                    const auto found = lookup(symbols_, name);
                    if (!found) return Error::UnknownSymbol;
                    symbol = *found;
                    break;
                }
                case 269: {
                    if (entry_open) {
                        if (auto error = flush(entry, symbol, sink)) return error;
                    }
                    entry_open = true;
                    if (p.value_len != 1) return Error::Malformed;
                    switch (at(bytes, p.value_from)) {
                        case '0': entry.side = Side::Bid; entry.kind = Kind::Level; break;
                        case '1': entry.side = Side::Ask; entry.kind = Kind::Level; break;
                        case '2': entry.side = Side::Bid; entry.kind = Kind::Trade; break;
                        default: return Error::Malformed;
                    }
                    break;
                }
                case 270:
                    if (!decimal(bytes, p, kPriceScale, entry.price)) return Error::Malformed;
                    break;
                case 271:
                    if (!decimal(bytes, p, kQtyScale, entry.qty)) return Error::Malformed;
                    break;
                default:
                    break; // sequence numbers, timestamps: real, just not book data
            }
        }
        if (entry_open) {
            if (auto error = flush(entry, symbol, sink)) return error;
        }
        return {};
    }

    static std::optional<Error> flush(Event& entry, std::optional<std::uint16_t> symbol, Sink& sink) {
        if (!symbol) return Error::Malformed;
        entry.symbol = *symbol;
        sink(entry);
        entry.price = 0;
        entry.qty = 0;
        return {};
    }

    std::span<const std::string_view> symbols_;
};

} // namespace t2t::feed
