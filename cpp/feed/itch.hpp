#pragma once

// NASDAQ ITCH 5.0, the subset that moves a book: S, A, E, X, D, U, P, framed
// SoupBinTCP-style with a two-byte big-endian length. Layouts per the
// TotalView-ITCH 5.0 specification. There is nothing clever here, which is
// the point of binary TradFi: the field is at the offset the spec says, and
// the fastest parse is a load.

#include "feed.hpp"

namespace t2t::feed {

template <typename Sink>
class Itch final {
public:
    explicit Itch(std::span<const std::string_view> symbols) : symbols_(symbols) {}

    Outcome parse(Bytes bytes, Sink& sink) const {
        std::size_t pos = 0;
        for (;;) {
            if (bytes.size() - pos < 2) {
                return {pos, {}};
            }
            const std::size_t length = be16(bytes, pos);
            if (length == 0) {
                return {pos, Error::Malformed};
            }
            if (bytes.size() - pos < 2 + length) {
                return {pos, {}}; // partial message: routine, not an error
            }
            if (auto error = message(bytes.subspan(pos + 2, length), sink)) {
                return {pos, *error};
            }
            pos += 2 + length;
        }
    }

private:
    std::optional<Error> message(Bytes body, Sink& sink) const {
        Event event{};
        switch (at(body, 0)) {
            case 'S':
                if (body.size() != 12) return Error::Malformed;
                return {}; // session plumbing, not book data
            case 'A': {
                if (body.size() != 36) return Error::Malformed;
                const auto locate = be16(body, 1);
                if (locate >= symbols_.size() || trimmed(body, 24) != symbols_[locate]) {
                    return Error::UnknownSymbol;
                }
                event.kind = Kind::Add;
                event.symbol = locate;
                event.order_id = be64(body, 11);
                if (!side_of(at(body, 19), event.side)) return Error::Malformed;
                event.qty = std::int64_t{be32(body, 20)} * kQtyScale;
                event.price = std::int64_t{be32(body, 32)} * (kPriceScale / 10'000);
                break;
            }
            case 'E':
                if (body.size() != 31) return Error::Malformed;
                event.kind = Kind::Execute;
                event.symbol = be16(body, 1);
                event.order_id = be64(body, 11);
                event.qty = std::int64_t{be32(body, 19)} * kQtyScale;
                event.aux = be64(body, 23);
                break;
            case 'X':
                if (body.size() != 23) return Error::Malformed;
                event.kind = Kind::Cancel;
                event.symbol = be16(body, 1);
                event.order_id = be64(body, 11);
                event.qty = std::int64_t{be32(body, 19)} * kQtyScale;
                break;
            case 'D':
                if (body.size() != 19) return Error::Malformed;
                event.kind = Kind::Delete;
                event.symbol = be16(body, 1);
                event.order_id = be64(body, 11);
                break;
            case 'U':
                if (body.size() != 35) return Error::Malformed;
                event.kind = Kind::Replace;
                event.symbol = be16(body, 1);
                event.order_id = be64(body, 11);
                event.aux = be64(body, 19);
                event.qty = std::int64_t{be32(body, 27)} * kQtyScale;
                event.price = std::int64_t{be32(body, 31)} * (kPriceScale / 10'000);
                break;
            case 'P':
                if (body.size() != 44) return Error::Malformed;
                event.kind = Kind::Trade;
                event.symbol = be16(body, 1);
                event.order_id = be64(body, 11);
                if (!side_of(at(body, 19), event.side)) return Error::Malformed;
                event.qty = std::int64_t{be32(body, 20)} * kQtyScale;
                event.price = std::int64_t{be32(body, 32)} * (kPriceScale / 10'000);
                event.aux = be64(body, 36);
                break;
            default:
                return Error::Malformed;
        }
        sink(event);
        return {};
    }

    static bool side_of(std::uint8_t byte, Side& out) {
        if (byte == 'B') {
            out = Side::Bid;
            return true;
        }
        if (byte == 'S') {
            out = Side::Ask;
            return true;
        }
        return false;
    }

    /// ITCH alpha fields are space-padded on the right.
    static std::string_view trimmed(Bytes body, std::size_t from) {
        std::size_t len = 8;
        while (len > 0 && at(body, from + len - 1) == ' ') {
            --len;
        }
        return {reinterpret_cast<const char*>(body.data() + from), len};
    }

    std::span<const std::string_view> symbols_;
};

} // namespace t2t::feed
