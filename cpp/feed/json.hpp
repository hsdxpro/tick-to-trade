#pragma once

// Binance-shaped JSON, newline-delimited: trades and depth updates. The
// honest hot-path position: you do not parse JSON generically. The schema is
// documented and stable, so this is a scanner for that schema -- no DOM, no
// allocation, unknown keys skipped as balanced structure so a new field from
// the exchange is a Tuesday, not an outage.

#include "feed.hpp"

#include <limits>

namespace t2t::feed {

template <typename Sink>
class Json final {
public:
    explicit Json(std::span<const std::string_view> symbols) : symbols_(symbols) {}

    Outcome parse(Bytes bytes, Sink& sink) const {
        std::size_t pos = 0;
        for (;;) {
            std::size_t line_end = pos;
            while (line_end < bytes.size() && at(bytes, line_end) != '\n') {
                ++line_end;
            }
            if (line_end == bytes.size()) {
                return {pos, {}}; // a line without its newline is a short read
            }
            Scan scan{bytes.first(line_end), pos};
            if (auto error = message(scan, sink)) {
                return {pos, *error};
            }
            scan.skip_ws();
            if (scan.at != scan.bytes.size()) {
                return {pos, Error::Malformed};
            }
            pos = line_end + 1;
        }
    }

private:
    struct Scan {
        Bytes bytes;
        std::size_t at;

        /// Bounds check and read, kept as two operations.
        ///
        /// The obvious `std::optional<std::uint8_t> peek()` costs an optional
        /// construction on *every byte inspected*, and a scanner inspects
        /// every byte of every message. Measured, that was the whole of this
        /// parser's deficit against its Rust twin. The pair below carries the
        /// same information and compiles to a compare and a load.
        [[nodiscard]] bool done() const { return at >= bytes.size(); }

        /// Precondition: `!done()`.
        [[nodiscard]] std::uint8_t current() const { return feed::at(bytes, at); }

        void skip_ws() {
            while (!done()) {
                const auto byte = current();
                if (byte != ' ' && byte != '\t' && byte != '\r') {
                    return;
                }
                ++at;
            }
        }

        bool expect(std::uint8_t byte) {
            skip_ws();
            if (!done() && current() == byte) {
                ++at;
                return true;
            }
            return false;
        }

        /// A string with no escapes: symbols, event types and decimal-as-
        /// string fields are documented that way, and refusing an escape is
        /// better than mis-decoding one.
        bool string(std::string_view& out) {
            if (!expect('"')) return false;
            const std::size_t from = at;
            for (;;) {
                if (done()) return false;
                const auto byte = current();
                if (byte == '"') {
                    out = {reinterpret_cast<const char*>(bytes.data() + from), at - from};
                    ++at;
                    return true;
                }
                if (byte == '\\') return false;
                ++at;
            }
        }

        /// Skips any value, balanced; the cost only exists for keys the
        /// schema does not need.
        bool skip_value() {
            skip_ws();
            if (done()) return false;
            const auto first = current();
            if (first == '"') {
                std::string_view ignored;
                return string(ignored);
            }
            if (first == '{' || first == '[') {
                int depth = 0;
                bool in_string = false;
                for (;;) {
                    if (done()) return false;
                    const auto byte = current();
                    ++at;
                    if (in_string) {
                        if (byte == '\\') {
                            ++at;
                        } else if (byte == '"') {
                            in_string = false;
                        }
                    } else if (byte == '"') {
                        in_string = true;
                    } else if (byte == '{' || byte == '[') {
                        ++depth;
                    } else if (byte == '}' || byte == ']') {
                        if (--depth == 0) return true;
                    }
                }
            }
            while (!done()) {
                const auto byte = current();
                if (byte == ',' || byte == '}' || byte == ']') return true;
                ++at;
            }
            return false;
        }

        /// "1234.5678": a decimal in a string, scaled during the scan.
        bool quoted_decimal(std::int64_t scale, std::int64_t& out) {
            std::string_view raw;
            if (!string(raw)) return false;
            std::size_t i = 0;
            std::int64_t value = 0;
            bool any = false;
            // Eighteen digits keeps the accumulation itself inside int64; the
            // scaling below is then the only step that can still overflow, and
            // it is checked. Without both, a long run of digits is signed
            // overflow -- undefined here, and a wrapped price entering the book
            // as fact wherever it is not.
            for (; i < raw.size() && raw[i] != '.'; ++i) {
                if (raw[i] < '0' || raw[i] > '9' || i >= 18) return false;
                value = value * 10 + (raw[i] - '0');
                any = true;
            }
            if (!any) return false;
            if (value > std::numeric_limits<std::int64_t>::max() / scale) return false;
            value *= scale;
            if (i < raw.size()) {
                ++i;
                std::int64_t worth = scale;
                for (; i < raw.size(); ++i) {
                    if (raw[i] < '0' || raw[i] > '9') return false;
                    worth /= 10;
                    if (worth == 0) return false;
                    value += (raw[i] - '0') * worth;
                }
            }
            out = value;
            return true;
        }
    };

    std::optional<Error> message(Scan& scan, Sink& sink) const {
        if (!scan.expect('{')) return Error::Malformed;
        std::string_view kind{};
        std::optional<std::uint16_t> symbol{};
        std::int64_t price = 0;
        std::int64_t qty = 0;
        bool maker_is_buyer = false;
        std::uint64_t trade_id = 0;

        for (;;) {
            scan.skip_ws();
            std::string_view key;
            if (!scan.string(key)) return Error::Malformed;
            if (!scan.expect(':')) return Error::Malformed;

            if (key == "e") {
                if (!scan.string(kind)) return Error::Malformed;
            } else if (key == "s") {
                std::string_view name;
                if (!scan.string(name)) return Error::Malformed;
                const auto found = lookup(symbols_, name);
                if (!found) return Error::UnknownSymbol;
                symbol = *found;
            } else if (key == "p") {
                if (!scan.quoted_decimal(kPriceScale, price)) return Error::Malformed;
            } else if (key == "q") {
                if (!scan.quoted_decimal(kQtyScale, qty)) return Error::Malformed;
            } else if (key == "m") {
                scan.skip_ws();
                if (scan.done()) return Error::Malformed;
                const auto byte = scan.current();
                if (byte == 't') {
                    scan.at += 4;
                    maker_is_buyer = true;
                } else if (byte == 'f') {
                    scan.at += 5;
                    maker_is_buyer = false;
                } else {
                    return Error::Malformed;
                }
            } else if (key == "t") {
                scan.skip_ws();
                std::uint64_t id = 0;
                int seen = 0;
                while (!scan.done()) {
                    const auto byte = scan.current();
                    if (byte < '0' || byte > '9') break;
                    // Nineteen digits is the most a uint64 holds. Past that a
                    // trade id wraps into a different, plausible id -- a
                    // corrupted field becoming a fact the system trusts
                    // rather than a message it rejects.
                    if (seen == 19) return Error::Malformed;
                    id = id * 10 + (byte - '0');
                    ++seen;
                    ++scan.at;
                }
                trade_id = id;
            } else if (key == "b" || key == "a") {
                if (!symbol) return Error::Malformed;
                const Side side = key == "b" ? Side::Bid : Side::Ask;
                if (!scan.expect('[')) return Error::Malformed;
                scan.skip_ws();
                while (!scan.done() && scan.current() != ']') {
                    if (!scan.expect('[')) return Error::Malformed;
                    Event event{};
                    event.kind = Kind::Level;
                    event.side = side;
                    event.symbol = *symbol;
                    if (!scan.quoted_decimal(kPriceScale, event.price)) return Error::Malformed;
                    if (!scan.expect(',')) return Error::Malformed;
                    if (!scan.quoted_decimal(kQtyScale, event.qty)) return Error::Malformed;
                    if (!scan.expect(']')) return Error::Malformed;
                    sink(event);
                    scan.skip_ws();
                    if (!scan.done() && scan.current() == ',') {
                        ++scan.at;
                        scan.skip_ws();
                    }
                }
                if (scan.done()) return Error::Malformed;
                ++scan.at; // the ']'
            } else {
                if (!scan.skip_value()) return Error::Malformed;
            }

            scan.skip_ws();
            if (scan.done()) return Error::Malformed;
            const auto byte = scan.current();
            if (byte == ',') {
                ++scan.at;
            } else if (byte == '}') {
                ++scan.at;
                break;
            } else {
                return Error::Malformed;
            }
        }

        if (kind == "trade") {
            if (!symbol) return Error::Malformed;
            Event event{};
            event.kind = Kind::Trade;
            // Binance semantics: m=true means the buyer was the maker, so the
            // aggressor hit the bid.
            event.side = maker_is_buyer ? Side::Ask : Side::Bid;
            event.symbol = *symbol;
            event.price = price;
            event.qty = qty;
            event.aux = trade_id;
            sink(event);
        }
        return {};
    }

    std::span<const std::string_view> symbols_;
};

} // namespace t2t::feed
