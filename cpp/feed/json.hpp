#pragma once

// Binance-shaped JSON, newline-delimited: trades and depth updates. The
// honest hot-path position: you do not parse JSON generically. The schema is
// documented and stable, so this is a scanner for that schema -- no DOM, no
// allocation, unknown keys skipped as balanced structure so a new field from
// the exchange is a Tuesday, not an outage.

#include "feed.hpp"

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

        [[nodiscard]] std::optional<std::uint8_t> peek() const {
            return at < bytes.size() ? std::optional{feed::at(bytes, at)} : std::nullopt;
        }

        void skip_ws() {
            while (true) {
                const auto byte = peek();
                if (byte == ' ' || byte == '\t' || byte == '\r') {
                    ++at;
                } else {
                    return;
                }
            }
        }

        bool expect(std::uint8_t byte) {
            skip_ws();
            if (peek() == byte) {
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
                const auto byte = peek();
                if (!byte) return false;
                if (*byte == '"') {
                    out = {reinterpret_cast<const char*>(bytes.data() + from), at - from};
                    ++at;
                    return true;
                }
                if (*byte == '\\') return false;
                ++at;
            }
        }

        /// Skips any value, balanced; the cost only exists for keys the
        /// schema does not need.
        bool skip_value() {
            skip_ws();
            const auto first = peek();
            if (!first) return false;
            if (*first == '"') {
                std::string_view ignored;
                return string(ignored);
            }
            if (*first == '{' || *first == '[') {
                int depth = 0;
                bool in_string = false;
                for (;;) {
                    const auto byte = peek();
                    if (!byte) return false;
                    ++at;
                    if (in_string) {
                        if (*byte == '\\') {
                            ++at;
                        } else if (*byte == '"') {
                            in_string = false;
                        }
                    } else if (*byte == '"') {
                        in_string = true;
                    } else if (*byte == '{' || *byte == '[') {
                        ++depth;
                    } else if (*byte == '}' || *byte == ']') {
                        if (--depth == 0) return true;
                    }
                }
            }
            while (true) {
                const auto byte = peek();
                if (!byte) return false;
                if (*byte == ',' || *byte == '}' || *byte == ']') return true;
                ++at;
            }
        }

        /// "1234.5678": a decimal in a string, scaled during the scan.
        bool quoted_decimal(std::int64_t scale, std::int64_t& out) {
            std::string_view raw;
            if (!string(raw)) return false;
            std::size_t i = 0;
            std::int64_t value = 0;
            bool any = false;
            for (; i < raw.size() && raw[i] != '.'; ++i) {
                if (raw[i] < '0' || raw[i] > '9') return false;
                value = value * 10 + (raw[i] - '0');
                any = true;
            }
            if (!any) return false;
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
                const auto byte = scan.peek();
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
                while (true) {
                    const auto byte = scan.peek();
                    if (!byte || *byte < '0' || *byte > '9') break;
                    id = id * 10 + (*byte - '0');
                    ++scan.at;
                }
                trade_id = id;
            } else if (key == "b" || key == "a") {
                if (!symbol) return Error::Malformed;
                const Side side = key == "b" ? Side::Bid : Side::Ask;
                if (!scan.expect('[')) return Error::Malformed;
                scan.skip_ws();
                while (scan.peek() != ']') {
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
                    if (scan.peek() == ',') {
                        ++scan.at;
                        scan.skip_ws();
                    }
                }
                ++scan.at; // the ']'
            } else {
                if (!scan.skip_value()) return Error::Malformed;
            }

            scan.skip_ws();
            const auto byte = scan.peek();
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
