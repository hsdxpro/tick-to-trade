#pragma once

// The pipeline's shared vocabulary, mirroring rust/pipeline/src/lib.rs: the
// BBO signal, the order command and its 32-byte wire form, the feed and
// strategy stages, and the probe builder. One header, used by the internal
// benchmark and the engine binary alike -- a benchmark of a copy measures
// the copy, so there is no copy.

#include "../book/book.hpp"
#include "../feed/itch.hpp"
#include "../feed/synth.hpp"

#include <array>
#include <cstring>
#include <optional>

namespace t2t::pipeline {

using book::Band;
using book::Books;
using feed::Event;
using feed::Side;

/// The band the harness's probes stay inside.
inline constexpr Band kBand{(1'000'000 - 3'200) * 10'000LL, 100 * 10'000LL, 5'070};

struct BboUpdate {
    std::uint16_t symbol{0};
    std::int64_t bid_price{0};
    std::int64_t bid_qty{0};
    std::int64_t ask_price{0};
    std::int64_t ask_qty{0};
};

inline constexpr std::size_t kOrderWireLen = 32;

struct OrderCommand {
    std::uint64_t client_order_id{0};
    std::uint16_t symbol{0};
    Side side{Side::Bid};
    std::int64_t price{0};
    std::int64_t qty{0};

    [[nodiscard]] std::array<std::byte, kOrderWireLen> encode() const {
        std::array<std::byte, kOrderWireLen> out{};
        std::memcpy(out.data(), &client_order_id, 8);
        std::memcpy(out.data() + 8, &symbol, 2);
        out[10] = static_cast<std::byte>(side);
        std::memcpy(out.data() + 16, &price, 8);
        std::memcpy(out.data() + 24, &qty, 8);
        return out;
    }
};

/// The feed stage: books, and the last touch seen for symbol zero.
class FeedStage final {
public:
    explicit FeedStage(std::size_t symbols, const Band& band) : books_(symbols, band) {}

    void operator()(const Event& event) {
        books_.apply(event);
        const auto& book = books_.symbol(0);
        const auto bid = book.bids.best().value_or(std::pair{0LL, 0LL});
        const auto ask = book.asks.best().value_or(std::pair{0LL, 0LL});
        if (bid.first != touch_bid_ || ask.first != touch_ask_) {
            touch_bid_ = bid.first;
            touch_ask_ = ask.first;
            moved_ = BboUpdate{0, bid.first, bid.second, ask.first, ask.second};
        }
    }

    [[nodiscard]] std::optional<BboUpdate> take_moved() {
        auto out = moved_;
        moved_.reset();
        return out;
    }

private:
    Books books_;
    std::int64_t touch_bid_{0};
    std::int64_t touch_ask_{0};
    std::optional<BboUpdate> moved_{};
};

/// The strategy stage: one order per bid price change with liquidity behind
/// it. Deliberately trivial; everything around it is what gets measured.
class Strategy final {
public:
    [[nodiscard]] std::optional<OrderCommand> decide(const BboUpdate& update) {
        std::optional<OrderCommand> order{};
        if (update.bid_price != last_bid_ && update.bid_qty > 0) {
            order = OrderCommand{++next_id_, update.symbol, Side::Ask, update.bid_price,
                                 std::min<std::int64_t>(update.bid_qty, 100'000'000)};
        }
        last_bid_ = update.bid_price;
        return order;
    }

private:
    std::int64_t last_bid_{0};
    std::uint64_t next_id_{0};
};

/// Probe datagrams, identical to the Rust builder: delete the previous
/// order, add the next at a price cycling inside the band.
namespace probe {

inline void frame(std::vector<std::byte>& out, const std::vector<std::byte>& body) {
    const auto len = static_cast<std::uint16_t>(body.size());
    out.push_back(static_cast<std::byte>(len >> 8));
    out.push_back(static_cast<std::byte>(len & 0xff));
    out.insert(out.end(), body.begin(), body.end());
}

[[nodiscard]] inline std::uint32_t price_of(std::size_t index) {
    return 1'000'000 + (static_cast<std::uint32_t>(index) % 2'000) * 100;
}

[[nodiscard]] inline std::vector<std::byte> datagram(std::optional<std::uint64_t> previous,
                                                     std::uint64_t order, std::uint32_t price) {
    using feed::synth::push_be16;
    using feed::synth::push_be32;
    using feed::synth::push_be64;
    using feed::synth::push_bytes;
    using feed::synth::push_zeros;

    std::vector<std::byte> out;
    out.reserve(2 + 19 + 2 + 36);
    if (previous) {
        std::vector<std::byte> body;
        body.push_back(static_cast<std::byte>('D'));
        push_be16(body, 0);
        push_zeros(body, 8);
        push_be64(body, *previous);
        frame(out, body);
    }
    std::vector<std::byte> body;
    body.push_back(static_cast<std::byte>('A'));
    push_be16(body, 0);
    push_zeros(body, 8);
    push_be64(body, order);
    body.push_back(static_cast<std::byte>('B'));
    push_be32(body, 100);
    push_bytes(body, "AAPL    ", 8);
    push_be32(body, price);
    frame(out, body);
    return out;
}

} // namespace probe

} // namespace t2t::pipeline
