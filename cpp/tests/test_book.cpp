// Custom book against the std-collections book, on the same events, agreeing
// exactly -- best prices, full ladder contents, order state -- plus the order
// map churned against unordered_map over clustered keys.

#include "../book/book.hpp"
#include "../book/reference.hpp"
#include "../feed/itch.hpp"
#include "../feed/synth.hpp"

#include <algorithm>
#include <cstdio>
#include <deque>
#include <map>
#include <utility>
#include <vector>

namespace {

using namespace t2t::feed;
using namespace t2t::book;

int failures = 0;

#define REQUIRE(expr)                                                       \
    do {                                                                    \
        if (!(expr)) {                                                      \
            ++failures;                                                     \
            std::printf("FAIL %s:%d: %s\n", __FILE__, __LINE__, #expr);     \
        }                                                                   \
    } while (0)

/// The band the ITCH generator's mid-walk stays inside, in price units.
constexpr Band kItchBand{100 * 10'000LL, 4'096};

bool agree(const Books& books, const ReferenceBooks& reference, std::size_t at_event) {
    for (std::size_t index = 0; index < reference.symbols.size(); ++index) {
        const auto& custom = books.symbol(static_cast<std::uint16_t>(index));
        const auto& std_book = reference.symbols[index];

        if (custom.bids.best() != std_book.best_bid()
            || custom.asks.best() != std_book.best_ask()) {
            std::printf("FAIL: best diverged on symbol %zu after event %zu\n", index, at_event);
            return false;
        }
        std::vector<std::pair<std::int64_t, std::int64_t>> got;
        custom.bids.for_each_from_touch([&](auto p, auto q) { got.emplace_back(p, q); });
        std::vector<std::pair<std::int64_t, std::int64_t>> want(std_book.bids.rbegin(),
                                                                std_book.bids.rend());
        if (got != want) {
            std::printf("FAIL: bid ladder diverged on symbol %zu\n", index);
            return false;
        }
        got.clear();
        custom.asks.for_each_from_touch([&](auto p, auto q) { got.emplace_back(p, q); });
        want.assign(std_book.asks.begin(), std_book.asks.end());
        if (got != want) {
            std::printf("FAIL: ask ladder diverged on symbol %zu\n", index);
            return false;
        }
        if (const_cast<SymbolBook&>(custom).orders.size() != std_book.orders.size()) {
            std::printf("FAIL: order count diverged on symbol %zu\n", index);
            return false;
        }
        for (const auto& [id, order] : std_book.orders) {
            auto* held = const_cast<SymbolBook&>(custom).orders.find(id);
            if (held == nullptr || !(*held == order)) {
                std::printf("FAIL: order %llu diverged on symbol %zu\n",
                            static_cast<unsigned long long>(id), index);
                return false;
            }
        }
    }
    return books.unknown_orders() == reference.unknown_orders;
}

void books_agree_across_a_quarter_million_events() {
    Rng rng{kGeneratorSeed};
    const auto stream = synth::itch(250'000, rng);
    std::vector<Event> events;
    struct Sink {
        std::vector<Event>* out;
        void operator()(const Event& e) const { out->push_back(e); }
    } sink{&events};
    REQUIRE(Itch<Sink>{synth::kTradfi}.parse(stream.bytes, sink).ok());

    Books books(4, kItchBand);
    ReferenceBooks reference(4);
    for (std::size_t i = 0; i < events.size(); ++i) {
        books.apply(events[i]);
        reference.apply(events[i]);
        if (i % 5'000 == 0) {
            REQUIRE(agree(books, reference, i));
            if (failures != 0) return;
        }
    }
    REQUIRE(agree(books, reference, events.size()));
    REQUIRE(reference.symbols[0].orders.size() > 100);
}

void order_map_survives_churn_against_unordered_map() {
    Rng rng{kGeneratorSeed ^ 0x22};
    OrderMap custom(64);
    std::unordered_map<std::uint64_t, Order> reference;

    for (int i = 0; i < 500'000; ++i) {
        const auto key = 1 + rng.below(4'096);
        if (rng.below(2) == 0) {
            const Order order{0, Side::Bid, static_cast<std::int64_t>(rng.below(1'000)),
                              static_cast<std::int64_t>(1 + rng.below(100))};
            custom.insert(key, order);
            reference[key] = order;
        } else {
            const auto mine = custom.remove(key);
            const auto it = reference.find(key);
            const bool theirs = it != reference.end();
            if (mine.has_value() != theirs || (theirs && !(*mine == it->second))) {
                REQUIRE(!"removal diverged");
                return;
            }
            if (theirs) {
                reference.erase(it);
            }
        }
    }
    REQUIRE(custom.size() == reference.size());
    for (const auto& [key, order] : reference) {
        auto* held = custom.find(key);
        REQUIRE(held != nullptr && *held == order);
    }
}

/// One ladder, three instruments that share nothing: a penny-tick equity near
/// $30, a cent-tick perpetual near $100,000, and a satoshi-tick pair near
/// $0.30. None declares a range, and each is walked with enough drift to drag
/// the window across dozens of its own widths.
///
/// Liquidity is aged out behind the touch the way a real book cancels it, so
/// the live set never falls a window behind and nothing is ever evicted. That
/// matters: eviction is lossy by design, and a test that tolerated it would be
/// comparing against a reference it had licence to disagree with. Here the
/// agreement is exact, every level, after every window the market crossed.
void one_ladder_serves_any_asset_and_any_tick() {
    struct Instrument {
        const char* name;
        std::int64_t tick;
        std::int64_t start;
    };
    // Prices in fixed point: 1e-4 for the equity and perp, 1e-8 for the pair.
    constexpr Instrument kInstruments[] = {
        {"equity, penny tick, $30", 100, 30 * 10'000LL},
        {"perp, cent tick, $100k", 100, 100'000 * 10'000LL},
        {"pair, satoshi tick, $0.30", 1, 30'000'000LL},
    };
    constexpr std::size_t kLive = 400; // resting levels before the oldest is pulled

    for (const auto& instrument : kInstruments) {
        Ladder ladder = Ladder::bids(Band{instrument.tick, 4'096});
        std::map<std::int64_t, std::int64_t> reference;
        std::deque<std::int64_t> resting;
        Rng rng{kGeneratorSeed ^ 0x5d};

        auto mid = instrument.start;
        for (int step = 0; step < 100'000; ++step) {
            // A drifting walk: two ticks a step on average, so 100,000 steps
            // carry the touch about fifty window widths from where it began.
            mid += (static_cast<std::int64_t>(rng.below(17)) - 6) * instrument.tick;
            const auto price =
                mid - static_cast<std::int64_t>(rng.below(64)) * instrument.tick;
            const auto qty = static_cast<std::int64_t>(1 + rng.below(500));
            ladder.set(price, qty);
            reference[price] = qty;
            resting.push_back(price);

            while (resting.size() > kLive) {
                const auto stale = resting.front();
                resting.pop_front();
                // Only pull it if no later step refreshed the same price.
                if (std::find(resting.begin(), resting.end(), stale) == resting.end()) {
                    ladder.set(stale, 0);
                    reference.erase(stale);
                }
            }
        }

        REQUIRE(ladder.off_grid() == 0);
        REQUIRE(ladder.evicted() == 0);
        std::vector<std::pair<std::int64_t, std::int64_t>> got;
        ladder.for_each_from_touch([&](auto p, auto q) { got.emplace_back(p, q); });
        const std::vector<std::pair<std::int64_t, std::int64_t>> want(reference.rbegin(),
                                                                      reference.rend());
        if (got != want) {
            ++failures;
            std::printf("FAIL: %s diverged: %zu levels against the reference's %zu\n",
                        instrument.name, got.size(), want.size());
        }
        // Without this the test would pass on a ladder that never moved.
        if (ladder.rebases() < 10) {
            ++failures;
            std::printf("FAIL: %s crossed only %llu windows\n", instrument.name,
                        static_cast<unsigned long long>(ladder.rebases()));
        }
    }
}

/// A deep book that follows the market keeps its depth.
///
/// This is the test that decides how the window moves. Centring it on the new
/// price is the obvious choice and the wrong one: it discards everything more
/// than half a window away, and a venue publishing full depth has most of its
/// book sitting exactly there. Shifting the minimum that admits the price,
/// plus a quarter window of hysteresis, keeps all of it.
///
/// 2,500 levels in a 4,096-tick window -- the shape of a full-depth feed --
/// walked five thousand ticks, further than the window is wide.
void a_deep_book_keeps_its_depth_as_the_market_moves() {
    constexpr std::int64_t kDepth = 2'500;
    Ladder ladder = Ladder::bids(Band{1, 4'096});
    std::map<std::int64_t, std::int64_t> reference;
    std::int64_t touch = 1'000'000;

    for (std::int64_t level = 0; level < kDepth; ++level) {
        ladder.set(touch - level, 10 + level);
        reference[touch - level] = 10 + level;
    }

    for (int sweep = 0; sweep < 50; ++sweep) {
        for (int step = 0; step < 100; ++step) {
            ++touch;
            ladder.set(touch, 10);
            reference[touch] = 10;
            ladder.set(touch - kDepth, 0);
            reference.erase(touch - kDepth);
        }
        REQUIRE(ladder.evicted() == 0);
        if (failures != 0) return;
    }

    std::vector<std::pair<std::int64_t, std::int64_t>> got;
    ladder.for_each_from_touch([&](auto p, auto q) { got.emplace_back(p, q); });
    const std::vector<std::pair<std::int64_t, std::int64_t>> want(reference.rbegin(),
                                                                  reference.rend());
    REQUIRE(got == want);
    REQUIRE(ladder.rebases() >= 2);
}

/// Shifting the window drops exactly the levels it strands and keeps the
/// rest untouched -- both directions, with survivors and with none.
///
/// The other tests all run with nothing evicted, which leaves the eviction
/// accounting itself unpinned: an off-by-one in the dropped range passes
/// every one of them. This test is the one that fails.
void a_window_shift_evicts_exactly_the_levels_it_strands() {
    Ladder ladder = Ladder::bids(Band{1, 4'096});
    std::map<std::int64_t, std::int64_t> reference;
    const std::int64_t touch = 1'000'000;
    for (std::int64_t level = 0; level < 2'500; ++level) {
        ladder.set(touch - level, 10 + level);
        reference[touch - level] = 10 + level;
    }
    REQUIRE(ladder.evicted() == 0);

    // Up: the market gaps half a window higher, stranding the lowest levels.
    const auto up = touch + 2'048;
    const auto before = ladder.depth();
    ladder.set(up, 7);
    reference[up] = 7;
    const auto dropped_up = static_cast<std::size_t>(ladder.evicted());
    REQUIRE(dropped_up > 0);
    REQUIRE(ladder.depth() + dropped_up == before + 1);
    // Survivors are exactly the highest reference levels, and the depth
    // counter agrees with a walk of the bitmap itself -- the walk is the
    // ground truth a drifting counter cannot hide from.
    std::vector<std::pair<std::int64_t, std::int64_t>> held;
    ladder.for_each_from_touch([&](auto p, auto q) { held.emplace_back(p, q); });
    REQUIRE(ladder.depth() == held.size());
    std::vector<std::pair<std::int64_t, std::int64_t>> expected;
    for (auto it = reference.rbegin(); it != reference.rend() && expected.size() < held.size(); ++it) {
        expected.emplace_back(it->first, it->second);
    }
    REQUIRE(held == expected);

    // A jump wider than the window drops everything it held.
    const auto far = up + 100'000;
    const auto held_now = static_cast<std::uint64_t>(ladder.depth());
    const auto evicted_before_far = ladder.evicted();
    ladder.set(far, 3);
    REQUIRE(ladder.evicted() == evicted_before_far + held_now);
    REQUIRE(ladder.depth() == 1);
    REQUIRE(ladder.best() == std::make_pair(std::int64_t{far}, std::int64_t{3}));

    // Down: rebuild below the survivor, then gap down far enough to strand
    // the top of the book but not all of it.
    reference.clear();
    reference[far] = 3;
    for (std::int64_t level = 1; level < 2'500; ++level) {
        ladder.set(far - level, 20 + level);
        reference[far - level] = 20 + level;
    }
    const auto evicted_before = static_cast<std::size_t>(ladder.evicted());
    const auto before_down = ladder.depth();
    const auto down = far - 4'600;
    ladder.set(down, 5);
    reference[down] = 5;
    const auto dropped_down = static_cast<std::size_t>(ladder.evicted()) - evicted_before;
    REQUIRE(dropped_down > 0);
    REQUIRE(ladder.depth() + dropped_down == before_down + 1);
    // The strands are the highest prices, so the ladder holds everything
    // below them: the reference minus its top `dropped_down` levels.
    held.clear();
    ladder.for_each_from_touch([&](auto p, auto q) { held.emplace_back(p, q); });
    REQUIRE(ladder.depth() == held.size());
    expected.clear();
    auto it = reference.rbegin();
    for (std::size_t skip = 0; skip < dropped_down && it != reference.rend(); ++skip) {
        ++it;
    }
    for (; it != reference.rend(); ++it) {
        expected.emplace_back(it->first, it->second);
    }
    REQUIRE(held == expected);
}

/// A price off the tick grid is refused and counted, not folded into the
/// neighbouring level -- the failure mode nobody notices until the book is
/// wrong and there is nothing to point at.
void an_off_grid_price_is_refused_rather_than_rounded() {
    Ladder ladder = Ladder::asks(Band{100, 4'096});
    ladder.set(10'000, 5);
    ladder.set(10'050, 7); // half a tick
    REQUIRE(ladder.off_grid() == 1);
    REQUIRE(ladder.depth() == 1);
    REQUIRE(ladder.best() == std::make_pair(std::int64_t{10'000}, std::int64_t{5}));
}

} // namespace

int main() {
    books_agree_across_a_quarter_million_events();
    order_map_survives_churn_against_unordered_map();
    one_ladder_serves_any_asset_and_any_tick();
    a_deep_book_keeps_its_depth_as_the_market_moves();
    a_window_shift_evicts_exactly_the_levels_it_strands();
    an_off_grid_price_is_refused_rather_than_rounded();
    if (failures == 0) {
        std::printf("book: all tests passed\n");
        return 0;
    }
    std::printf("book: %d failure(s)\n", failures);
    return 1;
}
