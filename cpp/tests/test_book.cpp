// Custom book against the std-collections book, on the same events, agreeing
// exactly -- best prices, full ladder contents, order state -- plus the order
// map churned against unordered_map over clustered keys.

#include "../book/book.hpp"
#include "../book/reference.hpp"
#include "../feed/itch.hpp"
#include "../feed/synth.hpp"

#include <cstdio>
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
constexpr Band kItchBand{(1'000'000 - 3'200) * 10'000LL, 100 * 10'000LL, 5'070};

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

} // namespace

int main() {
    books_agree_across_a_quarter_million_events();
    order_map_survives_churn_against_unordered_map();
    if (failures == 0) {
        std::printf("book: all tests passed\n");
        return 0;
    }
    std::printf("book: %d failure(s)\n", failures);
    return 1;
}
