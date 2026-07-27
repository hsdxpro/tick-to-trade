// Behaviour under one thread, correctness under two.
//
// The cross-thread tests are the ones TSAN instruments: compiled with
// -fsanitize=thread, a weakened memory ordering in the queue is reported as a
// data race on the slot bytes. Run both ways -- plain for behaviour, sanitized
// for orderings.

#include "../spsc.hpp"

#include <atomic>
#include <cstdint>
#include <cstdio>
#include <memory>
#include <string>
#include <thread>

namespace {

int failures = 0;

#define REQUIRE(expr)                                                       \
    do {                                                                    \
        if (!(expr)) {                                                      \
            ++failures;                                                     \
            std::printf("FAIL %s:%d: %s\n", __FILE__, __LINE__, #expr);     \
        }                                                                   \
    } while (0)

void values_cross_in_order() {
    t2t::SpscQueue<int> queue(4);
    for (int i = 0; i < 4; ++i) {
        REQUIRE(queue.try_push(int{i}));
    }
    REQUIRE(!queue.try_push(99)); // full at rounded capacity
    for (int i = 0; i < 4; ++i) {
        auto got = queue.try_pop();
        REQUIRE(got.has_value() && *got == i);
    }
    REQUIRE(!queue.try_pop().has_value());
}

void wraparound_reuses_slots_correctly() {
    t2t::SpscQueue<std::uint64_t> queue(4);
    for (std::uint64_t i = 0; i < 40'000; ++i) {
        REQUIRE(queue.try_push(std::uint64_t{i}));
        auto got = queue.try_pop();
        REQUIRE(got.has_value() && *got == i);
    }
}

void non_trivial_payloads_survive_the_crossing() {
    t2t::SpscQueue<std::string> queue(8);
    REQUIRE(queue.try_push(std::string(64, 'x'))); // past SSO, heap-backed
    REQUIRE(queue.try_push(std::string("short")));
    auto first = queue.try_pop();
    REQUIRE(first.has_value() && first->size() == 64);
    auto second = queue.try_pop();
    REQUIRE(second.has_value() && *second == "short");
}

void undelivered_items_destroy_exactly_once() {
    static std::atomic<int> live{0};
    struct Counted {
        Counted() { live.fetch_add(1); }
        Counted(Counted&&) noexcept { live.fetch_add(1); }
        Counted(const Counted&) = delete;
        Counted& operator=(const Counted&) = delete;
        Counted& operator=(Counted&&) = delete;
        ~Counted() { live.fetch_sub(1); }
    };
    {
        t2t::SpscQueue<Counted> queue(8);
        for (int i = 0; i < 5; ++i) {
            REQUIRE(queue.try_push(Counted{}));
        }
        (void)queue.try_pop(); // one delivered and destroyed by the caller
        // four remain in the ring when the queue itself dies
    }
    REQUIRE(live.load() == 0);
}

void a_million_items_cross_complete_and_ordered() {
    constexpr std::uint64_t kCount = 1'000'000;
    t2t::SpscQueue<std::uint64_t> queue(1024);

    std::thread producer([&queue] {
        for (std::uint64_t i = 0; i < kCount; ++i) {
            while (!queue.try_push(std::uint64_t{i})) {
            }
        }
    });

    std::uint64_t expected = 0;
    while (expected < kCount) {
        if (auto got = queue.try_pop()) {
            if (*got != expected) {
                ++failures;
                std::printf("FAIL: reordered at %llu\n",
                            static_cast<unsigned long long>(expected));
                break;
            }
            ++expected;
        }
    }
    producer.join();
    REQUIRE(!queue.try_pop().has_value());
}

void heap_payloads_cross_threads_without_racing() {
    // unique_ptr payloads make TSAN watch the pointee's memory as well as the
    // slot: a broken ordering shows up as a race on the boxed value.
    constexpr std::uint64_t kCount = 100'000;
    t2t::SpscQueue<std::unique_ptr<std::uint64_t>> queue(256);

    std::thread producer([&queue] {
        for (std::uint64_t i = 0; i < kCount; ++i) {
            auto boxed = std::make_unique<std::uint64_t>(i);
            while (!queue.try_push(std::move(boxed))) {
                boxed = std::make_unique<std::uint64_t>(i);
            }
        }
    });

    std::uint64_t expected = 0;
    while (expected < kCount) {
        if (auto got = queue.try_pop()) {
            if (**got != expected) {
                ++failures;
                std::printf("FAIL: boxed value corrupted at %llu\n",
                            static_cast<unsigned long long>(expected));
                break;
            }
            ++expected;
        }
    }
    producer.join();
}

} // namespace

int main() {
    values_cross_in_order();
    wraparound_reuses_slots_correctly();
    non_trivial_payloads_survive_the_crossing();
    undelivered_items_destroy_exactly_once();
    a_million_items_cross_complete_and_ordered();
    heap_payloads_cross_threads_without_racing();

    if (failures == 0) {
        std::printf("spsc: all tests passed\n");
        return 0;
    }
    std::printf("spsc: %d failure(s)\n", failures);
    return 1;
}
