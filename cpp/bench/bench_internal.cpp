// Internal tick-to-trade in C++, mirroring the Rust benchmark: the compute
// path on one thread, and the staged path across the production thread
// layout with the SPSC rings. Same probes, same stages the engine runs.

#include "../pipeline/pipeline.hpp"
#include "../spsc.hpp"

#include <algorithm>
#include <chrono>
#include <cstdio>
#include <thread>
#include <vector>

namespace {

using namespace t2t;
using namespace t2t::pipeline;

constexpr std::size_t kProbes = 100'000;
constexpr std::size_t kWarmup = 10'000;

using Clock = std::chrono::steady_clock;

void report(const char* name, std::vector<std::uint64_t>& samples) {
    std::sort(samples.begin(), samples.end());
    const auto at = [&](double q) {
        return samples[static_cast<std::size_t>(static_cast<double>(samples.size() - 1) * q)];
    };
    std::printf("%-46s min %6llu ns   p50 %6llu ns   p99 %6llu ns\n", name,
                static_cast<unsigned long long>(samples.front()),
                static_cast<unsigned long long>(at(0.5)),
                static_cast<unsigned long long>(at(0.99)));
}

std::vector<std::vector<std::byte>> probes() {
    std::vector<std::vector<std::byte>> out;
    out.reserve(kProbes + kWarmup);
    std::optional<std::uint64_t> previous{};
    for (std::size_t index = 0; index < kProbes + kWarmup; ++index) {
        const auto order = static_cast<std::uint64_t>(index) + 1;
        out.push_back(probe::datagram(previous, order, probe::price_of(index)));
        previous = order;
    }
    return out;
}

void compute_path() {
    const auto datagrams = probes();
    feed::Itch<FeedStage> parser{feed::synth::kTradfi};
    FeedStage stage(4, kBand);
    Strategy strategy;
    std::vector<std::uint64_t> samples;
    samples.reserve(kProbes);
    std::uint64_t orders = 0;

    for (std::size_t index = 0; index < datagrams.size(); ++index) {
        const auto started = Clock::now();
        (void)parser.parse(datagrams[index], stage);
        if (const auto update = stage.take_moved()) {
            if (const auto order = strategy.decide(*update)) {
                const auto encoded = order->encode();
                orders += static_cast<std::uint64_t>(encoded[0]) + 1;
            }
        }
        const auto elapsed =
            std::chrono::duration_cast<std::chrono::nanoseconds>(Clock::now() - started);
        if (index >= kWarmup) {
            samples.push_back(static_cast<std::uint64_t>(elapsed.count()));
        }
    }
    if (orders == 0) {
        std::printf("compute path produced no orders; nothing was measured\n");
        std::exit(1);
    }
    report("compute path (parse+book+decide+encode)", samples);
}

void staged_path() {
    const auto datagrams = probes();
    SpscQueue<std::pair<std::size_t, Clock::time_point>> to_feed(1024);
    SpscQueue<std::pair<BboUpdate, Clock::time_point>> to_strategy(1024);
    SpscQueue<std::pair<OrderCommand, Clock::time_point>> to_gateway(1024);

    std::thread feed_thread([&] {
        feed::Itch<FeedStage> parser{feed::synth::kTradfi};
        FeedStage stage(4, kBand);
        for (std::size_t n = 0; n < kProbes + kWarmup; ++n) {
            std::optional<std::pair<std::size_t, Clock::time_point>> item;
            while (!(item = to_feed.try_pop())) {
            }
            (void)parser.parse(datagrams[item->first], stage);
            if (const auto update = stage.take_moved()) {
                auto out = std::pair{*update, item->second};
                while (!to_strategy.try_push(std::move(out))) {
                }
            }
        }
    });

    std::thread strategy_thread([&] {
        Strategy strategy;
        for (std::size_t n = 0; n < kProbes + kWarmup; ++n) {
            std::optional<std::pair<BboUpdate, Clock::time_point>> item;
            while (!(item = to_strategy.try_pop())) {
            }
            if (const auto order = strategy.decide(item->first)) {
                auto out = std::pair{*order, item->second};
                while (!to_gateway.try_push(std::move(out))) {
                }
            }
        }
    });

    std::vector<std::uint64_t> samples;
    samples.reserve(kProbes);
    for (std::size_t index = 0; index < kProbes + kWarmup; ++index) {
        auto item = std::pair{index, Clock::now()};
        while (!to_feed.try_push(std::move(item))) {
        }
        std::optional<std::pair<OrderCommand, Clock::time_point>> answer;
        while (!(answer = to_gateway.try_pop())) {
        }
        const auto encoded = answer->first.encode();
        const auto elapsed =
            std::chrono::duration_cast<std::chrono::nanoseconds>(Clock::now() - answer->second);
        (void)encoded;
        if (index >= kWarmup) {
            samples.push_back(static_cast<std::uint64_t>(elapsed.count()));
        }
    }
    feed_thread.join();
    strategy_thread.join();
    report("staged path (3 threads, 3 hops incl. stand-in)", samples);
}

} // namespace

int main() {
    std::printf("internal tick-to-trade (C++), %zu probes after %zu warmup\n\n", kProbes,
                kWarmup);
    compute_path();
    staged_path();
    return 0;
}
