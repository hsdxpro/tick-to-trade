// Cross-thread throughput, same method as the Rust twin so the two numbers
// can be read against each other: N items from one spinning thread to
// another, three runs, best and worst printed.

#include "../spsc.hpp"

#include <algorithm>
#include <chrono>
#include <cstdint>
#include <cstdio>
#include <thread>
#include <vector>

namespace {

constexpr std::uint64_t kCount = 20'000'000;
constexpr int kRuns = 3;

double one_run() {
    t2t::SpscQueue<std::uint64_t> queue(1024);
    const auto started = std::chrono::steady_clock::now();

    std::thread producer([&queue] {
        for (std::uint64_t i = 0; i < kCount; ++i) {
            while (!queue.try_push(std::uint64_t{i})) {
            }
        }
    });

    std::uint64_t seen = 0;
    while (seen < kCount) {
        if (queue.try_pop()) {
            ++seen;
        }
    }
    producer.join();

    const std::chrono::duration<double> elapsed =
        std::chrono::steady_clock::now() - started;
    return static_cast<double>(kCount) / elapsed.count();
}

} // namespace

int main() {
    std::printf("%llu items, one producer thread to one consumer thread, both spinning\n\n",
                static_cast<unsigned long long>(kCount));
    std::vector<double> runs;
    runs.reserve(kRuns);
    for (int i = 0; i < kRuns; ++i) {
        runs.push_back(one_run());
    }
    std::sort(runs.begin(), runs.end());
    std::printf("t2t::SpscQueue  %12.0f/sec best  %12.0f/sec worst  %6.1f ns/item\n",
                runs.back(), runs.front(), 1e9 / runs.back());
    return 0;
}
