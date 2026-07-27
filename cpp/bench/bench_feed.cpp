// Parse throughput per format, same streams and same method as the Rust
// bench: one contiguous pre-generated buffer, best of three runs.

#include "../feed/feed.hpp"
#include "../feed/fix.hpp"
#include "../feed/itch.hpp"
#include "../feed/json.hpp"
#include "../feed/synth.hpp"

#include <algorithm>
#include <chrono>
#include <cstdio>
#include <vector>

namespace {

using namespace t2t::feed;

constexpr std::size_t kMessages = 1'000'000;
constexpr int kRuns = 3;

struct CountSink {
    std::uint64_t count = 0;
    void operator()(const Event&) { ++count; }
};

template <typename Parser>
void measure(const char* name, const synth::Generated& stream, const Parser& parser) {
    double best = 1e300;
    std::uint64_t events = 0;
    for (int run = 0; run < kRuns; ++run) {
        CountSink sink;
        const auto started = std::chrono::steady_clock::now();
        const auto outcome = parser.parse(stream.bytes, sink);
        const std::chrono::duration<double> elapsed =
            std::chrono::steady_clock::now() - started;
        if (!outcome.ok() || outcome.consumed != stream.bytes.size()) {
            std::printf("%s: parse failed mid-benchmark\n", name);
            return;
        }
        best = std::min(best, elapsed.count());
        events = sink.count;
    }
    std::printf("%-26s %7.1f ns/msg   %6.2fM msg/s   %8.2f MB/s   (%llu events)\n", name,
                best * 1e9 / static_cast<double>(kMessages),
                static_cast<double>(kMessages) / best / 1e6,
                static_cast<double>(stream.bytes.size()) / best / 1e6,
                static_cast<unsigned long long>(events));
}

} // namespace

int main() {
    std::printf("%zu messages per format, seed %#llx, best of %d runs\n\n", kMessages,
                static_cast<unsigned long long>(kGeneratorSeed), kRuns);

    Rng itch_rng{kGeneratorSeed};
    const auto itch_stream = synth::itch(kMessages, itch_rng);
    measure("ITCH 5.0 (binary)", itch_stream, Itch<CountSink>{synth::kTradfi});

    Rng fix_rng{kGeneratorSeed};
    const auto fix_stream = synth::fix(kMessages, fix_rng);
    measure("FIX 4.4 (tag=value)", fix_stream, Fix<CountSink>{synth::kTradfi});

    Rng json_rng{kGeneratorSeed};
    const auto json_stream = synth::json(kMessages, json_rng);
    measure("JSON (schema scanner)", json_stream, Json<CountSink>{synth::kCrypto});
    return 0;
}
