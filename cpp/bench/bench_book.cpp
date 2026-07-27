// Book maintenance: custom against std, blended and in isolation, same
// method and same stream as the Rust twin.

#include "../book/book.hpp"
#include "../book/reference.hpp"
#include "../feed/itch.hpp"
#include "../feed/synth.hpp"

#include <algorithm>
#include <chrono>
#include <cstdio>
#include <vector>

namespace {

using namespace t2t::feed;
using namespace t2t::book;

constexpr std::size_t kMessages = 1'000'000;
constexpr int kRuns = 3;
constexpr Band kItchBand{100 * 10'000LL, 4'096};

double timed(auto&& run) {
    double best = 1e300;
    for (int i = 0; i < kRuns; ++i) {
        const auto started = std::chrono::steady_clock::now();
        run();
        const std::chrono::duration<double> elapsed =
            std::chrono::steady_clock::now() - started;
        best = std::min(best, elapsed.count());
    }
    return best;
}

} // namespace

int main() {
    Rng rng{kGeneratorSeed};
    const auto stream = synth::itch(kMessages, rng);
    std::vector<Event> events;
    events.reserve(kMessages);
    struct Sink {
        std::vector<Event>* out;
        void operator()(const Event& e) const { out->push_back(e); }
    } sink{&events};
    if (!Itch<Sink>{synth::kTradfi}.parse(stream.bytes, sink).ok()) {
        std::printf("stream failed to parse\n");
        return 1;
    }
    std::printf("%zu ITCH events applied to 4 symbols, best of %d runs\n\n", events.size(),
                kRuns);

    const auto custom = timed([&] {
        Books books(4, kItchBand);
        for (const auto& event : events) {
            books.apply(event);
        }
    });
    const auto standard = timed([&] {
        ReferenceBooks books(4);
        for (const auto& event : events) {
            books.apply(event);
        }
    });
    std::printf("custom: ladder + open addressing %8.1f ns/event  %6.2fM events/s\n",
                custom * 1e9 / static_cast<double>(events.size()),
                static_cast<double>(events.size()) / custom / 1e6);
    std::printf("std: map + unordered_map         %8.1f ns/event  %6.2fM events/s\n",
                standard * 1e9 / static_cast<double>(events.size()),
                static_cast<double>(events.size()) / standard / 1e6);
    return 0;
}
