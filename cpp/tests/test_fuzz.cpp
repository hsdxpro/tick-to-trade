// Adversarial bytes against every parser, the C++ twin of rust/feed's fuzz
// suite. Same corruptions, same seed, same contract.
//
// This side matters more than the Rust one: an accumulator that merely wraps
// in Rust is undefined behaviour here, so the run below is built under ASan
// and UBSan in CI and the sanitizers are the real assertion. Bare, it still
// catches a hang, an out-of-bounds read the allocator happens to notice, and
// any claim to have consumed more bytes than were offered.

#include "../feed/feed.hpp"
#include "../feed/fix.hpp"
#include "../feed/itch.hpp"
#include "../feed/json.hpp"
#include "../feed/synth.hpp"

#include <cstdio>
#include <vector>

namespace {

using namespace t2t::feed;

int failures = 0;

struct Drop {
    void operator()(const Event&) const {}
};

/// What every parser owes for every input, valid or not: reject as it likes,
/// but never read past what it was given and never claim more consumed.
template <typename Parser>
void contract(const char* name, const Parser& parser, const std::vector<std::byte>& bytes,
              std::uint64_t seed) {
    Drop sink;
    const auto outcome = parser.parse(Bytes{bytes.data(), bytes.size()}, sink);
    if (outcome.consumed > bytes.size()) {
        ++failures;
        std::printf("FAIL %s: consumed %zu of %zu bytes (seed %llu)\n", name, outcome.consumed,
                    bytes.size(), static_cast<unsigned long long>(seed));
    }
}

/// Corrupts a copy of `source` in one of the ways a wire actually breaks.
std::vector<std::byte> corrupt(const std::vector<std::byte>& source, Rng& rng) {
    auto bytes = source;
    if (bytes.empty()) {
        return bytes;
    }
    const auto pick = [&](std::uint64_t bound) { return static_cast<std::size_t>(rng.below(bound)); };
    switch (rng.below(6)) {
        case 0: {  // A single flipped byte: the checksum and framing case.
            bytes[pick(bytes.size())] = static_cast<std::byte>(rng.below(256));
            break;
        }
        case 1: {  // A run replaced: a burst error.
            const auto at = pick(bytes.size());
            const auto len = std::min(pick(32), bytes.size() - at);
            for (std::size_t i = 0; i < len; ++i) {
                bytes[at + i] = static_cast<std::byte>(rng.below(256));
            }
            break;
        }
        case 2: {  // Digits everywhere: aimed at every accumulator at once.
            const auto at = pick(bytes.size());
            const auto len = std::min(pick(40), bytes.size() - at);
            for (std::size_t i = 0; i < len; ++i) {
                bytes[at + i] = static_cast<std::byte>('9');
            }
            break;
        }
        case 3: {  // Truncated, then extended with noise.
            bytes.resize(pick(bytes.size()));
            const auto tail = pick(64);
            for (std::size_t i = 0; i < tail; ++i) {
                bytes.push_back(static_cast<std::byte>(rng.below(256)));
            }
            break;
        }
        case 4: {  // Deleted from the middle: every field after shifts.
            const auto at = pick(bytes.size());
            const auto len = std::min(pick(16), bytes.size() - at);
            bytes.erase(bytes.begin() + static_cast<std::ptrdiff_t>(at),
                        bytes.begin() + static_cast<std::ptrdiff_t>(at + len));
            break;
        }
        default: {  // Inserted: the same, in the other direction.
            const auto at = pick(bytes.size());
            const auto count = pick(16);
            for (std::size_t i = 0; i < count; ++i) {
                bytes.insert(bytes.begin() + static_cast<std::ptrdiff_t>(at),
                             static_cast<std::byte>(rng.below(256)));
            }
            break;
        }
    }
    return bytes;
}

void every_parser_survives_corrupted_streams() {
    constexpr int kRounds = 20'000;
    Rng itch_rng{kGeneratorSeed};
    Rng fix_rng{kGeneratorSeed};
    Rng json_rng{kGeneratorSeed};
    const auto itch_stream = synth::itch(200, itch_rng);
    const auto fix_stream = synth::fix(200, fix_rng);
    const auto json_stream = synth::json(200, json_rng);

    const Itch<Drop> itch{synth::kTradfi};
    const Fix<Drop> fix{synth::kTradfi};
    const Json<Drop> json{synth::kCrypto};

    Rng rng{0x1234'5678'9abc'def0ULL};
    for (int round = 0; round < kRounds; ++round) {
        const auto seed = rng.state;
        contract("itch", itch, corrupt(itch_stream.bytes, rng), seed);
        contract("fix", fix, corrupt(fix_stream.bytes, rng), seed);
        contract("json", json, corrupt(json_stream.bytes, rng), seed);
        if (failures != 0) return;
    }
}

void every_parser_survives_arbitrary_bytes() {
    constexpr int kRounds = 10'000;
    const Itch<Drop> itch{synth::kTradfi};
    const Fix<Drop> fix{synth::kTradfi};
    const Json<Drop> json{synth::kCrypto};

    Rng rng{0x0fed'cba9'8765'4321ULL};
    for (int round = 0; round < kRounds; ++round) {
        const auto seed = rng.state;
        std::vector<std::byte> bytes(static_cast<std::size_t>(rng.below(512)));
        for (auto& byte : bytes) {
            byte = static_cast<std::byte>(rng.below(256));
        }
        contract("itch", itch, bytes, seed);
        contract("fix", fix, bytes, seed);
        contract("json", json, bytes, seed);
        if (failures != 0) return;
    }
}

} // namespace

int main() {
    every_parser_survives_corrupted_streams();
    every_parser_survives_arbitrary_bytes();
    if (failures == 0) {
        std::printf("fuzz: all tests passed\n");
        return 0;
    }
    std::printf("fuzz: %d failure(s)\n", failures);
    return 1;
}
