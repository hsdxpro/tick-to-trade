// The generator writes bytes and their meaning; the parser must recover the
// meaning from the bytes alone. Exact equality, every field, every message —
// plus the fingerprint tests, which pin this generator byte-for-byte to the
// Rust one so the two languages provably parse identical streams.

#include "../feed/feed.hpp"
#include "../feed/fix.hpp"
#include "../feed/itch.hpp"
#include "../feed/json.hpp"
#include "../feed/synth.hpp"

#include <cstdio>
#include <cstring>
#include <functional>
#include <vector>

namespace {

using namespace t2t::feed;

int failures = 0;

#define REQUIRE(expr)                                                       \
    do {                                                                    \
        if (!(expr)) {                                                      \
            ++failures;                                                     \
            std::printf("FAIL %s:%d: %s\n", __FILE__, __LINE__, #expr);     \
        }                                                                   \
    } while (0)

using Collect = std::vector<Event>;
struct CollectSink {
    Collect* out;
    void operator()(const Event& event) const { out->push_back(event); }
};

/// FNV-1a, the fingerprint both languages commit to. If either generator
/// drifts, its own fingerprint test fails before any cross-language
/// comparison gets a chance to mislead.
std::uint64_t fnv1a(const std::vector<std::byte>& bytes) {
    std::uint64_t hash = 0xcbf2'9ce4'8422'2325ULL;
    for (const auto byte : bytes) {
        hash ^= static_cast<std::uint64_t>(byte);
        hash *= 0x0000'0100'0000'01b3ULL;
    }
    return hash;
}

template <typename Parser>
void roundtrip(const char* name, const synth::Generated& generated, const Parser& parser) {
    Collect got;
    CollectSink sink{&got};
    const auto outcome = parser.parse(generated.bytes, sink);
    REQUIRE(outcome.ok());
    REQUIRE(outcome.consumed == generated.bytes.size());
    REQUIRE(got.size() == generated.events.size());
    const auto limit = std::min(got.size(), generated.events.size());
    for (std::size_t i = 0; i < limit; ++i) {
        if (!(got[i] == generated.events[i])) {
            ++failures;
            std::printf("FAIL %s: event %zu differs\n", name, i);
            return;
        }
    }
}

template <typename Parser>
void truncation(const char* name, const synth::Generated& generated, const Parser& parser) {
    const auto& bytes = generated.bytes;
    const std::size_t dense = std::min<std::size_t>(bytes.size(), 4'096);
    auto check = [&](std::size_t cut) {
        Collect got;
        CollectSink sink{&got};
        const auto outcome = parser.parse(Bytes{bytes.data(), cut}, sink);
        if (outcome.error.has_value() && *outcome.error != Error::NeedMore) {
            ++failures;
            std::printf("FAIL %s: prefix %zu of a valid stream refused\n", name, cut);
            return false;
        }
        if (outcome.consumed > cut) {
            ++failures;
            std::printf("FAIL %s: consumed past prefix %zu\n", name, cut);
            return false;
        }
        return true;
    };
    for (std::size_t cut = 0; cut <= dense; ++cut) {
        if (!check(cut)) return;
    }
    for (std::size_t cut = dense; cut < bytes.size(); cut += 997) {
        if (!check(cut)) return;
    }
}

/// Nineteen nines overflow an int64. Before the digit cap, accumulating them
/// was signed overflow and the wrapped value framed a message end past every
/// bound -- a malformed field must never cost more than a rejection.
void a_length_no_integer_can_hold_is_refused_not_a_crash() {
    const char* text = "8=FIX.4.4" "9=9999999999999999999" "35=W" "10=000";
    const auto* bytes = reinterpret_cast<const std::byte*>(text);
    struct Drop {
        void operator()(const Event&) const {}
    } sink;
    const auto outcome =
        Fix<Drop>{synth::kTradfi}.parse(Bytes{bytes, std::strlen(text)}, sink);
    if (outcome.ok() || outcome.error != Error::Malformed) {
        ++failures;
        std::printf("FAIL: an overflowing length was not refused as malformed\n");
    }
}

} // namespace

int main() {
    a_length_no_integer_can_hold_is_refused_not_a_crash();
    Rng itch_rng{kGeneratorSeed};
    const auto itch_stream = synth::itch(100'000, itch_rng);
    roundtrip("itch", itch_stream, Itch<CollectSink>{synth::kTradfi});

    Rng fix_rng{kGeneratorSeed};
    const auto fix_stream = synth::fix(100'000, fix_rng);
    roundtrip("fix", fix_stream, Fix<CollectSink>{synth::kTradfi});

    Rng json_rng{kGeneratorSeed};
    const auto json_stream = synth::json(100'000, json_rng);
    roundtrip("json", json_stream, Json<CollectSink>{synth::kCrypto});

    {
        Rng rng{kGeneratorSeed};
        truncation("itch", synth::itch(2'000, rng), Itch<CollectSink>{synth::kTradfi});
    }
    {
        Rng rng{kGeneratorSeed};
        truncation("fix", synth::fix(2'000, rng), Fix<CollectSink>{synth::kTradfi});
    }
    {
        Rng rng{kGeneratorSeed};
        truncation("json", synth::json(2'000, rng), Json<CollectSink>{synth::kCrypto});
    }

    // The cross-language pin: these constants are asserted against the same
    // streams by the Rust suite. A generator edited in one language fails
    // here in the other, which is what keeps "both languages parse identical
    // bytes" a checked fact instead of a comment.
    {
        Rng a{kGeneratorSeed};
        Rng b{kGeneratorSeed};
        Rng c{kGeneratorSeed};
        REQUIRE(fnv1a(synth::itch(1'000, a).bytes) == 0xa979'5807'4d83'd4f6ULL);
        REQUIRE(fnv1a(synth::fix(1'000, b).bytes) == 0xe78f'c2c9'75f9'd42cULL);
        REQUIRE(fnv1a(synth::json(1'000, c).bytes) == 0x98f0'cfe0'e468'699fULL);
    }

    if (failures == 0) {
        std::printf("feed: all tests passed\n");
        return 0;
    }
    std::printf("feed: %d failure(s)\n", failures);
    return 1;
}
