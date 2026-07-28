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
#include <string>
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

/// A field whose digits wrap must be refused, not reinterpreted.
///
/// The accumulators wrap on purpose -- bounding them per digit cost 18% of
/// the FIX parser -- so nothing traps when a run is too long, and only an
/// explicit length check stands between a wrapped value and the book. These
/// two messages differ in exactly one way: the length of the first tag.
void a_field_whose_digits_wrap_is_refused_not_reinterpreted() {
    const auto soh = static_cast<char>(kSoh);
    // Builds a message with a correct BodyLength and checksum, so the only
    // thing either can be rejected for is its opening tag.
    const auto message = [&](const std::string& begin_tag) {
        const std::string body = std::string{"35=W"} + soh + "55=AAPL" + soh;
        std::string out = begin_tag + "=FIX.4.4" + soh + "9=" + std::to_string(body.size())
                        + soh + body;
        unsigned sum = 0;
        for (const auto byte : out) {
            sum += static_cast<unsigned char>(byte);
        }
        char trailer[8];
        std::snprintf(trailer, sizeof trailer, "10=%03u", sum % 256);
        return out + trailer + soh;
    };
    struct Drop {
        void operator()(const Event&) const {}
    } sink;
    const auto parse = [&](const std::string& text) {
        return Fix<Drop>{synth::kTradfi}.parse(
            Bytes{reinterpret_cast<const std::byte*>(text.data()), text.size()}, sink);
    };

    const auto sound = message("8");
    const auto control = parse(sound);
    if (!control.ok() || control.consumed != sound.size()) {
        ++failures;
        std::printf("FAIL: the control message must parse, or the case below proves nothing\n");
    }

    // 2^32 + 8 accumulated into a uint32 wraps to exactly 8, so without the
    // length bound this reads as tag 8 and the parser carries on through a
    // message whose first field it has misidentified.
    const auto wrapped = parse(message("4294967304"));
    if (wrapped.ok() || wrapped.error != Error::Malformed) {
        ++failures;
        std::printf("FAIL: a tag whose digits wrapped to 8 was accepted as BeginString\n");
    }

    // Twenty digits into a uint64 trade id.
    struct DropJson {
        void operator()(const Event&) const {}
    } json_sink;
    const std::string body =
        R"({"e":"trade","E":1700000000000,"s":"BTCUSDT","t":18446744073709551617,)"
        R"("p":"10.5","q":"2.0","m":false})" "\n";
    const auto id = Json<DropJson>{synth::kCrypto}.parse(
        Bytes{reinterpret_cast<const std::byte*>(body.data()), body.size()}, json_sink);
    if (id.ok() || id.error != Error::Malformed) {
        ++failures;
        std::printf("FAIL: a trade id whose digits wrapped was accepted\n");
    }
}

/// Scaling by 1e8 is what makes a long price unrepresentable, and a wrapped
/// price is worse than a rejected message: it enters the book as a fact. In
/// C++ the accumulation is signed overflow outright -- undefined, not wrapped.
void a_price_no_integer_can_hold_is_refused_not_wrapped() {
    struct Drop {
        void operator()(const Event&) const {}
    } sink;
    const auto parse = [&](const std::string& body) {
        return Json<Drop>{synth::kCrypto}.parse(
            Bytes{reinterpret_cast<const std::byte*>(body.data()), body.size()}, sink);
    };
    const auto message = [](const char* price, const char* qty) {
        return std::string{R"({"e":"trade","E":1700000000000,"s":"BTCUSDT","t":1,"p":")"}
             + price + R"(","q":")" + qty + R"(","m":false})" + '\n';
    };

    // A shape the parser does accept, so the rejections below are about the
    // numbers rather than about the message being unrecognisable.
    if (!parse(message("10.5", "2.0")).ok()) {
        ++failures;
        std::printf("FAIL: a well-formed trade was refused\n");
    }

    const char* cases[][2] = {
        {"99999999999999999999", "1.0"},
        {"1.0", "99999999999999999999"},
        // Inside the digit cap, unrepresentable only once scaled.
        {"999999999999999999", "1.0"},
        {"1.0", "999999999999999999"},
        // 2^64 + 1: accumulating wraps to 1, so a checked multiply alone
        // sees a representable price of 1.0 and accepts it. Only refusing
        // the digits catches it, which is why both halves of the guard
        // exist -- and in C++ the wrap itself is undefined behaviour.
        {"18446744073709551617", "1.0"},
        {"1.0", "18446744073709551617"},
    };
    for (const auto& entry : cases) {
        const auto outcome = parse(message(entry[0], entry[1]));
        if (outcome.ok() || outcome.error != Error::Malformed) {
            ++failures;
            std::printf("FAIL: accepted an unrepresentable number p=%s q=%s\n",
                        entry[0], entry[1]);
        }
    }
}

/// Nineteen nines overflow an int64. Before the digit cap, accumulating them
/// was signed overflow and the wrapped value framed a message end past every
/// bound -- a malformed field must never cost more than a rejection.
void a_length_no_integer_can_hold_is_refused_not_a_crash() {
    // Fields joined through the named separator rather than an escape. FIX
    // delimits with an unprintable byte, and writing it inline leaves either
    // an invisible control character in the source or a hex escape that
    // swallows the digit after it.
    const auto soh = static_cast<char>(kSoh);
    const std::string text = std::string{"8=FIX.4.4"} + soh + "9=9999999999999999999"
                           + soh + "35=W" + soh + "10=000" + soh;
    struct Drop {
        void operator()(const Event&) const {}
    } sink;
    const auto outcome = Fix<Drop>{synth::kTradfi}.parse(
        Bytes{reinterpret_cast<const std::byte*>(text.data()), text.size()}, sink);
    if (outcome.ok() || outcome.error != Error::Malformed) {
        ++failures;
        std::printf("FAIL: an overflowing length was not refused as malformed\n");
    }
}

} // namespace

int main() {
    a_field_whose_digits_wrap_is_refused_not_reinterpreted();
    a_price_no_integer_can_hold_is_refused_not_wrapped();
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
