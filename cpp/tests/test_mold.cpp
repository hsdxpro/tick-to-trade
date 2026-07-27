// A/B arbitration: every sequence delivered exactly once, in order, from two
// independently lossy lines. The same properties the Rust suite asserts.

#include "../feed/mold.hpp"

#include <cstdio>
#include <vector>

namespace {

using namespace t2t::feed;
using namespace t2t::feed::mold;

int failures = 0;

#define REQUIRE(expr)                                                       \
    do {                                                                    \
        if (!(expr)) {                                                      \
            ++failures;                                                     \
            std::printf("FAIL %s:%d: %s\n", __FILE__, __LINE__, #expr);     \
        }                                                                   \
    } while (0)

std::array<std::byte, 10> session_id(const char* text) {
    std::array<std::byte, 10> out{};
    std::memcpy(out.data(), text, 10);
    return out;
}

const auto kSession = session_id("SESSION001");

Header header(std::uint64_t sequence, std::uint16_t count) {
    return Header{kSession, sequence, count};
}

void headers_roundtrip() {
    const auto original = header(12'345, 7);
    const auto bytes = original.encode();
    const auto parsed = Header::parse(Bytes{bytes.data(), bytes.size()});
    REQUIRE(parsed.has_value() && *parsed == original);
    REQUIRE(!Header::parse(Bytes{bytes.data(), 5}).has_value());
}

void the_second_line_fills_what_the_first_dropped() {
    Arbitrator arbitrator(kSession, 1);
    REQUIRE(arbitrator.admit(header(1, 1), {}) == Admit::Deliver);
    REQUIRE(arbitrator.admit(header(1, 1), {}) == Admit::Duplicate);
    REQUIRE(arbitrator.admit(header(2, 1), {}) == Admit::Deliver);
    REQUIRE(arbitrator.admit(header(2, 1), {}) == Admit::Duplicate);
    // Line A dropped 3; line B's copy arrives exactly in sequence.
    REQUIRE(arbitrator.admit(header(3, 1), {}) == Admit::Deliver);
    REQUIRE(arbitrator.expected() == 4);
    REQUIRE(arbitrator.duplicates() == 2);
    REQUIRE(arbitrator.gaps() == 0);
}

void a_hole_both_lines_lost_is_stashed_then_released() {
    Arbitrator arbitrator(kSession, 1);
    REQUIRE(arbitrator.admit(header(1, 1), {}) == Admit::Deliver);
    const char* payload = "three";
    REQUIRE(arbitrator.admit(header(3, 1),
                             Bytes{reinterpret_cast<const std::byte*>(payload), 5})
            == Admit::Gap);
    REQUIRE(arbitrator.expected() == 2);

    arbitrator.recover(2, 1);
    std::vector<std::size_t> released;
    arbitrator.drain_stash([&](Bytes bytes) { released.push_back(bytes.size()); });
    REQUIRE(released.size() == 1 && released[0] == 5);
    REQUIRE(arbitrator.expected() == 4);
}

void a_hole_wider_than_the_stash_demands_a_snapshot() {
    Arbitrator arbitrator(kSession, 1);
    REQUIRE(arbitrator.admit(header(10'000, 1), {}) == Admit::Unrecoverable);
    arbitrator.resynchronize(kSession, 10'000);
    REQUIRE(arbitrator.admit(header(10'000, 1), {}) == Admit::Deliver);
}

void a_new_session_is_refused() {
    Arbitrator arbitrator(kSession, 1);
    auto other = header(1, 1);
    other.session = session_id("SESSION002");
    REQUIRE(arbitrator.admit(other, {}) == Admit::SessionChanged);
}

void two_lossy_lines_deliver_every_sequence_exactly_once() {
    Rng rng{0x5eed'0000'a1b2'c3d4};
    Arbitrator arbitrator(kSession, 1);
    std::vector<std::uint64_t> seen;
    std::uint64_t sequence = 1;

    for (int n = 0; n < 20'000; ++n) {
        const bool lost_on_a = rng.below(100) < 20;
        const bool lost_on_b = rng.below(100) < 20;
        bool lines[2] = {!lost_on_a, !lost_on_b};
        if (rng.below(2) == 0) {
            std::swap(lines[0], lines[1]);
        }
        bool delivered = false;
        for (const bool arrived : lines) {
            if (!arrived) continue;
            switch (arbitrator.admit(header(sequence, 1), {})) {
                case Admit::Deliver:
                    if (delivered) { REQUIRE(!"delivered twice"); return; }
                    delivered = true;
                    seen.push_back(sequence);
                    break;
                case Admit::Duplicate:
                    break;
                default:
                    REQUIRE(!"unexpected admit outcome");
                    return;
            }
        }
        if (lost_on_a && lost_on_b) {
            arbitrator.recover(sequence, 1);
            seen.push_back(sequence);
        }
        ++sequence;
    }

    REQUIRE(seen.size() == 20'000);
    for (std::size_t i = 1; i < seen.size(); ++i) {
        if (seen[i] != seen[i - 1] + 1) {
            REQUIRE(!"sequences arrived out of order");
            return;
        }
    }
    REQUIRE(arbitrator.duplicates() > 1'000);
}

} // namespace

int main() {
    headers_roundtrip();
    the_second_line_fills_what_the_first_dropped();
    a_hole_both_lines_lost_is_stashed_then_released();
    a_hole_wider_than_the_stash_demands_a_snapshot();
    a_new_session_is_refused();
    two_lossy_lines_deliver_every_sequence_exactly_once();

    if (failures == 0) {
        std::printf("mold: all tests passed\n");
        return 0;
    }
    std::printf("mold: %d failure(s)\n", failures);
    return 1;
}
