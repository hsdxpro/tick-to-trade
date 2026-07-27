#pragma once

// MoldUDP64 framing and A/B line arbitration -- the C++ twin of
// rust/feed/src/mold.rs, same protocol and same decisions.
//
// The fast path is one compare, and that is a consequence of the redundancy
// rather than a shortcut: a packet lost on line A arrives in sequence on line
// B, so the common failure heals with no bookkeeping. Only a packet arriving
// ahead of expectation means both lines lost the same data, and only then is
// anything else touched.

#include "feed.hpp"

#include <array>
#include <cstring>
#include <vector>

namespace t2t::feed::mold {

inline constexpr std::size_t kHeaderLen = 10 + 8 + 2;
inline constexpr std::uint16_t kEndOfSession = 0xFFFF;

struct Header {
    std::array<std::byte, 10> session{};
    std::uint64_t sequence{0};
    std::uint16_t count{0};

    friend bool operator==(const Header&, const Header&) = default;

    [[nodiscard]] static std::optional<Header> parse(Bytes bytes) {
        if (bytes.size() < kHeaderLen) {
            return std::nullopt;
        }
        Header out;
        std::memcpy(out.session.data(), bytes.data(), 10);
        out.sequence = be64(bytes, 10);
        out.count = be16(bytes, 18);
        return out;
    }

    [[nodiscard]] std::array<std::byte, kHeaderLen> encode() const {
        std::array<std::byte, kHeaderLen> out{};
        std::memcpy(out.data(), session.data(), 10);
        for (int i = 0; i < 8; ++i) {
            out[10 + i] = static_cast<std::byte>(sequence >> (56 - 8 * i));
        }
        out[18] = static_cast<std::byte>(count >> 8);
        out[19] = static_cast<std::byte>(count & 0xff);
        return out;
    }
};

enum class Admit : std::uint8_t {
    Deliver,
    Duplicate,
    Gap,
    Unrecoverable,
    SessionChanged,
};

/// Packets held while a gap is open; a wider hole means a snapshot, not more
/// memory.
inline constexpr std::size_t kStash = 64;
inline constexpr std::size_t kStashPayload = 1'500;

class Arbitrator final {
public:
    Arbitrator(std::array<std::byte, 10> session, std::uint64_t first_sequence)
        : session_(session), expected_(first_sequence), stash_(kStash * kStashPayload) {}

    /// The hot path: one comparison and one add.
    [[nodiscard]] Admit admit(const Header& header, Bytes payload) {
        if (header.sequence == expected_) [[likely]] {
            if (header.session != session_) {
                return Admit::SessionChanged;
            }
            expected_ += header.count;
            ++delivered_;
            return Admit::Deliver;
        }
        return admit_slow(header, payload);
    }

    /// After recovery, releases whatever is now in sequence.
    template <typename Deliver>
    void drain_stash(Deliver&& deliver) {
        for (;;) {
            const auto slot = static_cast<std::size_t>(expected_ % kStash);
            if (!held_[slot]) {
                return;
            }
            deliver(Bytes{stash_.data() + slot * kStashPayload, length_[slot]});
            held_[slot] = false;
            ++recovered_;
            ++delivered_;
            ++expected_;
        }
    }

    void recover(std::uint64_t sequence, std::uint16_t count) {
        if (sequence == expected_) {
            expected_ += count;
            ++recovered_;
        }
    }

    void resynchronize(std::array<std::byte, 10> session, std::uint64_t sequence) {
        session_ = session;
        expected_ = sequence;
        held_.fill(false);
    }

    [[nodiscard]] std::uint64_t expected() const { return expected_; }
    [[nodiscard]] std::uint64_t delivered() const { return delivered_; }
    [[nodiscard]] std::uint64_t duplicates() const { return duplicates_; }
    [[nodiscard]] std::uint64_t gaps() const { return gaps_; }
    [[nodiscard]] std::uint64_t recovered() const { return recovered_; }

private:
    /// Everything that is not in-sequence delivery, kept out of line so the
    /// common case stays a compare and a branch. No cold attribute: MSVC does
    /// not have one, and a private out-of-line method the hot path never
    /// calls already keeps the icache clean on both compilers.
    Admit admit_slow(const Header& header, Bytes payload) {
        if (header.session != session_) {
            return Admit::SessionChanged;
        }
        if (header.sequence < expected_) {
            ++duplicates_;
            return Admit::Duplicate;
        }
        const auto missing = header.sequence - expected_;
        ++gaps_;
        if (missing >= kStash || payload.size() > kStashPayload) {
            return Admit::Unrecoverable;
        }
        const auto slot = static_cast<std::size_t>(header.sequence % kStash);
        std::memcpy(stash_.data() + slot * kStashPayload, payload.data(), payload.size());
        length_[slot] = static_cast<std::uint16_t>(payload.size());
        held_[slot] = true;
        return Admit::Gap;
    }

    std::array<std::byte, 10> session_;
    std::uint64_t expected_;
    std::vector<std::byte> stash_;
    std::array<std::uint16_t, kStash> length_{};
    std::array<bool, kStash> held_{};
    std::uint64_t delivered_{0};
    std::uint64_t duplicates_{0};
    std::uint64_t gaps_{0};
    std::uint64_t recovered_{0};
};

} // namespace t2t::feed::mold
