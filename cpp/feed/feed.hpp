#pragma once

// Market data parsing: three wire formats, one event type, no allocation.
// The C++ twin of rust/feed -- same normalized event, same fixed-point
// scales, same generator seed producing byte-identical streams, so the
// benchmark numbers compare parsers rather than workloads.

#include <cstddef>
#include <cstdint>
#include <optional>
#include <span>
#include <string_view>

namespace t2t::feed {

/// Fixed-point scales: 1e8 covers crypto precision and represents ITCH's four
/// implied decimals exactly. Floats never appear -- two parsers disagreeing in
/// the eighth decimal is a real bug class, and integers cannot have it.
inline constexpr std::int64_t kPriceScale = 100'000'000;
inline constexpr std::int64_t kQtyScale = 100'000'000;

enum class Kind : std::uint8_t {
    Add = 0,
    Execute = 1,
    Cancel = 2,
    Delete = 3,
    Replace = 4,
    Level = 5,
    Trade = 6,
};

enum class Side : std::uint8_t { Bid = 0, Ask = 1 };

/// One normalized event, layout-mirrored with the Rust struct. Fields a kind
/// does not use are zero.
struct Event {
    Kind kind{Kind::Trade};
    Side side{Side::Bid};
    std::uint16_t symbol{0};
    std::int64_t price{0};
    std::int64_t qty{0};
    std::uint64_t order_id{0};
    std::uint64_t aux{0};

    friend bool operator==(const Event&, const Event&) = default;
};

enum class Error : std::uint8_t {
    /// The buffer ends mid-message. Routine: read more, re-offer.
    NeedMore,
    /// The bytes cannot be this format.
    Malformed,
    /// A symbol the table does not know: configuration drift, worth stopping.
    UnknownSymbol,
};

/// Consumed byte count, or why parsing stopped.
struct Outcome {
    std::size_t consumed{0};
    std::optional<Error> error{};

    [[nodiscard]] bool ok() const { return !error.has_value(); }
};

using Bytes = std::span<const std::byte>;

[[nodiscard]] inline std::uint8_t at(Bytes b, std::size_t i) {
    return static_cast<std::uint8_t>(b[i]);
}

[[nodiscard]] inline std::uint16_t be16(Bytes b, std::size_t i) {
    return static_cast<std::uint16_t>((at(b, i) << 8) | at(b, i + 1));
}

[[nodiscard]] inline std::uint32_t be32(Bytes b, std::size_t i) {
    return (std::uint32_t{at(b, i)} << 24) | (std::uint32_t{at(b, i + 1)} << 16)
         | (std::uint32_t{at(b, i + 2)} << 8) | std::uint32_t{at(b, i + 3)};
}

[[nodiscard]] inline std::uint64_t be64(Bytes b, std::size_t i) {
    return (std::uint64_t{be32(b, i)} << 32) | std::uint64_t{be32(b, i + 4)};
}

/// A handful of symbols, linear scan: a hash costs more than it saves at this
/// size, and feed handlers subscribe to few symbols by design.
[[nodiscard]] inline std::optional<std::uint16_t> lookup(
    std::span<const std::string_view> table, std::string_view name) {
    for (std::size_t i = 0; i < table.size(); ++i) {
        if (table[i] == name) {
            return static_cast<std::uint16_t>(i);
        }
    }
    return std::nullopt;
}

/// The seed both languages generate from, so "the same million messages"
/// means the same bytes.
inline constexpr std::uint64_t kGeneratorSeed = 0x5eed'f00d'0000'0001ULL;

/// SplitMix64, five lines, bit-identical to the Rust twin.
struct Rng {
    std::uint64_t state;

    std::uint64_t next() {
        state += 0x9e37'79b9'7f4a'7c15ULL;
        std::uint64_t z = state;
        z = (z ^ (z >> 30)) * 0xbf58'476d'1ce4'e5b9ULL;
        z = (z ^ (z >> 27)) * 0x94d0'49bb'1331'11ebULL;
        return z ^ (z >> 31);
    }

    std::uint64_t below(std::uint64_t bound) { return next() % bound; }
};

} // namespace t2t::feed
