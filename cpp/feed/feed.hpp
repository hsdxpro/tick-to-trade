#pragma once

// Market data parsing: three wire formats, one event type, no allocation.
// The C++ twin of rust/feed -- same normalized event, same fixed-point
// scales, same generator seed producing byte-identical streams, so the
// benchmark numbers compare parsers rather than workloads.

#include <bit>
#include <cstddef>
#include <cstdint>
#include <cstring>
#include <optional>
#include <span>
#include <string_view>
#include <type_traits>

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

/// Reverses the bytes of an unsigned integer.
///
/// `std::byteswap` says exactly this, but it is C++23, and C++23 is a preview
/// mode on MSVC rather than a supported standard. The baseline here is C++20 --
/// the newest standard all three compilers implement properly -- so the library
/// function is used when the compiler advertises it and spelled out when it
/// does not. Both forms compile to the same `bswap`; the shift-and-OR version
/// measured within 2% of the intrinsic, which is why this costs nothing.
template <typename T>
[[nodiscard]] constexpr T byteswap(T value) {
#if defined(__cpp_lib_byteswap)
    return std::byteswap(value);
#else
    static_assert(std::is_unsigned_v<T>, "byte order applies to unsigned integers");
    T out{};
    for (std::size_t i = 0; i < sizeof(T); ++i) {
        out = static_cast<T>(out << 8) | static_cast<T>((value >> (i * 8)) & 0xFF);
    }
    return out;
#endif
}

/// A big-endian field: one unaligned load and a byte swap.
///
/// The `memcpy` is the only strictly-conforming way to reinterpret the bytes,
/// and every compiler folds it into the single load it describes.
template <typename T>
[[nodiscard]] inline T be(Bytes b, std::size_t i) {
    T value{};
    std::memcpy(&value, b.data() + i, sizeof(T));
    return byteswap(value);
}

[[nodiscard]] inline std::uint16_t be16(Bytes b, std::size_t i) {
    return be<std::uint16_t>(b, i);
}

[[nodiscard]] inline std::uint32_t be32(Bytes b, std::size_t i) {
    return be<std::uint32_t>(b, i);
}

[[nodiscard]] inline std::uint64_t be64(Bytes b, std::size_t i) {
    return be<std::uint64_t>(b, i);
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
