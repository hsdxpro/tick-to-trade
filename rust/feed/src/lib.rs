//! Market data parsing: three wire formats, one event type, no allocation.
//!
//! The formats are chosen to cover the three shapes feeds actually take:
//!
//! - **ITCH 5.0** ([`itch`]) — binary TradFi, order-by-order. Fixed layouts,
//!   big-endian, type-tagged. Parsing is field loads and one switch; the craft
//!   is in not adding anything on top.
//! - **FIX 4.4** ([`fix`]) — text TradFi, tag=value with SOH delimiters.
//!   Parsing is a single left-to-right scan; the craft is in never looking at
//!   a byte twice and never calling out to a general-purpose number parser.
//! - **JSON** ([`json`]) — crypto, Binance-shaped trade and depth messages.
//!   The honest hot-path answer is that you do not parse JSON generically at
//!   all: the schema is known, so the parser is a scanner for that schema,
//!   and the benchmark shows what that specialization buys over a general
//!   JSON library.
//!
//! Every parser emits the same [`Event`], so everything downstream — the
//! book, the strategy, the benchmarks — is format-blind. Prices and
//! quantities normalize to fixed-point with [`PRICE_SCALE`] and [`QTY_SCALE`];
//! floats never appear, because two parsers disagreeing in the eighth decimal
//! is a real bug class and integers cannot have it.
//!
//! # Verification
//!
//! Each format has a generator that produces a byte stream *and* the events
//! that stream must decode to. The parser's output is compared exactly, which
//! makes every test differential: layout mistakes cannot hide, because the
//! generator and parser are written against the spec independently rather
//! than one calling the other. Truncation tests feed every prefix of a stream
//! and require a clean "need more" instead of a panic or an overrun, because
//! a TCP segment boundary lands mid-message eventually, always.

pub mod fix;
pub mod itch;
pub mod json;
pub mod mold;
pub mod synth;

/// Fixed-point scale for prices: 1e8, enough for crypto's satoshi-style
/// precision and exactly representable for ITCH's four implied decimals.
pub const PRICE_SCALE: i64 = 100_000_000;

/// Fixed-point scale for quantities, same reasoning.
pub const QTY_SCALE: i64 = 100_000_000;

#[derive(Clone, Copy, Debug, Eq, PartialEq)]
#[repr(u8)]
pub enum Kind {
    /// A new resting order (order-by-order feeds).
    Add = 0,
    /// Part or all of a resting order traded.
    Execute = 1,
    /// Shares removed from a resting order without trading.
    Cancel = 2,
    /// A resting order removed entirely.
    Delete = 3,
    /// An order re-priced or re-sized: old ID dies, `aux` carries the new one.
    Replace = 4,
    /// An absolute price-level quantity (level-based feeds). Quantity zero
    /// means the level is gone.
    Level = 5,
    /// A print.
    Trade = 6,
}

#[derive(Clone, Copy, Debug, Eq, PartialEq)]
#[repr(u8)]
pub enum Side {
    Bid = 0,
    Ask = 1,
}

/// One normalized market data event.
///
/// A plain struct rather than an enum with payloads, mirrored exactly in the
/// C++ implementation, so the two sides stay layout-comparable and the queues
/// move it without indirection. Fields a kind does not use are zero.
#[derive(Clone, Copy, Debug, Eq, PartialEq)]
pub struct Event {
    pub kind: Kind,
    pub side: Side,
    /// Index into the feed's symbol table.
    pub symbol: u16,
    /// Fixed-point, [`PRICE_SCALE`].
    pub price: i64,
    /// Fixed-point, [`QTY_SCALE`].
    pub qty: i64,
    /// Order reference for order-by-order feeds.
    pub order_id: u64,
    /// Second identifier where a message carries one: the replacement order
    /// ID for [`Kind::Replace`], the match number for executions and prints.
    pub aux: u64,
}

impl Event {
    #[must_use]
    pub const fn zeroed() -> Self {
        Self {
            kind: Kind::Trade,
            side: Side::Bid,
            symbol: 0,
            price: 0,
            qty: 0,
            order_id: 0,
            aux: 0,
        }
    }
}

/// Why a parser stopped.
#[derive(Clone, Copy, Debug, Eq, PartialEq)]
pub enum FeedError {
    /// The buffer ends mid-message. Not a fault: read more and re-offer.
    NeedMore,
    /// The bytes cannot be this format. The offset says where.
    Malformed { offset: usize },
    /// A symbol the table does not know. Real handlers subscribe per symbol,
    /// so an unknown one is configuration drift worth stopping on.
    UnknownSymbol { offset: usize },
}

/// Where parsed events go. Monomorphized away in release builds: the
/// benchmarks' sink compiles to a counter increment, so the numbers measure
/// parsing rather than delivery.
pub trait Sink {
    fn accept(&mut self, event: &Event);
}

impl<F: FnMut(&Event)> Sink for F {
    fn accept(&mut self, event: &Event) {
        self(event);
    }
}

/// Parses whole messages from the front of `bytes`, emitting into `sink`.
/// Returns how many bytes were consumed; the caller re-offers the tail glued
/// to the next read. This is the contract all three formats share, and it is
/// exactly the shape a stream transport hands you: bytes, in order, split
/// wherever the network felt like it.
pub trait Parser {
    /// # Errors
    /// [`FeedError::NeedMore`] is the routine one. The others mean the stream
    /// is not what it claims to be, and resynchronization is the caller's
    /// policy decision, not the parser's.
    fn parse(&self, bytes: &[u8], sink: &mut impl Sink) -> Result<usize, FeedError>;
}

/// Matches a symbol against a small table, returning its index.
///
/// Linear over at most a handful of entries, each a single slice compare.
/// A hash would cost more than it saves at this size, and feed handlers
/// subscribe to few symbols by design — the fan-out to thousands happens
/// upstream, at the venue.
#[must_use]
pub fn lookup(table: &[&[u8]], name: &[u8]) -> Option<u16> {
    table
        .iter()
        .position(|known| *known == name)
        .map(|index| index as u16)
}

/// A deterministic generator seed shared by the tests and benchmarks of both
/// language implementations, so "the same million messages" means the same
/// bytes in Rust and C++ and the numbers stay comparable.
pub const GENERATOR_SEED: u64 = 0x5eed_f00d_0000_0001;

/// SplitMix64. Chosen because it is trivially portable — the C++ generator is
/// these same five lines, so both languages generate identical streams.
#[derive(Clone, Debug)]
pub struct Rng(pub u64);

impl Rng {
    pub fn step(&mut self) -> u64 {
        self.0 = self.0.wrapping_add(0x9e37_79b9_7f4a_7c15);
        let mut z = self.0;
        z = (z ^ (z >> 30)).wrapping_mul(0xbf58_476d_1ce4_e5b9);
        z = (z ^ (z >> 27)).wrapping_mul(0x94d0_49bb_1331_11eb);
        z ^ (z >> 31)
    }

    /// Uniform in `[0, bound)`, biased and fine: the generator makes test
    /// traffic, not statistics.
    pub fn below(&mut self, bound: u64) -> u64 {
        self.step() % bound
    }
}
