//! Book maintenance: the normalized event stream in, best-bid-offer and depth
//! out. L3 keeps every order; L2 keeps the aggregate per price.
//!
//! # Why the structures are custom, and what the benchmark owes for that
//!
//! Two structures do all the work, and neither is from a library:
//!
//! - [`Ladder`]: a dense quantity array over the instrument's price band,
//!   with an occupancy bitmap for finding the next live level. A price is an
//!   index and an update is a store, because a feed handler knows the
//!   instrument's band and tick size and a general map is not allowed to.
//! - [`OrderMap`]: order ID to order, open addressing, linear probing, a
//!   power-of-two table. A general map pays for a hashing policy that resists
//!   adversarial keys; exchange order references are not adversarial, so the
//!   cheapest adequate mix wins. Deletion is backward-shift rather than
//!   tombstones, so the table never degrades with churn — and churn is the
//!   workload: an order book is mostly cancels.
//!
//! Both claims are measured, not asserted: the benchmarks apply the same
//! event stream to these and to the same books on the standard library's
//! collections, blended and in isolation. If a standard structure ever wins,
//! the custom one has no reason to exist — and that rule has already fired
//! once. The first ladder here was a sorted array, on the theory that
//! near-touch clustering makes a packed tail scan unbeatable. The isolation
//! benchmark showed `BTreeMap` beating it anyway: mid-array removals memmove,
//! lazy deletion only relocated the cost, and the theory died on the bench.
//! The banded array replaced it and wins by an order of magnitude, which is
//! the difference between defending an idea and defending a number.
//!
//! Correctness is differential, as everywhere in this repository: a reference
//! book built on `BTreeMap` and `HashMap` applies the same events, and the
//! two must agree on best prices, level contents and order state exactly.

use t2t_feed::{Event, Kind, Side};

/// An instrument's price grid: the tick size, and how wide a window of it to
/// keep resident.
///
/// No floor. The window finds its own place from the first price it sees and
/// follows the market from there, so an instrument needs no advance
/// declaration of where it will trade -- which is the only way one structure
/// serves a $30 equity on penny ticks and a $100,000 perpetual on cent ticks
/// without either wasting gigabytes or refusing prices.
#[derive(Clone, Copy, Debug)]
pub struct Band {
    /// Smallest price increment, in the same fixed-point units as prices.
    pub tick: i64,
    /// Ticks held resident. Memory is `ticks * 9` bytes a side; the window
    /// only has to span the live depth of the book, not the instrument's
    /// lifetime range.
    pub ticks: usize,
}

/// One side of a book: a dense quantity array over a window of the price
/// grid, with an occupancy bitmap above it, and the window following the
/// market.
///
/// This is the third ladder this crate has had, and the previous two are the
/// reason it looks like this. A sorted array lost to `BTreeMap` on the
/// isolation benchmark -- inserts and removals memmove, and lazy deletion
/// only moved the cost around. The structure that matches the access pattern
/// starts from what a feed handler knows and a general map must not assume:
/// prices live on a grid, so a price is an index and an update is one store.
///
/// # Two things that took a second pass
///
/// **The window moves.** A fixed band forced every instrument to declare its
/// range in advance and refused -- loudly -- anything outside it. Real books
/// hold liquidity in a narrow neighbourhood of the touch while the touch
/// itself wanders all day, so the window recentres when a price falls outside
/// it, carrying the live levels across. The base moves by a whole number of
/// ticks, so every level moves by the same index delta, and the carry is one
/// bulk move: a `copy_within` of the quantities and a word-and-bit shift of
/// the bitmap, with the stranded levels counted off the end. It happens when
/// the market has moved half a window, which is rarely, and the alternative
/// was an array sized for a year of price history.
///
/// **The index is a multiply, not a divide.** Turning a price into an index
/// is `(price - base) / tick`, and a 64-bit integer divide is tens of cycles
/// -- on a path that runs per message, that was the single most expensive
/// instruction in the book. It is now a reciprocal multiply computed once when
/// the ladder is built, and it is exact rather than approximate-then-corrected:
/// see [`Ladder::index_of`] for the scale that makes it so, and
/// [`Ladder::new`] for the tick-and-width bound that scale requires.
///
/// Cost shape: `add`/`set` are O(1). `best` is a read. Re-finding the touch
/// after it empties is a bitmap walk, O(1) on clustered books.
#[derive(Clone, Debug)]
pub struct Ladder {
    /// Quantity at each tick index; zero means no level.
    qty: Vec<i64>,
    /// One bit per tick: is there quantity. The walk structure.
    occupied: Vec<u64>,
    /// Price at index zero. Moves when the market leaves the window.
    base: i64,
    tick: i64,
    /// `ceil(2^shift / tick)`, the divide's replacement.
    reciprocal: u64,
    shift: u32,
    /// Price the window spans. `ticks * tick`, and neither ever changes, so it
    /// is stored rather than recomputed on a path that runs per message.
    span: u64,
    /// Tick index of the best level, or `usize::MAX` when empty.
    best: usize,
    /// Bids' best is the highest occupied index; asks' the lowest.
    descending: bool,
    len: usize,
    /// Windows recentred, and prices refused for not lying on the grid.
    /// Counted rather than logged: an operator watching either climb learns
    /// something, and the hot path pays an increment.
    rebases: u64,
    off_grid: u64,
    evicted: u64,
}

impl Ladder {
    const EMPTY: usize = usize::MAX;

    #[must_use]
    pub fn bids(band: Band) -> Self {
        Self::new(band, false)
    }

    #[must_use]
    pub fn asks(band: Band) -> Self {
        Self::new(band, true)
    }

    /// The scale is picked from the width, and the width then bounds the tick.
    ///
    /// `shift = 63 - ceil(log2(ticks))` is the largest scale at which the
    /// product in [`Ladder::index_of`] provably cannot overflow 64 bits. That
    /// choice costs a ceiling on `ticks * tick`, asserted here rather than
    /// left to be discovered: at the 4,096-tick default it permits ticks up to
    /// about 5.5e11, which is six orders of magnitude past the coarsest tick
    /// any venue quotes in fixed point.
    fn new(band: Band, descending: bool) -> Self {
        // Four levels is what makes the quarter-window headroom at least one
        // tick, which is what guarantees a rebase always moves the base.
        assert!(
            band.tick > 0 && band.ticks >= 4,
            "a grid needs a positive tick and at least four levels"
        );
        let width = band.ticks.next_power_of_two().trailing_zeros();
        let shift = 63 - width;
        let span = (band.ticks as u64).checked_mul(band.tick as u64);
        assert!(
            span.is_some_and(|span| span <= 1 << shift),
            "ticks * tick must fit the reciprocal's scale: widen the tick or narrow the window"
        );
        Self {
            qty: vec![0; band.ticks],
            occupied: vec![0; band.ticks.div_ceil(64)],
            // Unplaced: the first price decides where the window sits.
            base: i64::MIN,
            tick: band.tick,
            reciprocal: (1_u64 << shift).div_ceil(band.tick as u64),
            shift,
            span: band.ticks as u64 * band.tick as u64,
            best: Self::EMPTY,
            descending,
            len: 0,
            rebases: 0,
            off_grid: 0,
            evicted: 0,
        }
    }

    /// `(price - base) / tick`, as a multiply.
    ///
    /// With `m = ceil(2^s / tick)`, `tick * m` lies in `[2^s, 2^s + tick)`. For
    /// an on-grid offset `k * tick` the product is `k * (tick * m)`, which lies
    /// in `[k*2^s, k*2^s + offset)`, so the shift recovers `k` exactly whenever
    /// `offset < 2^s` -- guaranteed by the span bound [`Ladder::new`] asserts.
    /// The same bound keeps the product itself under `2^64`.
    ///
    /// An off-grid offset needs no separate argument: whatever index comes
    /// back, `index * tick` is a multiple of the tick and the offset is not, so
    /// the equality check in [`Ladder::index_at`] cannot pass.
    #[inline]
    fn index_of(&self, offset: u64) -> usize {
        ((offset * self.reciprocal) >> self.shift) as usize
    }

    /// Where `price` sits, recentring the window if it falls outside.
    ///
    /// `None` when the price is not on the grid: a venue that publishes
    /// off-tick prices is a venue whose tick size was configured wrongly, and
    /// silently rounding into a neighbouring level would corrupt the book in
    /// the way nobody notices. It is counted and refused.
    #[inline]
    fn locate(&mut self, price: i64) -> Option<usize> {
        // The offset is computed once. Routing this through `locate_placed`
        // read `base` and recomputed the span a second time on every message,
        // to re-answer a question this branch had just answered.
        let mut offset = self.offset_of(price);
        if offset >= self.span {
            self.rebase(price);
            offset = self.offset_of(price);
        }
        self.index_at(offset)
    }

    /// The index for an offset already known to be inside the window.
    #[inline]
    fn index_at(&mut self, offset: u64) -> Option<usize> {
        let index = self.index_of(offset);
        // The multiply is exact only for on-grid offsets, so this check is
        // both the grid validation and the proof the index is right.
        if index * (self.tick as usize) != offset as usize {
            self.off_grid += 1;
            return None;
        }
        Some(index)
    }

    /// Distance from the window's base, as an unsigned value.
    ///
    /// The wrap is the point: one unsigned comparison against the span decides
    /// *both* "below the window" and "above it", and the unplaced base of
    /// `i64::MIN` lands far above any span, so a fresh ladder takes the same
    /// branch a breached one does with no flag to test.
    #[inline]
    fn offset_of(&self, price: i64) -> u64 {
        (price as u64).wrapping_sub(self.base as u64)
    }

    /// Recentres the window on `price`, carrying live levels across.
    ///
    /// Out of line and marked cold: it runs when the market has walked half a
    /// window, and the hot path should not carry its instructions.
    #[cold]
    fn rebase(&mut self, price: i64) {
        let width = self.qty.len() as i64;
        let span = width * self.tick;
        // A quarter window of headroom past the edge that was breached.
        //
        // Centring on the new price would be the obvious move and is the wrong
        // one: it discards half the window's existing coverage every time,
        // and a book whose far side is a hundred levels out loses it on the
        // first excursion. Shifting just past the breached edge keeps three
        // quarters of what was there, and the quarter of headroom is
        // hysteresis -- without it a price oscillating across the boundary
        // would rebase on every message.
        let headroom = (width / 4) * self.tick;
        let target = if self.base == i64::MIN {
            price - span / 2 // first placement: centre, nothing to preserve
        } else if price < self.base {
            price - headroom
        } else {
            price - span + headroom
        };
        // Snap to the existing grid so on-grid prices stay on it. The snap
        // cannot land back on the current base: a breach is at least a full
        // span away from the far edge, and the headroom is at least one tick
        // by the width the constructor requires, so the move is never zero.
        let new_base = if self.base == i64::MIN {
            target
        } else {
            self.base + ((target - self.base) / self.tick) * self.tick
        };
        if self.len > 0 {
            // The base moves by a whole number of ticks, so every level moves
            // by the same index delta. That makes the carry one bulk move
            // rather than a per-level replay: this used to walk each live
            // level and re-place it, O(live) with a scratch buffer, and is now
            // O(width) at copy speed with no scratch at all.
            self.shift_levels(((new_base - self.base) / self.tick) as isize);
        }
        self.base = new_base;
        self.rebases += 1;
    }

    /// Moves every held level down by `delta` indices (up when negative),
    /// dropping the levels the move pushes off either end.
    ///
    /// Dropping them is correct, not lossy bookkeeping: a level a whole window
    /// away from where the market now trades is stale depth, and holding it
    /// would mean sizing for an instrument's lifetime range.
    fn shift_levels(&mut self, delta: isize) {
        debug_assert!(delta != 0, "a rebase that moves nothing is a bug upstream");
        let width = self.qty.len();
        if delta.unsigned_abs() >= width {
            // The window jumped clear of everything it held.
            self.evicted += self.len as u64;
            self.qty.iter_mut().for_each(|slot| *slot = 0);
            self.occupied.iter_mut().for_each(|word| *word = 0);
            self.len = 0;
            self.best = Self::EMPTY;
            return;
        }
        let d = delta.unsigned_abs();
        let dropped = if delta > 0 {
            // Window moved up: indices fall by `d`, the lowest `d` fall out.
            let dropped = self.take_bits(0, d);
            self.qty.copy_within(d.., 0);
            self.qty[width - d..].iter_mut().for_each(|slot| *slot = 0);
            self.shift_bits_down(d);
            dropped
        } else {
            // Window moved down: indices rise by `d`, the highest `d` fall out.
            // Their bits are cleared before the shift so nothing real is ever
            // pushed into the slack above `width` in the top word.
            let dropped = self.take_bits(width - d, width);
            self.qty.copy_within(..width - d, d);
            self.qty[..d].iter_mut().for_each(|slot| *slot = 0);
            self.shift_bits_up(d);
            dropped
        };
        self.evicted += dropped;
        self.len -= dropped as usize;
        self.best = self.find_best();
    }

    /// Zeroes bits `[from, to)` and returns how many were set.
    fn take_bits(&mut self, from: usize, to: usize) -> u64 {
        let mut taken = 0;
        let mut at = from;
        while at < to {
            let word = at / 64;
            let hi = ((word + 1) * 64).min(to);
            let low_mask = !0_u64 << (at % 64);
            let mask = match hi - word * 64 {
                64 => low_mask,
                bits => ((1_u64 << bits) - 1) & low_mask,
            };
            taken += u64::from((self.occupied[word] & mask).count_ones());
            self.occupied[word] &= !mask;
            at = hi;
        }
        taken
    }

    /// Moves every set bit `d` positions toward index zero.
    fn shift_bits_down(&mut self, d: usize) {
        let words = self.occupied.len();
        let (jump, bits) = (d / 64, (d % 64) as u32);
        for word in 0..words {
            let low = self.occupied.get(word + jump).copied().unwrap_or(0);
            let high = self.occupied.get(word + jump + 1).copied().unwrap_or(0);
            self.occupied[word] = if bits == 0 {
                low
            } else {
                (low >> bits) | (high << (64 - bits))
            };
        }
    }

    /// Moves every set bit `d` positions away from index zero. Bits that
    /// would land at or beyond `width` must already be cleared.
    fn shift_bits_up(&mut self, d: usize) {
        let words = self.occupied.len();
        let (jump, bits) = (d / 64, (d % 64) as u32);
        for word in (0..words).rev() {
            let high = if word >= jump {
                self.occupied[word - jump]
            } else {
                0
            };
            let low = if word > jump {
                self.occupied[word - jump - 1]
            } else {
                0
            };
            self.occupied[word] = if bits == 0 {
                high
            } else {
                (high << bits) | (low >> (64 - bits))
            };
        }
    }

    /// Re-finds the touch by scanning the bitmap from the best end.
    fn find_best(&self) -> usize {
        if self.descending {
            for (word, &held) in self.occupied.iter().enumerate() {
                if held != 0 {
                    return word * 64 + held.trailing_zeros() as usize;
                }
            }
        } else {
            for (word, &held) in self.occupied.iter().enumerate().rev() {
                if held != 0 {
                    return word * 64 + 63 - held.leading_zeros() as usize;
                }
            }
        }
        Self::EMPTY
    }

    #[inline]
    fn mark(&mut self, index: usize) {
        self.occupied[index / 64] |= 1 << (index % 64);
        self.len += 1;
        if self.best == Self::EMPTY || self.better(index, self.best) {
            self.best = index;
        }
    }

    /// Whether `a` is a better (closer to the touch) index than `b`.
    #[inline]
    fn better(&self, a: usize, b: usize) -> bool {
        if self.descending { a < b } else { a > b }
    }

    /// Adds signed quantity at a price; a level reaching zero is removed.
    #[inline]
    pub fn add(&mut self, price: i64, delta: i64) {
        let Some(index) = self.locate(price) else {
            return;
        };
        let was = self.qty[index];
        let now = (was + delta).max(0);
        self.qty[index] = now;
        self.transition(index, was, now);
    }

    /// Sets the absolute quantity at a price; zero removes (L2 feeds).
    #[inline]
    pub fn set(&mut self, price: i64, qty: i64) {
        let Some(index) = self.locate(price) else {
            return;
        };
        let was = self.qty[index];
        let now = qty.max(0);
        self.qty[index] = now;
        self.transition(index, was, now);
    }

    #[inline]
    fn transition(&mut self, index: usize, was: i64, now: i64) {
        if (was > 0) == (now > 0) {
            return; // quantity changed, occupancy did not
        }
        if now > 0 {
            self.mark(index);
        } else {
            self.occupied[index / 64] &= !(1 << (index % 64));
            self.len -= 1;
            if index == self.best {
                self.best = self.next_worse(index);
            }
        }
    }

    /// The nearest occupied index on the worse side of `from`, or EMPTY.
    ///
    /// One masked word, then whole words: the bitmap makes the emptiness
    /// between levels free to skip, which is the point of carrying it.
    fn next_worse(&self, from: usize) -> usize {
        let mut word_index = from / 64;
        let bit = from % 64;
        if self.descending {
            // Asks: worse is higher. The shift can reach 64, which u128 takes.
            let keep = !(((1_u128 << (bit + 1)) - 1) as u64);
            let mut word = self.occupied[word_index] & keep;
            loop {
                if word != 0 {
                    return word_index * 64 + word.trailing_zeros() as usize;
                }
                word_index += 1;
                if word_index == self.occupied.len() {
                    return Self::EMPTY;
                }
                word = self.occupied[word_index];
            }
        } else {
            // Bids: worse is lower.
            let mut word = self.occupied[word_index] & (((1_u128 << bit) - 1) as u64);
            loop {
                if word != 0 {
                    return word_index * 64 + 63 - word.leading_zeros() as usize;
                }
                if word_index == 0 {
                    return Self::EMPTY;
                }
                word_index -= 1;
                word = self.occupied[word_index];
            }
        }
    }

    /// The touch: best price and its quantity.
    #[must_use]
    #[inline]
    pub fn best(&self) -> Option<(i64, i64)> {
        if self.best == Self::EMPTY {
            return None;
        }
        Some((self.price_at(self.best), self.qty[self.best]))
    }

    #[inline]
    fn price_at(&self, index: usize) -> i64 {
        self.base + index as i64 * self.tick
    }

    /// Live levels held.
    #[must_use]
    pub const fn depth(&self) -> usize {
        self.len
    }

    /// Windows recentred since construction.
    #[must_use]
    pub const fn rebases(&self) -> u64 {
        self.rebases
    }

    /// Prices refused for not lying on the tick grid.
    #[must_use]
    pub const fn off_grid(&self) -> u64 {
        self.off_grid
    }

    /// Levels dropped by a rebase for sitting a whole window from the market.
    /// A climbing count means the window is too narrow for the instrument.
    #[must_use]
    pub const fn evicted(&self) -> u64 {
        self.evicted
    }

    /// Visits live levels from the touch outward.
    pub fn for_each_from_touch(&self, mut visit: impl FnMut(i64, i64)) {
        let mut at = self.best;
        while at != Self::EMPTY {
            visit(self.price_at(at), self.qty[at]);
            at = self.next_worse(at);
        }
    }
}

/// What one execution or cancel took off an order.
///
/// Named rather than a tuple because three of these four fields are only
/// meaningful together, and a caller that mixes up `price` and `taken` gets a
/// book that is wrong in a way nothing reports.
#[derive(Clone, Copy, Debug, Eq, PartialEq)]
pub struct Reduced {
    pub side: Side,
    pub price: i64,
    /// How much came off, which is `want` clamped to what was resting.
    pub taken: i64,
    /// Whether the order is finished and no longer in the table.
    pub emptied: bool,
}

/// An order as the L3 book holds it.
#[derive(Clone, Copy, Debug, Eq, PartialEq)]
pub struct Order {
    pub symbol: u16,
    pub side: Side,
    pub price: i64,
    pub qty: i64,
}

impl Default for Order {
    fn default() -> Self {
        Self {
            symbol: 0,
            side: Side::Bid,
            price: 0,
            qty: 0,
        }
    }
}

/// Order ID to [`Order`]: open addressing, linear probing, power-of-two
/// capacity, backward-shift deletion.
///
/// ID zero is reserved as the empty marker. That is a documented contract,
/// not a hack: every order-by-order feed this repository parses uses nonzero
/// references (ITCH order reference numbers start at 1), and the reservation
/// buys a table with no separate occupancy metadata — one probe touches one
/// cache line that holds both the key and the payload.
#[derive(Debug)]
pub struct OrderMap {
    slots: Vec<Slot>,
    mask: usize,
    len: usize,
}

/// One table slot: the key beside the order it addresses, in one allocation.
///
/// This started as parallel `keys` and `values` vectors, which is the shape
/// that reads well and the wrong one for the access pattern. Every successful
/// lookup wants the value the instant the key matches, and two allocations put
/// those on two cache lines -- so the common case paid two misses where one
/// would do. Interleaved, the line that answers the key question carries the
/// answer with it.
///
/// The trade is that a probe now strides 32 bytes rather than 8, so a long
/// chain touches more lines. Worth 7% on the isolation benchmark even so.
#[derive(Clone, Copy, Debug, Default)]
struct Slot {
    key: u64,
    order: Order,
}

impl OrderMap {
    /// Grow at three-quarters full.
    ///
    /// Halving this was the obvious follow-up to interleaving -- shorter
    /// clusters, fewer lines touched per probe -- and five runs a side put it
    /// 1.4% ahead, which is inside this machine's noise. Not kept: the table
    /// doubles, and one that no longer fits L2 loses more than short clusters
    /// win. Measured in both directions rather than assumed.
    const MAX_LOAD_NUMERATOR: usize = 3;
    const MAX_LOAD_DENOMINATOR: usize = 4;

    #[must_use]
    pub fn with_capacity(capacity: usize) -> Self {
        let size = capacity.next_power_of_two().max(16) * 2;
        Self {
            slots: vec![Slot::default(); size],
            mask: size - 1,
            len: 0,
        }
    }

    /// Fibonacci multiplicative mix: one multiply, one shift. Exchange order
    /// references are sequential-ish, which a multiplicative mix spreads
    /// fine; SipHash-grade defense against chosen keys defends against a
    /// counterparty this table will never meet.
    #[inline]
    fn slot(&self, key: u64) -> usize {
        (key.wrapping_mul(0x9e37_79b9_7f4a_7c15) >> 32) as usize & self.mask
    }

    pub fn insert(&mut self, key: u64, value: Order) {
        debug_assert_ne!(key, 0, "order ID zero is the reserved empty marker");
        if (self.len + 1) * Self::MAX_LOAD_DENOMINATOR > self.slots.len() * Self::MAX_LOAD_NUMERATOR
        {
            self.grow();
        }
        let mut at = self.slot(key);
        loop {
            let held = self.slots[at].key;
            if held == 0 || held == key {
                if held == 0 {
                    self.len += 1;
                }
                self.slots[at] = Slot { key, order: value };
                return;
            }
            at = (at + 1) & self.mask;
        }
    }

    #[must_use]
    pub fn get(&self, key: u64) -> Option<&Order> {
        let mut at = self.slot(key);
        loop {
            let slot = &self.slots[at];
            match slot.key {
                0 => return None,
                held if held == key => return Some(&slot.order),
                _ => at = (at + 1) & self.mask,
            }
        }
    }

    /// Takes up to `want` from an order, removing it if nothing remains.
    ///
    /// One probe for what the book asks on every execution and every cancel:
    /// how much came off, from which side and price, and whether the order is
    /// finished. Reading the order and then removing it asks the table the
    /// same question twice -- two hashes, two walks of one bucket -- and on a
    /// table of any size the second walk is its own cache miss.
    pub fn reduce(&mut self, key: u64, want: i64) -> Option<Reduced> {
        let at = self.find(key)?;
        let order = &mut self.slots[at].order;
        let taken = want.min(order.qty);
        order.qty -= taken;
        let reduced = Reduced {
            side: order.side,
            price: order.price,
            taken,
            emptied: order.qty == 0,
        };
        if reduced.emptied {
            self.vacate(at);
        }
        Some(reduced)
    }

    /// The slot holding `key`, or `None`.
    #[inline]
    fn find(&self, key: u64) -> Option<usize> {
        let mut at = self.slot(key);
        loop {
            match self.slots[at].key {
                0 => return None,
                held if held == key => return Some(at),
                _ => at = (at + 1) & self.mask,
            }
        }
    }

    /// Removes and returns the order, closing the probe gap by shifting
    /// followers back -- so lookups never wade through tombstones, no matter
    /// how many billions of cancels have passed through.
    pub fn remove(&mut self, key: u64) -> Option<Order> {
        let at = self.find(key)?;
        let removed = self.slots[at].order;
        self.vacate(at);
        Some(removed)
    }

    /// Empties a slot already known to hold an entry, closing the probe gap
    /// behind it.
    fn vacate(&mut self, at: usize) {
        self.len -= 1;
        // Backward-shift: any follower whose home slot is at or before the
        // hole moves into it, until an empty slot ends the cluster.
        let mut hole = at;
        let mut probe = (at + 1) & self.mask;
        loop {
            let slot = self.slots[probe];
            if slot.key == 0 {
                break;
            }
            let home = self.slot(slot.key);
            // Does `probe`'s entry want to be at or before `hole`? Handle the
            // ring wraparound by measuring distances from the home slot.
            let hole_distance = hole.wrapping_sub(home) & self.mask;
            let probe_distance = probe.wrapping_sub(home) & self.mask;
            if hole_distance <= probe_distance {
                self.slots[hole] = slot;
                hole = probe;
            }
            probe = (probe + 1) & self.mask;
        }
        self.slots[hole].key = 0;
    }

    #[must_use]
    pub fn len(&self) -> usize {
        self.len
    }

    #[must_use]
    pub fn is_empty(&self) -> bool {
        self.len == 0
    }

    fn grow(&mut self) {
        let old = std::mem::replace(&mut self.slots, vec![Slot::default(); (self.mask + 1) * 2]);
        self.mask = self.slots.len() - 1;
        self.len = 0;
        for slot in old {
            if slot.key != 0 {
                self.insert(slot.key, slot.order);
            }
        }
    }
}

/// One symbol's book: the order table and both sides' ladders.
#[derive(Debug)]
pub struct SymbolBook {
    pub orders: OrderMap,
    pub bids: Ladder,
    pub asks: Ladder,
}

impl SymbolBook {
    #[must_use]
    pub fn new(band: Band) -> Self {
        Self {
            orders: OrderMap::with_capacity(4_096),
            bids: Ladder::bids(band),
            asks: Ladder::asks(band),
        }
    }

    fn side_mut(&mut self, side: Side) -> &mut Ladder {
        match side {
            Side::Bid => &mut self.bids,
            Side::Ask => &mut self.asks,
        }
    }
}

/// Books for every symbol in the feed's table, applying the normalized
/// stream. Unknown order IDs are counted, not applied: on a real feed that
/// is a gap you missed, and the number on the counter is the alarm.
#[derive(Debug)]
pub struct Books {
    symbols: Vec<SymbolBook>,
    pub unknown_orders: u64,
}

impl Books {
    #[must_use]
    pub fn new(symbol_count: usize, band: Band) -> Self {
        Self {
            symbols: (0..symbol_count).map(|_| SymbolBook::new(band)).collect(),
            unknown_orders: 0,
        }
    }

    #[must_use]
    pub fn symbol(&self, index: u16) -> &SymbolBook {
        &self.symbols[index as usize]
    }

    pub fn apply(&mut self, event: &Event) {
        let book = &mut self.symbols[event.symbol as usize];
        match event.kind {
            Kind::Add => {
                book.orders.insert(
                    event.order_id,
                    Order {
                        symbol: event.symbol,
                        side: event.side,
                        price: event.price,
                        qty: event.qty,
                    },
                );
                book.side_mut(event.side).add(event.price, event.qty);
            }
            Kind::Execute | Kind::Cancel => {
                let Some(hit) = book.orders.reduce(event.order_id, event.qty) else {
                    self.unknown_orders += 1;
                    return;
                };
                book.side_mut(hit.side).add(hit.price, -hit.taken);
            }
            Kind::Delete => {
                let Some(order) = book.orders.remove(event.order_id) else {
                    self.unknown_orders += 1;
                    return;
                };
                book.side_mut(order.side).add(order.price, -order.qty);
            }
            Kind::Replace => {
                let Some(old) = book.orders.remove(event.order_id) else {
                    self.unknown_orders += 1;
                    return;
                };
                book.side_mut(old.side).add(old.price, -old.qty);
                book.orders.insert(
                    event.aux,
                    Order {
                        symbol: event.symbol,
                        side: old.side,
                        price: event.price,
                        qty: event.qty,
                    },
                );
                book.side_mut(old.side).add(event.price, event.qty);
            }
            Kind::Level => {
                book.side_mut(event.side).set(event.price, event.qty);
            }
            Kind::Trade => {} // prints inform, they do not move the book
        }
    }
}

pub mod reference;
