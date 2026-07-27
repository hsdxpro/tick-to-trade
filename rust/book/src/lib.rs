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

/// One side of a book: a dense quantity array over the instrument's price
/// band, with an occupancy bitmap above it.
///
/// This is the third ladder this crate has had, and the previous two are the
/// reason it looks like this. A sorted array lost to `BTreeMap` on the
/// isolation benchmark -- inserts and removals memmove, and lazy deletion
/// only moved the cost around. The structure that actually matches the
/// access pattern starts from what a feed handler knows and a general map
/// must not assume: an instrument has a price band and a tick size, so a
/// price is an index, and an update is one store. Finding the next live
/// level after the touch empties is a bitmap walk -- first-set-bit over a
/// few words -- instead of a tree descent.
///
/// Cost shape: `add`/`set` are O(1) always, wherever the price sits. `best`
/// is a read. Re-finding the touch after it empties is O(band/64) worst
/// case and O(1) on clustered feeds, where the next live level is bits away.
/// The band is memory: eight bytes a tick, plus a bit -- 40 KB a side for a
/// 5,000-tick band, of which only the touch region is ever hot.
#[derive(Clone, Debug)]
pub struct Ladder {
    /// Quantity at each tick index; zero means no level.
    qty: Vec<i64>,
    /// One bit per tick: is there quantity. The walk structure.
    occupied: Vec<u64>,
    /// Lowest representable price, inclusive.
    floor: i64,
    tick: i64,
    /// Tick index of the best level, or `usize::MAX` when empty.
    best: usize,
    /// Bids' best is the highest occupied index; asks' the lowest.
    descending: bool,
    len: usize,
}

impl Ladder {
    const EMPTY: usize = usize::MAX;

    /// A side over `[floor, floor + ticks * tick)`.
    #[must_use]
    pub fn bids(floor: i64, tick: i64, ticks: usize) -> Self {
        Self::new(floor, tick, ticks, false)
    }

    #[must_use]
    pub fn asks(floor: i64, tick: i64, ticks: usize) -> Self {
        Self::new(floor, tick, ticks, true)
    }

    fn new(floor: i64, tick: i64, ticks: usize, descending: bool) -> Self {
        assert!(tick > 0 && ticks > 0);
        Self {
            qty: vec![0; ticks],
            occupied: vec![0; ticks.div_ceil(64)],
            floor,
            tick,
            best: Self::EMPTY,
            descending,
            len: 0,
        }
    }

    /// A price the band cannot address is a configuration error, and saying
    /// so beats folding it into the nearest representable level: a book that
    /// silently rounds is wrong in the exact way nobody notices.
    #[inline]
    fn index(&self, price: i64) -> usize {
        let offset = price - self.floor;
        let index = offset / self.tick;
        assert!(
            offset >= 0 && offset % self.tick == 0 && (index as usize) < self.qty.len(),
            "price {price} is outside the configured band or off-tick"
        );
        index as usize
    }

    /// Whether `a` is a better (closer to the touch) index than `b`.
    #[inline]
    fn better(&self, a: usize, b: usize) -> bool {
        if self.descending { a < b } else { a > b }
    }

    /// Adds signed quantity at a price; a level reaching zero is removed.
    #[inline]
    pub fn add(&mut self, price: i64, delta: i64) {
        let index = self.index(price);
        let was = self.qty[index];
        let now = (was + delta).max(0);
        self.qty[index] = now;
        self.transition(index, was, now);
    }

    /// Sets the absolute quantity at a price; zero removes (L2 feeds).
    #[inline]
    pub fn set(&mut self, price: i64, qty: i64) {
        let index = self.index(price);
        let was = self.qty[index];
        let now = qty.max(0);
        self.qty[index] = now;
        self.transition(index, was, now);
    }

    #[inline]
    fn transition(&mut self, index: usize, was: i64, now: i64) {
        if (was > 0) == (now > 0) {
            return; // quantity changed, occupancy did not: nothing to maintain
        }
        if now > 0 {
            self.occupied[index / 64] |= 1 << (index % 64);
            self.len += 1;
            if self.best == Self::EMPTY || self.better(index, self.best) {
                self.best = index;
            }
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
    /// between levels free to skip, which is the entire point of carrying it.
    fn next_worse(&self, from: usize) -> usize {
        if self.descending {
            // Asks: worse is higher. Mask off `from` and everything below it;
            // the shift count can reach 64, which u128 absorbs cleanly.
            let mut word_index = from / 64;
            let keep = !(((1_u128 << ((from % 64) + 1)) - 1) as u64);
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
            let mut word_index = from / 64;
            let mut word = self.occupied[word_index] & ((1_u128 << (from % 64)) - 1) as u64;
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
        Some((
            self.floor + self.best as i64 * self.tick,
            self.qty[self.best],
        ))
    }

    /// Live levels held.
    #[must_use]
    pub fn depth(&self) -> usize {
        self.len
    }

    /// Visits live levels from the touch outward.
    pub fn for_each_from_touch(&self, mut visit: impl FnMut(i64, i64)) {
        let mut at = self.best;
        while at != Self::EMPTY {
            visit(self.floor + at as i64 * self.tick, self.qty[at]);
            at = self.next_worse(at);
        }
    }
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
    keys: Vec<u64>,
    values: Vec<Order>,
    mask: usize,
    len: usize,
}

impl OrderMap {
    #[must_use]
    pub fn with_capacity(capacity: usize) -> Self {
        let size = capacity.next_power_of_two().max(16) * 2;
        Self {
            keys: vec![0; size],
            values: vec![Order::default(); size],
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
        if (self.len + 1) * 4 > self.keys.len() * 3 {
            self.grow();
        }
        let mut at = self.slot(key);
        loop {
            if self.keys[at] == 0 || self.keys[at] == key {
                if self.keys[at] == 0 {
                    self.len += 1;
                }
                self.keys[at] = key;
                self.values[at] = value;
                return;
            }
            at = (at + 1) & self.mask;
        }
    }

    #[must_use]
    pub fn get(&self, key: u64) -> Option<&Order> {
        let mut at = self.slot(key);
        loop {
            match self.keys[at] {
                0 => return None,
                held if held == key => return Some(&self.values[at]),
                _ => at = (at + 1) & self.mask,
            }
        }
    }

    pub fn get_mut(&mut self, key: u64) -> Option<&mut Order> {
        let mut at = self.slot(key);
        loop {
            match self.keys[at] {
                0 => return None,
                held if held == key => return Some(&mut self.values[at]),
                _ => at = (at + 1) & self.mask,
            }
        }
    }

    /// Removes and returns the order, closing the probe gap by shifting
    /// followers back — so lookups never wade through tombstones, no matter
    /// how many billions of cancels have passed through.
    pub fn remove(&mut self, key: u64) -> Option<Order> {
        let mut at = self.slot(key);
        loop {
            match self.keys[at] {
                0 => return None,
                held if held == key => break,
                _ => at = (at + 1) & self.mask,
            }
        }
        let removed = self.values[at];
        self.len -= 1;
        // Backward-shift: any follower whose home slot is at or before the
        // hole moves into it, until an empty slot ends the cluster.
        let mut hole = at;
        let mut probe = (at + 1) & self.mask;
        loop {
            let key_here = self.keys[probe];
            if key_here == 0 {
                break;
            }
            let home = self.slot(key_here);
            // Does `probe`'s entry want to be at or before `hole`? Handle the
            // ring wraparound by measuring distances from the home slot.
            let hole_distance = hole.wrapping_sub(home) & self.mask;
            let probe_distance = probe.wrapping_sub(home) & self.mask;
            if hole_distance <= probe_distance {
                self.keys[hole] = key_here;
                self.values[hole] = self.values[probe];
                hole = probe;
            }
            probe = (probe + 1) & self.mask;
        }
        self.keys[hole] = 0;
        Some(removed)
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
        let old_keys = std::mem::replace(&mut self.keys, vec![0; (self.mask + 1) * 2]);
        let old_values = std::mem::replace(
            &mut self.values,
            vec![Order::default(); (self.mask + 1) * 2],
        );
        self.mask = self.keys.len() - 1;
        self.len = 0;
        for (key, value) in old_keys.into_iter().zip(old_values) {
            if key != 0 {
                self.insert(key, value);
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

/// An instrument's price band: what the dense ladders are sized from.
///
/// Real feed handlers have this: the venue publishes tick size and price
/// limits per instrument, and a handler that meets a price outside them is
/// looking at corruption, not a market.
#[derive(Clone, Copy, Debug)]
pub struct Band {
    pub floor: i64,
    pub tick: i64,
    pub ticks: usize,
}

impl SymbolBook {
    #[must_use]
    pub fn new(band: Band) -> Self {
        Self {
            orders: OrderMap::with_capacity(4_096),
            bids: Ladder::bids(band.floor, band.tick, band.ticks),
            asks: Ladder::asks(band.floor, band.tick, band.ticks),
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
                let Some(order) = book.orders.get_mut(event.order_id) else {
                    self.unknown_orders += 1;
                    return;
                };
                let taken = event.qty.min(order.qty);
                order.qty -= taken;
                let (side, price, gone) = (order.side, order.price, order.qty == 0);
                if gone {
                    book.orders.remove(event.order_id);
                }
                book.side_mut(side).add(price, -taken);
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
