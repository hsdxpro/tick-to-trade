//! The custom book against the std-collections book, on the same events,
//! agreeing exactly: best prices, full level contents, order state.

use std::collections::{BTreeMap, VecDeque};
use t2t_book::{Band, Books, Ladder, reference::ReferenceBooks};
use t2t_feed::{Event, GENERATOR_SEED, Kind, Parser, Rng, Side, Sink, synth};

/// The grid the ITCH generator quotes on: one-cent ticks in PRICE_SCALE units.
/// The window is not sized to the generator's range -- it finds and follows it.
const ITCH_BAND: Band = Band {
    tick: 100 * 10_000,
    ticks: 4_096,
};

struct Collect(Vec<Event>);
impl Sink for Collect {
    fn accept(&mut self, event: &Event) {
        self.0.push(*event);
    }
}

fn itch_events(count: usize) -> Vec<Event> {
    let stream = synth::itch(count, &mut Rng(GENERATOR_SEED));
    let parser = t2t_feed::itch::Itch {
        symbols: synth::TRADFI,
    };
    let mut collected = Collect(Vec::new());
    parser.parse(&stream.bytes, &mut collected).unwrap();
    collected.0
}

fn assert_agree(books: &Books, reference: &ReferenceBooks, at_event: usize) {
    for index in 0..reference.symbols.len() {
        let custom = books.symbol(index as u16);
        let std_book = &reference.symbols[index];

        assert_eq!(
            custom.bids.best(),
            std_book.best_bid(),
            "best bid diverged on symbol {index} after event {at_event}"
        );
        assert_eq!(
            custom.asks.best(),
            std_book.best_ask(),
            "best ask diverged on symbol {index} after event {at_event}"
        );
        // Full contents, both sides, from the touch outward.
        let mut bids: Vec<(i64, i64)> = Vec::new();
        custom.bids.for_each_from_touch(|p, q| bids.push((p, q)));
        let std_bids: Vec<(i64, i64)> = std_book.bids.iter().rev().map(|(p, q)| (*p, *q)).collect();
        assert_eq!(bids, std_bids, "bid ladder diverged on symbol {index}");
        let mut asks: Vec<(i64, i64)> = Vec::new();
        custom.asks.for_each_from_touch(|p, q| asks.push((p, q)));
        let std_asks: Vec<(i64, i64)> = std_book.asks.iter().map(|(p, q)| (*p, *q)).collect();
        assert_eq!(asks, std_asks, "ask ladder diverged on symbol {index}");

        assert_eq!(
            custom.orders.len(),
            std_book.orders.len(),
            "order count diverged on symbol {index}"
        );
        // Every order the reference holds, the custom map must return
        // identically. Same count plus same membership is same contents.
        for (id, order) in &std_book.orders {
            assert_eq!(
                custom.orders.get(*id),
                Some(order),
                "order {id} diverged on symbol {index}"
            );
        }
    }
    assert_eq!(books.unknown_orders, reference.unknown_orders);
}

/// A quarter million ITCH events, compared in full every few thousand and at
/// the end. The periodic comparison is what catches a divergence that a later
/// event happens to cancel out.
#[test]
fn custom_and_std_books_agree_on_a_quarter_million_events() {
    let events = itch_events(250_000);
    let mut books = Books::new(synth::TRADFI.len(), ITCH_BAND);
    let mut reference = ReferenceBooks::new(synth::TRADFI.len());
    for (index, event) in events.iter().enumerate() {
        books.apply(event);
        reference.apply(event);
        if index % 5_000 == 0 {
            assert_agree(&books, &reference, index);
        }
    }
    assert_agree(&books, &reference, events.len());
    assert!(
        books.symbol(0).orders.len() > 100,
        "the stream left almost nothing resting, so the comparison proved little"
    );
}

/// L2 semantics: absolute sets, including zero as removal, agree too.
#[test]
fn level_sets_agree_with_the_reference() {
    let mut rng = Rng(GENERATOR_SEED ^ 0x11);
    let mut books = Books::new(
        1,
        Band { tick: 1, ticks: 1_024 },
    );
    let mut reference = ReferenceBooks::new(1);
    for step in 0..100_000 {
        let event = Event {
            kind: Kind::Level,
            side: if rng.below(2) == 0 {
                Side::Bid
            } else {
                Side::Ask
            },
            symbol: 0,
            price: 1_000 + rng.below(200) as i64,
            // A third of updates remove the level outright.
            qty: if rng.below(3) == 0 {
                0
            } else {
                rng.below(1_000_000) as i64
            },
            order_id: 0,
            aux: 0,
        };
        books.apply(&event);
        reference.apply(&event);
        if step % 2_500 == 0 {
            assert_agree(&books, &reference, step);
        }
    }
    assert_agree(&books, &reference, 100_000);
}

/// The order map under adversarial churn: interleaved insert and remove with
/// clustered keys, which is what stresses backward-shift deletion. The
/// reference is a HashMap doing the same operations.
#[test]
fn order_map_survives_heavy_churn_against_hashmap() {
    use std::collections::HashMap;
    use t2t_book::{Order, OrderMap};

    let mut rng = Rng(GENERATOR_SEED ^ 0x22);
    let mut custom = OrderMap::with_capacity(64);
    let mut reference: HashMap<u64, Order> = HashMap::new();

    for _ in 0..500_000 {
        // Clustered keys on purpose: sequential IDs in a small window force
        // long probe chains and constant backward shifts.
        let key = 1 + rng.below(4_096);
        if rng.below(2) == 0 {
            let order = Order {
                symbol: 0,
                side: Side::Bid,
                price: rng.below(1_000) as i64,
                qty: 1 + rng.below(100) as i64,
            };
            custom.insert(key, order);
            reference.insert(key, order);
        } else {
            assert_eq!(
                custom.remove(key),
                reference.remove(&key),
                "removal of {key} diverged"
            );
        }
    }
    assert_eq!(custom.len(), reference.len());
    for (key, order) in &reference {
        assert_eq!(custom.get(*key), Some(order), "key {key} diverged");
    }
}

/// One structure, instruments that share nothing: a penny-tick equity near
/// $30, a cent-tick perpetual near $100,000, and a satoshi-tick pair near
/// $0.30. None declares its range in advance; each is fed a random walk that
/// wanders far enough to move the window repeatedly.
///
/// This is the generality claim, tested rather than asserted. The old fixed
/// band would have needed ten million entries for the perpetual and refused
/// every price outside a guessed range.
#[test]
fn one_ladder_serves_any_asset_and_any_tick() {
    // (name, tick, starting price) in 1e8 fixed point.
    let instruments: [(&str, i64, i64); 3] = [
        ("equity, penny tick, $30", 1_000_000, 30 * 100_000_000),
        ("perp, cent tick, $100k", 1_000_000, 100_000 * 100_000_000),
        ("pair, satoshi tick, $0.30", 1, 30_000_000),
    ];
    // Resting levels a side before the oldest is pulled.
    const LIVE: usize = 400;

    for (name, tick, start) in instruments {
        let mut rng = Rng(GENERATOR_SEED ^ start as u64);
        let mut ladder = Ladder::bids(Band { tick, ticks: 4_096 });
        let mut reference: BTreeMap<i64, i64> = BTreeMap::new();
        let mut resting: VecDeque<i64> = VecDeque::new();
        let mut mid = start;

        for _ in 0..100_000 {
            // A drifting walk: two ticks a step on average, so the touch ends
            // about fifty window widths from where it started.
            mid += (rng.below(17) as i64 - 6) * tick;
            let price = mid - rng.below(64) as i64 * tick;
            let qty = 1 + rng.below(500) as i64;
            ladder.set(price, qty);
            reference.insert(price, qty);
            resting.push_back(price);

            while resting.len() > LIVE {
                let stale = resting.pop_front().expect("len checked");
                // Only pull it if no later step refreshed the same price.
                if !resting.contains(&stale) {
                    ladder.set(stale, 0);
                    reference.remove(&stale);
                }
            }
        }

        // Eviction is lossy by design, so a test that tolerated it would be
        // comparing against a reference it had licence to disagree with.
        // Ageing liquidity out behind the touch, the way a real book cancels
        // it, keeps the live set inside the window and makes the agreement
        // exact -- every level, after every window the market crossed.
        assert_eq!(ladder.off_grid(), 0, "{name}: an on-grid price was refused");
        assert_eq!(ladder.evicted(), 0, "{name}: the live set fell behind the window");
        let mut got = Vec::new();
        ladder.for_each_from_touch(|price, qty| got.push((price, qty)));
        let want: Vec<(i64, i64)> = reference.iter().rev().map(|(p, q)| (*p, *q)).collect();
        assert_eq!(got, want, "{name}: diverged from the reference ladder");
        // Without this the test would pass on a ladder that never moved.
        assert!(
            ladder.rebases() >= 10,
            "{name}: crossed only {} windows",
            ladder.rebases()
        );
    }
}


/// A price that is not a multiple of the tick is refused and counted, not
/// rounded into a neighbouring level.
/// A deep book that follows the market keeps its depth.
///
/// This is the test that decides how the window moves. Centring it on the new
/// price is the obvious choice and the wrong one: it discards everything more
/// than half a window away, and a venue publishing full depth has most of its
/// book sitting exactly there. Shifting the minimum that admits the price,
/// plus a quarter window of hysteresis so an oscillation across the boundary
/// does not rebase on every message, keeps all of it.
///
/// 2,500 levels in a 4,096-tick window -- the shape of a full-depth feed --
/// walked five thousand ticks, further than the window is wide. Nothing may
/// be evicted, and the ladder must still agree with the reference exactly.
#[test]
fn a_deep_book_keeps_its_depth_as_the_market_moves() {
    const DEPTH: i64 = 2_500;
    let mut ladder = Ladder::bids(Band { tick: 1, ticks: 4_096 });
    let mut reference: BTreeMap<i64, i64> = BTreeMap::new();
    let mut touch = 1_000_000_i64;

    for level in 0..DEPTH {
        ladder.set(touch - level, 10 + level);
        reference.insert(touch - level, 10 + level);
    }

    // Walk the touch up past a full window, carrying the depth with it.
    for _ in 0..50 {
        for _ in 0..100 {
            touch += 1;
            ladder.set(touch, 10);
            reference.insert(touch, 10);
            let dropped = touch - DEPTH;
            ladder.set(dropped, 0);
            reference.remove(&dropped);
        }
        assert_eq!(
            ladder.evicted(),
            0,
            "depth was thrown away with the touch at {touch}"
        );
    }

    let mut got = Vec::new();
    ladder.for_each_from_touch(|price, qty| got.push((price, qty)));
    let want: Vec<(i64, i64)> = reference.iter().rev().map(|(p, q)| (*p, *q)).collect();
    assert_eq!(got, want, "the ladder lost levels the reference kept");
    assert!(ladder.rebases() >= 2, "the window never moved, so nothing was proven");
}

#[test]
fn an_off_grid_price_is_refused_rather_than_rounded() {
    let mut books = Books::new(1, Band { tick: 100, ticks: 256 });
    let on_grid = Event {
        kind: Kind::Level,
        side: Side::Bid,
        symbol: 0,
        price: 10_000,
        qty: 5,
        order_id: 0,
        aux: 0,
    };
    books.apply(&on_grid);
    assert_eq!(books.symbol(0).bids.best(), Some((10_000, 5)));

    let off_grid = Event {
        price: 10_050, // half a tick
        ..on_grid
    };
    books.apply(&off_grid);
    assert_eq!(
        books.symbol(0).bids.best(),
        Some((10_000, 5)),
        "an off-grid price disturbed the book"
    );
    assert_eq!(books.symbol(0).bids.off_grid(), 1);
}
