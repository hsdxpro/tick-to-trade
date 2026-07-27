//! The custom book against the std-collections book, on the same events,
//! agreeing exactly: best prices, full level contents, order state.

use t2t_book::{Band, Books, reference::ReferenceBooks};

/// The band the ITCH generator's mid-walk stays inside, in PRICE_SCALE units:
/// raw ITCH [1_000_000 - 3_200, 1_500_000 + 3_200] with one-cent ticks.
const ITCH_BAND: Band = Band {
    floor: (1_000_000 - 3_200) * 10_000,
    tick: 100 * 10_000,
    ticks: 5_070,
};
use t2t_feed::{Event, GENERATOR_SEED, Kind, Parser, Rng, Side, Sink, synth};

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
        Band {
            floor: 1_000,
            tick: 1,
            ticks: 256,
        },
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
