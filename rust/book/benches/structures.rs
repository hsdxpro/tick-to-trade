//! The two custom structures against their standard-library counterparts, in
//! isolation, on the exact operation streams the book workload produces.
//!
//! The blended benchmark says whether the whole book wins; this one says
//! which structure is responsible. The operation streams are precomputed by
//! replaying the events once through a reference book, so every contender
//! executes literally the same sequence of inserts, lookups, removals and
//! level updates.

use std::collections::{BTreeMap, HashMap};
use std::time::Instant;
use t2t_book::{Ladder, Order, OrderMap};

const GRID: t2t_book::Band = t2t_book::Band {
    tick: 100 * 10_000,
    ticks: 4_096,
};
use t2t_feed::{Event, GENERATOR_SEED, Kind, Parser, Rng, Side, Sink, synth};

const MESSAGES: usize = 1_000_000;
const RUNS: usize = 3;

struct Collect(Vec<Event>);
impl Sink for Collect {
    fn accept(&mut self, event: &Event) {
        self.0.push(*event);
    }
}

/// One ladder operation: signed delta at a price on one side of one symbol.
#[derive(Clone, Copy)]
struct LadderOp {
    symbol: u16,
    side: Side,
    price: i64,
    delta: i64,
}

/// One order-map operation.
#[derive(Clone, Copy)]
enum MapOp {
    Insert(u64, Order),
    /// Lookup that then decrements in place (execute/cancel partial).
    Reduce(u64, i64),
    Remove(u64),
}

fn measure(name: &str, mut run: impl FnMut() -> u64) {
    let mut best = f64::MAX;
    let mut fingerprint = 0;
    for _ in 0..RUNS {
        let started = Instant::now();
        fingerprint = run();
        best = best.min(started.elapsed().as_secs_f64());
    }
    println!("{name:<34} best {best:>8.4}s   (check {fingerprint})");
}

fn main() {
    let stream = synth::itch(MESSAGES, &mut Rng(GENERATOR_SEED));
    let parser = t2t_feed::itch::Itch {
        symbols: synth::TRADFI,
    };
    let mut collected = Collect(Vec::with_capacity(MESSAGES));
    parser.parse(&stream.bytes, &mut collected).unwrap();

    // Precompute both op streams with one reference pass, so the contenders
    // below never do each other's work.
    let mut orders: HashMap<u64, Order> = HashMap::new();
    let mut ladder_ops: Vec<LadderOp> = Vec::new();
    let mut map_ops: Vec<MapOp> = Vec::new();
    for event in &collected.0 {
        match event.kind {
            Kind::Add => {
                let order = Order {
                    symbol: event.symbol,
                    side: event.side,
                    price: event.price,
                    qty: event.qty,
                };
                orders.insert(event.order_id, order);
                map_ops.push(MapOp::Insert(event.order_id, order));
                ladder_ops.push(LadderOp {
                    symbol: event.symbol,
                    side: event.side,
                    price: event.price,
                    delta: event.qty,
                });
            }
            Kind::Execute | Kind::Cancel => {
                let Some(order) = orders.get_mut(&event.order_id) else {
                    continue;
                };
                let taken = event.qty.min(order.qty);
                order.qty -= taken;
                ladder_ops.push(LadderOp {
                    symbol: event.symbol,
                    side: order.side,
                    price: order.price,
                    delta: -taken,
                });
                if order.qty == 0 {
                    orders.remove(&event.order_id);
                    map_ops.push(MapOp::Remove(event.order_id));
                } else {
                    map_ops.push(MapOp::Reduce(event.order_id, taken));
                }
            }
            Kind::Delete => {
                let Some(order) = orders.remove(&event.order_id) else {
                    continue;
                };
                map_ops.push(MapOp::Remove(event.order_id));
                ladder_ops.push(LadderOp {
                    symbol: event.symbol,
                    side: order.side,
                    price: order.price,
                    delta: -order.qty,
                });
            }
            Kind::Replace => {
                let Some(old) = orders.remove(&event.order_id) else {
                    continue;
                };
                map_ops.push(MapOp::Remove(event.order_id));
                ladder_ops.push(LadderOp {
                    symbol: event.symbol,
                    side: old.side,
                    price: old.price,
                    delta: -old.qty,
                });
                let new_order = Order {
                    symbol: event.symbol,
                    side: old.side,
                    price: event.price,
                    qty: event.qty,
                };
                orders.insert(event.aux, new_order);
                map_ops.push(MapOp::Insert(event.aux, new_order));
                ladder_ops.push(LadderOp {
                    symbol: event.symbol,
                    side: old.side,
                    price: event.price,
                    delta: event.qty,
                });
            }
            Kind::Level | Kind::Trade => {}
        }
    }
    println!(
        "{} ladder ops, {} map ops, from {} ITCH events; best of {RUNS} runs\n",
        ladder_ops.len(),
        map_ops.len(),
        collected.0.len()
    );

    let ns = |seconds: f64, count: usize| seconds * 1e9 / count as f64;
    let _ = ns;

    measure("ladder: custom banded bitmap", || {
        let mut sides: Vec<[Ladder; 2]> = (0..4)
            .map(|_| {
                [Ladder::bids(GRID), Ladder::asks(GRID)]
            })
            .collect();
        for op in &ladder_ops {
            sides[op.symbol as usize][op.side as usize].add(op.price, op.delta);
        }
        sides
            .iter()
            .map(|s| (s[0].depth() + s[1].depth()) as u64)
            .sum()
    });

    measure("ladder: std BTreeMap", || {
        let mut sides: Vec<[BTreeMap<i64, i64>; 2]> =
            (0..4).map(|_| [BTreeMap::new(), BTreeMap::new()]).collect();
        for op in &ladder_ops {
            let levels = &mut sides[op.symbol as usize][op.side as usize];
            let slot = levels.entry(op.price).or_insert(0);
            *slot += op.delta;
            if *slot <= 0 {
                levels.remove(&op.price);
            }
        }
        sides.iter().map(|s| (s[0].len() + s[1].len()) as u64).sum()
    });

    measure("orders: custom open addressing", || {
        let mut map = OrderMap::with_capacity(4_096);
        for op in &map_ops {
            match op {
                MapOp::Insert(key, order) => map.insert(*key, *order),
                MapOp::Reduce(key, taken) => {
                    if let Some(order) = map.get_mut(*key) {
                        order.qty -= taken;
                    }
                }
                MapOp::Remove(key) => {
                    map.remove(*key);
                }
            }
        }
        map.len() as u64
    });

    measure("orders: std HashMap (SipHash)", || {
        let mut map: HashMap<u64, Order> = HashMap::new();
        for op in &map_ops {
            match op {
                MapOp::Insert(key, order) => {
                    map.insert(*key, *order);
                }
                MapOp::Reduce(key, taken) => {
                    if let Some(order) = map.get_mut(key) {
                        order.qty -= taken;
                    }
                }
                MapOp::Remove(key) => {
                    map.remove(key);
                }
            }
        }
        map.len() as u64
    });
}
