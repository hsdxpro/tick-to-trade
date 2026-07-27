//! The same books on the standard library's collections: `BTreeMap` for the
//! ladders, `HashMap` for the orders.
//!
//! Two jobs. In the tests it is the differential reference — different
//! primitives, same semantics, exact agreement required. In the benchmark it
//! is the baseline the custom structures must beat to justify existing.

use crate::Order;
use std::collections::{BTreeMap, HashMap};
use t2t_feed::{Event, Kind, Side};

#[derive(Debug, Default)]
pub struct ReferenceSymbol {
    pub orders: HashMap<u64, Order>,
    pub bids: BTreeMap<i64, i64>,
    pub asks: BTreeMap<i64, i64>,
}

impl ReferenceSymbol {
    fn add(&mut self, side: Side, price: i64, delta: i64) {
        let levels = match side {
            Side::Bid => &mut self.bids,
            Side::Ask => &mut self.asks,
        };
        let slot = levels.entry(price).or_insert(0);
        *slot += delta;
        if *slot <= 0 {
            levels.remove(&price);
        }
    }

    fn set(&mut self, side: Side, price: i64, qty: i64) {
        let levels = match side {
            Side::Bid => &mut self.bids,
            Side::Ask => &mut self.asks,
        };
        if qty <= 0 {
            levels.remove(&price);
        } else {
            levels.insert(price, qty);
        }
    }

    #[must_use]
    pub fn best_bid(&self) -> Option<(i64, i64)> {
        self.bids.iter().next_back().map(|(p, q)| (*p, *q))
    }

    #[must_use]
    pub fn best_ask(&self) -> Option<(i64, i64)> {
        self.asks.iter().next().map(|(p, q)| (*p, *q))
    }
}

#[derive(Debug)]
pub struct ReferenceBooks {
    pub symbols: Vec<ReferenceSymbol>,
    pub unknown_orders: u64,
}

impl ReferenceBooks {
    #[must_use]
    pub fn new(symbol_count: usize) -> Self {
        Self {
            symbols: (0..symbol_count)
                .map(|_| ReferenceSymbol::default())
                .collect(),
            unknown_orders: 0,
        }
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
                book.add(event.side, event.price, event.qty);
            }
            Kind::Execute | Kind::Cancel => {
                let Some(order) = book.orders.get_mut(&event.order_id) else {
                    self.unknown_orders += 1;
                    return;
                };
                let taken = event.qty.min(order.qty);
                order.qty -= taken;
                let (side, price, gone) = (order.side, order.price, order.qty == 0);
                if gone {
                    book.orders.remove(&event.order_id);
                }
                book.add(side, price, -taken);
            }
            Kind::Delete => {
                let Some(order) = book.orders.remove(&event.order_id) else {
                    self.unknown_orders += 1;
                    return;
                };
                book.add(order.side, order.price, -order.qty);
            }
            Kind::Replace => {
                let Some(old) = book.orders.remove(&event.order_id) else {
                    self.unknown_orders += 1;
                    return;
                };
                book.add(old.side, old.price, -old.qty);
                book.orders.insert(
                    event.aux,
                    Order {
                        symbol: event.symbol,
                        side: old.side,
                        price: event.price,
                        qty: event.qty,
                    },
                );
                book.add(old.side, event.price, event.qty);
            }
            Kind::Level => book.set(event.side, event.price, event.qty),
            Kind::Trade => {}
        }
    }
}
