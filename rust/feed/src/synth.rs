//! Generators: a byte stream and the events it must decode to, from one seed.
//!
//! The generator is the other half of every parser test. It writes bytes
//! against the wire specification and records what those bytes mean; the
//! parser reads the bytes against the same specification, independently. When
//! the two agree on a million randomized messages, a layout mistake would
//! have to exist identically in both directions to hide, which is the same
//! differential argument the matching-engine repositories use.
//!
//! The C++ generators are these functions transliterated, seeded identically,
//! so both languages parse byte-identical streams and the benchmark numbers
//! compare parsers rather than workloads.

use crate::{Event, Kind, PRICE_SCALE, QTY_SCALE, Rng, Side};

pub const TRADFI: &[&[u8]] = &[b"AAPL", b"MSFT", b"NVDA", b"TSLA"];
pub const CRYPTO: &[&[u8]] = &[b"BTCUSDT", b"ETHUSDT", b"SOLUSDT"];

#[derive(Debug, Default)]
pub struct Generated {
    pub bytes: Vec<u8>,
    pub events: Vec<Event>,
}

// ------------------------------------------------------------------ ITCH

/// One step of a symbol's mid-price walk, and a price near it.
///
/// Real feeds cluster activity at the touch: the mid wanders one tick at a
/// time and orders land within a few dozen ticks of it. A uniform price draw
/// -- the first version of this generator -- put every insert in the middle
/// of the ladder, a distribution no market produces, and it inverted which
/// data structures win downstream. The workload's shape is part of the
/// specification, so it is written here rather than assumed.
fn walk_price(mid: &mut i64, rng: &mut Rng) -> u32 {
    // One-cent steps in raw ITCH units (four implied decimals).
    *mid += (rng.below(3) as i64 - 1) * 100;
    *mid = (*mid).clamp(1_000_000, 1_500_000);
    let offset = (rng.below(65) as i64 - 32) * 100;
    (*mid + offset) as u32
}

pub fn itch(count: usize, rng: &mut Rng) -> Generated {
    let mut out = Generated::default();
    let mut live: Vec<(u64, u16)> = Vec::new(); // order → symbol, for realistic E/X/D/U
    let mut next_order = 1_u64;
    let mut mids = [1_250_000_i64; 4];

    for _ in 0..count {
        let symbol = rng.below(TRADFI.len() as u64) as u16;
        let roll = rng.below(100);
        // Weighted like a real session: adds dominate, deletes and
        // executions follow, replaces and prints are the tail.
        if roll < 40 || live.is_empty() {
            let order = next_order;
            next_order += 1;
            let side = if rng.below(2) == 0 {
                Side::Bid
            } else {
                Side::Ask
            };
            let shares = 1 + rng.below(5_000) as u32;
            let price = walk_price(&mut mids[symbol as usize], rng);
            live.push((order, symbol));
            push_itch_add(&mut out, symbol, order, side, shares, price);
        } else {
            let pick = rng.below(live.len() as u64) as usize;
            let (order, symbol) = live[pick];
            match roll {
                40..=59 => {
                    let shares = 1 + rng.below(500) as u32;
                    let match_no = rng.step();
                    push_itch_exec(&mut out, symbol, order, shares, match_no);
                }
                60..=74 => {
                    let shares = 1 + rng.below(500) as u32;
                    push_itch_cancel(&mut out, symbol, order, shares);
                }
                75..=89 => {
                    live.swap_remove(pick);
                    push_itch_delete(&mut out, symbol, order);
                }
                90..=95 => {
                    let replacement = next_order;
                    next_order += 1;
                    live[pick] = (replacement, symbol);
                    let shares = 1 + rng.below(5_000) as u32;
                    let price = walk_price(&mut mids[symbol as usize], rng);
                    push_itch_replace(&mut out, symbol, order, replacement, shares, price);
                }
                _ => {
                    let side = if rng.below(2) == 0 {
                        Side::Bid
                    } else {
                        Side::Ask
                    };
                    let shares = 1 + rng.below(500) as u32;
                    let price = walk_price(&mut mids[symbol as usize], rng);
                    let match_no = rng.step();
                    push_itch_trade(&mut out, symbol, order, side, shares, price, match_no);
                }
            }
        }
    }
    out
}

fn frame(bytes: &mut Vec<u8>, body: &[u8]) {
    bytes.extend_from_slice(&(body.len() as u16).to_be_bytes());
    bytes.extend_from_slice(body);
}

fn stock_field(symbol: u16) -> [u8; 8] {
    let mut field = [b' '; 8];
    let name = TRADFI[symbol as usize];
    field[..name.len()].copy_from_slice(name);
    field
}

fn push_itch_add(
    out: &mut Generated,
    symbol: u16,
    order: u64,
    side: Side,
    shares: u32,
    price: u32,
) {
    let mut body = Vec::with_capacity(36);
    body.push(b'A');
    body.extend_from_slice(&symbol.to_be_bytes());
    body.extend_from_slice(&[0; 2]); // tracking
    body.extend_from_slice(&[0; 6]); // timestamp
    body.extend_from_slice(&order.to_be_bytes());
    body.push(if side == Side::Bid { b'B' } else { b'S' });
    body.extend_from_slice(&shares.to_be_bytes());
    body.extend_from_slice(&stock_field(symbol));
    body.extend_from_slice(&price.to_be_bytes());
    frame(&mut out.bytes, &body);
    out.events.push(Event {
        kind: Kind::Add,
        side,
        symbol,
        price: i64::from(price) * (PRICE_SCALE / 10_000),
        qty: i64::from(shares) * QTY_SCALE,
        order_id: order,
        aux: 0,
    });
}

fn push_itch_exec(out: &mut Generated, symbol: u16, order: u64, shares: u32, match_no: u64) {
    let mut body = Vec::with_capacity(31);
    body.push(b'E');
    body.extend_from_slice(&symbol.to_be_bytes());
    body.extend_from_slice(&[0; 2]);
    body.extend_from_slice(&[0; 6]);
    body.extend_from_slice(&order.to_be_bytes());
    body.extend_from_slice(&shares.to_be_bytes());
    body.extend_from_slice(&match_no.to_be_bytes());
    frame(&mut out.bytes, &body);
    out.events.push(Event {
        kind: Kind::Execute,
        side: Side::Bid,
        symbol,
        price: 0,
        qty: i64::from(shares) * QTY_SCALE,
        order_id: order,
        aux: match_no,
    });
}

fn push_itch_cancel(out: &mut Generated, symbol: u16, order: u64, shares: u32) {
    let mut body = Vec::with_capacity(23);
    body.push(b'X');
    body.extend_from_slice(&symbol.to_be_bytes());
    body.extend_from_slice(&[0; 2]);
    body.extend_from_slice(&[0; 6]);
    body.extend_from_slice(&order.to_be_bytes());
    body.extend_from_slice(&shares.to_be_bytes());
    frame(&mut out.bytes, &body);
    out.events.push(Event {
        kind: Kind::Cancel,
        side: Side::Bid,
        symbol,
        price: 0,
        qty: i64::from(shares) * QTY_SCALE,
        order_id: order,
        aux: 0,
    });
}

fn push_itch_delete(out: &mut Generated, symbol: u16, order: u64) {
    let mut body = Vec::with_capacity(19);
    body.push(b'D');
    body.extend_from_slice(&symbol.to_be_bytes());
    body.extend_from_slice(&[0; 2]);
    body.extend_from_slice(&[0; 6]);
    body.extend_from_slice(&order.to_be_bytes());
    frame(&mut out.bytes, &body);
    out.events.push(Event {
        kind: Kind::Delete,
        side: Side::Bid,
        symbol,
        price: 0,
        qty: 0,
        order_id: order,
        aux: 0,
    });
}

fn push_itch_replace(
    out: &mut Generated,
    symbol: u16,
    order: u64,
    replacement: u64,
    shares: u32,
    price: u32,
) {
    let mut body = Vec::with_capacity(35);
    body.push(b'U');
    body.extend_from_slice(&symbol.to_be_bytes());
    body.extend_from_slice(&[0; 2]);
    body.extend_from_slice(&[0; 6]);
    body.extend_from_slice(&order.to_be_bytes());
    body.extend_from_slice(&replacement.to_be_bytes());
    body.extend_from_slice(&shares.to_be_bytes());
    body.extend_from_slice(&price.to_be_bytes());
    frame(&mut out.bytes, &body);
    out.events.push(Event {
        kind: Kind::Replace,
        side: Side::Bid,
        symbol,
        price: i64::from(price) * (PRICE_SCALE / 10_000),
        qty: i64::from(shares) * QTY_SCALE,
        order_id: order,
        aux: replacement,
    });
}

fn push_itch_trade(
    out: &mut Generated,
    symbol: u16,
    order: u64,
    side: Side,
    shares: u32,
    price: u32,
    match_no: u64,
) {
    let mut body = Vec::with_capacity(44);
    body.push(b'P');
    body.extend_from_slice(&symbol.to_be_bytes());
    body.extend_from_slice(&[0; 2]);
    body.extend_from_slice(&[0; 6]);
    body.extend_from_slice(&order.to_be_bytes());
    body.push(if side == Side::Bid { b'B' } else { b'S' });
    body.extend_from_slice(&shares.to_be_bytes());
    body.extend_from_slice(&stock_field(symbol));
    body.extend_from_slice(&price.to_be_bytes());
    body.extend_from_slice(&match_no.to_be_bytes());
    frame(&mut out.bytes, &body);
    out.events.push(Event {
        kind: Kind::Trade,
        side,
        symbol,
        price: i64::from(price) * (PRICE_SCALE / 10_000),
        qty: i64::from(shares) * QTY_SCALE,
        order_id: order,
        aux: match_no,
    });
}

// ------------------------------------------------------------------- FIX

pub fn fix(count: usize, rng: &mut Rng) -> Generated {
    let mut out = Generated::default();
    for sequence in 0..count {
        let symbol = rng.below(TRADFI.len() as u64) as u16;
        let entries = 1 + rng.below(3);
        let mut body = Vec::new();
        push_fix(&mut body, 35, b"X");
        push_fix(&mut body, 49, b"VENUE");
        push_fix(&mut body, 56, b"HANDLER");
        push_fix(&mut body, 34, format!("{}", sequence + 1).as_bytes());
        push_fix(&mut body, 55, TRADFI[symbol as usize]);
        push_fix(&mut body, 268, format!("{entries}").as_bytes());
        for _ in 0..entries {
            let is_trade = rng.below(5) == 0;
            let side = if rng.below(2) == 0 {
                Side::Bid
            } else {
                Side::Ask
            };
            let price_cents = 10_000 + rng.below(90_000) as i64; // dollars.cents
            let qty_whole = 1 + rng.below(9_999) as i64;
            let type_byte: &[u8] = if is_trade {
                b"2"
            } else if side == Side::Bid {
                b"0"
            } else {
                b"1"
            };
            push_fix(&mut body, 269, type_byte);
            push_fix(
                &mut body,
                270,
                format!("{}.{:02}", price_cents / 100, price_cents % 100).as_bytes(),
            );
            push_fix(&mut body, 271, format!("{qty_whole}").as_bytes());
            out.events.push(Event {
                kind: if is_trade { Kind::Trade } else { Kind::Level },
                side: if is_trade { Side::Bid } else { side },
                symbol,
                price: price_cents * (PRICE_SCALE / 100),
                qty: qty_whole * QTY_SCALE,
                order_id: 0,
                aux: 0,
            });
        }

        let mut message = Vec::new();
        push_fix(&mut message, 8, b"FIX.4.4");
        push_fix(&mut message, 9, format!("{}", body.len()).as_bytes());
        message.extend_from_slice(&body);
        let sum: u32 = message.iter().map(|b| u32::from(*b)).sum();
        push_fix(&mut message, 10, format!("{:03}", sum % 256).as_bytes());
        out.bytes.extend_from_slice(&message);
    }
    out
}

fn push_fix(out: &mut Vec<u8>, tag: u32, value: &[u8]) {
    out.extend_from_slice(tag.to_string().as_bytes());
    out.push(b'=');
    out.extend_from_slice(value);
    out.push(0x01);
}

// ------------------------------------------------------------------ JSON

pub fn json(count: usize, rng: &mut Rng) -> Generated {
    let mut out = Generated::default();
    for _ in 0..count {
        let symbol = rng.below(CRYPTO.len() as u64) as u16;
        let name = core::str::from_utf8(CRYPTO[symbol as usize]).unwrap();
        if rng.below(2) == 0 {
            // Trade, field order as Binance sends it.
            let price = 1_000_000_000 + rng.below(4_000_000_000) as i64; // 1e8 scale: 10.0 .. 50.0
            let qty = 1_000_000 + rng.below(500_000_000) as i64;
            let trade_id = rng.below(1 << 40);
            let maker_is_buyer = rng.below(2) == 0;
            out.bytes.extend_from_slice(
                format!(
                    "{{\"e\":\"trade\",\"E\":1700000000000,\"s\":\"{name}\",\"t\":{trade_id},\"p\":\"{}\",\"q\":\"{}\",\"m\":{maker_is_buyer}}}\n",
                    scaled(price),
                    scaled(qty),
                )
                .as_bytes(),
            );
            out.events.push(Event {
                kind: Kind::Trade,
                side: if maker_is_buyer { Side::Ask } else { Side::Bid },
                symbol,
                price,
                qty,
                order_id: 0,
                aux: trade_id,
            });
        } else {
            let bids = 1 + rng.below(3);
            let asks = 1 + rng.below(3);
            let mut line = format!(
                "{{\"e\":\"depthUpdate\",\"E\":1700000000000,\"s\":\"{name}\",\"U\":1,\"u\":2,\"b\":["
            );
            for i in 0..bids {
                let price = 1_000_000_000 + rng.below(4_000_000_000) as i64;
                let qty = rng.below(500_000_000) as i64; // zero allowed: level removal
                if i > 0 {
                    line.push(',');
                }
                line.push_str(&format!("[\"{}\",\"{}\"]", scaled(price), scaled(qty)));
                out.events.push(Event {
                    kind: Kind::Level,
                    side: Side::Bid,
                    symbol,
                    price,
                    qty,
                    order_id: 0,
                    aux: 0,
                });
            }
            line.push_str("],\"a\":[");
            for i in 0..asks {
                let price = 1_000_000_000 + rng.below(4_000_000_000) as i64;
                let qty = rng.below(500_000_000) as i64;
                if i > 0 {
                    line.push(',');
                }
                line.push_str(&format!("[\"{}\",\"{}\"]", scaled(price), scaled(qty)));
                out.events.push(Event {
                    kind: Kind::Level,
                    side: Side::Ask,
                    symbol,
                    price,
                    qty,
                    order_id: 0,
                    aux: 0,
                });
            }
            line.push_str("]}\n");
            out.bytes.extend_from_slice(line.as_bytes());
        }
    }
    out
}

/// A fixed-point value as the decimal string an exchange would send, trailing
/// zeros trimmed the way real feeds trim them.
fn scaled(value: i64) -> String {
    let whole = value / PRICE_SCALE;
    let frac = value % PRICE_SCALE;
    if frac == 0 {
        return format!("{whole}");
    }
    let digits = format!("{frac:08}");
    format!("{whole}.{}", digits.trim_end_matches('0'))
}
