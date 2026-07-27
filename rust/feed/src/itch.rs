//! NASDAQ ITCH 5.0, the message subset that moves a book.
//!
//! Layouts are from the TotalView-ITCH 5.0 specification; each message is
//! framed SoupBinTCP-style with a two-byte big-endian length, which is how
//! ITCH arrives over TCP and how the canonical sample files are laid out.
//!
//! The subset: system event (S), add order (A), order executed (E), order
//! cancel (X), order delete (D), order replace (U), trade (P). Everything a
//! price-time book needs; auctions, imbalances and administrative messages
//! are recognized by length and skipped as what they are — messages for
//! somebody else, not errors.
//!
//! There is nothing clever here, which is the point of binary TradFi formats:
//! the field is at the offset the spec says, in the width the spec says, and
//! the fastest parse is a load. `stock_locate` is used directly as the symbol
//! index — that is what the field exists for — and the stock's alpha field is
//! checked against the table on Add, off the fast path's common case, because
//! that is the message where a mislabeled feed would first lie to you.

use crate::{Event, FeedError, Kind, PRICE_SCALE, Parser, Side, Sink};

/// ITCH prices carry four implied decimals; ours carry eight.
const PRICE_UP: i64 = PRICE_SCALE / 10_000;
/// ITCH quantities are whole shares.
const QTY_UP: i64 = crate::QTY_SCALE;

#[derive(Debug)]
pub struct Itch<'a> {
    /// `stock_locate` indexes this table; Add and Trade carry the alpha name
    /// as well, and the two must agree.
    pub symbols: &'a [&'a [u8]],
}

#[inline]
fn be16(b: &[u8]) -> u16 {
    u16::from_be_bytes([b[0], b[1]])
}
#[inline]
fn be32(b: &[u8]) -> u32 {
    u32::from_be_bytes([b[0], b[1], b[2], b[3]])
}
#[inline]
fn be64(b: &[u8]) -> u64 {
    u64::from_be_bytes([b[0], b[1], b[2], b[3], b[4], b[5], b[6], b[7]])
}

impl Itch<'_> {
    fn message(&self, body: &[u8], at: usize, sink: &mut impl Sink) -> Result<(), FeedError> {
        let mut event = Event::zeroed();
        match body[0] {
            // System event: type(1) locate(2) tracking(2) timestamp(6) code(1)
            b'S' if body.len() == 12 => return Ok(()), // session plumbing, not book data
            // Add order: ... orderRef(8) side(1) shares(4) stock(8) price(4)
            b'A' if body.len() == 36 => {
                let locate = be16(&body[1..]);
                let stock = trimmed(&body[24..32]);
                match self.symbols.get(locate as usize) {
                    Some(known) if *known == stock => {}
                    _ => return Err(FeedError::UnknownSymbol { offset: at }),
                }
                event.kind = Kind::Add;
                event.symbol = locate;
                event.order_id = be64(&body[11..]);
                event.side = side(body[19], at)?;
                event.qty = i64::from(be32(&body[20..])) * QTY_UP;
                event.price = i64::from(be32(&body[32..])) * PRICE_UP;
            }
            // Order executed: ... orderRef(8) shares(4) matchNo(8)
            b'E' if body.len() == 31 => {
                event.kind = Kind::Execute;
                event.symbol = be16(&body[1..]);
                event.order_id = be64(&body[11..]);
                event.qty = i64::from(be32(&body[19..])) * QTY_UP;
                event.aux = be64(&body[23..]);
            }
            // Order cancel: ... orderRef(8) canceledShares(4)
            b'X' if body.len() == 23 => {
                event.kind = Kind::Cancel;
                event.symbol = be16(&body[1..]);
                event.order_id = be64(&body[11..]);
                event.qty = i64::from(be32(&body[19..])) * QTY_UP;
            }
            // Order delete: ... orderRef(8)
            b'D' if body.len() == 19 => {
                event.kind = Kind::Delete;
                event.symbol = be16(&body[1..]);
                event.order_id = be64(&body[11..]);
            }
            // Order replace: ... origRef(8) newRef(8) shares(4) price(4)
            b'U' if body.len() == 35 => {
                event.kind = Kind::Replace;
                event.symbol = be16(&body[1..]);
                event.order_id = be64(&body[11..]);
                event.aux = be64(&body[19..]);
                event.qty = i64::from(be32(&body[27..])) * QTY_UP;
                event.price = i64::from(be32(&body[31..])) * PRICE_UP;
            }
            // Trade (non-cross): ... orderRef(8) side(1) shares(4) stock(8) price(4) matchNo(8)
            b'P' if body.len() == 44 => {
                event.kind = Kind::Trade;
                event.symbol = be16(&body[1..]);
                event.order_id = be64(&body[11..]);
                event.side = side(body[19], at)?;
                event.qty = i64::from(be32(&body[20..])) * QTY_UP;
                event.price = i64::from(be32(&body[32..])) * PRICE_UP;
                event.aux = be64(&body[36..]);
            }
            _ => return Err(FeedError::Malformed { offset: at }),
        }
        sink.accept(&event);
        Ok(())
    }
}

fn side(byte: u8, at: usize) -> Result<Side, FeedError> {
    match byte {
        b'B' => Ok(Side::Bid),
        b'S' => Ok(Side::Ask),
        _ => Err(FeedError::Malformed { offset: at }),
    }
}

/// ITCH alpha fields are space-padded on the right.
fn trimmed(field: &[u8]) -> &[u8] {
    let end = field
        .iter()
        .rposition(|b| *b != b' ')
        .map_or(0, |last| last + 1);
    &field[..end]
}

impl Parser for Itch<'_> {
    fn parse(&self, bytes: &[u8], sink: &mut impl Sink) -> Result<usize, FeedError> {
        let mut at = 0;
        loop {
            let Some(header) = bytes.get(at..at + 2) else {
                return Ok(at);
            };
            let length = be16(header) as usize;
            if length == 0 {
                return Err(FeedError::Malformed { offset: at });
            }
            let Some(body) = bytes.get(at + 2..at + 2 + length) else {
                return Ok(at); // a partial message is the routine case, not an error
            };
            self.message(body, at, sink)?;
            at += 2 + length;
        }
    }
}
