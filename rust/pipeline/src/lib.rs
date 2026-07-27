//! The pipeline's shared vocabulary: the signal the feed thread hands the
//! strategy, the order the strategy hands the gateway, and the order's wire
//! form. Three stages, two rings, one direction.
//!
//! The stage boundaries are where they are because of what each thread must
//! never do: the feed thread never blocks (it is the only reader of the
//! socket), the strategy never touches a socket (its latency is the decision,
//! not the I/O), and the gateway never parses (bytes out is its whole job).

pub mod affinity;
pub mod session;
pub mod transport;

use t2t_book::{Band, Books};
use t2t_feed::{Event, Side, Sink};

/// What the feed stage tells the strategy: the touch moved.
#[derive(Clone, Copy, Debug, Eq, PartialEq)]
pub struct BboUpdate {
    pub symbol: u16,
    pub bid_price: i64,
    pub bid_qty: i64,
    pub ask_price: i64,
    pub ask_qty: i64,
}

/// What the strategy tells the gateway to send.
#[derive(Clone, Copy, Debug, Eq, PartialEq)]
pub struct OrderCommand {
    pub client_order_id: u64,
    pub symbol: u16,
    pub side: Side,
    pub price: i64,
    pub qty: i64,
}

/// One order on the wire: 32 bytes, little-endian, fixed layout.
pub const ORDER_WIRE_LEN: usize = 32;

impl OrderCommand {
    #[must_use]
    pub fn encode(&self) -> [u8; ORDER_WIRE_LEN] {
        let mut out = [0_u8; ORDER_WIRE_LEN];
        out[..8].copy_from_slice(&self.client_order_id.to_le_bytes());
        out[8..10].copy_from_slice(&self.symbol.to_le_bytes());
        out[10] = self.side as u8;
        out[16..24].copy_from_slice(&self.price.to_le_bytes());
        out[24..32].copy_from_slice(&self.qty.to_le_bytes());
        out
    }

    #[must_use]
    pub fn decode(bytes: &[u8; ORDER_WIRE_LEN]) -> Option<Self> {
        let side = match bytes[10] {
            0 => Side::Bid,
            1 => Side::Ask,
            _ => return None,
        };
        Some(Self {
            client_order_id: u64::from_le_bytes(bytes[..8].try_into().ok()?),
            symbol: u16::from_le_bytes(bytes[8..10].try_into().ok()?),
            side,
            price: i64::from_le_bytes(bytes[16..24].try_into().ok()?),
            qty: i64::from_le_bytes(bytes[24..32].try_into().ok()?),
        })
    }
}

#[cfg(test)]
mod tests {
    use super::*;

    #[test]
    fn orders_roundtrip_the_wire() {
        let order = OrderCommand {
            client_order_id: 7,
            symbol: 2,
            side: Side::Ask,
            price: 1_234_500_000_000,
            qty: 500_000_000,
        };
        assert_eq!(OrderCommand::decode(&order.encode()), Some(order));
    }

    #[test]
    fn a_corrupt_side_is_refused() {
        let mut bytes = OrderCommand {
            client_order_id: 1,
            symbol: 0,
            side: Side::Bid,
            price: 1,
            qty: 1,
        }
        .encode();
        bytes[10] = 9;
        assert_eq!(OrderCommand::decode(&bytes), None);
    }
}

/// The grid the probes trade on: one-cent ticks, a window wide enough for a
/// book's live depth. Where it sits is the ladder's business, not ours.
pub const BAND: Band = Band {
    tick: 100 * 10_000,
    ticks: 4_096,
};

/// The feed stage's state: books, and the last touch seen for symbol zero.
///
/// Lives here rather than in the engine binary so the internal-latency
/// benchmark exercises the identical code the deployed pipeline runs --
/// a benchmark of a copy measures the copy.
#[derive(Debug)]
pub struct FeedStage {
    pub books: Books,
    touch: (i64, i64),
    moved: Option<BboUpdate>,
}

impl FeedStage {
    #[must_use]
    pub fn new(symbols: usize, band: Band) -> Self {
        Self {
            books: Books::new(symbols, band),
            touch: (0, 0),
            moved: None,
        }
    }

    /// The update the last batch of events produced, if the touch moved.
    pub fn take_moved(&mut self) -> Option<BboUpdate> {
        self.moved.take()
    }
}

impl Sink for FeedStage {
    fn accept(&mut self, event: &Event) {
        self.books.apply(event);
        let book = self.books.symbol(0);
        let bid = book.bids.best().unwrap_or((0, 0));
        let ask = book.asks.best().unwrap_or((0, 0));
        if (bid.0, ask.0) != self.touch {
            self.touch = (bid.0, ask.0);
            self.moved = Some(BboUpdate {
                symbol: 0,
                bid_price: bid.0,
                bid_qty: bid.1,
                ask_price: ask.0,
                ask_qty: ask.1,
            });
        }
    }
}

/// The strategy stage: one order per bid price change with liquidity behind
/// it. Deliberately trivial -- the decision is the deployment's business;
/// everything around it is what this repository measures.
#[derive(Debug, Default)]
pub struct Strategy {
    last_bid: i64,
    next_id: u64,
}

impl Strategy {
    #[must_use]
    pub fn decide(&mut self, update: &BboUpdate) -> Option<OrderCommand> {
        let order = (update.bid_price != self.last_bid && update.bid_qty > 0).then(|| {
            self.next_id += 1;
            OrderCommand {
                client_order_id: self.next_id,
                symbol: update.symbol,
                side: Side::Ask,
                price: update.bid_price,
                qty: update.bid_qty.min(100_000_000),
            }
        });
        self.last_bid = update.bid_price;
        order
    }
}

/// The probe stream, generated in exactly one place.
///
/// Mold-framed ITCH: each datagram deletes the previous probe's order and adds
/// the next, so the book holds one live order and every probe moves the bid.
/// Sequence numbers advance by message count, which is what MoldUDP64 counts
/// and what the arbitrator expects.
///
/// The harness sends this and the internal benchmark replays it, so both
/// measure the same traffic through the same framing.
pub mod probe {
    use t2t_feed::mold::{HEADER_LEN, Header};

    /// The session every probe stream runs under.
    pub const SESSION: [u8; 10] = *b"T2TPROBE01";
    /// Sequence of the first message.
    pub const FIRST_SEQUENCE: u64 = 1;

    fn frame(out: &mut Vec<u8>, body: &[u8]) {
        out.extend_from_slice(&(body.len() as u16).to_be_bytes());
        out.extend_from_slice(body);
    }

    /// Generates the stream, owning the sequence and order-id state so no
    /// caller has to reproduce it.
    #[derive(Debug)]
    pub struct Probes {
        sequence: u64,
        previous: Option<u64>,
        index: usize,
    }

    impl Default for Probes {
        fn default() -> Self {
            Self::new()
        }
    }

    impl Probes {
        #[must_use]
        pub const fn new() -> Self {
            Self {
                sequence: FIRST_SEQUENCE,
                previous: None,
                index: 0,
            }
        }

        /// The next Mold-framed datagram.
        pub fn next_datagram(&mut self) -> Vec<u8> {
            // Cycles inside the book's band; a monotonic climb would leave it.
            let price = 1_000_000 + ((self.index as u32) % 2_000) * 100;
            let order = self.index as u64 + 1;

            let mut body = Vec::with_capacity(HEADER_LEN + 19 + 2 + 36 + 2);
            let mut messages = 0_u16;
            if let Some(previous) = self.previous {
                let mut delete = Vec::with_capacity(19);
                delete.push(b'D');
                delete.extend_from_slice(&0_u16.to_be_bytes());
                delete.extend_from_slice(&[0; 8]);
                delete.extend_from_slice(&previous.to_be_bytes());
                frame(&mut body, &delete);
                messages += 1;
            }
            let mut add = Vec::with_capacity(36);
            add.push(b'A');
            add.extend_from_slice(&0_u16.to_be_bytes());
            add.extend_from_slice(&[0; 8]);
            add.extend_from_slice(&order.to_be_bytes());
            add.push(b'B');
            add.extend_from_slice(&100_u32.to_be_bytes());
            add.extend_from_slice(b"AAPL    ");
            add.extend_from_slice(&price.to_be_bytes());
            frame(&mut body, &add);
            messages += 1;

            let header = Header {
                session: SESSION,
                sequence: self.sequence,
                count: messages,
            };
            self.sequence += u64::from(messages);
            self.previous = Some(order);
            self.index += 1;

            let mut datagram = Vec::with_capacity(HEADER_LEN + body.len());
            datagram.extend_from_slice(&header.encode());
            datagram.extend_from_slice(&body);
            datagram
        }
    }
}
