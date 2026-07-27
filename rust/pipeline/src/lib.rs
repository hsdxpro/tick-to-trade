//! The pipeline's shared vocabulary: the signal the feed thread hands the
//! strategy, the order the strategy hands the gateway, and the order's wire
//! form. Three stages, two rings, one direction.
//!
//! The stage boundaries are where they are because of what each thread must
//! never do: the feed thread never blocks (it is the only reader of the
//! socket), the strategy never touches a socket (its latency is the decision,
//! not the I/O), and the gateway never parses (bytes out is its whole job).

use t2t_feed::Side;

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
