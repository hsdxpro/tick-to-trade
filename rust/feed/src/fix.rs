//! FIX 4.4 tag=value, the market data subset: snapshot (35=W) and incremental
//! refresh (35=X), entries carrying side (269), price (270) and size (271).
//!
//! Text TradFi is a different discipline from binary. There are no offsets to
//! load from — the message is `tag=value<SOH>` pairs of varying width — so the
//! whole game is one left-to-right pass that never re-reads a byte, integer
//! parsing done inline instead of through a general routine, and decimal
//! prices scaled into fixed-point during the scan rather than through a float.
//!
//! The checksum (10) is verified. It is one add per byte, it is the only
//! integrity the protocol offers, and a parser that skips it is quietly
//! trusting the network. BodyLength (9) is used to frame: that is what it is
//! for, and it is what makes partial-buffer detection O(1) instead of a scan
//! for the trailer.

use crate::{Event, FeedError, Kind, PRICE_SCALE, Parser, QTY_SCALE, Side, Sink, lookup};

const SOH: u8 = 0x01;

#[derive(Debug)]
pub struct Fix<'a> {
    pub symbols: &'a [&'a [u8]],
}

/// One `tag=value` pair, and the cursor moves past its SOH.
///
/// Running out of bytes mid-pair is `NeedMore` -- the routine stream case --
/// and a byte that cannot belong to a pair is `Malformed`. Keeping the two
/// apart here is what lets the framing logic above stay free of heuristics
/// about what a truncated header probably looks like.
#[inline]
fn pair<'b>(bytes: &'b [u8], at: &mut usize, start: usize) -> Result<(u32, &'b [u8]), FeedError> {
    let mut tag = 0_u32;
    let mut i = *at;
    loop {
        match bytes.get(i) {
            // Nine digits is every tag any FIX dictionary defines and the most
            // a u32 holds without wrapping. A longer run is a malformed field,
            // not a tag, and must not be accumulated into one.
            Some(d @ b'0'..=b'9') if i - *at < 9 => {
                tag = tag * 10 + u32::from(d - b'0');
                i += 1;
            }
            Some(b'=') if i > *at => break,
            None => return Err(FeedError::NeedMore),
            Some(_) => return Err(FeedError::Malformed { offset: start }),
        }
    }
    i += 1;
    let value_from = i;
    loop {
        match bytes.get(i) {
            Some(&SOH) => break,
            Some(_) => i += 1,
            None => return Err(FeedError::NeedMore),
        }
    }
    let value = &bytes[value_from..i];
    *at = i + 1;
    Ok((tag, value))
}

#[inline]
fn int(value: &[u8], at: usize) -> Result<i64, FeedError> {
    // Eighteen digits fit i64 with room to spare; no FIX engine sends more.
    // Letting a longer run of digits wrap turned one malformed length field
    // into an out-of-range frame end, which is a crash rather than a reject.
    if value.is_empty() || value.len() > 18 {
        return Err(FeedError::Malformed { offset: at });
    }
    let mut out = 0_i64;
    for byte in value {
        match byte {
            b'0'..=b'9' => out = out * 10 + i64::from(byte - b'0'),
            _ => return Err(FeedError::Malformed { offset: at }),
        }
    }
    Ok(out)
}

/// A decimal like `43250.1` into fixed-point, no float anywhere: the integer
/// part scales whole, the fraction is padded to exactly the scale's width.
#[inline]
fn decimal(value: &[u8], scale: i64, at: usize) -> Result<i64, FeedError> {
    let dot = value.iter().position(|b| *b == b'.');
    let (whole, frac) = match dot {
        Some(index) => (&value[..index], &value[index + 1..]),
        None => (value, &value[..0]),
    };
    let mut out = int(whole, at)? * scale;
    let mut worth = scale;
    for byte in frac {
        match byte {
            b'0'..=b'9' => {
                worth /= 10;
                if worth == 0 {
                    return Err(FeedError::Malformed { offset: at }); // more precision than the scale holds
                }
                out += i64::from(byte - b'0') * worth;
            }
            _ => return Err(FeedError::Malformed { offset: at }),
        }
    }
    Ok(out)
}

impl Parser for Fix<'_> {
    fn parse(&self, bytes: &[u8], sink: &mut impl Sink) -> Result<usize, FeedError> {
        let mut at = 0;
        loop {
            let start = at;
            if at == bytes.len() {
                return Ok(at);
            }
            // 8=FIX.4.4, then 9=length, which frames the rest without
            // scanning it. NeedMore from either is a short read: stop cleanly
            // at the message boundary and let the caller refill.
            let mut cursor = at;
            let (tag, _) = match pair(bytes, &mut cursor, start) {
                Ok(pair) => pair,
                Err(FeedError::NeedMore) => return Ok(at),
                Err(e) => return Err(e),
            };
            if tag != 8 {
                return Err(FeedError::Malformed { offset: start });
            }
            let (tag, length_value) = match pair(bytes, &mut cursor, start) {
                Ok(pair) => pair,
                Err(FeedError::NeedMore) => return Ok(at),
                Err(e) => return Err(e),
            };
            if tag != 9 {
                return Err(FeedError::Malformed { offset: start });
            }
            let body_length = int(length_value, start)? as usize;
            // body, then "10=xxx" + SOH: 7 bytes of trailer.
            let end = cursor + body_length + 7;
            if end > bytes.len() {
                return Ok(at);
            }

            // Checksum covers everything before the "10=" tag, modulo 256.
            let claimed = &bytes[end - 7..end];
            if &claimed[..3] != b"10=" || claimed[6] != SOH {
                return Err(FeedError::Malformed { offset: start });
            }
            let sum: u32 = bytes[start..end - 7].iter().map(|b| u32::from(*b)).sum();
            if i64::from(sum % 256) != int(&claimed[3..6], start)? {
                return Err(FeedError::Malformed { offset: start });
            }

            self.body(bytes, cursor, end - 7, start, sink)?;
            at = end;
        }
    }
}

impl Fix<'_> {
    /// The body between BodyLength and the checksum trailer: one pass, one
    /// entry emitted per completed 269/270/271 group.
    fn body(
        &self,
        bytes: &[u8],
        mut at: usize,
        end: usize,
        start: usize,
        sink: &mut impl Sink,
    ) -> Result<(), FeedError> {
        let mut is_market_data = false;
        let mut symbol: Option<u16> = None;
        let mut entry = Event::zeroed();
        entry.kind = Kind::Level;
        let mut entry_open = false;

        while at < end {
            let (tag, value) = pair(bytes, &mut at, start).map_err(|e| match e {
                // The frame is fully buffered, so running out of bytes here
                // means the structure overran its own BodyLength.
                FeedError::NeedMore => FeedError::Malformed { offset: start },
                other => other,
            })?;
            match tag {
                35 => is_market_data = value == b"W" || value == b"X",
                55 => {
                    symbol = Some(
                        lookup(self.symbols, value)
                            .ok_or(FeedError::UnknownSymbol { offset: start })?,
                    );
                }
                // 269 opens an entry group; flush the previous one.
                269 => {
                    if entry_open {
                        self.flush(&mut entry, symbol, start, sink)?;
                    }
                    entry_open = true;
                    entry.kind = Kind::Level;
                    (entry.side, entry.kind) = match value {
                        b"0" => (Side::Bid, Kind::Level),
                        b"1" => (Side::Ask, Kind::Level),
                        b"2" => (Side::Bid, Kind::Trade),
                        _ => return Err(FeedError::Malformed { offset: start }),
                    };
                }
                270 => entry.price = decimal(value, PRICE_SCALE, start)?,
                271 => entry.qty = decimal(value, QTY_SCALE, start)?,
                _ => {} // sequence numbers, timestamps: real, just not book data
            }
        }
        if entry_open {
            self.flush(&mut entry, symbol, start, sink)?;
        }
        if !is_market_data {
            // Heartbeats and the like: valid FIX, nothing to emit.
        }
        Ok(())
    }

    fn flush(
        &self,
        entry: &mut Event,
        symbol: Option<u16>,
        start: usize,
        sink: &mut impl Sink,
    ) -> Result<(), FeedError> {
        let Some(symbol) = symbol else {
            return Err(FeedError::Malformed { offset: start });
        };
        entry.symbol = symbol;
        sink.accept(entry);
        entry.price = 0;
        entry.qty = 0;
        Ok(())
    }
}
