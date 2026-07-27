//! Binance-shaped JSON: trade events and depth updates, newline-delimited as
//! they come off a combined websocket stream.
//!
//! The honest hot-path position on JSON: you do not parse it generically.
//! The exchange documents the schema, the schema is stable, and a scanner
//! written against it does a fraction of the work of a general parser — no
//! DOM, no allocation, no unicode escapes in fields that are documented as
//! symbols and decimal strings. The benchmark runs serde_json over the same
//! stream to price that difference rather than assert it.
//!
//! The scanner tolerates key order and unknown keys (it skips balanced
//! structure), because exchanges add fields without notice, and a handler
//! that breaks on a new field is an outage on a calm Tuesday. What it does
//! not tolerate is a field it needs arriving malformed — that is a
//! [`FeedError::Malformed`], not a guess.

use crate::{Event, FeedError, Kind, PRICE_SCALE, Parser, QTY_SCALE, Side, Sink, lookup};

#[derive(Debug)]
pub struct Json<'a> {
    pub symbols: &'a [&'a [u8]],
}

/// A cursor over one message's bytes. Methods return `Malformed` with the
/// message's start offset, so errors point at the message, not mid-token.
struct Scan<'b> {
    bytes: &'b [u8],
    at: usize,
    start: usize,
}

impl<'b> Scan<'b> {
    fn err<T>(&self) -> Result<T, FeedError> {
        Err(FeedError::Malformed { offset: self.start })
    }

    fn peek(&self) -> Option<u8> {
        self.bytes.get(self.at).copied()
    }

    fn skip_ws(&mut self) {
        while matches!(self.peek(), Some(b' ' | b'\t' | b'\r')) {
            self.at += 1;
        }
    }

    fn expect(&mut self, byte: u8) -> Result<(), FeedError> {
        self.skip_ws();
        if self.peek() == Some(byte) {
            self.at += 1;
            Ok(())
        } else {
            self.err()
        }
    }

    /// A JSON string with no escapes, which is what symbols, event types and
    /// decimal-as-string fields are. An escape in one of those is malformed
    /// by the exchange's own schema, and refusing is better than decoding.
    fn string(&mut self) -> Result<&'b [u8], FeedError> {
        self.expect(b'"')?;
        let from = self.at;
        loop {
            match self.peek() {
                Some(b'"') => {
                    let out = &self.bytes[from..self.at];
                    self.at += 1;
                    return Ok(out);
                }
                Some(b'\\') => return self.err(),
                Some(_) => self.at += 1,
                None => return self.err(),
            }
        }
    }

    /// Skips any value: nested structure balanced, strings opaque. The cost
    /// only exists for keys the schema does not need.
    fn skip_value(&mut self) -> Result<(), FeedError> {
        self.skip_ws();
        match self.peek() {
            Some(b'"') => {
                self.string()?;
                Ok(())
            }
            Some(b'{' | b'[') => {
                let mut depth = 0_i32;
                let mut in_string = false;
                loop {
                    let Some(byte) = self.peek() else {
                        return self.err();
                    };
                    self.at += 1;
                    match byte {
                        b'"' if !in_string => in_string = true,
                        b'"' if in_string => in_string = false,
                        b'\\' if in_string => self.at += 1,
                        b'{' | b'[' if !in_string => depth += 1,
                        b'}' | b']' if !in_string => {
                            depth -= 1;
                            if depth == 0 {
                                return Ok(());
                            }
                        }
                        _ => {}
                    }
                }
            }
            _ => {
                // number, true, false, null: run to a delimiter
                while let Some(byte) = self.peek() {
                    if matches!(byte, b',' | b'}' | b']' | b'\n') {
                        return Ok(());
                    }
                    self.at += 1;
                }
                self.err()
            }
        }
    }

    /// `"1234.5678"` — a decimal wrapped in a string, the way exchanges ship
    /// prices so floats never touch them. Scaled during the scan.
    fn quoted_decimal(&mut self, scale: i64) -> Result<i64, FeedError> {
        let raw = self.string()?;
        let dot = raw.iter().position(|b| *b == b'.');
        let (whole, frac) = match dot {
            Some(index) => (&raw[..index], &raw[index + 1..]),
            None => (raw, &raw[..0]),
        };
        if whole.is_empty() {
            return self.err();
        }
        let mut out = 0_i64;
        for byte in whole {
            match byte {
                b'0'..=b'9' => out = out * 10 + i64::from(byte - b'0'),
                _ => return self.err(),
            }
        }
        out *= scale;
        let mut worth = scale;
        for byte in frac {
            match byte {
                b'0'..=b'9' => {
                    worth /= 10;
                    if worth == 0 {
                        return self.err();
                    }
                    out += i64::from(byte - b'0') * worth;
                }
                _ => return self.err(),
            }
        }
        Ok(out)
    }
}

impl Json<'_> {
    /// One `{...}` message. Two shapes: trade, and depth update whose `b`/`a`
    /// arrays carry `["price","qty"]` levels.
    fn message(&self, scan: &mut Scan<'_>, sink: &mut impl Sink) -> Result<(), FeedError> {
        scan.expect(b'{')?;
        let mut kind: Option<&[u8]> = None;
        let mut symbol: Option<u16> = None;
        let mut price = 0_i64;
        let mut qty = 0_i64;
        let mut maker_is_buyer = false;
        let mut trade_id = 0_u64;
        // Depth levels are emitted as they stream past, but only once the
        // symbol is known; Binance puts "s" before the arrays, and the
        // generator follows the real field order.
        loop {
            scan.skip_ws();
            let key = scan.string()?;
            scan.expect(b':')?;
            match key {
                b"e" => kind = Some(scan.string()?),
                b"s" => {
                    let name = scan.string()?;
                    symbol = Some(
                        lookup(self.symbols, name)
                            .ok_or(FeedError::UnknownSymbol { offset: scan.start })?,
                    );
                }
                b"p" => price = scan.quoted_decimal(PRICE_SCALE)?,
                b"q" => qty = scan.quoted_decimal(QTY_SCALE)?,
                b"m" => {
                    scan.skip_ws();
                    maker_is_buyer = match scan.peek() {
                        Some(b't') => {
                            scan.at += 4;
                            true
                        }
                        Some(b'f') => {
                            scan.at += 5;
                            false
                        }
                        _ => return scan.err(),
                    };
                }
                b"t" => {
                    scan.skip_ws();
                    let mut id = 0_u64;
                    while let Some(byte @ b'0'..=b'9') = scan.peek() {
                        id = id * 10 + u64::from(byte - b'0');
                        scan.at += 1;
                    }
                    trade_id = id;
                }
                b"b" | b"a" => {
                    let Some(symbol) = symbol else {
                        return scan.err();
                    };
                    let side = if key == b"b" { Side::Bid } else { Side::Ask };
                    scan.expect(b'[')?;
                    scan.skip_ws();
                    while scan.peek() != Some(b']') {
                        scan.expect(b'[')?;
                        let price = scan.quoted_decimal(PRICE_SCALE)?;
                        scan.expect(b',')?;
                        let qty = scan.quoted_decimal(QTY_SCALE)?;
                        scan.expect(b']')?;
                        let mut event = Event::zeroed();
                        event.kind = Kind::Level;
                        event.side = side;
                        event.symbol = symbol;
                        event.price = price;
                        event.qty = qty;
                        sink.accept(&event);
                        scan.skip_ws();
                        if scan.peek() == Some(b',') {
                            scan.at += 1;
                            scan.skip_ws();
                        }
                    }
                    scan.at += 1; // the ']'
                }
                _ => scan.skip_value()?,
            }
            scan.skip_ws();
            match scan.peek() {
                Some(b',') => scan.at += 1,
                Some(b'}') => {
                    scan.at += 1;
                    break;
                }
                _ => return scan.err(),
            }
        }

        if kind == Some(b"trade") {
            let Some(symbol) = symbol else {
                return scan.err();
            };
            let mut event = Event::zeroed();
            event.kind = Kind::Trade;
            // Binance semantics: m=true means the buyer was the maker, so the
            // aggressor hit the bid.
            event.side = if maker_is_buyer { Side::Ask } else { Side::Bid };
            event.symbol = symbol;
            event.price = price;
            event.qty = qty;
            event.aux = trade_id;
            sink.accept(&event);
        }
        Ok(())
    }
}

impl Parser for Json<'_> {
    fn parse(&self, bytes: &[u8], sink: &mut impl Sink) -> Result<usize, FeedError> {
        let mut at = 0;
        loop {
            // One message per line; a line without its newline is a short read.
            let Some(line_end) = bytes[at..].iter().position(|b| *b == b'\n') else {
                return Ok(at);
            };
            let mut scan = Scan {
                bytes: &bytes[..at + line_end],
                at,
                start: at,
            };
            self.message(&mut scan, sink)?;
            scan.skip_ws();
            if scan.at != scan.bytes.len() {
                return Err(FeedError::Malformed { offset: at });
            }
            at += line_end + 1;
        }
    }
}
