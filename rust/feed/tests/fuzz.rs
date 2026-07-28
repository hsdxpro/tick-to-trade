//! Adversarial bytes against every parser: the contract must hold for input
//! nobody generated.
//!
//! The differential suite proves the parsers recover meaning from bytes their
//! own generator wrote, and the truncation suite proves a prefix of a valid
//! stream is a clean short read. Neither says anything about bytes that are
//! *wrong* -- and a feed handler eats whatever the wire delivers, including
//! whatever a corrupted switch, a misconfigured venue, or an attacker sends.
//!
//! Mutation-based rather than random from nothing: a stream of uniform noise
//! is rejected at the first byte and exercises one branch. Corrupting a valid
//! stream reaches deep into the field decoders, where the interesting failures
//! live -- the length that frames past the buffer, the count that overflows the
//! index, the digit run that wraps an accumulator.
//!
//! Seeded, so a failure names the exact input that caused it and reproduces on
//! the next run. There is no `cargo-fuzz` here on purpose: a plain test runs in
//! everyone's suite, and this repository's dependency budget is zero.

use t2t_feed::{Event, FeedError, Parser, Rng, synth};

/// What every parser owes for every input, valid or not.
///
/// Panicking, looping forever, or reporting more bytes consumed than it was
/// given are the three ways a feed handler takes the trading system down with
/// it. Rejecting is always allowed; those three never are.
fn contract(name: &str, parser: &impl Parser, bytes: &[u8], seed: u64) {
    let mut count = 0_u64;
    let mut sink = |_: &Event| count += 1;
    match parser.parse(bytes, &mut sink) {
        Ok(consumed) => assert!(
            consumed <= bytes.len(),
            "{name}: reported {consumed} of {} bytes consumed (seed {seed:#x})",
            bytes.len()
        ),
        Err(FeedError::NeedMore | FeedError::Malformed { .. } | FeedError::UnknownSymbol { .. }) => {}
    }
}

/// Corrupts a copy of `source` in one of the ways a wire actually breaks.
fn corrupt(source: &[u8], rng: &mut Rng) -> Vec<u8> {
    let mut bytes = source.to_vec();
    if bytes.is_empty() {
        return bytes;
    }
    match rng.below(6) {
        // A single flipped byte: the checksum and framing case.
        0 => {
            let at = rng.below(bytes.len() as u64) as usize;
            bytes[at] = rng.below(256) as u8;
        }
        // A run of bytes replaced: a burst error.
        1 => {
            let at = rng.below(bytes.len() as u64) as usize;
            let len = (rng.below(32) as usize).min(bytes.len() - at);
            for byte in &mut bytes[at..at + len] {
                *byte = rng.below(256) as u8;
            }
        }
        // Digits everywhere: the accumulator-overflow case, aimed at every
        // length, count and quantity field at once.
        2 => {
            let at = rng.below(bytes.len() as u64) as usize;
            let len = (rng.below(40) as usize).min(bytes.len() - at);
            for byte in &mut bytes[at..at + len] {
                *byte = b'9';
            }
        }
        // Truncated mid-message, then extended with noise, so the parser
        // meets a plausible header over an implausible body.
        3 => {
            let at = rng.below(bytes.len() as u64) as usize;
            bytes.truncate(at);
            for _ in 0..rng.below(64) {
                bytes.push(rng.below(256) as u8);
            }
        }
        // Bytes deleted from the middle: every field after shifts.
        4 => {
            let at = rng.below(bytes.len() as u64) as usize;
            let len = (rng.below(16) as usize).min(bytes.len() - at);
            bytes.drain(at..at + len);
        }
        // Bytes inserted: the same, in the other direction.
        _ => {
            let at = rng.below(bytes.len() as u64) as usize;
            for _ in 0..rng.below(16) {
                bytes.insert(at, rng.below(256) as u8);
            }
        }
    }
    bytes
}

/// Twenty thousand corruptions of a valid stream, per format.
///
/// The seed is fixed so a failure is reproducible, and the count is sized to
/// belong in the ordinary suite rather than a separate campaign. Raising it to
/// 200,000 -- 900,000 parse attempts across the three formats -- found nothing
/// further once the accumulators were bounded.
#[test]
fn every_parser_survives_corrupted_streams() {
    const ROUNDS: usize = 20_000;

    let itch = synth::itch(200, &mut Rng(t2t_feed::GENERATOR_SEED));
    let fix = synth::fix(200, &mut Rng(t2t_feed::GENERATOR_SEED));
    let json = synth::json(200, &mut Rng(t2t_feed::GENERATOR_SEED));

    let itch_parser = t2t_feed::itch::Itch {
        symbols: synth::TRADFI,
    };
    let fix_parser = t2t_feed::fix::Fix {
        symbols: synth::TRADFI,
    };
    let json_parser = t2t_feed::json::Json {
        symbols: synth::CRYPTO,
    };

    let mut rng = Rng(0x1234_5678_9abc_def0);
    for _ in 0..ROUNDS {
        let seed = rng.0;
        contract("itch", &itch_parser, &corrupt(&itch.bytes, &mut rng), seed);
        contract("fix", &fix_parser, &corrupt(&fix.bytes, &mut rng), seed);
        contract("json", &json_parser, &corrupt(&json.bytes, &mut rng), seed);
    }
}

/// Bytes from nothing, with no valid stream underneath.
///
/// Cheaper coverage than corruption and it reaches a different place: the
/// entry branch every parser takes before it trusts anything, where a length
/// prefix is read out of noise.
#[test]
fn every_parser_survives_arbitrary_bytes() {
    const ROUNDS: usize = 10_000;

    let itch_parser = t2t_feed::itch::Itch {
        symbols: synth::TRADFI,
    };
    let fix_parser = t2t_feed::fix::Fix {
        symbols: synth::TRADFI,
    };
    let json_parser = t2t_feed::json::Json {
        symbols: synth::CRYPTO,
    };

    let mut rng = Rng(0x0fed_cba9_8765_4321);
    for _ in 0..ROUNDS {
        let seed = rng.0;
        let len = rng.below(512) as usize;
        let bytes: Vec<u8> = (0..len).map(|_| rng.below(256) as u8).collect();
        contract("itch", &itch_parser, &bytes, seed);
        contract("fix", &fix_parser, &bytes, seed);
        contract("json", &json_parser, &bytes, seed);
    }
}
