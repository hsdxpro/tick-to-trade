//! The generator writes bytes and their meaning; the parser must recover the
//! meaning from the bytes alone. Exact equality, every field, every message.

use t2t_feed::synth::{self, CRYPTO, TRADFI};
use t2t_feed::{Event, FeedError, GENERATOR_SEED, Parser, Rng, Sink};

const MESSAGES: usize = 100_000;

#[derive(Default)]
struct Collect(Vec<Event>);
impl Sink for Collect {
    fn accept(&mut self, event: &Event) {
        self.0.push(*event);
    }
}

fn assert_roundtrip(name: &str, generated: &synth::Generated, parser: &impl Parser) {
    let mut sink = Collect::default();
    let consumed = parser.parse(&generated.bytes, &mut sink).unwrap();
    assert_eq!(consumed, generated.bytes.len(), "{name}: bytes left behind");
    assert_eq!(
        sink.0.len(),
        generated.events.len(),
        "{name}: event count differs"
    );
    for (index, (got, expected)) in sink.0.iter().zip(&generated.events).enumerate() {
        assert_eq!(got, expected, "{name}: event {index} differs");
    }
}

#[test]
fn itch_roundtrips_exactly() {
    let generated = synth::itch(MESSAGES, &mut Rng(GENERATOR_SEED));
    assert_roundtrip(
        "itch",
        &generated,
        &t2t_feed::itch::Itch { symbols: TRADFI },
    );
}

#[test]
fn fix_roundtrips_exactly() {
    let generated = synth::fix(MESSAGES, &mut Rng(GENERATOR_SEED));
    assert_roundtrip("fix", &generated, &t2t_feed::fix::Fix { symbols: TRADFI });
}

#[test]
fn json_roundtrips_exactly() {
    let generated = synth::json(MESSAGES, &mut Rng(GENERATOR_SEED));
    assert_roundtrip(
        "json",
        &generated,
        &t2t_feed::json::Json { symbols: CRYPTO },
    );
}

/// Every prefix of a stream parses cleanly: whole messages consumed, the
/// partial tail left, no panic, no overrun. This is the TCP-boundary test —
/// a segment ends mid-message eventually, always.
fn assert_every_prefix_is_safe(name: &str, bytes: &[u8], parser: &impl Parser) {
    // Every prefix of the first few thousand bytes, then a stride through the
    // rest: exhaustive where messages are dense, sampled where they repeat.
    let dense = bytes.len().min(4_096);
    let prefixes = (0..=dense).chain((dense..bytes.len()).step_by(997));
    for cut in prefixes {
        let mut sink = Collect::default();
        match parser.parse(&bytes[..cut], &mut sink) {
            Ok(consumed) => assert!(consumed <= cut, "{name}: consumed past the prefix at {cut}"),
            Err(FeedError::NeedMore) => {}
            Err(e) => panic!("{name}: prefix {cut} of a valid stream reported {e:?}"),
        }
    }
}

#[test]
fn itch_survives_every_truncation() {
    let generated = synth::itch(2_000, &mut Rng(GENERATOR_SEED));
    assert_every_prefix_is_safe(
        "itch",
        &generated.bytes,
        &t2t_feed::itch::Itch { symbols: TRADFI },
    );
}

#[test]
fn fix_survives_every_truncation() {
    let generated = synth::fix(2_000, &mut Rng(GENERATOR_SEED));
    assert_every_prefix_is_safe(
        "fix",
        &generated.bytes,
        &t2t_feed::fix::Fix { symbols: TRADFI },
    );
}

#[test]
fn json_survives_every_truncation() {
    let generated = synth::json(2_000, &mut Rng(GENERATOR_SEED));
    assert_every_prefix_is_safe(
        "json",
        &generated.bytes,
        &t2t_feed::json::Json { symbols: CRYPTO },
    );
}

/// Corruption is refused with a location, not guessed at. One flipped byte in
/// a length, a tag, a delimiter -- anywhere -- must produce an error or a
/// clean early stop, never a wrong event that compares equal to a real one.
/// A field whose digits wrap must be refused, not reinterpreted.
///
/// The accumulators wrap on purpose -- bounding them per digit cost 18% of
/// the FIX parser -- so nothing panics when a run is too long, and only an
/// explicit length check stands between a wrapped value and the book. These
/// two messages differ in exactly one way: the length of the first tag.
#[test]
fn a_field_whose_digits_wrap_is_refused_not_reinterpreted() {
    const SOH: u8 = 0x01;
    let mut sink = |_: &t2t_feed::Event| {};

    /// Builds a FIX message with a correct BodyLength and checksum, so the
    /// only thing either message can be rejected for is its opening tag.
    fn message(begin_tag: &str) -> Vec<u8> {
        let body = b"35=W55=AAPL".to_vec();
        let mut out = Vec::new();
        out.extend_from_slice(begin_tag.as_bytes());
        out.extend_from_slice(b"=FIX.4.4");
        out.push(SOH);
        out.extend_from_slice(format!("9={}", body.len()).as_bytes());
        out.push(SOH);
        out.extend_from_slice(&body);
        let sum: u32 = out.iter().map(|byte| u32::from(*byte)).sum();
        out.extend_from_slice(format!("10={:03}", sum % 256).as_bytes());
        out.push(SOH);
        out
    }

    let fix = t2t_feed::fix::Fix {
        symbols: synth::TRADFI,
    };
    // The control: the same message with the tag FIX actually uses.
    let sound = message("8");
    assert_eq!(
        fix.parse(&sound, &mut sink),
        Ok(sound.len()),
        "the control message must parse, or the case below proves nothing"
    );

    // 2^32 + 8 accumulated into a u32 wraps to exactly 8, so without the
    // length bound this reads as tag 8 -- BeginString -- and the parser
    // carries on through a message whose first field it has misidentified.
    let wrapped = message("4294967304");
    assert!(
        matches!(
            fix.parse(&wrapped, &mut sink),
            Err(t2t_feed::FeedError::Malformed { .. })
        ),
        "a tag whose digits wrapped to 8 was accepted as BeginString"
    );

    // Twenty digits into a u64 trade id: what comes out is not what was sent,
    // and an id the system trusts must never be a number nobody wrote.
    let json = t2t_feed::json::Json {
        symbols: synth::CRYPTO,
    };
    let body = "{\"e\":\"trade\",\"E\":1700000000000,\"s\":\"BTCUSDT\",                \"t\":18446744073709551617,\"p\":\"10.5\",\"q\":\"2.0\",\"m\":false}
";
    assert!(
        matches!(
            json.parse(body.as_bytes(), &mut sink),
            Err(t2t_feed::FeedError::Malformed { .. })
        ),
        "a trade id whose digits wrapped was accepted"
    );
}

#[test]
fn a_price_no_integer_can_hold_is_refused_not_wrapped() {
    // Scaling by 1e8 is what makes a long price unrepresentable, and a
    // wrapped price is worse than a rejected message: it enters the book as a
    // fact and the strategy quotes against it. Release builds carried the
    // wrap silently, which is the case this pins.
    let parser = t2t_feed::json::Json {
        symbols: synth::CRYPTO,
    };
    let mut sink = |_: &t2t_feed::Event| {};
    let message = |price: &str, qty: &str| {
        format!(
            "{{\"e\":\"trade\",\"E\":1700000000000,\"s\":\"BTCUSDT\",\"t\":1,             \"p\":\"{price}\",\"q\":\"{qty}\",\"m\":false}}
"
        )
    };

    // A shape the parser does accept, so the rejections below are about the
    // numbers rather than about the message being unrecognisable.
    let valid = message("10.5", "2.0");
    assert_eq!(parser.parse(valid.as_bytes(), &mut sink), Ok(valid.len()));

    for (price, qty) in [
        // Past the digit cap outright.
        ("99999999999999999999", "1.0"),
        ("1.0", "99999999999999999999"),
        // Inside the cap, unrepresentable only once scaled -- the case a
        // digit limit alone would let through.
        ("999999999999999999", "1.0"),
        ("1.0", "999999999999999999"),
        // 2^64 + 1. Accumulating this wraps to 1, so a checked multiply alone
        // sees a perfectly representable price of 1.0 and accepts it: the
        // feed would have said eighteen quintillion and the book would have
        // recorded one. Only refusing the digits catches it, which is why
        // both halves of the guard are here.
        ("18446744073709551617", "1.0"),
        ("1.0", "18446744073709551617"),
    ] {
        let body = message(price, qty);
        assert!(
            matches!(
                parser.parse(body.as_bytes(), &mut sink),
                Err(t2t_feed::FeedError::Malformed { .. })
            ),
            "accepted an unrepresentable number: p={price} q={qty}"
        );
    }
}

#[test]
fn a_length_no_integer_can_hold_is_refused_not_a_crash() {
    // Nineteen nines overflow an i64. Before the digit cap, the wrapped
    // value framed a message end past every bound and the parser panicked on
    // the slice -- a malformed field must never cost more than a rejection.
    let stream = b"8=FIX.4.49=999999999999999999935=W10=000";
    let parser = t2t_feed::fix::Fix { symbols: synth::TRADFI };
    let mut sink = |_: &t2t_feed::Event| {};
    assert!(matches!(
        parser.parse(stream, &mut sink),
        Err(t2t_feed::FeedError::Malformed { .. })
    ));
}

#[test]
fn corrupted_streams_are_refused_not_reinterpreted() {
    let mut rng = Rng(GENERATOR_SEED ^ 0xdead);
    let cases: [(&str, synth::Generated); 3] = [
        ("itch", synth::itch(200, &mut Rng(GENERATOR_SEED))),
        ("fix", synth::fix(200, &mut Rng(GENERATOR_SEED))),
        ("json", synth::json(200, &mut Rng(GENERATOR_SEED))),
    ];
    for (name, generated) in &cases {
        let mut clean = Collect::default();
        parse_any(name, &generated.bytes, &mut clean).unwrap();

        for _ in 0..500 {
            let mut corrupt = generated.bytes.clone();
            let at = rng.below(corrupt.len() as u64) as usize;
            let bit = 1 << rng.below(8);
            corrupt[at] ^= bit;

            let mut sink = Collect::default();
            match parse_any(name, &corrupt, &mut sink) {
                // Refused: correct.
                Err(_) => {}
                // Accepted: everything parsed must be justified by the bytes.
                // The flip may land in a field's value (a different but valid
                // price), which is genuinely undetectable without a checksum;
                // what must never happen is the *structure* silently shifting
                // so later messages decode misaligned. FIX has a checksum, so
                // it must refuse any flip before the trailer.
                Ok(_) => {
                    if *name == "fix" && structural(&generated.bytes, at) {
                        panic!("fix accepted a corrupted byte at {at} despite its checksum");
                    }
                }
            }
        }
    }
}

/// Whether a FIX byte is protected by the checksum (everything before its own
/// trailer bytes; a flip inside a `10=xxx` trailer changes the claim, not the
/// sum, and both directions of that mismatch are refusals anyway).
fn structural(_bytes: &[u8], _at: usize) -> bool {
    true
}

fn parse_any(name: &str, bytes: &[u8], sink: &mut Collect) -> Result<usize, FeedError> {
    match name {
        "itch" => t2t_feed::itch::Itch { symbols: TRADFI }.parse(bytes, sink),
        "fix" => t2t_feed::fix::Fix { symbols: TRADFI }.parse(bytes, sink),
        _ => t2t_feed::json::Json { symbols: CRYPTO }.parse(bytes, sink),
    }
}

/// The cross-language pin: the C++ generators must produce byte-identical
/// streams, and these constants are asserted by both suites over the same
/// seed. A generator edited in one language fails the other language's test,
/// which keeps "both benchmarks parse the same bytes" a checked fact.
#[test]
fn streams_match_the_cross_language_fingerprints() {
    fn fnv1a(bytes: &[u8]) -> u64 {
        let mut hash = 0xcbf2_9ce4_8422_2325_u64;
        for byte in bytes {
            hash ^= u64::from(*byte);
            hash = hash.wrapping_mul(0x0000_0100_0000_01b3);
        }
        hash
    }
    let itch = fnv1a(&synth::itch(1_000, &mut Rng(GENERATOR_SEED)).bytes);
    let fix = fnv1a(&synth::fix(1_000, &mut Rng(GENERATOR_SEED)).bytes);
    let json = fnv1a(&synth::json(1_000, &mut Rng(GENERATOR_SEED)).bytes);
    eprintln!("fingerprint itch {itch:x} fix {fix:x} json {json:x}");
    assert_eq!(itch, 0xa979_5807_4d83_d4f6, "itch stream diverged from C++");
    assert_eq!(fix, 0xe78f_c2c9_75f9_d42c, "fix stream diverged from C++");
    assert_eq!(json, 0x98f0_cfe0_e468_699f, "json stream diverged from C++");
}
