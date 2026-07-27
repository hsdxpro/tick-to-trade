//! Parse throughput per format: messages a second, nanoseconds a message,
//! and bytes a second, over one contiguous pre-generated stream.
//!
//! The JSON row runs twice — once through the schema scanner, once through
//! serde_json into a mirror struct — because "specialized beats general" is a
//! claim that deserves a number next to it rather than italics.

use std::time::Instant;
use t2t_feed::synth;
use t2t_feed::{Event, GENERATOR_SEED, Parser, Rng, Sink};

const MESSAGES: usize = 1_000_000;
const RUNS: usize = 3;

struct Count(u64);
impl Sink for Count {
    fn accept(&mut self, _event: &Event) {
        self.0 += 1;
    }
}

fn measure(name: &str, bytes: usize, messages: usize, mut run: impl FnMut() -> u64) {
    let mut per_run: Vec<f64> = Vec::with_capacity(RUNS);
    let mut events = 0;
    for _ in 0..RUNS {
        let started = Instant::now();
        events = run();
        per_run.push(started.elapsed().as_secs_f64());
    }
    per_run.sort_by(f64::total_cmp);
    let best = per_run[0];
    println!(
        "{name:<26} {:>7.1} ns/msg   {:>6.2}M msg/s   {:>7.2} MB/s   ({events} events)",
        best * 1e9 / messages as f64,
        messages as f64 / best / 1e6,
        bytes as f64 / best / 1e6,
    );
}

fn main() {
    println!("{MESSAGES} messages per format, seed {GENERATOR_SEED:#x}, best of {RUNS} runs\n");

    let itch = synth::itch(MESSAGES, &mut Rng(GENERATOR_SEED));
    let parser = t2t_feed::itch::Itch {
        symbols: synth::TRADFI,
    };
    measure("ITCH 5.0 (binary)", itch.bytes.len(), MESSAGES, || {
        let mut sink = Count(0);
        let consumed = parser.parse(&itch.bytes, &mut sink).unwrap();
        assert_eq!(consumed, itch.bytes.len());
        sink.0
    });

    let fix = synth::fix(MESSAGES, &mut Rng(GENERATOR_SEED));
    let parser = t2t_feed::fix::Fix {
        symbols: synth::TRADFI,
    };
    measure("FIX 4.4 (tag=value)", fix.bytes.len(), MESSAGES, || {
        let mut sink = Count(0);
        let consumed = parser.parse(&fix.bytes, &mut sink).unwrap();
        assert_eq!(consumed, fix.bytes.len());
        sink.0
    });

    let json = synth::json(MESSAGES, &mut Rng(GENERATOR_SEED));
    let parser = t2t_feed::json::Json {
        symbols: synth::CRYPTO,
    };
    measure("JSON (schema scanner)", json.bytes.len(), MESSAGES, || {
        let mut sink = Count(0);
        let consumed = parser.parse(&json.bytes, &mut sink).unwrap();
        assert_eq!(consumed, json.bytes.len());
        sink.0
    });

    measure("JSON (serde_json)", json.bytes.len(), MESSAGES, || {
        serde_baseline(&json.bytes)
    });

    println!(
        "\nThe serde row is not a criticism of serde_json: it parses arbitrary \
         JSON and this scanner parses one schema. The gap is what knowing the \
         schema is worth on a hot path."
    );
}

/// The general-purpose baseline: serde into the natural mirror of the schema.
fn serde_baseline(bytes: &[u8]) -> u64 {
    #[derive(serde::Deserialize)]
    struct Msg<'a> {
        e: &'a str,
        #[serde(default)]
        p: Option<&'a str>,
        #[serde(default)]
        q: Option<&'a str>,
        #[serde(default)]
        b: Option<Vec<(&'a str, &'a str)>>,
        #[serde(default)]
        a: Option<Vec<(&'a str, &'a str)>>,
    }
    let mut events = 0_u64;
    for line in bytes.split(|b| *b == b'\n').filter(|l| !l.is_empty()) {
        let msg: Msg = serde_json::from_slice(line).unwrap();
        match msg.e {
            "trade" => {
                let _ = (msg.p, msg.q);
                events += 1;
            }
            _ => {
                events += msg.b.map_or(0, |b| b.len() as u64);
                events += msg.a.map_or(0, |a| a.len() as u64);
            }
        }
    }
    events
}
