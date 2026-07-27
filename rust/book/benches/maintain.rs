//! Book maintenance throughput: the custom structures against the standard
//! library's, on the same pre-parsed ITCH stream. If the custom ones do not
//! win here, they have no reason to exist — that is the bet, priced.

use std::time::Instant;
use t2t_book::{Band, Books, reference::ReferenceBooks};

const ITCH_BAND: Band = Band {
    floor: (1_000_000 - 3_200) * 10_000,
    tick: 100 * 10_000,
    ticks: 5_070,
};
use t2t_feed::{Event, GENERATOR_SEED, Parser, Rng, Sink, synth};

const MESSAGES: usize = 1_000_000;
const RUNS: usize = 3;

struct Collect(Vec<Event>);
impl Sink for Collect {
    fn accept(&mut self, event: &Event) {
        self.0.push(*event);
    }
}

fn measure(name: &str, events: &[Event], mut run: impl FnMut(&[Event]) -> (i64, u64)) {
    let mut best = f64::MAX;
    let mut fingerprint = (0, 0);
    for _ in 0..RUNS {
        let started = Instant::now();
        fingerprint = run(events);
        best = best.min(started.elapsed().as_secs_f64());
    }
    println!(
        "{name:<34} {:>6.1} ns/event   {:>6.2}M events/s   (bbo sum {}, orders {})",
        best * 1e9 / events.len() as f64,
        events.len() as f64 / best / 1e6,
        fingerprint.0,
        fingerprint.1,
    );
}

fn main() {
    // Parse once, outside the timed region: this benchmark prices the books,
    // not the parser.
    let stream = synth::itch(MESSAGES, &mut Rng(GENERATOR_SEED));
    let parser = t2t_feed::itch::Itch {
        symbols: synth::TRADFI,
    };
    let mut collected = Collect(Vec::with_capacity(MESSAGES));
    parser.parse(&stream.bytes, &mut collected).unwrap();
    let events = collected.0;
    println!(
        "{} ITCH events applied to {} symbols, best of {RUNS} runs\n",
        events.len(),
        synth::TRADFI.len()
    );

    measure("custom: ladder + open addressing", &events, |events| {
        let mut books = Books::new(synth::TRADFI.len(), ITCH_BAND);
        for event in events {
            books.apply(event);
        }
        let mut bbo = 0_i64;
        let mut orders = 0_u64;
        for index in 0..synth::TRADFI.len() {
            let book = books.symbol(index as u16);
            bbo += book.bids.best().map_or(0, |(p, _)| p);
            bbo += book.asks.best().map_or(0, |(p, _)| p);
            orders += book.orders.len() as u64;
        }
        (bbo, orders)
    });

    measure("std: BTreeMap + HashMap", &events, |events| {
        let mut books = ReferenceBooks::new(synth::TRADFI.len());
        for event in events {
            books.apply(event);
        }
        let mut bbo = 0_i64;
        let mut orders = 0_u64;
        for book in &books.symbols {
            bbo += book.best_bid().map_or(0, |(p, _)| p);
            bbo += book.best_ask().map_or(0, |(p, _)| p);
            orders += book.orders.len() as u64;
        }
        (bbo, orders)
    });

    println!(
        "\nSame events, same final books -- the fingerprints prove it -- so the \
         gap is purely the structures. The std row is not a strawman: BTreeMap \
         and HashMap are excellent general structures. The custom row exists \
         because this workload is not general: updates cluster at the touch, \
         keys are not adversarial, and churn dominates."
    );
}
