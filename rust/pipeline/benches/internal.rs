//! Internal tick-to-trade: datagram bytes in hand to order bytes ready, no
//! network stack anywhere. The wire harness answers "what does a counterparty
//! experience"; this answers "what does the pipeline itself cost", and the
//! difference between the two numbers is the network stacks' bill.
//!
//! Two shapes, because they answer different questions:
//!
//! - **Compute path**: parse, book, touch detection, decision, encode, on one
//!   thread. The floor: what the work costs with no handoffs at all.
//! - **Staged path**: the same work spread across the production thread
//!   layout -- feed, strategy, gateway -- connected by the same rings. The
//!   probe enters through a third ring standing in for the socket, so the
//!   measured path carries one hop more than production; the hop benchmark
//!   prices that stand-in, and readers can subtract it.
//!
//! Both use the identical stage types the engine binary runs. A benchmark of
//! a copy measures the copy.

use std::time::Instant;
use t2t_feed::Parser;
use t2t_feed::synth::TRADFI;
use t2t_pipeline::{BAND, BboUpdate, FeedStage, OrderCommand, Strategy, probe};

const PROBES: usize = 100_000;
const WARMUP: usize = 10_000;

fn report(name: &str, samples: &mut [u64]) {
    samples.sort_unstable();
    let at = |q: f64| samples[((samples.len() - 1) as f64 * q) as usize];
    println!(
        "{name:<44} min {:>6} ns   p50 {:>6} ns   p99 {:>6} ns",
        samples[0],
        at(0.5),
        at(0.99)
    );
}

fn probes() -> Vec<Vec<u8>> {
    let mut out = Vec::with_capacity(PROBES + WARMUP);
    let mut previous = None;
    for index in 0..PROBES + WARMUP {
        let order = index as u64 + 1;
        out.push(probe::datagram(previous, order, probe::price_of(index)));
        previous = Some(order);
    }
    out
}

/// Everything on one thread: the pure compute floor.
fn compute_path() {
    let datagrams = probes();
    let parser = t2t_feed::itch::Itch { symbols: TRADFI };
    let mut feed = FeedStage::new(TRADFI.len(), BAND);
    let mut strategy = Strategy::default();
    let mut samples = Vec::with_capacity(PROBES);
    let mut orders = 0_u64;

    for (index, datagram) in datagrams.iter().enumerate() {
        let started = Instant::now();
        parser.parse(datagram, &mut feed).unwrap();
        if let Some(update) = feed.take_moved() {
            if let Some(order) = strategy.decide(&update) {
                let encoded = order.encode();
                orders += u64::from(encoded[0]) + 1;
            }
        }
        let elapsed = started.elapsed();
        if index >= WARMUP {
            samples.push(elapsed.as_nanos() as u64);
        }
    }
    assert!(orders > 0);
    report("compute path (parse+book+decide+encode)", &mut samples);
}

/// The production thread layout, rings included. The probe enters through a
/// ring standing in for the socket read, so this path is one hop heavier
/// than the deployed engine; the hop bench prices the difference.
fn staged_path() {
    let datagrams = std::sync::Arc::new(probes());
    let (mut to_feed, mut feed_in) = t2t_spsc::channel::<(usize, Instant)>(1024);
    let (mut to_strategy, mut strategy_in) = t2t_spsc::channel::<(BboUpdate, Instant)>(1024);
    let (mut to_gateway, mut gateway_in) = t2t_spsc::channel::<(OrderCommand, Instant)>(1024);

    let feed_data = std::sync::Arc::clone(&datagrams);
    let feed = std::thread::spawn(move || {
        let parser = t2t_feed::itch::Itch { symbols: TRADFI };
        let mut stage = FeedStage::new(TRADFI.len(), BAND);
        for _ in 0..PROBES + WARMUP {
            let (index, t0) = loop {
                if let Some(item) = feed_in.try_pop() {
                    break item;
                }
                std::hint::spin_loop();
            };
            parser.parse(&feed_data[index], &mut stage).unwrap();
            if let Some(update) = stage.take_moved() {
                let mut item = (update, t0);
                while let Err(back) = to_strategy.try_push(item) {
                    item = back;
                    std::hint::spin_loop();
                }
            }
        }
    });

    let strategy = std::thread::spawn(move || {
        let mut stage = Strategy::default();
        for _ in 0..PROBES + WARMUP {
            let (update, t0) = loop {
                if let Some(item) = strategy_in.try_pop() {
                    break item;
                }
                std::hint::spin_loop();
            };
            if let Some(order) = stage.decide(&update) {
                let mut item = (order, t0);
                while let Err(back) = to_gateway.try_push(item) {
                    item = back;
                    std::hint::spin_loop();
                }
            }
        }
    });

    // Gateway work happens here on the measuring thread: encode, then stamp.
    let mut samples = Vec::with_capacity(PROBES);
    for index in 0..PROBES + WARMUP {
        let t0 = Instant::now();
        let mut item = (index, t0);
        while let Err(back) = to_feed.try_push(item) {
            item = back;
            std::hint::spin_loop();
        }
        let (order, sent) = loop {
            if let Some(item) = gateway_in.try_pop() {
                break item;
            }
            std::hint::spin_loop();
        };
        let encoded = order.encode();
        let elapsed = sent.elapsed();
        std::hint::black_box(encoded);
        if index >= WARMUP {
            samples.push(elapsed.as_nanos() as u64);
        }
    }
    feed.join().unwrap();
    strategy.join().unwrap();
    report(
        "staged path (3 threads, 3 hops incl. stand-in)",
        &mut samples,
    );
}

fn main() {
    println!(
        "internal tick-to-trade: bytes in hand to order bytes ready, \
         {PROBES} probes after {WARMUP} warmup\n"
    );
    compute_path();
    staged_path();
    println!(
        "\nThe staged path carries one ring hop more than the deployed engine \
         (the stand-in for the socket read); subtract the hop benchmark's p50 \
         to compare. The gap to the wire harness's numbers is the two network \
         stacks."
    );
}
