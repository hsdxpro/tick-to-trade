//! Where the internal latency goes, stage by stage.
//!
//! A flamegraph attributes CPU *time* and answers a throughput question. The
//! question here is a latency one — which stage owns the nanoseconds on the
//! critical path — so each stage is measured directly, on the same probe
//! stream, with the same stage objects the engine deploys.
//!
//! Per-stage figures are amortized over the whole run rather than sampled per
//! probe, because a single stage costs less than the clock's own resolution;
//! timing each individually would measure the clock. The percentages are
//! therefore exact shares of a measured total, not estimates.
//!
//! On a host with `perf`, `perf record` over `bench_internal` gives the
//! instruction-level view this deliberately does not: WSL2 ships no matching
//! perf build, so that measurement waits for a real Linux box rather than
//! being approximated here.

use std::time::Instant;
use t2t_feed::Parser;
use t2t_feed::synth::TRADFI;
use t2t_pipeline::{BAND, FeedStage, Strategy, probe};

const PROBES: usize = 1_000_000;

fn probes() -> Vec<Vec<u8>> {
    let mut out = Vec::with_capacity(PROBES);
    let mut previous = None;
    for index in 0..PROBES {
        let order = index as u64 + 1;
        out.push(probe::datagram(previous, order, probe::price_of(index)));
        previous = Some(order);
    }
    out
}

fn main() {
    let datagrams = probes();
    let parser = t2t_feed::itch::Itch { symbols: TRADFI };

    // Whole path, for the denominator.
    let mut feed = FeedStage::new(TRADFI.len(), BAND);
    let mut strategy = Strategy::default();
    let mut sink = 0_u64;
    let started = Instant::now();
    for datagram in &datagrams {
        parser.parse(datagram, &mut feed).unwrap();
        if let Some(update) = feed.take_moved()
            && let Some(order) = strategy.decide(&update)
        {
            sink += u64::from(order.encode()[0]);
        }
    }
    let whole = started.elapsed().as_secs_f64();

    // Parse plus book, without decision or encoding.
    let mut feed = FeedStage::new(TRADFI.len(), BAND);
    let started = Instant::now();
    for datagram in &datagrams {
        parser.parse(datagram, &mut feed).unwrap();
        std::hint::black_box(feed.take_moved());
    }
    let parse_and_book = started.elapsed().as_secs_f64();

    // Parse alone: the same bytes into a sink that only counts.
    let mut counted = 0_u64;
    let started = Instant::now();
    for datagram in &datagrams {
        parser
            .parse(datagram, &mut |_: &t2t_feed::Event| counted += 1)
            .unwrap();
    }
    let parse_only = started.elapsed().as_secs_f64();
    assert!(counted > 0 && sink > 0);

    let ns = |seconds: f64| seconds * 1e9 / PROBES as f64;
    let share = |seconds: f64| seconds / whole * 100.0;

    println!("internal path attribution, {PROBES} probes (2 ITCH messages each)\n");
    println!(
        "  {:<28} {:>7.1} ns   {:>5.1}%",
        "parse (ITCH framing+fields)",
        ns(parse_only),
        share(parse_only)
    );
    println!(
        "  {:<28} {:>7.1} ns   {:>5.1}%",
        "book (ladder + order map)",
        ns(parse_and_book - parse_only),
        share(parse_and_book - parse_only)
    );
    println!(
        "  {:<28} {:>7.1} ns   {:>5.1}%",
        "decide + encode",
        ns(whole - parse_and_book),
        share(whole - parse_and_book)
    );
    println!("  {:<28} {:>7.1} ns   100.0%", "total", ns(whole));
    println!(
        "\nEach probe carries a delete and an add, so the book does two \
         updates and the parser two messages. Divide by two for a per-message \
         figure comparable with the parser benchmark."
    );
}
