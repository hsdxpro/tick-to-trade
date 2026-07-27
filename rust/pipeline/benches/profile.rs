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
use t2t_feed::mold::{Arbitrator, HEADER_LEN, Header};
use t2t_feed::synth::TRADFI;
use t2t_pipeline::probe::{self, Probes};
use t2t_pipeline::{BAND, FeedStage, Strategy};

const PROBES: usize = 1_000_000;

fn probes() -> Vec<Vec<u8>> {
    let mut out = Vec::with_capacity(PROBES);
    let mut stream = Probes::new();
    for _ in 0..PROBES {
        out.push(stream.next_datagram());
    }
    out
}

fn main() {
    let datagrams = probes();
    let parser = t2t_feed::itch::Itch { symbols: TRADFI };

    // Whole path, for the denominator.
    let mut feed = FeedStage::new(TRADFI.len(), BAND);
    let mut strategy = Strategy::default();
    let mut arbitrator = Arbitrator::new(probe::SESSION, probe::FIRST_SEQUENCE);
    let mut sink = 0_u64;
    let started = Instant::now();
    for datagram in &datagrams {
        let header = Header::parse(datagram).unwrap();
        arbitrator.admit(&header, &datagram[HEADER_LEN..]);
        parser.parse(&datagram[HEADER_LEN..], &mut feed).unwrap();
        if let Some(update) = feed.take_moved()
            && let Some(order) = strategy.decide(&update)
        {
            sink += u64::from(order.encode()[0]);
        }
    }
    let whole = started.elapsed().as_secs_f64();

    // Arbitration plus parse plus book, without decision or encoding.
    let mut feed = FeedStage::new(TRADFI.len(), BAND);
    let mut arbitrator = Arbitrator::new(probe::SESSION, probe::FIRST_SEQUENCE);
    let started = Instant::now();
    for datagram in &datagrams {
        let header = Header::parse(datagram).unwrap();
        arbitrator.admit(&header, &datagram[HEADER_LEN..]);
        parser.parse(&datagram[HEADER_LEN..], &mut feed).unwrap();
        std::hint::black_box(feed.take_moved());
    }
    let through_book = started.elapsed().as_secs_f64();

    // Arbitration plus parse: the same bytes into a sink that only counts.
    let mut counted = 0_u64;
    let mut arbitrator = Arbitrator::new(probe::SESSION, probe::FIRST_SEQUENCE);
    let started = Instant::now();
    for datagram in &datagrams {
        let header = Header::parse(datagram).unwrap();
        arbitrator.admit(&header, &datagram[HEADER_LEN..]);
        parser
            .parse(&datagram[HEADER_LEN..], &mut |_: &t2t_feed::Event| {
                counted += 1
            })
            .unwrap();
    }
    let through_parse = started.elapsed().as_secs_f64();

    // Arbitration alone: header decode and the one comparison.
    let mut arbitrator = Arbitrator::new(probe::SESSION, probe::FIRST_SEQUENCE);
    let started = Instant::now();
    for datagram in &datagrams {
        let header = Header::parse(datagram).unwrap();
        std::hint::black_box(arbitrator.admit(&header, &datagram[HEADER_LEN..]));
    }
    let arbitrate_only = started.elapsed().as_secs_f64();
    assert!(counted > 0 && sink > 0);

    let ns = |seconds: f64| seconds * 1e9 / PROBES as f64;
    let share = |seconds: f64| seconds / whole * 100.0;

    println!("internal path attribution, {PROBES} probes (2 ITCH messages each)\n");
    println!(
        "  {:<28} {:>7.1} ns   {:>5.1}%",
        "arbitrate (Mold A/B)",
        ns(arbitrate_only),
        share(arbitrate_only)
    );
    println!(
        "  {:<28} {:>7.1} ns   {:>5.1}%",
        "parse (ITCH framing+fields)",
        ns(through_parse - arbitrate_only),
        share(through_parse - arbitrate_only)
    );
    println!(
        "  {:<28} {:>7.1} ns   {:>5.1}%",
        "book (ladder + order map)",
        ns(through_book - through_parse),
        share(through_book - through_parse)
    );
    // A stage cheaper than the difference between two timings of the whole
    // path cannot be resolved by subtraction, and printing a negative number
    // would be dressing noise as a measurement.
    let decide = whole - through_book;
    if ns(decide) < 1.0 {
        println!("  {:<28} {:>7} ns   {:>5}", "decide + encode", "<1", "~0%");
    } else {
        println!(
            "  {:<28} {:>7.1} ns   {:>5.1}%",
            "decide + encode",
            ns(decide),
            share(decide)
        );
    }
    println!("  {:<28} {:>7.1} ns   100.0%", "total", ns(whole));
    println!(
        "\nEach probe carries a delete and an add, so the book does two \
         updates and the parser two messages. Divide by two for a per-message \
         figure comparable with the parser benchmark."
    );
}
