//! Where the internal latency goes, stage by stage.
//!
//! A flamegraph attributes CPU *time* and answers a throughput question. The
//! question here is a latency one — which stage owns the nanoseconds on the
//! critical path — so each stage is measured directly, on the same probe
//! stream, with the same stage objects the engine deploys.
//!
//! Per-stage figures are amortized over the whole run rather than sampled per
//! probe, because a single stage costs less than the clock's own resolution;
//! timing each individually would measure the clock.
//!
//! A stage is the difference between two variants of the path, so its error is
//! the error of both, and two things had to be fixed before the differences
//! meant anything:
//!
//! - **Each variant is timed [`ROUNDS`] times and the best kept.** Timed once,
//!   the error exceeded the smallest stage: `decide + encode` read 1.4 ns on one
//!   run and 17.1 ns on the next, and 1.4 ns is about four cycles to reach a
//!   decision and lay down an order — not a cost, just the gap between two
//!   110 ms measurements.
//! - **The variants are interleaved, not run in blocks.** All of one variant
//!   then all of the next puts minutes between the two timings a stage is the
//!   difference of, so a machine that warms up over the run charges the drift to
//!   whichever stage happens to be measured last. A round times every variant
//!   before any variant repeats.
//!
//! The gap between the two fastest rounds is printed, because that is the
//! resolution of a reported minimum: a stage nearer it than to its own figure is
//! not separated from its neighbour, whatever the figure says. On a machine busy
//! enough that even the runner-up is disturbed, every small stage reads `below
//! noise` -- which is the method declining to attribute rather than dressing an
//! interruption as a cost.
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

/// Passes over all four variants. The best timing of each is kept: it is the
/// round least disturbed by everything else on the machine.
///
/// Nine rather than five so that the two fastest rounds are both clean on a
/// machine that hiccups occasionally. At five, one interruption in four rounds
/// left the runner-up disturbed too, and the whole table fell back to `below
/// noise` for stages that do resolve when measured on a quiet host.
const ROUNDS: usize = 9;

fn probes() -> Vec<Vec<u8>> {
    let mut out = Vec::with_capacity(PROBES);
    let mut stream = Probes::new();
    for _ in 0..PROBES {
        out.push(stream.next_datagram());
    }
    out
}

/// The two fastest timings of one variant, in seconds.
///
/// Two, because the figure reported is the minimum and what matters is how
/// repeatable *that* is. The slowest round measures an interruption -- a cold
/// start, a scheduler decision -- and spread taken from it says the method is
/// blind when it is not.
#[derive(Clone, Copy, Debug)]
struct Timing {
    best: f64,
    second: f64,
}

impl Timing {
    const fn new() -> Self {
        Self {
            best: f64::INFINITY,
            second: f64::INFINITY,
        }
    }

    fn observe(&mut self, seconds: f64) {
        if seconds < self.best {
            self.second = self.best;
            self.best = seconds;
        } else if seconds < self.second {
            self.second = seconds;
        }
    }

    /// How far the second-fastest round sat from the fastest: the repeatability
    /// of the number actually reported.
    const fn spread(&self) -> f64 {
        self.second - self.best
    }
}

fn main() {
    let datagrams = probes();
    let parser = t2t_feed::itch::Itch { symbols: TRADFI };
    let mut sink = 0_u64;
    let mut counted = 0_u64;

    let mut whole = Timing::new();
    let mut through_book = Timing::new();
    let mut through_parse = Timing::new();
    let mut arbitrate_only = Timing::new();

    for _ in 0..ROUNDS {
        // Whole path, for the denominator.
        {
            let mut feed = FeedStage::new(TRADFI.len(), BAND);
            let mut strategy = Strategy::default();
            let mut arbitrator = Arbitrator::new(probe::SESSION, probe::FIRST_SEQUENCE);
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
            whole.observe(started.elapsed().as_secs_f64());
        }

        // Arbitration plus parse plus book, without decision or encoding.
        {
            let mut feed = FeedStage::new(TRADFI.len(), BAND);
            let mut arbitrator = Arbitrator::new(probe::SESSION, probe::FIRST_SEQUENCE);
            let started = Instant::now();
            for datagram in &datagrams {
                let header = Header::parse(datagram).unwrap();
                arbitrator.admit(&header, &datagram[HEADER_LEN..]);
                parser.parse(&datagram[HEADER_LEN..], &mut feed).unwrap();
                std::hint::black_box(feed.take_moved());
            }
            through_book.observe(started.elapsed().as_secs_f64());
        }

        // Arbitration plus parse: the same bytes into a sink that only counts.
        {
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
            through_parse.observe(started.elapsed().as_secs_f64());
        }

        // Arbitration alone: header decode and the one comparison.
        {
            let mut arbitrator = Arbitrator::new(probe::SESSION, probe::FIRST_SEQUENCE);
            let started = Instant::now();
            for datagram in &datagrams {
                let header = Header::parse(datagram).unwrap();
                std::hint::black_box(arbitrator.admit(&header, &datagram[HEADER_LEN..]));
            }
            arbitrate_only.observe(started.elapsed().as_secs_f64());
        }
    }
    assert!(counted > 0 && sink > 0);

    let ns = |seconds: f64| seconds * 1e9 / PROBES as f64;
    let share = |seconds: f64| seconds / whole.best * 100.0;
    // How repeatable the whole path's minimum was, per probe. A stage smaller
    // than this is not resolved by subtraction, whatever it prints.
    let noise = ns(whole.spread());

    println!(
        "internal path attribution, {PROBES} probes (2 ITCH messages each), \
         best of {ROUNDS} interleaved rounds\n"
    );
    let stage = |name: &str, seconds: f64| {
        // Measured against the run's own spread rather than a number written
        // here. A stage under it is not resolved by subtraction -- printing a
        // figure anyway is how `decide + encode` came to be published as
        // 1.4 ns, which reads as a cost and is really a floor.
        if ns(seconds) < noise {
            println!("  {name:<28} {:>7} ns   {:>5}", "below", "noise");
        } else {
            println!(
                "  {:<28} {:>7.1} ns   {:>5.1}%",
                name,
                ns(seconds),
                share(seconds)
            );
        }
    };
    stage("arbitrate (Mold A/B)", arbitrate_only.best);
    stage(
        "parse (ITCH framing+fields)",
        through_parse.best - arbitrate_only.best,
    );
    stage(
        "book (ladder + order map)",
        through_book.best - through_parse.best,
    );
    stage("decide + encode", whole.best - through_book.best);
    println!("  {:<28} {:>7.1} ns   100.0%", "total", ns(whole.best));
    println!(
        "\nThe whole path's two fastest rounds sat {noise:.1} ns/probe apart, \
         which is the resolution: a stage nearer that than to its own figure is \
         not separated from its neighbour."
    );
    println!(
        "Each probe carries a delete and an add, so the book does two \
         updates and the parser two messages. Divide by two for a per-message \
         figure comparable with the parser benchmark."
    );
}
