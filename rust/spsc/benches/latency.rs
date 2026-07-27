//! One-way hop latency, which the throughput number does not reveal.
//!
//! 1.8 ns/item is what the ring sustains when both sides stream; it says
//! nothing about how long ONE item takes to cross when the consumer is
//! spinning empty -- that is a cache line changing cores plus the consumer
//! noticing, and it is the number a tick-to-trade budget actually spends.
//! Measured as ping-pong round trip over two rings, halved. The halving
//! assumes symmetric directions, which two identical rings on two identical
//! cores justify.

use std::time::Instant;

const SAMPLES: usize = 100_000;
const WARMUP: usize = 10_000;

fn main() {
    let (mut ping_tx, mut ping_rx) = t2t_spsc::channel::<Instant>(64);
    let (mut pong_tx, mut pong_rx) = t2t_spsc::channel::<Instant>(64);

    let echo = std::thread::spawn(move || {
        for _ in 0..SAMPLES + WARMUP {
            loop {
                if let Some(t) = ping_rx.try_pop() {
                    let mut item = t;
                    while let Err(back) = pong_tx.try_push(item) {
                        item = back;
                    }
                    break;
                }
                std::hint::spin_loop();
            }
        }
    });

    let mut samples = Vec::with_capacity(SAMPLES);
    for round in 0..SAMPLES + WARMUP {
        let started = Instant::now();
        let mut item = started;
        while let Err(back) = ping_tx.try_push(item) {
            item = back;
        }
        let echoed = loop {
            if let Some(t) = pong_rx.try_pop() {
                break t;
            }
            std::hint::spin_loop();
        };
        let rtt = started.elapsed();
        assert_eq!(echoed, started);
        if round >= WARMUP {
            samples.push(rtt.as_nanos() as u64 / 2);
        }
    }
    echo.join().unwrap();

    samples.sort_unstable();
    let at = |q: f64| samples[((samples.len() - 1) as f64 * q) as usize];
    println!(
        "one-way SPSC hop latency, {} samples (ping-pong / 2):",
        samples.len()
    );
    println!(
        "  min {:>6} ns   p50 {:>6} ns   p99 {:>6} ns",
        samples[0],
        at(0.5),
        at(0.99)
    );
    println!(
        "\nUnpinned threads: the tail includes every scheduler decision the OS \
         made during the run. The p50 is the honest planning number."
    );
}
