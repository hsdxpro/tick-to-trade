//! Cross-thread throughput and per-op cost, against the obvious alternatives.
//!
//! Not criterion: the interesting number is a sustained cross-core stream, and
//! a custom harness makes the method visible — send N items from one thread to
//! another, both spinning, and report items per second and nanoseconds per
//! item. Three runs, worst printed alongside best, because a single number
//! from a shared machine is a guess wearing confidence.

use std::time::Instant;

const COUNT: u64 = 20_000_000;
const RUNS: usize = 3;

fn measure(name: &str, run: impl Fn() -> f64) {
    let mut per_second: Vec<f64> = (0..RUNS).map(|_| run()).collect();
    per_second.sort_by(f64::total_cmp);
    println!(
        "{name:<28} {:>12.0}/sec best   {:>12.0}/sec worst   {:>6.1} ns/item",
        per_second[RUNS - 1],
        per_second[0],
        1e9 / per_second[RUNS - 1]
    );
}

fn spsc_run() -> f64 {
    let (mut tx, mut rx) = t2t_spsc::channel::<u64>(1024);
    let started = Instant::now();
    let producer = std::thread::spawn(move || {
        for i in 0..COUNT {
            let mut item = i;
            while let Err(back) = tx.try_push(item) {
                item = back;
                std::hint::spin_loop();
            }
        }
    });
    let mut seen = 0;
    while seen < COUNT {
        if rx.try_pop().is_some() {
            seen += 1;
        } else {
            std::hint::spin_loop();
        }
    }
    producer.join().unwrap();
    COUNT as f64 / started.elapsed().as_secs_f64()
}

fn crossbeam_run() -> f64 {
    let queue = std::sync::Arc::new(crossbeam_queue::ArrayQueue::<u64>::new(1024));
    let tx = std::sync::Arc::clone(&queue);
    let started = Instant::now();
    let producer = std::thread::spawn(move || {
        for i in 0..COUNT {
            let mut item = i;
            while let Err(back) = tx.push(item) {
                item = back;
                std::hint::spin_loop();
            }
        }
    });
    let mut seen = 0;
    while seen < COUNT {
        if queue.pop().is_some() {
            seen += 1;
        } else {
            std::hint::spin_loop();
        }
    }
    producer.join().unwrap();
    COUNT as f64 / started.elapsed().as_secs_f64()
}

fn std_mpsc_run() -> f64 {
    let (tx, rx) = std::sync::mpsc::sync_channel::<u64>(1024);
    let started = Instant::now();
    let producer = std::thread::spawn(move || {
        for i in 0..COUNT {
            tx.send(i).unwrap();
        }
    });
    let mut seen = 0;
    while seen < COUNT {
        if rx.recv().is_ok() {
            seen += 1;
        }
    }
    producer.join().unwrap();
    COUNT as f64 / started.elapsed().as_secs_f64()
}

fn main() {
    println!("{COUNT} items, one producer thread to one consumer thread, both spinning\n");
    measure("t2t-spsc", spsc_run);
    measure("crossbeam ArrayQueue (MPMC)", crossbeam_run);
    measure("std sync_channel", std_mpsc_run);
    println!(
        "\ncrossbeam's ArrayQueue pays for MPMC safety it cannot give up; \
         sync_channel pays for blocking. Neither is wrong -- they answer \
         different questions, and the gap is the price of the answer."
    );
}
