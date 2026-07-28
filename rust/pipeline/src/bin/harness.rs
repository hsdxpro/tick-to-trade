//! The counterparty, and therefore the clock.
//!
//! Tick-to-trade is measured here and only here, because this process is the
//! only place both endpoints exist: T0 immediately before the tick's UDP
//! datagram is sent, T1 when the resulting order's bytes arrive back on TCP.
//! The engine's own `send` return times prove nothing -- a send can complete
//! into a kernel buffer long before or after the wire sees it -- but bytes
//! cannot *arrive* at this socket before they were truly sent. What T1 - T0
//! contains is the whole path a real venue would price: down the harness's
//! stack, up the engine's, parse, book, decision, and the whole way back.
//!
//! Each probe is one ITCH Add that improves symbol 0's best bid by one tick,
//! which the engine's strategy answers with exactly one order. One in, one
//! out, no ambiguity about which response answers which probe.
//!
//! # Two modes, because they answer different questions
//!
//! **Closed loop** (the default) sends a probe, waits for its order, sends the
//! next. One request in flight, so the figure is service time with no queue in
//! front of it -- what a client feels when the system is otherwise idle.
//!
//! **Open loop** (`--rate`) sends on a schedule regardless of whether replies
//! keep up, and measures from the moment each probe was *due* rather than the
//! moment it left. That difference is the whole point. A closed-loop harness
//! that meets a stalled system politely stops sending, so the stall is missing
//! from its samples entirely -- it measures the system's good moods and calls
//! the average a latency. The name for that is coordinated omission, and the
//! correction is to hold the schedule and charge every probe for the time it
//! spent waiting to be sent.
//!
//! ```text
//! harness [--feed 127.0.0.1:9701] [--orders 127.0.0.1:9702] [--probes 20000]
//!         [--rate 0]
//! ```

use std::io::{ErrorKind, Read, Write};
use std::net::{TcpListener, UdpSocket};
use std::time::{Duration, Instant};
use t2t_pipeline::{ORDER_WIRE_LEN, OrderCommand, probe::Probes};

fn argument(name: &str, default: &str) -> String {
    let mut arguments = std::env::args().skip(1);
    while let Some(argument) = arguments.next() {
        if argument == name {
            return arguments.next().unwrap_or_else(|| default.to_string());
        }
    }
    default.to_string()
}

fn main() -> std::io::Result<()> {
    let feed_address = argument("--feed", "127.0.0.1:9701");
    let orders_address = argument("--orders", "127.0.0.1:9702");
    let probes: usize = argument("--probes", "20000").parse().unwrap_or(20_000);
    // Probes a second. Zero means closed loop: one in flight, no schedule.
    let rate: u64 = argument("--rate", "0").parse().unwrap_or(0);

    let listener = TcpListener::bind(&orders_address)?;
    println!("harness: awaiting the engine on {orders_address}");
    let (mut orders, _) = listener.accept()?;
    orders.set_nodelay(true)?;
    orders.set_nonblocking(true)?;

    let socket = UdpSocket::bind("127.0.0.1:0")?;
    socket.connect(&feed_address)?;

    let mut samples = Vec::with_capacity(probes);
    let mut scratch = [0_u8; ORDER_WIRE_LEN];
    let warmup = probes / 10;
    let total = probes + warmup;
    // The venue's side of the order session: acknowledge in batches, as a real
    // one does, so the engine's retain window drains and its send path is
    // exercised rather than stalled.
    const ACKNOWLEDGE_EVERY: usize = 256;

    let read_order = |orders: &mut std::net::TcpStream,
                          scratch: &mut [u8; ORDER_WIRE_LEN]|
     -> std::io::Result<bool> {
        let mut filled = 0;
        while filled < ORDER_WIRE_LEN {
            match orders.read(&mut scratch[filled..]) {
                Ok(0) => {
                    eprintln!("engine closed its order connection");
                    return Ok(false);
                }
                Ok(bytes) => filled += bytes,
                Err(e) if e.kind() == ErrorKind::WouldBlock => std::hint::spin_loop(),
                Err(e) => return Err(e),
            }
        }
        Ok(true)
    };

    if rate == 0 {
        let mut stream = Probes::new();
        for index in 0..total {
            let datagram = stream.next_datagram();
            let started = Instant::now();
            socket.send(&datagram)?;
            if !read_order(&mut orders, &mut scratch)? {
                return Ok(());
            }
            let elapsed = started.elapsed();
            // Acknowledgement is off the measured path: T1 is already taken.
            if (index + 1).is_multiple_of(ACKNOWLEDGE_EVERY) {
                let acknowledged =
                    OrderCommand::decode(&scratch).map_or(0, |order| order.client_order_id);
                orders.write_all(&acknowledged.to_le_bytes())?;
            }
            if index >= warmup {
                samples.push(elapsed.as_nanos() as u64);
            }
        }
    } else {
        let period = Duration::from_nanos(1_000_000_000 / rate);
        // The schedule is shared arithmetic rather than shared state: probe
        // `i` is due at `start + i * period` on both sides, and the engine
        // answers one order per probe in order, so the reader knows what each
        // arrival was owed without the sender telling it.
        let start = Instant::now() + Duration::from_millis(50);
        let sender = std::thread::spawn(move || -> std::io::Result<()> {
            let mut stream = Probes::new();
            for index in 0..total {
                let due = start + period * u32::try_from(index).unwrap_or(u32::MAX);
                // Spun rather than slept: a sleep rounds up to the scheduler's
                // granularity, which at these rates is the whole period.
                while Instant::now() < due {
                    std::hint::spin_loop();
                }
                socket.send(&stream.next_datagram())?;
            }
            Ok(())
        });

        for index in 0..total {
            if !read_order(&mut orders, &mut scratch)? {
                return Ok(());
            }
            let arrived = Instant::now();
            let due = start + period * u32::try_from(index).unwrap_or(u32::MAX);
            if (index + 1).is_multiple_of(ACKNOWLEDGE_EVERY) {
                let acknowledged =
                    OrderCommand::decode(&scratch).map_or(0, |order| order.client_order_id);
                orders.write_all(&acknowledged.to_le_bytes())?;
            }
            if index >= warmup {
                // From when it was due, not when it left. A probe held back
                // because the previous one had not returned still owes its
                // wait; charging only from departure is what hides a stall.
                samples.push(arrived.saturating_duration_since(due).as_nanos() as u64);
            }
        }
        sender.join().expect("probe sender panicked")?;
    }

    samples.sort_unstable();
    let at = |q: f64| samples[((samples.len() - 1) as f64 * q) as usize];
    let shape = if rate == 0 {
        "closed loop, one probe in flight".to_string()
    } else {
        format!("open loop, {rate} probes/sec, measured from when each was due")
    };
    println!(
        "\ntick-to-trade, wire to wire, {} probes (after {warmup} warmup)\n  {shape}",
        samples.len()
    );
    println!("  min   {:>9} ns", samples[0]);
    println!("  p50   {:>9} ns", at(0.50));
    println!("  p99   {:>9} ns", at(0.99));
    println!("  p99.9 {:>9} ns", at(0.999));
    println!("  max   {:>9} ns", *samples.last().unwrap());
    println!(
        "\nT0 is taken before the tick leaves this process, T1 when the order's \
         bytes arrive back, so the figure includes both network stacks and every \
         pipeline stage. Nothing the engine reports about itself is trusted."
    );
    if rate == 0 {
        println!(
            "Closed loop measures service time with nothing queued in front of \
             it. Run --rate to hold a schedule instead: the gap between the two \
             is what a stalled system costs a client who could not wait."
        );
    }
    Ok(())
}
