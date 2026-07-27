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
//! ```text
//! harness [--feed 127.0.0.1:9701] [--orders 127.0.0.1:9702] [--probes 20000]
//! ```

use std::io::{ErrorKind, Read};
use std::net::{TcpListener, UdpSocket};
use std::time::Instant;
use t2t_pipeline::{ORDER_WIRE_LEN, probe};

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
    let mut previous: Option<u64> = None;

    for index in 0..probes + warmup {
        let price = probe::price_of(index);
        let order = index as u64 + 1;
        let datagram = probe::datagram(previous, order, price);
        previous = Some(order);

        let started = Instant::now();
        socket.send(&datagram)?;
        let mut filled = 0;
        while filled < ORDER_WIRE_LEN {
            match orders.read(&mut scratch[filled..]) {
                Ok(0) => {
                    eprintln!("engine closed its order connection");
                    return Ok(());
                }
                Ok(bytes) => filled += bytes,
                Err(e) if e.kind() == ErrorKind::WouldBlock => std::hint::spin_loop(),
                Err(e) => return Err(e),
            }
        }
        let elapsed = started.elapsed();
        if index >= warmup {
            samples.push(elapsed.as_nanos() as u64);
        }
    }

    samples.sort_unstable();
    let at = |q: f64| samples[((samples.len() - 1) as f64 * q) as usize];
    println!(
        "\ntick-to-trade, wire to wire, {} probes (after {warmup} warmup)",
        samples.len()
    );
    println!("  min {:>8} ns", samples[0]);
    println!("  p50 {:>8} ns", at(0.50));
    println!("  p99 {:>8} ns", at(0.99));
    println!("  max {:>8} ns", *samples.last().unwrap());
    println!(
        "\nT0 is taken before the tick leaves this process, T1 when the order's \
         bytes arrive back, so the figure includes both network stacks and every \
         pipeline stage. Nothing the engine reports about itself is trusted."
    );
    Ok(())
}
