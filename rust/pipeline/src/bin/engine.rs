//! The trading system under test: three busy-polling threads, two rings.
//!
//! ```text
//! UDP ticks -> [feed: parse + book + BBO] -ring-> [strategy] -ring-> [gateway] -> TCP orders
//! ```
//!
//! Everything spins. There is no blocking call anywhere on the path, because
//! a blocking call is a wakeup, and a wakeup is tens of microseconds of
//! scheduler on a bad day -- the harness's numbers would measure the kernel's
//! mood instead of this pipeline. The cost is three cores at 100%, which is
//! what every latency-critical trading process pays on purpose.
//!
//! The strategy is deliberately trivial: an order per best-bid improvement.
//! A real signal belongs to whoever deploys this; what is being measured --
//! and what the harness prices wire-to-wire -- is everything around it.
//!
//! ```text
//! engine --feed 127.0.0.1:9701 --orders 127.0.0.1:9702
//! ```

use std::io::Write;
use std::net::{TcpStream, UdpSocket};
use t2t_feed::Parser;
use t2t_feed::synth::TRADFI;
use t2t_pipeline::affinity;
use t2t_pipeline::transport::{BusyPoll, Receiver};
use t2t_pipeline::{BAND, BboUpdate, FeedStage, OrderCommand, Strategy};

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

    // The engine busy-polls its feed: rxlat measures this against blocking,
    // and busy-poll wins on a commodity NIC by skipping the wakeup.
    let mut feed_rx = BusyPoll::new(UdpSocket::bind(&feed_address)?)?;
    let mut orders = TcpStream::connect(&orders_address)?;
    orders.set_nodelay(true)?;
    println!("engine: feed {feed_address}, orders {orders_address}");

    let (mut to_strategy, mut from_feed) = t2t_spsc::channel::<BboUpdate>(1024);
    let (mut to_gateway, mut from_strategy) = t2t_spsc::channel::<OrderCommand>(1024);

    // One core per stage, so a migration cannot cost a stage its warm caches
    // mid-burst. Reported rather than enforced: a container with a restricted
    // mask should run slower, not refuse to start.
    let cores = affinity::available();
    println!("engine: {cores} cores available");

    // Feed: the socket's only reader, and the owner of the books.
    let feed = std::thread::spawn(move || -> std::io::Result<()> {
        let _ = affinity::pin_to(0);
        let parser = t2t_feed::itch::Itch { symbols: TRADFI };
        let mut sink = FeedStage::new(TRADFI.len(), BAND);
        let mut datagram = [0_u8; 2048];
        loop {
            let Some(received) = feed_rx.recv(&mut datagram)? else {
                std::hint::spin_loop();
                continue;
            };
            // A datagram carries whole messages; a tail would be a framing
            // bug on the sender's side and shows up as a parse error here.
            if parser.parse(&datagram[..received], &mut sink).is_err() {
                eprintln!("engine: undecodable datagram dropped");
                continue;
            }
            if let Some(update) = sink.take_moved() {
                // The ring is sized for bursts; a full ring means the
                // strategy died, and spinning is the only honest option left.
                let mut item = update;
                while let Err(back) = to_strategy.try_push(item) {
                    item = back;
                    std::hint::spin_loop();
                }
            }
        }
    });

    // Strategy: one decision, no I/O.
    let strategy = std::thread::spawn(move || {
        let _ = affinity::pin_to(1 % cores);
        let mut strategy = Strategy::default();
        loop {
            let Some(update) = from_feed.try_pop() else {
                std::hint::spin_loop();
                continue;
            };
            if let Some(order) = strategy.decide(&update) {
                let mut item = order;
                while let Err(back) = to_gateway.try_push(item) {
                    item = back;
                    std::hint::spin_loop();
                }
            }
        }
    });

    // Gateway: bytes out, nothing else.
    let _ = affinity::pin_to(2 % cores);
    loop {
        let Some(order) = from_strategy.try_pop() else {
            std::hint::spin_loop();
            continue;
        };
        orders.write_all(&order.encode())?;
        if feed.is_finished() || strategy.is_finished() {
            return Ok(());
        }
    }
}
