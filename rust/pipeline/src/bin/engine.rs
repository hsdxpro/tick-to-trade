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

use std::io::{Read, Write};
use std::net::{TcpStream, UdpSocket};
use std::time::{Duration, Instant};
use t2t_feed::Parser;
use t2t_feed::mold::{Admit, Arbitrator, HEADER_LEN, Header};
use t2t_feed::synth::TRADFI;
use t2t_pipeline::affinity;
use t2t_pipeline::session::{Action, Inbound, Session};
use t2t_pipeline::transport::{BusyPoll, Receiver};
use t2t_pipeline::{BAND, BboUpdate, FeedStage, OrderCommand, Strategy, probe};

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
        let mut arbitrator = Arbitrator::new(probe::SESSION, probe::FIRST_SEQUENCE);
        let mut datagram = [0_u8; 2048];
        loop {
            let Some(received) = feed_rx.recv(&mut datagram)? else {
                std::hint::spin_loop();
                continue;
            };
            let Ok(header) = Header::parse(&datagram[..received]) else {
                continue; // shorter than a header: not ours
            };
            let payload = &datagram[HEADER_LEN..received];
            match arbitrator.admit(&header, payload) {
                Admit::Deliver => {
                    if parser.parse(payload, &mut sink).is_err() {
                        eprintln!("engine: undecodable payload dropped");
                        continue;
                    }
                    // Delivery may have been the packet a stashed one was
                    // waiting on. Without this drain, a routine UDP reorder
                    // stranded the early packet until the stream declared
                    // itself unrecoverable -- the arbitrator was wired in,
                    // its recovery path was not.
                    arbitrator.drain_stash(|stashed| {
                        if parser.parse(stashed, &mut sink).is_err() {
                            eprintln!("engine: undecodable stashed payload dropped");
                        }
                    });
                }
                // The other line's copy of something already delivered.
                Admit::Duplicate => continue,
                // Ahead of a hole. The packet is stashed; the other line's
                // copy -- or the reordered original on this one -- fills the
                // hole and the drain above releases it. Resynchronizing here
                // would abandon the hole and drop the late packet as a
                // duplicate, turning every reorder into silent book loss.
                Admit::Gap { missing } => {
                    eprintln!("engine: {missing} message(s) in flight behind a gap");
                    continue;
                }
                // Wider than the stash. A venue-grade handler asks the rewind
                // server; this one reports and resynchronizes, because
                // inventing a recovery channel the harness does not serve
                // would be scaffolding pretending to be a feature.
                Admit::Unrecoverable { missing } => {
                    eprintln!("engine: {missing} messages lost beyond recovery");
                    arbitrator.resynchronize(header.session, header.sequence);
                    continue;
                }
                Admit::SessionChanged => {
                    eprintln!("engine: venue re-sequenced; resynchronizing");
                    arbitrator.resynchronize(header.session, header.sequence);
                    continue;
                }
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

    // Gateway: bytes out through the session, and nothing else on the fast
    // path. Acknowledgements and heartbeat timing are polled only when the
    // ring is empty -- a clock read or a socket poll per order would cost more
    // than the order does.
    let _ = affinity::pin_to(2 % cores);
    orders.set_nonblocking(true)?;
    let mut session = Session::new(
        Duration::from_secs(1),
        Duration::from_secs(5),
        Instant::now(),
    );
    /// Idle spins between polls of the inbound socket and the clock. Each
    /// spin is a few nanoseconds, so this is tens of microseconds of
    /// granularity against a heartbeat measured in seconds.
    const POLL_EVERY: u32 = 4_096;
    let mut idle = 0_u32;
    let mut inbound = [0_u8; 8];
    // TCP owes no respect to message boundaries, so an acknowledgement can
    // arrive split. Bytes accumulate until eight are held; treating a short
    // read as noise instead misaligned the stream for good, and every later
    // "acknowledgement" was garbage that freed retained slots early.
    let mut inbound_filled = 0_usize;

    loop {
        if let Some(order) = from_strategy.try_pop() {
            idle = 0;
            let Some(bytes) = session.prepare(&order) else {
                // Every retained slot is unacknowledged: sending would lose
                // the ability to resend, which is worse than not sending.
                eprintln!("engine: order window full; venue is not acknowledging");
                continue;
            };
            write_all_nonblocking(&mut orders, bytes)?;
            continue;
        }

        idle = idle.wrapping_add(1);
        if idle.is_multiple_of(POLL_EVERY) {
            // The venue acknowledges with the highest sequence it holds.
            match orders.read(&mut inbound[inbound_filled..]) {
                Ok(0) => return Ok(()), // venue closed
                Ok(bytes) => {
                    inbound_filled += bytes;
                    if inbound_filled == inbound.len() {
                        inbound_filled = 0;
                        session.received(
                            Inbound::Acknowledged(u64::from_le_bytes(inbound)),
                            Instant::now(),
                        );
                    }
                }
                Err(e) if e.kind() == std::io::ErrorKind::WouldBlock => {}
                Err(_) => return Ok(()), // venue gone; the deadline would say so later
            }
            match session.due(Instant::now()) {
                Action::Idle => {}
                Action::SendHeartbeat => session.heartbeat_sent(Instant::now()),
                Action::PeerIsDead => {
                    eprintln!("engine: venue silent past its deadline");
                    return Ok(());
                }
            }
            if feed.is_finished() || strategy.is_finished() {
                return Ok(());
            }
        }
        std::hint::spin_loop();
    }
}

/// Writes every byte to a nonblocking stream, spinning on back-pressure.
///
/// The socket is nonblocking so the gateway can poll acknowledgements without
/// a second thread; a full send buffer therefore means the venue is behind,
/// and spinning is the same answer the rest of the pipeline gives.
fn write_all_nonblocking(stream: &mut TcpStream, mut bytes: &[u8]) -> std::io::Result<()> {
    while !bytes.is_empty() {
        match stream.write(bytes) {
            Ok(0) => return Err(std::io::Error::from(std::io::ErrorKind::WriteZero)),
            Ok(written) => bytes = &bytes[written..],
            Err(e) if e.kind() == std::io::ErrorKind::WouldBlock => std::hint::spin_loop(),
            Err(e) => return Err(e),
        }
    }
    Ok(())
}
