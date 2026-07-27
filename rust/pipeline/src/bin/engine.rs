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

use std::io::{ErrorKind, Write};
use std::net::{TcpStream, UdpSocket};
use t2t_book::{Band, Books};
use t2t_feed::synth::TRADFI;
use t2t_feed::{Event, Parser, Sink};
use t2t_pipeline::{BboUpdate, OrderCommand};

/// The band the harness's ticks stay inside; see the generator's mid-walk.
const BAND: Band = Band {
    floor: (1_000_000 - 3_200) * 10_000,
    tick: 100 * 10_000,
    ticks: 5_070,
};

fn argument(name: &str, default: &str) -> String {
    let mut arguments = std::env::args().skip(1);
    while let Some(argument) = arguments.next() {
        if argument == name {
            return arguments.next().unwrap_or_else(|| default.to_string());
        }
    }
    default.to_string()
}

/// Applies parsed events and notices when symbol 0's touch moves.
struct BookSink {
    books: Books,
    touch: (i64, i64),
    moved: Option<BboUpdate>,
}

impl Sink for BookSink {
    fn accept(&mut self, event: &Event) {
        self.books.apply(event);
        let book = self.books.symbol(0);
        let bid = book.bids.best().unwrap_or((0, 0));
        let ask = book.asks.best().unwrap_or((0, 0));
        if (bid.0, ask.0) != self.touch {
            self.touch = (bid.0, ask.0);
            self.moved = Some(BboUpdate {
                symbol: 0,
                bid_price: bid.0,
                bid_qty: bid.1,
                ask_price: ask.0,
                ask_qty: ask.1,
            });
        }
    }
}

fn main() -> std::io::Result<()> {
    let feed_address = argument("--feed", "127.0.0.1:9701");
    let orders_address = argument("--orders", "127.0.0.1:9702");

    let socket = UdpSocket::bind(&feed_address)?;
    socket.set_nonblocking(true)?;
    let mut orders = TcpStream::connect(&orders_address)?;
    orders.set_nodelay(true)?;
    println!("engine: feed {feed_address}, orders {orders_address}");

    let (mut to_strategy, mut from_feed) = t2t_spsc::channel::<BboUpdate>(1024);
    let (mut to_gateway, mut from_strategy) = t2t_spsc::channel::<OrderCommand>(1024);

    // Feed: the socket's only reader, and the owner of the books.
    let feed = std::thread::spawn(move || -> std::io::Result<()> {
        let parser = t2t_feed::itch::Itch { symbols: TRADFI };
        let mut sink = BookSink {
            books: Books::new(TRADFI.len(), BAND),
            touch: (0, 0),
            moved: None,
        };
        let mut datagram = [0_u8; 2048];
        loop {
            let received = match socket.recv_from(&mut datagram) {
                Ok((bytes, _)) => bytes,
                Err(e) if e.kind() == ErrorKind::WouldBlock => {
                    std::hint::spin_loop();
                    continue;
                }
                Err(e) => return Err(e),
            };
            // A datagram carries whole messages; a tail would be a framing
            // bug on the sender's side and shows up as a parse error here.
            if parser.parse(&datagram[..received], &mut sink).is_err() {
                eprintln!("engine: undecodable datagram dropped");
                continue;
            }
            if let Some(update) = sink.moved.take() {
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

    // Strategy: one decision, no I/O. An order per best-bid improvement.
    let strategy = std::thread::spawn(move || {
        let mut last_bid = 0_i64;
        let mut next_id = 1_u64;
        loop {
            let Some(update) = from_feed.try_pop() else {
                std::hint::spin_loop();
                continue;
            };
            // Any bid price change with liquidity behind it. Change, not
            // improvement: the harness cycles its probe price inside the
            // band, and what matters is one deterministic order per probe.
            if update.bid_price != last_bid && update.bid_qty > 0 {
                let order = OrderCommand {
                    client_order_id: next_id,
                    symbol: update.symbol,
                    side: t2t_feed::Side::Ask,
                    price: update.bid_price,
                    qty: update.bid_qty.min(100_000_000),
                };
                next_id += 1;
                let mut item = order;
                while let Err(back) = to_gateway.try_push(item) {
                    item = back;
                    std::hint::spin_loop();
                }
            }
            last_bid = update.bid_price;
        }
    });

    // Gateway: bytes out, nothing else.
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
