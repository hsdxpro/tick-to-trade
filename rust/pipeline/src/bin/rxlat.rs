//! What the receive discipline costs: the same UDP ping-pong, received three
//! ways. This is the transport decision priced in isolation, on loopback,
//! before any kernel-bypass conversation starts.
//!
//! - **blocking**: `recv` parks the thread; the wakeup is the cost. This is
//!   what every non-latency-critical service does, and the baseline.
//! - **busy-poll**: nonblocking `recv` in a spin. Burns a core to skip the
//!   wakeup; what the engine binaries in this repository do.
//! - **io_uring** (Linux): completion-based, one pre-posted receive, reaped
//!   by polling the completion queue from userspace. The syscall leaves the
//!   hot path; the copy and the stack remain.
//!
//! Method: two UDP sockets in one process ping-pong a timestamped packet;
//! the reported figure is round trip halved. The echo side always busy-polls
//! so the variant under test is the only thing changing.
//!
//! ```text
//! rxlat [--rounds 50000]
//! ```

use std::net::UdpSocket;
use std::time::Instant;

const WARMUP: usize = 5_000;

fn report(name: &str, samples: &mut [u64]) {
    samples.sort_unstable();
    let at = |q: f64| samples[((samples.len() - 1) as f64 * q) as usize];
    println!(
        "{name:<26} min {:>6} ns   p50 {:>6} ns   p99 {:>6} ns",
        samples[0],
        at(0.5),
        at(0.99)
    );
}

/// The echo peer: busy-polls, returns every packet, stops on a zero byte.
fn echo(socket: UdpSocket) -> std::thread::JoinHandle<()> {
    socket.set_nonblocking(true).unwrap();
    std::thread::spawn(move || {
        let mut packet = [0_u8; 16];
        loop {
            match socket.recv_from(&mut packet) {
                Ok((bytes, from)) => {
                    if bytes == 1 {
                        return;
                    }
                    let _ = socket.send_to(&packet[..bytes], from);
                }
                Err(_) => std::hint::spin_loop(),
            }
        }
    })
}

fn pinger(rounds: usize) -> (UdpSocket, std::thread::JoinHandle<()>, usize) {
    let echo_socket = UdpSocket::bind("127.0.0.1:0").unwrap();
    let echo_address = echo_socket.local_addr().unwrap();
    let handle = echo(echo_socket);
    let socket = UdpSocket::bind("127.0.0.1:0").unwrap();
    socket.connect(echo_address).unwrap();
    (socket, handle, rounds + WARMUP)
}

fn blocking(rounds: usize) {
    let (socket, handle, total) = pinger(rounds);
    let mut packet = [7_u8; 16];
    let mut samples = Vec::with_capacity(rounds);
    for round in 0..total {
        let started = Instant::now();
        socket.send(&packet).unwrap();
        socket.recv(&mut packet).unwrap();
        if round >= WARMUP {
            samples.push(started.elapsed().as_nanos() as u64 / 2);
        }
    }
    socket.send(&[0]).unwrap();
    handle.join().unwrap();
    report("blocking recv", &mut samples);
}

fn busy_poll(rounds: usize) {
    let (socket, handle, total) = pinger(rounds);
    socket.set_nonblocking(true).unwrap();
    let mut packet = [7_u8; 16];
    let mut samples = Vec::with_capacity(rounds);
    for round in 0..total {
        let started = Instant::now();
        socket.send(&packet).unwrap();
        loop {
            match socket.recv(&mut packet) {
                Ok(_) => break,
                Err(_) => std::hint::spin_loop(),
            }
        }
        if round >= WARMUP {
            samples.push(started.elapsed().as_nanos() as u64 / 2);
        }
    }
    socket.send(&[0]).unwrap();
    handle.join().unwrap();
    report("busy-poll recv", &mut samples);
}

/// io_uring with a polled completion queue: the receive is posted before the
/// ping goes out, so the packet lands in a buffer the kernel already owns
/// and the hot loop never enters the kernel to ask.
#[cfg(target_os = "linux")]
fn uring(rounds: usize) {
    use io_uring::{IoUring, opcode, types};
    use std::os::fd::AsRawFd;

    let (socket, handle, total) = pinger(rounds);
    let mut ring = IoUring::new(8).unwrap();
    let mut packet = [7_u8; 16];
    let mut inbox = [0_u8; 16];
    let mut samples = Vec::with_capacity(rounds);

    for round in 0..total {
        let receive = opcode::Recv::new(
            types::Fd(socket.as_raw_fd()),
            inbox.as_mut_ptr(),
            inbox.len() as u32,
        )
        .build();
        // Sound: `inbox` outlives the submission, and the completion is
        // reaped before the next iteration touches it.
        unsafe { ring.submission().push(&receive).unwrap() };
        ring.submit().unwrap();

        let started = Instant::now();
        socket.send(&packet).unwrap();
        loop {
            if let Some(completion) = ring.completion().next() {
                assert!(completion.result() > 0, "recv failed in the ring");
                break;
            }
            std::hint::spin_loop();
        }
        if round >= WARMUP {
            samples.push(started.elapsed().as_nanos() as u64 / 2);
        }
        packet[0] = packet[0].wrapping_add(1);
    }
    socket.send(&[0]).unwrap();
    handle.join().unwrap();
    report("io_uring recv (polled CQ)", &mut samples);
}

fn main() {
    let rounds: usize = std::env::args()
        .skip_while(|argument| argument != "--rounds")
        .nth(1)
        .and_then(|value| value.parse().ok())
        .unwrap_or(50_000);

    println!("UDP receive discipline, one-way (ping-pong / 2), {rounds} rounds\n");
    blocking(rounds);
    busy_poll(rounds);
    #[cfg(target_os = "linux")]
    uring(rounds);
    #[cfg(not(target_os = "linux"))]
    println!("io_uring recv               (Linux only; run this binary there)");
}
