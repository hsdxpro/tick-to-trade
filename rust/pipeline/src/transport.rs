//! Receive transports behind one trait, so a stage takes bytes and never
//! learns where they came from -- and so the rxlat benchmark measures each
//! the same way. Two are real and measured; two are documented seams.
//!
//! # What runs
//!
//! - [`Blocking`] and [`BusyPoll`]: the two disciplines every commodity-NIC
//!   deployment actually chooses between. Both are measured by `rxlat` on
//!   Windows and Linux, and the gap between them is what busy-polling a core
//!   buys.
//! - **io_uring** (Linux): a completion-based receive with a userspace-polled
//!   queue. Measured by `rxlat`. The finding is worth stating plainly: at
//!   one-packet-in-flight ping-pong it does *not* beat busy-poll, because its
//!   advantage is amortizing syscalls across a batch and there is no batch
//!   here. It pulls ahead when many receives are in flight, which a real feed
//!   handler has and a latency probe does not.
//!
//! # What is a documented seam, and why
//!
//! - **AF_XDP**: a kernel-bypass path that still works on an ordinary NIC via
//!   the generic XDP hook -- the honest "fastest without special hardware".
//!   It needs a network interface bound to an XDP program, which a loopback
//!   probe cannot provide and CI cannot assume, so the shape is written in
//!   [`af_xdp`] and the measurement waits for a host with a spare interface.
//! - **DPDK**: full kernel bypass, poll-mode driver, hugepages, a NIC bound
//!   away from the kernel. [`dpdk`] writes the receive loop against the DPDK
//!   API so the shape is real and reviewable, but it is never compiled in
//!   this repository's builds and never measured, because a DPDK number from
//!   a machine without a DPDK-bound NIC would be a fabrication. It is here to
//!   show the interface is one poll-mode driver away, not to claim a figure.

use std::io;

/// A source of whole datagrams. `recv` returns the bytes received into `buf`,
/// or `None` when nothing is ready -- a busy-polling caller spins on `None`,
/// a blocking one never sees it.
pub trait Receiver {
    /// # Errors
    /// Propagates a genuine socket failure. Not-ready is `Ok(None)`, not an
    /// error, because on the hot path "nothing yet" is the common case and
    /// must not cost an error construction.
    fn recv(&mut self, buf: &mut [u8]) -> io::Result<Option<usize>>;
}

/// Parks the thread until a datagram arrives. The wakeup is the cost.
#[derive(Debug)]
pub struct Blocking(pub std::net::UdpSocket);

impl Receiver for Blocking {
    fn recv(&mut self, buf: &mut [u8]) -> io::Result<Option<usize>> {
        self.0.recv(buf).map(Some)
    }
}

/// Spins on a nonblocking socket. Burns a core to skip the wakeup.
#[derive(Debug)]
pub struct BusyPoll(pub std::net::UdpSocket);

impl BusyPoll {
    /// # Errors
    /// Fails if the socket cannot be put into nonblocking mode.
    pub fn new(socket: std::net::UdpSocket) -> io::Result<Self> {
        socket.set_nonblocking(true)?;
        Ok(Self(socket))
    }
}

impl Receiver for BusyPoll {
    fn recv(&mut self, buf: &mut [u8]) -> io::Result<Option<usize>> {
        match self.0.recv(buf) {
            Ok(bytes) => Ok(Some(bytes)),
            Err(e) if e.kind() == io::ErrorKind::WouldBlock => Ok(None),
            Err(e) => Err(e),
        }
    }
}

pub mod af_xdp {
    //! AF_XDP: kernel bypass on a commodity NIC through the generic XDP hook.
    //!
    //! The measured path once a host has an interface to bind. The receive
    //! side is a poll of an `XSK` socket's RX ring; the shape is the same
    //! `Receiver` as the others, so the pipeline above does not change.
    //!
    //! Sketch, not a build target. A real implementation binds an `AF_XDP`
    //! socket to `(interface, queue)`, shares a UMEM frame pool with the
    //! kernel, and receives by reaping the RX ring:
    //!
    //! ```ignore
    //! let umem = Umem::new(frame_count, frame_size)?;
    //! let mut xsk = XskSocket::new(&umem, "eth0", queue_id, XdpFlags::GENERIC)?;
    //! xsk.fill_ring().reserve(n).submit();          // hand the kernel frames
    //! loop {
    //!     for frame in xsk.rx_ring().poll() {       // no syscall on the hot path
    //!         handle(umem.frame(frame));            // zero-copy into the UMEM
    //!         xsk.fill_ring().give_back(frame);
    //!     }
    //! }
    //! ```
    //!
    //! `XdpFlags::GENERIC` is the "works on any NIC" mode -- slower than a
    //! native-driver XDP program but requiring no special hardware, which is
    //! the deployment this repository targets. Wiring it needs a spare
    //! interface the loopback probe and CI cannot supply, so it stays a
    //! documented seam with a measured busy-poll baseline to beat.
}

#[cfg(feature = "dpdk")]
pub mod dpdk {
    //! DPDK: full kernel bypass, written to be reviewable, never compiled.
    //!
    //! Behind `--features dpdk`, which no build in this repository enables,
    //! because a DPDK figure from a machine with no DPDK-bound NIC would be
    //! invented and this repository does not invent figures.
    //!
    //! Compiled only with `--features dpdk` against a real DPDK install and a
    //! NIC bound to a poll-mode driver. The receive is `rte_eth_rx_burst`,
    //! which returns a batch of packets straight from the NIC's DMA ring with
    //! no kernel involvement at all -- the reason DPDK exists.
    //!
    //! ```ignore
    //! let mut packets: [*mut rte_mbuf; BURST] = [null_mut(); BURST];
    //! loop {
    //!     let received = rte_eth_rx_burst(port, queue, packets.as_mut_ptr(), BURST);
    //!     for &packet in &packets[..received as usize] {
    //!         handle(mbuf_bytes(packet));
    //!         rte_pktmbuf_free(packet);
    //!     }
    //! }
    //! ```
    //!
    //! The burst is where DPDK wins: one call drains many packets, so the
    //! per-packet cost of asking the NIC approaches zero -- the amortization
    //! io_uring reaches for and hardware bypass completes.
}
