//! MoldUDP64 framing and A/B line arbitration — how ITCH actually arrives.
//!
//! Real feeds are sequenced datagrams sent twice down independent paths. The
//! handler's job is to deliver each sequence exactly once, in order, from
//! whichever line got there first, and to notice when both lost the same
//! packet.
//!
//! # The fast path is one compare
//!
//! No reorder buffer is consulted when things are going well, and that is a
//! consequence of the redundancy rather than a shortcut: if line A drops a
//! packet, line B's copy arrives *in sequence*, so the common failure heals
//! without any bookkeeping. Duplicates are the other line's copy of something
//! already delivered — one comparison rejects them. Only a packet arriving
//! *ahead* of expectation means both lines lost the same data, and only then
//! does anything else run.
//!
//! ```text
//! sequence == expected   deliver          (the overwhelming case: 1 compare)
//! sequence <  expected   duplicate, drop  (the other line, or a retransmit)
//! sequence >  expected   gap: stash, recover
//! ```
//!
//! The stash is a fixed ring touched only while a gap is open. Nothing is
//! allocated after construction, and nothing is scanned when the stream is
//! healthy.

use crate::FeedError;

/// MoldUDP64 header: session, sequence, message count.
///
/// The session field is carried and checked but never interpreted — a change
/// means the venue restarted its sequencing and the handler must resynchronize
/// rather than reason about numbers from two different sessions.
pub const HEADER_LEN: usize = 10 + 8 + 2;

/// A count of `0xFFFF` marks end of session in MoldUDP64.
pub const END_OF_SESSION: u16 = 0xFFFF;

#[derive(Clone, Copy, Debug, Eq, PartialEq)]
pub struct Header {
    pub session: [u8; 10],
    pub sequence: u64,
    pub count: u16,
}

impl Header {
    /// # Errors
    /// [`FeedError::NeedMore`] if the datagram is shorter than a header.
    pub fn parse(bytes: &[u8]) -> Result<Self, FeedError> {
        if bytes.len() < HEADER_LEN {
            return Err(FeedError::NeedMore);
        }
        let mut session = [0_u8; 10];
        session.copy_from_slice(&bytes[..10]);
        Ok(Self {
            session,
            sequence: u64::from_be_bytes(bytes[10..18].try_into().unwrap_or_default()),
            count: u16::from_be_bytes([bytes[18], bytes[19]]),
        })
    }

    #[must_use]
    pub fn encode(&self) -> [u8; HEADER_LEN] {
        let mut out = [0_u8; HEADER_LEN];
        out[..10].copy_from_slice(&self.session);
        out[10..18].copy_from_slice(&self.sequence.to_be_bytes());
        out[18..].copy_from_slice(&self.count.to_be_bytes());
        out
    }
}

/// What the arbitrator decided about a packet.
#[derive(Clone, Copy, Debug, Eq, PartialEq)]
pub enum Admit {
    /// In sequence. Parse the payload.
    Deliver,
    /// Already seen on the other line, or a retransmit that raced. Drop it.
    Duplicate,
    /// Ahead of expectation: both lines lost `missing` packets. The payload
    /// has been stashed; recovery must fill the hole.
    Gap { missing: u64 },
    /// Ahead, and the stash is full — the hole is wider than the handler can
    /// hold. Only a snapshot resynchronizes from here.
    Unrecoverable { missing: u64 },
    /// The venue re-sequenced. Everything held is meaningless.
    SessionChanged,
}

/// Packets stashed while a gap is open. Sized for the reordering a redundant
/// feed actually produces; wider holes mean a snapshot, not more memory.
const STASH: usize = 64;
/// Largest payload a stashed packet may carry, matching a jumbo-free MTU.
const STASH_PAYLOAD: usize = 1_500;

/// Delivers a sequenced feed exactly once, in order, from any number of lines.
pub struct Arbitrator {
    session: [u8; 10],
    expected: u64,
    /// Ring of stashed payloads, indexed by `sequence % STASH`.
    stash: Box<[[u8; STASH_PAYLOAD]]>,
    stash_len: [u16; STASH],
    stash_held: [bool; STASH],
    delivered: u64,
    duplicates: u64,
    gaps: u64,
    recovered: u64,
}

impl std::fmt::Debug for Arbitrator {
    fn fmt(&self, f: &mut std::fmt::Formatter<'_>) -> std::fmt::Result {
        f.debug_struct("Arbitrator")
            .field("expected", &self.expected)
            .field("delivered", &self.delivered)
            .field("duplicates", &self.duplicates)
            .field("gaps", &self.gaps)
            .field("recovered", &self.recovered)
            .finish()
    }
}

impl Arbitrator {
    #[must_use]
    pub fn new(session: [u8; 10], first_sequence: u64) -> Self {
        Self {
            session,
            expected: first_sequence,
            stash: vec![[0_u8; STASH_PAYLOAD]; STASH].into_boxed_slice(),
            stash_len: [0; STASH],
            stash_held: [false; STASH],
            delivered: 0,
            duplicates: 0,
            gaps: 0,
            recovered: 0,
        }
    }

    /// Offers a packet. The hot path — in-sequence delivery — is one
    /// comparison and one add; nothing else is touched.
    #[inline]
    pub fn admit(&mut self, header: &Header, payload: &[u8]) -> Admit {
        if header.sequence == self.expected {
            if header.session != self.session {
                return Admit::SessionChanged;
            }
            self.expected += u64::from(header.count);
            self.delivered += 1;
            return Admit::Deliver;
        }
        self.admit_slow(header, payload)
    }

    /// Everything that is not in-sequence delivery, kept out of line so the
    /// common case stays a compare and a branch.
    #[cold]
    fn admit_slow(&mut self, header: &Header, payload: &[u8]) -> Admit {
        if header.session != self.session {
            return Admit::SessionChanged;
        }
        if header.sequence < self.expected {
            self.duplicates += 1;
            return Admit::Duplicate;
        }
        let missing = header.sequence - self.expected;
        self.gaps += 1;
        if missing >= STASH as u64 || payload.len() > STASH_PAYLOAD {
            return Admit::Unrecoverable { missing };
        }
        let slot = (header.sequence % STASH as u64) as usize;
        self.stash[slot][..payload.len()].copy_from_slice(payload);
        self.stash_len[slot] = payload.len() as u16;
        self.stash_held[slot] = true;
        Admit::Gap { missing }
    }

    /// After recovery has supplied the missing packets, drains whatever was
    /// stashed and is now in sequence. Returns each payload in order.
    ///
    /// Called only when a gap was open, so the walk costs nothing in a healthy
    /// stream.
    pub fn drain_stash(&mut self, mut deliver: impl FnMut(&[u8])) {
        loop {
            let slot = (self.expected % STASH as u64) as usize;
            if !self.stash_held[slot] {
                return;
            }
            let length = self.stash_len[slot] as usize;
            deliver(&self.stash[slot][..length]);
            self.stash_held[slot] = false;
            self.recovered += 1;
            self.delivered += 1;
            // A stashed packet's own count is not known here; MoldUDP64
            // recovery replays one sequence at a time, so advancing by one is
            // the contract.
            self.expected += 1;
        }
    }

    /// Accepts a recovered packet, filling the hole from the front.
    pub fn recover(&mut self, sequence: u64, count: u16) {
        if sequence == self.expected {
            self.expected += u64::from(count);
            self.recovered += 1;
        }
    }

    /// Resynchronizes to a snapshot: a new session, a new starting sequence,
    /// nothing held.
    pub fn resynchronize(&mut self, session: [u8; 10], sequence: u64) {
        self.session = session;
        self.expected = sequence;
        self.stash_held = [false; STASH];
    }

    #[must_use]
    pub const fn expected(&self) -> u64 {
        self.expected
    }
    #[must_use]
    pub const fn delivered(&self) -> u64 {
        self.delivered
    }
    #[must_use]
    pub const fn duplicates(&self) -> u64 {
        self.duplicates
    }
    #[must_use]
    pub const fn gaps(&self) -> u64 {
        self.gaps
    }
    #[must_use]
    pub const fn recovered(&self) -> u64 {
        self.recovered
    }
}

#[cfg(test)]
mod tests {
    use super::*;

    const SESSION: [u8; 10] = *b"SESSION001";

    fn header(sequence: u64, count: u16) -> Header {
        Header {
            session: SESSION,
            sequence,
            count,
        }
    }

    #[test]
    fn headers_roundtrip() {
        let original = header(12_345, 7);
        assert_eq!(Header::parse(&original.encode()), Ok(original));
    }

    #[test]
    fn a_short_datagram_needs_more_rather_than_panicking() {
        assert_eq!(Header::parse(&[0; 5]), Err(FeedError::NeedMore));
    }

    #[test]
    fn in_sequence_packets_deliver_and_advance_by_their_count() {
        let mut arbitrator = Arbitrator::new(SESSION, 1);
        assert_eq!(arbitrator.admit(&header(1, 3), &[]), Admit::Deliver);
        assert_eq!(arbitrator.expected(), 4);
        assert_eq!(arbitrator.admit(&header(4, 1), &[]), Admit::Deliver);
        assert_eq!(arbitrator.expected(), 5);
        assert_eq!(arbitrator.duplicates(), 0);
    }

    /// The whole point of two lines: B's copy of a packet A already delivered
    /// is rejected, and B's copy of a packet A *dropped* is delivered.
    #[test]
    fn the_second_line_fills_what_the_first_dropped_and_repeats_nothing() {
        let mut arbitrator = Arbitrator::new(SESSION, 1);
        // A delivers 1 and 2; B's copies are duplicates.
        assert_eq!(arbitrator.admit(&header(1, 1), &[]), Admit::Deliver);
        assert_eq!(arbitrator.admit(&header(1, 1), &[]), Admit::Duplicate);
        assert_eq!(arbitrator.admit(&header(2, 1), &[]), Admit::Deliver);
        assert_eq!(arbitrator.admit(&header(2, 1), &[]), Admit::Duplicate);
        // A drops 3. B has it, and it arrives exactly in sequence.
        assert_eq!(arbitrator.admit(&header(3, 1), &[]), Admit::Deliver);
        assert_eq!(arbitrator.expected(), 4);
        assert_eq!(arbitrator.duplicates(), 2);
        assert_eq!(arbitrator.gaps(), 0, "a healed line is not a gap");
    }

    #[test]
    fn a_hole_both_lines_lost_is_a_gap_and_the_ahead_packet_is_stashed() {
        let mut arbitrator = Arbitrator::new(SESSION, 1);
        assert_eq!(arbitrator.admit(&header(1, 1), &[]), Admit::Deliver);
        // 2 is lost on both lines; 3 arrives.
        assert_eq!(
            arbitrator.admit(&header(3, 1), b"three"),
            Admit::Gap { missing: 1 }
        );
        assert_eq!(arbitrator.expected(), 2, "the hole stays open");

        // Recovery supplies 2, then the stash releases 3 in order.
        arbitrator.recover(2, 1);
        let mut delivered = Vec::new();
        arbitrator.drain_stash(|payload| delivered.push(payload.to_vec()));
        assert_eq!(delivered, vec![b"three".to_vec()]);
        assert_eq!(arbitrator.expected(), 4);
    }

    #[test]
    fn a_hole_wider_than_the_stash_demands_a_snapshot() {
        let mut arbitrator = Arbitrator::new(SESSION, 1);
        match arbitrator.admit(&header(10_000, 1), &[]) {
            Admit::Unrecoverable { missing } => assert_eq!(missing, 9_999),
            other => panic!("expected an unrecoverable gap, got {other:?}"),
        }
        arbitrator.resynchronize(SESSION, 10_000);
        assert_eq!(arbitrator.admit(&header(10_000, 1), &[]), Admit::Deliver);
    }

    #[test]
    fn a_new_session_is_refused_rather_than_reasoned_about() {
        let mut arbitrator = Arbitrator::new(SESSION, 1);
        let mut other = header(1, 1);
        other.session = *b"SESSION002";
        assert_eq!(arbitrator.admit(&other, &[]), Admit::SessionChanged);
    }

    /// Randomized: two lines, independent loss, occasional double loss. Every
    /// sequence must be delivered exactly once and in order.
    #[test]
    fn two_lossy_lines_deliver_every_sequence_exactly_once() {
        let mut rng = crate::Rng(0x5eed_0000_a1b2_c3d4);
        let mut arbitrator = Arbitrator::new(SESSION, 1);
        let mut seen = Vec::new();
        let mut sequence = 1_u64;

        for _ in 0..20_000 {
            let lost_on_a = rng.below(100) < 20;
            let lost_on_b = rng.below(100) < 20;
            let payload = sequence.to_be_bytes();

            // Line A, then line B, in whichever order the network chose.
            let mut lines = [!lost_on_a, !lost_on_b];
            if rng.below(2) == 0 {
                lines.swap(0, 1);
            }
            let mut delivered_this_sequence = false;
            for arrived in lines {
                if !arrived {
                    continue;
                }
                match arbitrator.admit(&header(sequence, 1), &payload) {
                    Admit::Deliver => {
                        assert!(!delivered_this_sequence, "delivered twice");
                        delivered_this_sequence = true;
                        seen.push(sequence);
                    }
                    Admit::Duplicate => {}
                    other => panic!("unexpected {other:?} with no gaps possible"),
                }
            }
            if lost_on_a && lost_on_b {
                // Both lines lost it: a real gap. Recovery supplies it.
                arbitrator.recover(sequence, 1);
                seen.push(sequence);
            }
            sequence += 1;
        }

        assert_eq!(seen.len(), 20_000, "a sequence was delivered twice or lost");
        assert!(
            seen.windows(2).all(|pair| pair[1] == pair[0] + 1),
            "sequences arrived out of order"
        );
        assert_eq!(arbitrator.expected(), sequence);
        assert!(
            arbitrator.duplicates() > 1_000,
            "the test never exercised the duplicate path"
        );
    }
}
