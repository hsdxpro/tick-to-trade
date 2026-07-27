//! Order entry as a session, not a socket: sequenced, heartbeated, resendable.
//!
//! An order stream needs three things a raw socket does not give it. Every
//! message must be numbered, so a reconnecting client and venue can agree on
//! what was received. Silence must be distinguishable from death, so a
//! heartbeat interval exists. And a gap must be fillable, so sent messages
//! are retained until acknowledged.
//!
//! This is what FIX's session layer and OUCH-over-SoupBinTCP both do; the
//! shapes differ, the obligations do not.
//!
//! # None of it is on the hot path
//!
//! Sending an order is: stamp the next sequence, copy 32 bytes into a retain
//! ring, write. No allocation, no scan, no clock read — the clock is read by
//! whoever drives the heartbeat, which is the same loop that was already
//! spinning. Acknowledgement advances an index; retransmission walks the ring
//! and is by definition not the fast path.

use crate::{ORDER_WIRE_LEN, OrderCommand};
use std::time::{Duration, Instant};

/// Messages retained for retransmission.
///
/// One power-of-two ring, so the mask replaces a modulo. A gap wider than this
/// means the peer is far enough behind that resending is the wrong answer —
/// the session is torn down and re-established, which is what real venues do
/// rather than replay unbounded history.
const RETAIN: usize = 4_096;

/// What arrived from the venue.
#[derive(Clone, Copy, Debug, Eq, PartialEq)]
pub enum Inbound {
    /// The venue has everything up to and including this sequence.
    Acknowledged(u64),
    /// The venue is missing from this sequence onward.
    ResendFrom(u64),
    /// Proof of life, nothing more.
    Heartbeat,
}

/// What the session decided the caller should do now.
#[derive(Clone, Copy, Debug, Eq, PartialEq)]
pub enum Action {
    /// Nothing is due.
    Idle,
    /// Send a heartbeat: the peer has heard nothing for an interval.
    SendHeartbeat,
    /// The peer has been silent past its deadline. It is gone; reconnect.
    PeerIsDead,
}

/// An order-entry session over a stream that is already connected.
pub struct Session {
    next_sequence: u64,
    acknowledged: u64,
    retained: Box<[[u8; ORDER_WIRE_LEN]]>,
    /// Interval after which silence from us is filled with a heartbeat.
    heartbeat_every: Duration,
    /// Silence from the peer that means it is gone. Conventionally a small
    /// multiple of the interval, so one lost heartbeat is not a funeral.
    peer_timeout: Duration,
    last_sent: Instant,
    last_heard: Instant,
    /// Something went out since the last poll; see [`Session::prepare`].
    sent_since_poll: bool,
    heartbeats_sent: u64,
    resends: u64,
}

impl std::fmt::Debug for Session {
    fn fmt(&self, f: &mut std::fmt::Formatter<'_>) -> std::fmt::Result {
        f.debug_struct("Session")
            .field("next_sequence", &self.next_sequence)
            .field("acknowledged", &self.acknowledged)
            .field("in_flight", &self.in_flight())
            .field("heartbeats_sent", &self.heartbeats_sent)
            .field("resends", &self.resends)
            .finish()
    }
}

impl Session {
    #[must_use]
    pub fn new(heartbeat_every: Duration, peer_timeout: Duration, now: Instant) -> Self {
        Self {
            next_sequence: 1,
            acknowledged: 0,
            retained: vec![[0_u8; ORDER_WIRE_LEN]; RETAIN].into_boxed_slice(),
            heartbeat_every,
            peer_timeout,
            last_sent: now,
            last_heard: now,
            sent_since_poll: false,
            heartbeats_sent: 0,
            resends: 0,
        }
    }

    /// Stamps and retains an order, returning the bytes to write.
    ///
    /// **No clock read.** A send resets the heartbeat timer, but a heartbeat
    /// interval is measured in seconds and `Instant::now` costs tens of
    /// nanoseconds on the order path — a bad trade at any interval. So a send
    /// only raises a flag, and [`Self::due`], which reads the clock anyway
    /// because it must, folds the flag in. The heartbeat can therefore be
    /// postponed by at most one poll interval, which is microseconds against
    /// an interval of seconds, and it errs toward *not* sending a redundant
    /// heartbeat rather than toward missing a needed one.
    ///
    /// `None` when too many are unacknowledged: the ring would overwrite a
    /// message the venue may still ask for, and losing the ability to resend
    /// is worse than refusing to send. The caller sheds or waits.
    #[inline]
    pub fn prepare(&mut self, order: &OrderCommand) -> Option<&[u8; ORDER_WIRE_LEN]> {
        if self.in_flight() >= RETAIN as u64 {
            return None;
        }
        let sequence = self.next_sequence;
        let slot = (sequence as usize) & (RETAIN - 1);
        // The sequence rides in the client order id: the venue echoes it back
        // on acknowledgement, so no extra wire field is needed.
        let mut stamped = *order;
        stamped.client_order_id = sequence;
        self.retained[slot] = stamped.encode();
        self.next_sequence += 1;
        self.sent_since_poll = true;
        Some(&self.retained[slot])
    }

    /// Applies something the venue said.
    pub fn received(&mut self, inbound: Inbound, now: Instant) {
        self.last_heard = now;
        match inbound {
            Inbound::Acknowledged(sequence) => {
                // Monotonic: a stale acknowledgement never rewinds the window.
                self.acknowledged = self.acknowledged.max(sequence);
            }
            Inbound::ResendFrom(_) | Inbound::Heartbeat => {}
        }
    }

    /// Messages the venue has not confirmed.
    #[must_use]
    pub const fn in_flight(&self) -> u64 {
        self.next_sequence - 1 - self.acknowledged
    }

    /// What is due, given the time. Called from the idle path, which is where
    /// the clock read belongs: two comparisons, and the folding-in of any send
    /// that happened since the last poll.
    pub fn due(&mut self, now: Instant) -> Action {
        if self.sent_since_poll {
            self.sent_since_poll = false;
            self.last_sent = now;
        }
        if now.duration_since(self.last_heard) >= self.peer_timeout {
            return Action::PeerIsDead;
        }
        if now.duration_since(self.last_sent) >= self.heartbeat_every {
            return Action::SendHeartbeat;
        }
        Action::Idle
    }

    /// Records that a heartbeat went out, resetting the send timer.
    pub fn heartbeat_sent(&mut self, now: Instant) {
        self.last_sent = now;
        self.heartbeats_sent += 1;
    }

    /// Replays retained messages from `sequence` onward.
    ///
    /// `false` when the request reaches past what is retained: the session
    /// cannot honour it and must be torn down rather than answer with a hole.
    pub fn resend_from(
        &mut self,
        sequence: u64,
        mut write: impl FnMut(&[u8; ORDER_WIRE_LEN]),
    ) -> bool {
        if sequence <= self.acknowledged || sequence >= self.next_sequence {
            return sequence >= self.next_sequence; // nothing to do is success
        }
        if self.next_sequence - sequence > RETAIN as u64 {
            return false;
        }
        for held in sequence..self.next_sequence {
            write(&self.retained[(held as usize) & (RETAIN - 1)]);
            self.resends += 1;
        }
        true
    }

    #[must_use]
    pub const fn heartbeats_sent(&self) -> u64 {
        self.heartbeats_sent
    }
    #[must_use]
    pub const fn resends(&self) -> u64 {
        self.resends
    }
}

#[cfg(test)]
mod tests {
    use super::*;
    use t2t_feed::Side;

    fn order() -> OrderCommand {
        OrderCommand {
            client_order_id: 0,
            symbol: 1,
            side: Side::Bid,
            price: 1_000,
            qty: 10,
        }
    }

    fn session(now: Instant) -> Session {
        Session::new(Duration::from_millis(100), Duration::from_millis(500), now)
    }

    #[test]
    fn sequences_start_at_one_and_advance() {
        let now = Instant::now();
        let mut session = session(now);
        for expected in 1..=5_u64 {
            let bytes = *session.prepare(&order()).unwrap();
            let decoded = OrderCommand::decode(&bytes).unwrap();
            assert_eq!(decoded.client_order_id, expected);
        }
        assert_eq!(session.in_flight(), 5);
    }

    #[test]
    fn acknowledgement_clears_the_window_and_never_rewinds() {
        let now = Instant::now();
        let mut session = session(now);
        for _ in 0..10 {
            session.prepare(&order()).unwrap();
        }
        session.received(Inbound::Acknowledged(7), now);
        assert_eq!(session.in_flight(), 3);
        session.received(Inbound::Acknowledged(4), now); // stale
        assert_eq!(session.in_flight(), 3, "a stale ack rewound the window");
    }

    #[test]
    fn a_resend_replays_exactly_the_requested_range_in_order() {
        let now = Instant::now();
        let mut session = session(now);
        for _ in 0..10 {
            session.prepare(&order()).unwrap();
        }
        let mut replayed = Vec::new();
        assert!(session.resend_from(4, |bytes| {
            replayed.push(OrderCommand::decode(bytes).unwrap().client_order_id);
        }));
        assert_eq!(replayed, vec![4, 5, 6, 7, 8, 9, 10]);
    }

    #[test]
    fn a_resend_past_what_is_retained_is_refused_not_answered_with_a_hole() {
        let now = Instant::now();
        let mut session = session(now);
        for _ in 0..(RETAIN + 10) {
            if session.prepare(&order()).is_none() {
                session.received(Inbound::Acknowledged(session.next_sequence - 1), now);
                session.prepare(&order()).unwrap();
            }
        }
        assert!(
            !session.resend_from(1, |_| {}),
            "the session claimed to resend history it had overwritten"
        );
    }

    #[test]
    fn the_window_refuses_rather_than_overwriting_an_unacknowledged_message() {
        let now = Instant::now();
        let mut session = session(now);
        for _ in 0..RETAIN {
            assert!(session.prepare(&order()).is_some());
        }
        assert!(
            session.prepare(&order()).is_none(),
            "an unacknowledged message was about to be overwritten"
        );
        session.received(Inbound::Acknowledged(1), now);
        assert!(session.prepare(&order()).is_some());
    }

    #[test]
    fn silence_produces_a_heartbeat_then_declares_the_peer_gone() {
        let start = Instant::now();
        let mut session = session(start);
        assert_eq!(session.due(start), Action::Idle);
        assert_eq!(
            session.due(start + Duration::from_millis(150)),
            Action::SendHeartbeat
        );
        session.heartbeat_sent(start + Duration::from_millis(150));
        assert_eq!(
            session.due(start + Duration::from_millis(200)),
            Action::Idle,
            "the heartbeat did not reset the send timer"
        );
        // The peer's silence is measured separately from ours.
        assert_eq!(
            session.due(start + Duration::from_millis(600)),
            Action::PeerIsDead
        );
    }

    #[test]
    fn hearing_from_the_peer_postpones_declaring_it_gone() {
        let start = Instant::now();
        let mut session = session(start);
        session.received(Inbound::Heartbeat, start + Duration::from_millis(400));
        assert_ne!(
            session.due(start + Duration::from_millis(600)),
            Action::PeerIsDead,
            "a heartbeat from the peer was not counted as life"
        );
    }
}
