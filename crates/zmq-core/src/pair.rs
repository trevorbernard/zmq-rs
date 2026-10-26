//! PAIR routing (RFC 31), as in libzmq's `pair_t`: one peer at a time.

use std::collections::VecDeque;
use std::task::{Context as TaskContext, Poll};

use zmq_pipe::{ReaderGone, TrySendError};
use zmtp::Message;

use crate::socket::{PeerKey, PeerList};

/// PAIR's routing. The oldest peer is the socket's one peer; any other is
/// rejected by closing its connection, as libzmq does. Once the peer's
/// session ends for good, the next peer to arrive takes its place.
#[derive(Debug, Default)]
pub struct Pair {
    active: Option<PeerKey>,
    /// Messages the last peer delivered before it went away, still to be
    /// received.
    leftover: VecDeque<Message>,
}

impl Pair {
    /// Settles which peer is the one, rejecting the others. A peer whose
    /// session has ended for good, chosen or not, hands over what it
    /// delivered and makes way for the next.
    pub fn sync(&mut self, peers: &mut PeerList) {
        if self.active.is_some_and(|key| peers.get(key).is_none()) {
            self.active = None;
        }
        let leftover = &mut self.leftover;
        let mut active = self.active;
        // Peers are oldest first, and every peer but the one is rejected,
        // so the one comes first and newcomers follow it.
        peers.retain_mut(|peer| {
            let key = peer.key();
            if active.is_some_and(|active| active != key) {
                return false;
            }
            let connection = &mut peer.connection;
            if connection.tx.is_closed() || connection.rx.is_closed() {
                while let Ok(message) = connection.rx.try_recv() {
                    leftover.push_back(message);
                }
                active = None;
                return false;
            }
            active = Some(key);
            true
        });
        self.active = active;
    }

    /// Sends `message` to the peer. Hands it back, with `cx` registered, if
    /// there is no peer or its queue is full.
    pub fn poll_send(
        &mut self,
        peers: &mut PeerList,
        message: Message,
        cx: &mut TaskContext<'_>,
    ) -> Result<(), Message> {
        let mut message = message;
        message.set_metadata(None);
        // A peer found closed is replaced, so look twice at most.
        for _ in 0..2 {
            self.sync(peers);
            let Some(peer) = self.active.and_then(|key| peers.get_mut(key)) else {
                return Err(message);
            };
            match peer.poll_send(message, cx) {
                Ok(()) => return Ok(()),
                Err(TrySendError::Full(back)) => return Err(back),
                Err(TrySendError::Closed(back)) => message = back,
            }
        }
        Err(message)
    }

    /// Whether the peer has room for a message, so that
    /// [`poll_send`](Self::poll_send) would succeed. Registers `cx` with it
    /// if not.
    pub fn poll_ready(&mut self, peers: &mut PeerList, cx: &mut TaskContext<'_>) -> Poll<()> {
        // A peer found closed is replaced, so look twice at most.
        for _ in 0..2 {
            self.sync(peers);
            let Some(peer) = self.active.and_then(|key| peers.get_mut(key)) else {
                return Poll::Pending;
            };
            match peer.poll_ready(cx) {
                Poll::Ready(Ok(())) => return Poll::Ready(()),
                Poll::Ready(Err(ReaderGone)) => {}
                Poll::Pending => return Poll::Pending,
            }
        }
        Poll::Pending
    }

    /// The next message from the peer, or `Pending` with `cx` registered.
    pub fn poll_recv(&mut self, peers: &mut PeerList, cx: &mut TaskContext<'_>) -> Poll<Message> {
        loop {
            self.sync(peers);
            if let Some(message) = self.leftover.pop_front() {
                return Poll::Ready(message);
            }
            let Some(peer) = self.active.and_then(|key| peers.get_mut(key)) else {
                return Poll::Pending;
            };
            match peer.connection.rx.poll_recv(cx) {
                Poll::Ready(Some(message)) => return Poll::Ready(message),
                // Closed and drained: the next `sync` replaces it.
                Poll::Ready(None) => {}
                Poll::Pending => return Poll::Pending,
            }
        }
    }
}

#[cfg(test)]
mod tests {
    use super::*;
    use crate::test_support::*;
    use std::sync::atomic::Ordering;
    use std::task::Waker;

    fn noop() -> TaskContext<'static> {
        TaskContext::from_waker(Waker::noop())
    }

    #[test]
    fn the_first_peer_is_the_one_and_later_ones_are_rejected() {
        let (a, mut sa) = connection(10);
        let (b, sb) = connection(10);
        let mut peers = list(vec![a, b]);
        let mut pair = Pair::default();
        pair.poll_send(&mut peers, msg("hi"), &mut noop()).unwrap();
        assert_eq!(drain(&mut sa), [msg("hi")]);
        assert!(sb.to_socket.is_closed(), "the second peer was turned away");
        assert_eq!(peers.len(), 1);

        let (c, sc) = connection(10);
        peers.push(c);
        pair.sync(&mut peers);
        assert!(sc.to_socket.is_closed(), "so is a later one");
        assert!(!sa.to_socket.is_closed());
    }

    #[test]
    fn receives_from_its_peer() {
        let (a, mut sa) = connection(10);
        let mut peers = list(vec![a]);
        let mut pair = Pair::default();
        sa.to_socket.try_send(msg("1")).unwrap();
        assert_eq!(
            pair.poll_recv(&mut peers, &mut noop()),
            Poll::Ready(msg("1"))
        );
        assert_eq!(pair.poll_recv(&mut peers, &mut noop()), Poll::Pending);
    }

    #[test]
    fn sending_waits_for_a_peer_and_for_room() {
        let mut peers = PeerList::default();
        let mut pair = Pair::default();
        assert_eq!(
            pair.poll_send(&mut peers, msg("1"), &mut noop()),
            Err(msg("1")),
            "no peer yet"
        );
        let (a, mut sa) = connection(1);
        peers.push(a);
        pair.poll_send(&mut peers, msg("1"), &mut noop()).unwrap();
        let (count, waker) = waker();
        let mut cx = TaskContext::from_waker(&waker);
        assert_eq!(pair.poll_send(&mut peers, msg("2"), &mut cx), Err(msg("2")));
        assert_eq!(drain(&mut sa), [msg("1")]);
        assert!(count.0.load(Ordering::SeqCst) > 0, "room wakes the sender");
        pair.poll_send(&mut peers, msg("2"), &mut cx).unwrap();
    }

    #[test]
    fn a_departed_peers_last_messages_arrive_before_the_next_peers() {
        let (a, sa) = connection(10);
        let mut peers = list(vec![a]);
        let mut pair = Pair::default();
        pair.sync(&mut peers);
        {
            let mut sa = sa;
            sa.to_socket.try_send(msg("last words")).unwrap();
        }
        let (b, mut sb) = connection(10);
        peers.push(b);
        sb.to_socket.try_send(msg("hello")).unwrap();
        pair.poll_send(&mut peers, msg("to the newcomer"), &mut noop())
            .unwrap();
        assert_eq!(drain(&mut sb), [msg("to the newcomer")]);
        assert_eq!(
            pair.poll_recv(&mut peers, &mut noop()),
            Poll::Ready(msg("last words"))
        );
        assert_eq!(
            pair.poll_recv(&mut peers, &mut noop()),
            Poll::Ready(msg("hello"))
        );
        assert_eq!(peers.len(), 1);
    }

    #[test]
    fn a_peer_gone_before_it_was_chosen_is_skipped() {
        let (a, sa) = connection(10);
        let (b, mut sb) = connection(10);
        let mut peers = list(vec![a, b]);
        {
            let mut sa = sa;
            sa.to_socket.try_send(msg("goodbye")).unwrap();
        }
        let mut pair = Pair::default();
        pair.poll_send(&mut peers, msg("for b"), &mut noop())
            .unwrap();
        assert_eq!(drain(&mut sb), [msg("for b")]);
        assert_eq!(
            pair.poll_recv(&mut peers, &mut noop()),
            Poll::Ready(msg("goodbye"))
        );
    }

    #[test]
    fn a_receiver_is_woken_when_its_peer_leaves() {
        let (a, sa) = connection(10);
        let mut peers = list(vec![a]);
        let mut pair = Pair::default();
        let (count, waker) = waker();
        let mut cx = TaskContext::from_waker(&waker);
        assert_eq!(pair.poll_recv(&mut peers, &mut cx), Poll::Pending);
        drop(sa);
        assert!(count.0.load(Ordering::SeqCst) > 0, "its departure wakes it");
        assert_eq!(pair.poll_recv(&mut peers, &mut cx), Poll::Pending);
        assert!(peers.is_empty());
    }
}
