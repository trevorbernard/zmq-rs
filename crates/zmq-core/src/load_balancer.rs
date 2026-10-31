//! The routing strategy of RFC 28-31 for sends: to the next peer with room.

use std::task::{Context as TaskContext, Poll};

use zmq_pipe::TrySendError;
use zmtp::Message;

use crate::peers::{PeerKey, PeerList};

/// Round robin over the peers that have room, like libzmq's `lb_t`.
#[derive(Debug, Default)]
pub struct LoadBalancer {
    next: usize,
    /// For a socket that never receives: see [`send_only`](Self::send_only).
    discards_input: bool,
}

impl LoadBalancer {
    /// A balancer for a socket that never receives, such as PUSH. As each
    /// peer's turn comes, what it sent is discarded, and it is dropped once
    /// its session is gone. Round robin brings every peer's turn in time,
    /// at the cost of a look at each peer passed over, not at every peer.
    pub fn send_only() -> Self {
        Self {
            discards_input: true,
            ..Self::default()
        }
    }

    /// Sends `message` to the next peer with room and returns that peer.
    /// Hands the message back, with `cx` registered on every peer, if none
    /// has room or there are no peers.
    pub fn poll_send(
        &mut self,
        peers: &mut PeerList,
        mut message: Message,
        cx: &mut TaskContext<'_>,
    ) -> Result<PeerKey, Message> {
        message.set_metadata(None);
        let count = peers.peers.len();
        let mut gone = Vec::new();
        for offset in 0..count {
            let index = (self.next + offset) % count;
            let peer = &mut peers.peers[index];
            if self.discards_input && !peer.discard_input() {
                gone.push(peer.key);
                continue;
            }
            // Closed peers are skipped; full ones register `cx`.
            match peer.poll_send(message, cx) {
                Ok(()) => {
                    self.next = (index + 1) % count;
                    let key = peer.key;
                    peers.remove_finished(gone);
                    return Ok(key);
                }
                Err(TrySendError::Full(back)) => message = back,
                Err(TrySendError::Closed(back)) => {
                    gone.push(peer.key);
                    message = back;
                }
            }
        }
        peers.remove_finished(gone);
        Err(message)
    }

    /// Whether some peer has room for a message, so that
    /// [`poll_send`](Self::poll_send) would succeed. Registers `cx` with
    /// every peer if none has.
    pub fn poll_ready(&mut self, peers: &mut PeerList, cx: &mut TaskContext<'_>) -> Poll<()> {
        for peer in &mut peers.peers {
            if let Poll::Ready(Ok(())) = peer.poll_ready(cx) {
                return Poll::Ready(());
            }
        }
        Poll::Pending
    }
}

#[cfg(test)]
mod tests {
    use super::*;
    use crate::noop_context;
    use crate::test_support::*;
    use std::sync::atomic::Ordering;

    #[test]
    fn load_balancer_round_robins() {
        let (a, mut sa) = connection(10);
        let (b, mut sb) = connection(10);
        let (c, mut sc) = connection(10);
        let mut peers = list(vec![a, b, c]);
        let (_, waker) = waker();
        let mut cx = TaskContext::from_waker(&waker);
        let mut lb = LoadBalancer::default();
        for text in ["1", "2", "3", "4", "5", "6"] {
            lb.poll_send(&mut peers, msg(text), &mut cx).unwrap();
        }
        assert_eq!(drain(&mut sa), [msg("1"), msg("4")]);
        assert_eq!(drain(&mut sb), [msg("2"), msg("5")]);
        assert_eq!(drain(&mut sc), [msg("3"), msg("6")]);
    }

    #[test]
    fn load_balancer_skips_full_peers() {
        let (a, mut sa) = connection(1);
        let (b, mut sb) = connection(10);
        let mut peers = list(vec![a, b]);
        let (_, waker) = waker();
        let mut cx = TaskContext::from_waker(&waker);
        let mut lb = LoadBalancer::default();
        for text in ["1", "2", "3", "4"] {
            lb.poll_send(&mut peers, msg(text), &mut cx).unwrap();
        }
        assert_eq!(drain(&mut sa), [msg("1")]);
        assert_eq!(drain(&mut sb), [msg("2"), msg("3"), msg("4")]);
    }

    #[test]
    fn load_balancer_waits_when_every_peer_is_full() {
        let (a, mut sa) = connection(1);
        let mut peers = list(vec![a]);
        let (count, waker) = waker();
        let mut cx = TaskContext::from_waker(&waker);
        let mut lb = LoadBalancer::default();
        lb.poll_send(&mut peers, msg("1"), &mut cx).unwrap();
        assert_eq!(lb.poll_send(&mut peers, msg("2"), &mut cx), Err(msg("2")));
        sa.from_socket.try_recv().unwrap();
        assert!(count.0.load(Ordering::SeqCst) > 0, "room wakes the sender");
        lb.poll_send(&mut peers, msg("2"), &mut cx).unwrap();
    }

    #[test]
    fn load_balancer_without_peers_hands_the_message_back() {
        let (_, waker) = waker();
        let mut cx = TaskContext::from_waker(&waker);
        let mut lb = LoadBalancer::default();
        let mut peers = PeerList::default();
        assert_eq!(lb.poll_send(&mut peers, msg("x"), &mut cx), Err(msg("x")));
    }

    #[test]
    fn a_non_blocking_send_keeps_the_waker_waiting_for_room() {
        let (a, mut sa) = connection(1);
        let mut peers = list(vec![a]);
        let mut lb = LoadBalancer::default();
        assert!(
            lb.poll_send(&mut peers, msg("fills"), &mut noop_context())
                .is_ok()
        );
        let (count, waker) = waker();
        let mut cx = TaskContext::from_waker(&waker);
        assert_eq!(lb.poll_ready(&mut peers, &mut cx), Poll::Pending);
        assert_eq!(
            lb.poll_send(&mut peers, msg("full"), &mut noop_context()),
            Err(msg("full"))
        );
        assert_eq!(drain(&mut sa), [msg("fills")]);
        assert!(
            count.0.load(Ordering::SeqCst) > 0,
            "room wakes the waiting task"
        );
    }

    #[test]
    fn sending_drops_peers_that_are_gone_once_nothing_they_sent_is_unread() {
        let (a, sa) = connection(10);
        let (b, mut sb) = connection(10);
        let (c, mut sc) = connection(10);
        sb.to_socket.try_send(msg("unread")).unwrap();
        drop(sa);
        drop(sb);
        let mut peers = list(vec![a, b, c]);
        let (_, waker) = waker();
        let mut cx = TaskContext::from_waker(&waker);
        let mut lb = LoadBalancer::default();
        assert!(lb.poll_send(&mut peers, msg("x"), &mut cx).is_ok());
        assert_eq!(drain(&mut sc), [msg("x")]);
        assert_eq!(peers.len(), 2, "the gone peer with unread input stays");
    }

    #[test]
    fn a_send_only_balancer_discards_input_and_drops_peers_that_are_gone() {
        let (a, mut sa) = connection(10);
        let (b, mut sb) = connection(10);
        sa.to_socket.try_send(msg("ignored")).unwrap();
        sb.to_socket.try_send(msg("unread")).unwrap();
        drop(sb);
        let mut peers = list(vec![a, b]);
        let mut lb = LoadBalancer::send_only();
        let mut cx = noop_context();
        assert!(lb.poll_send(&mut peers, msg("1"), &mut cx).is_ok());
        assert!(lb.poll_send(&mut peers, msg("2"), &mut cx).is_ok());
        assert_eq!(drain(&mut sa), [msg("1"), msg("2")]);
        assert_eq!(
            peers.len(),
            1,
            "the gone peer is dropped, its input discarded"
        );
        let rx = &mut peers.iter_mut().next().unwrap().connection.rx;
        assert!(
            rx.try_recv().is_err(),
            "what the live peer sent is discarded"
        );
    }
}
