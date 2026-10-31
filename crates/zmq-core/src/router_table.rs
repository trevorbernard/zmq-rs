//! ROUTER's routing (RFC 28): to a peer by its routing id.

use std::collections::HashMap;
use std::fmt;
use std::mem;
use std::task::{Context as TaskContext, Poll};

use zmq_pipe::TrySendError;
use zmtp::{Frame, Message};

use crate::peers::{Peer, PeerCursor, PeerKey, PeerList, PeerMap, random_u32};

/// ROUTER's map between peers and their routing ids, as in libzmq's
/// `router_t`.
///
/// A peer's routing id is the identity it announced in its handshake. A
/// peer that announced none gets a generated one: `0x00` and a 32-bit
/// counter starting at a random value. A peer announcing an identity
/// already in use is ignored: its messages are dropped and nothing can be
/// sent to it. With [`handover`](Self::set_handover) it takes the identity
/// over instead, and the peer that had it is disconnected.
#[derive(Debug)]
pub struct RouterTable {
    by_identity: HashMap<Box<[u8]>, PeerKey>,
    by_peer: PeerMap<Route>,
    /// Peers whose handshake had not completed when last looked at.
    waiting: Vec<PeerKey>,
    cursor: PeerCursor,
    next_generated: u32,
    handover: bool,
}

/// What became of a peer's announced identity.
#[derive(Debug, Clone, PartialEq, Eq)]
enum Route {
    Routable(Frame),
    /// Duplicate identity: libzmq ignores the newcomer.
    Ignored,
}

/// Why a send to one peer, by its routing id, client id or peer id, could
/// not go without waiting. What was being sent is handed back.
#[derive(Debug, Clone, PartialEq, Eq)]
#[non_exhaustive]
pub enum SendError<T> {
    /// No peer has that id, or it has gone (libzmq's `EHOSTUNREACH`).
    UnknownPeer(T),
    /// The peer's queue is full (libzmq's `EAGAIN`).
    Full(T),
}

impl<T> fmt::Display for SendError<T> {
    fn fmt(&self, f: &mut fmt::Formatter<'_>) -> fmt::Result {
        f.write_str(match self {
            Self::UnknownPeer(_) => "no peer has that id",
            Self::Full(_) => "the peer's queue is full",
        })
    }
}

impl<T: fmt::Debug> std::error::Error for SendError<T> {}

impl Default for RouterTable {
    fn default() -> Self {
        Self {
            by_identity: HashMap::new(),
            by_peer: PeerMap::default(),
            waiting: Vec::new(),
            cursor: PeerCursor::default(),
            // libzmq starts the generated ids at a random value.
            next_generated: random_u32(),
            handover: false,
        }
    }
}

impl RouterTable {
    /// Whether a peer announcing an identity already in use takes it over,
    /// disconnecting the peer that had it (libzmq's `ZMQ_ROUTER_HANDOVER`).
    /// Otherwise the newcomer is ignored.
    pub fn set_handover(&mut self, on: bool) {
        self.handover = on;
    }

    /// Identifies peers whose handshake has completed and forgets peers that
    /// are gone.
    pub fn sync(&mut self, peers: &mut PeerList) {
        if self.cursor.removed_since(peers) {
            let live = |key: &PeerKey| peers.get(*key).is_some();
            self.by_peer.retain(|key, _| live(key));
            self.by_identity.retain(|_, key| live(key));
            self.waiting.retain(live);
        }
        let added = self.cursor.added_since(peers);
        self.waiting.extend(added.iter().map(Peer::key));
        for key in mem::take(&mut self.waiting) {
            let Some(peer) = peers.get(key) else {
                continue;
            };
            match peer.connection.peer() {
                Some(established) => self.identify(peers, key, established.peer_identity),
                None => self.waiting.push(key),
            }
        }
    }

    fn identify(&mut self, peers: &mut PeerList, key: PeerKey, announced: Frame) {
        let routing_id = if announced.is_empty() {
            let mut generated = [0u8; 5];
            generated[1..].copy_from_slice(&self.next_generated.to_be_bytes());
            self.next_generated = self.next_generated.wrapping_add(1);
            Frame::copy_from_slice(&generated)
        } else {
            announced
        };
        if let Some(&holder) = self.by_identity.get(routing_id.as_slice()) {
            if !self.handover {
                self.by_peer.insert(key, Route::Ignored);
                return;
            }
            self.by_peer.remove(&holder);
            peers.remove_key(holder);
        }
        self.by_identity.insert(routing_id.as_slice().into(), key);
        self.by_peer.insert(key, Route::Routable(routing_id));
    }

    /// The routing id of `peer`, if it has been identified and not ignored.
    #[must_use]
    pub fn routing_id(&self, peer: PeerKey) -> Option<&Frame> {
        match self.by_peer.get(&peer) {
            Some(Route::Routable(routing_id)) => Some(routing_id),
            Some(Route::Ignored) | None => None,
        }
    }

    /// Sends `message` to the peer with `routing_id` if it is known and has
    /// room. Never blocks.
    pub fn try_send(
        &mut self,
        peers: &mut PeerList,
        routing_id: &[u8],
        mut message: Message,
    ) -> Result<(), SendError<Message>> {
        message.set_metadata(None);
        let Some(&key) = self.by_identity.get(routing_id) else {
            return Err(SendError::UnknownPeer(message));
        };
        let Some(peer) = peers.get_mut(key) else {
            return Err(SendError::UnknownPeer(message));
        };
        match peer.connection.tx.try_send(message) {
            Ok(()) => Ok(()),
            Err(TrySendError::Full(message)) => Err(SendError::Full(message)),
            Err(TrySendError::Closed(message)) => {
                peers.remove_finished([key]);
                Err(SendError::UnknownPeer(message))
            }
        }
    }

    /// Sends the message in `slot` to the peer with `routing_id`, or
    /// returns `Pending` with `cx` registered while that peer is full. Fails
    /// with the message if no peer has that routing id or it has gone.
    pub fn poll_send(
        &mut self,
        peers: &mut PeerList,
        routing_id: &[u8],
        slot: &mut Option<Message>,
        cx: &mut TaskContext<'_>,
    ) -> Poll<Result<(), Message>> {
        if let Some(message) = slot {
            message.set_metadata(None);
        }
        let Some(message) = slot.take() else {
            return Poll::Ready(Ok(()));
        };
        let Some(&key) = self.by_identity.get(routing_id) else {
            return Poll::Ready(Err(message));
        };
        let Some(peer) = peers.get_mut(key) else {
            return Poll::Ready(Err(message));
        };
        match peer.poll_send(message, cx) {
            Ok(()) => Poll::Ready(Ok(())),
            Err(TrySendError::Closed(message)) => {
                peers.remove_finished([key]);
                Poll::Ready(Err(message))
            }
            Err(TrySendError::Full(message)) => {
                *slot = Some(message);
                Poll::Pending
            }
        }
    }
}

#[cfg(test)]
mod properties;

#[cfg(test)]
mod tests {
    use super::*;
    use crate::test_support::*;

    #[test]
    fn a_router_send_drops_a_peer_that_is_gone() {
        let (a, sa) = connection(10);
        sa.establish("alice");
        let mut peers = list(vec![a]);
        let mut table = RouterTable::default();
        table.sync(&mut peers);
        drop(sa);
        assert_eq!(
            table.try_send(&mut peers, b"alice", msg("x")),
            Err(SendError::UnknownPeer(msg("x")))
        );
        assert_eq!(peers.len(), 0);
    }

    #[test]
    fn router_routes_by_announced_identity() {
        let (a, mut sa) = connection(10);
        let (b, mut sb) = connection(10);
        sa.establish("alice");
        sb.establish("bob");
        let mut peers = list(vec![a, b]);
        let mut table = RouterTable::default();
        table.sync(&mut peers);
        assert_eq!(table.try_send(&mut peers, b"bob", msg("hi bob")), Ok(()));
        assert_eq!(
            table.try_send(&mut peers, b"alice", msg("hi alice")),
            Ok(())
        );
        assert_eq!(drain(&mut sa), [msg("hi alice")]);
        assert_eq!(drain(&mut sb), [msg("hi bob")]);
    }

    #[test]
    fn router_hands_back_messages_it_cannot_route() {
        let (a, mut sa) = connection(1);
        sa.establish("alice");
        let mut peers = list(vec![a]);
        let mut table = RouterTable::default();
        table.sync(&mut peers);
        assert_eq!(
            table.try_send(&mut peers, b"nobody", msg("x")),
            Err(SendError::UnknownPeer(msg("x")))
        );
        assert_eq!(table.try_send(&mut peers, b"alice", msg("1")), Ok(()));
        assert_eq!(
            table.try_send(&mut peers, b"alice", msg("2")),
            Err(SendError::Full(msg("2")))
        );
        assert_eq!(drain(&mut sa), [msg("1")]);
    }

    #[test]
    fn router_waits_for_the_handshake_before_routing() {
        let (a, sa) = connection(10);
        let mut peers = list(vec![a]);
        let key = peers.iter().next().unwrap().key();
        let mut table = RouterTable::default();
        table.sync(&mut peers);
        assert_eq!(table.routing_id(key), None, "not identified yet");
        sa.establish("late");
        table.sync(&mut peers);
        assert_eq!(table.routing_id(key), Some(&Frame::from("late")));
    }

    #[test]
    fn router_generates_ids_for_anonymous_peers() {
        let (a, sa) = connection(10);
        let (b, sb) = connection(10);
        sa.establish("");
        sb.establish("");
        let mut peers = list(vec![a, b]);
        let keys: Vec<PeerKey> = peers.iter().map(Peer::key).collect();
        let mut table = RouterTable::default();
        table.sync(&mut peers);
        let first = table.routing_id(keys[0]).unwrap().clone();
        let second = table.routing_id(keys[1]).unwrap().clone();
        for id in [&first, &second] {
            assert_eq!(id.len(), 5);
            assert_eq!(id[0], 0, "generated ids start with a zero octet");
        }
        let counter = |id: &Frame| u32::from_be_bytes(id[1..].try_into().unwrap());
        assert_eq!(counter(&second), counter(&first).wrapping_add(1));
    }

    #[test]
    fn router_ignores_a_duplicate_identity() {
        let (a, mut sa) = connection(10);
        let (b, mut sb) = connection(10);
        sa.establish("same");
        sb.establish("same");
        let mut peers = list(vec![a, b]);
        let keys: Vec<PeerKey> = peers.iter().map(Peer::key).collect();
        let mut table = RouterTable::default();
        table.sync(&mut peers);
        assert_eq!(table.routing_id(keys[1]), None, "the newcomer is ignored");
        assert_eq!(table.try_send(&mut peers, b"same", msg("x")), Ok(()));
        assert_eq!(drain(&mut sa), [msg("x")]);
        assert!(drain(&mut sb).is_empty());
    }

    #[test]
    fn with_handover_a_duplicate_identity_replaces_the_holder() {
        let (a, sa) = connection(10);
        let (b, mut sb) = connection(10);
        sa.establish("same");
        sb.establish("same");
        let mut peers = list(vec![a, b]);
        let keys: Vec<PeerKey> = peers.iter().map(Peer::key).collect();
        let mut table = RouterTable::default();
        table.set_handover(true);
        table.sync(&mut peers);
        assert_eq!(table.routing_id(keys[0]), None);
        assert_eq!(table.routing_id(keys[1]), Some(&Frame::from("same")));
        assert!(peers.get(keys[0]).is_none(), "the holder is disconnected");
        assert_eq!(table.try_send(&mut peers, b"same", msg("x")), Ok(()));
        assert_eq!(drain(&mut sb), [msg("x")]);
    }

    #[test]
    fn router_forgets_peers_that_are_gone() {
        let (a, sa) = connection(10);
        sa.establish("gone");
        let mut peers = list(vec![a]);
        let mut table = RouterTable::default();
        table.sync(&mut peers);
        drop(sa);
        assert_eq!(
            table.try_send(&mut peers, b"gone", msg("w")),
            Err(SendError::UnknownPeer(msg("w"))),
            "gone but not yet forgotten"
        );
        peers.retain_mut(|peer| !peer.connection.tx.is_closed());
        table.sync(&mut peers);
        assert_eq!(
            table.try_send(&mut peers, b"gone", msg("x")),
            Err(SendError::UnknownPeer(msg("x")))
        );
        // A new peer may now take the identity.
        let (b, mut sb) = connection(10);
        sb.establish("gone");
        peers.push(b);
        table.sync(&mut peers);
        assert_eq!(table.try_send(&mut peers, b"gone", msg("y")), Ok(()));
        assert_eq!(drain(&mut sb), [msg("y")]);
    }
}
