//! SERVER's routing (RFC 41): to a peer by a numeric id.

use std::collections::HashMap;
use std::num::NonZeroU32;
use std::task::{Context as TaskContext, Poll};

use zmq_pipe::TrySendError;
use zmtp::{Frame, Message};

use crate::fair_queue::FairQueue;
use crate::peers::{PeerCursor, PeerKey, PeerList, PeerMap, random_u32};
use crate::router_table::SendError;

/// SERVER's routing (RFC 41), as libzmq's `server_t`: every peer gets a
/// non-zero 32-bit id, from a counter that starts at a random value;
/// replies go to a peer by its id. Single-part messages only.
#[derive(Debug)]
pub struct ServerTable {
    by_id: HashMap<NonZeroU32, PeerKey>,
    by_peer: PeerMap<NonZeroU32>,
    cursor: PeerCursor,
    next_id: u32,
    recv: FairQueue,
}

impl Default for ServerTable {
    fn default() -> Self {
        Self {
            by_id: HashMap::new(),
            by_peer: PeerMap::default(),
            cursor: PeerCursor::default(),
            recv: FairQueue::default(),
            // libzmq starts its ids at a random value.
            next_id: random_u32(),
        }
    }
}

impl ServerTable {
    /// Gives new peers their ids and forgets peers that are gone.
    pub fn sync(&mut self, peers: &mut PeerList) {
        if self.cursor.removed_since(peers) {
            self.by_peer.retain(|key, _| peers.get(*key).is_some());
            self.by_id.retain(|_, key| peers.get(*key).is_some());
        }
        for peer in self.cursor.added_since(peers) {
            let id = loop {
                let candidate = self.next_id;
                self.next_id = self.next_id.wrapping_add(1);
                // Never zero, and never one still in use after wrapping.
                if let Some(id) = NonZeroU32::new(candidate)
                    && !self.by_id.contains_key(&id)
                {
                    break id;
                }
            };
            self.by_id.insert(id, peer.key);
            self.by_peer.insert(peer.key, id);
        }
    }

    /// The next single-part message and the id of the client that sent it,
    /// or `Pending` with `cx` registered on every peer. Multipart messages
    /// are dropped, as RFC 41 requires.
    pub fn poll_recv(
        &mut self,
        peers: &mut PeerList,
        cx: &mut TaskContext<'_>,
    ) -> Poll<(NonZeroU32, Frame)> {
        self.sync(peers);
        loop {
            let Poll::Ready((key, message)) = self.recv.poll_recv(peers, cx) else {
                return Poll::Pending;
            };
            let (frame, rest) = message.split_first();
            if !rest.is_empty() {
                continue;
            }
            if let Some(&id) = self.by_peer.get(&key) {
                return Poll::Ready((id, frame.clone()));
            }
        }
    }

    /// Sends `frame` to the client with `id` if it is known and has room.
    /// Never blocks.
    pub fn try_send(
        &mut self,
        peers: &mut PeerList,
        id: NonZeroU32,
        frame: Frame,
    ) -> Result<(), SendError<Frame>> {
        self.sync(peers);
        let Some(peer) = self.by_id.get(&id).and_then(|&key| peers.get_mut(key)) else {
            return Err(SendError::UnknownPeer(frame));
        };
        peer.connection
            .tx
            .try_send(Message::new(frame))
            .map_err(|err| match err {
                TrySendError::Full(message) => SendError::Full(only_frame(message)),
                // The client's session has ended.
                TrySendError::Closed(message) => SendError::UnknownPeer(only_frame(message)),
            })
    }

    /// Sends the frame in `slot` to the client with `id`, or returns
    /// `Pending` with `cx` registered while that client is full. Fails with
    /// the frame if no client has that id or it has gone.
    pub fn poll_send(
        &mut self,
        peers: &mut PeerList,
        id: NonZeroU32,
        slot: &mut Option<Frame>,
        cx: &mut TaskContext<'_>,
    ) -> Poll<Result<(), Frame>> {
        let Some(frame) = slot.take() else {
            return Poll::Ready(Ok(()));
        };
        self.sync(peers);
        let Some(peer) = self.by_id.get(&id).and_then(|&key| peers.get_mut(key)) else {
            return Poll::Ready(Err(frame));
        };
        match peer.poll_send(Message::new(frame), cx) {
            Ok(()) => Poll::Ready(Ok(())),
            Err(TrySendError::Closed(message)) => Poll::Ready(Err(only_frame(message))),
            Err(TrySendError::Full(message)) => {
                *slot = Some(only_frame(message));
                Poll::Pending
            }
        }
    }

    /// The id of the peer with `key`, once it is in the list.
    pub fn id_of(&mut self, peers: &mut PeerList, key: PeerKey) -> Option<NonZeroU32> {
        self.sync(peers);
        self.by_peer.get(&key).copied()
    }

    /// Closes the connection to the client with `id`. Returns whether there
    /// was one.
    pub fn disconnect(&mut self, peers: &mut PeerList, id: NonZeroU32) -> bool {
        self.sync(peers);
        let Some(key) = self.by_id.remove(&id) else {
            return false;
        };
        self.by_peer.remove(&key);
        peers.remove_key(key);
        true
    }
}

/// The frame of a message built from one.
fn only_frame(message: Message) -> Frame {
    message.into_parts().0
}
