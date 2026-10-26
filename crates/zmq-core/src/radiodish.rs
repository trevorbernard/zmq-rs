//! RADIO and DISH routing (RFC 48), as libzmq's `radio_t` and `dish_t`:
//! single-part messages sent to a group, matched exactly. DISHes tell
//! their RADIO peers which groups they joined, in the [`Membership`]
//! message form.

use std::collections::{HashMap, HashSet};
use std::task::{Context as TaskContext, Poll};

use zmtp::{Frame, Group, Membership, Message};

use crate::FairQueue;
use crate::socket::{PeerCursor, PeerList, PeerSet};

/// RADIO's routing: the peers in each group, and fan-out to them.
#[derive(Debug, Default)]
pub struct Radio {
    /// As libzmq's `radio_t`, so a message goes only to its group's peers.
    members: HashMap<Group, PeerSet>,
    /// Reads only peers that sent something, so that a send does not look
    /// at every peer.
    upstream: FairQueue,
    cursor: PeerCursor,
}

/// DISH's routing: the socket's groups, told to every peer, and filtering
/// of what arrives.
#[derive(Debug, Default)]
pub struct Dish {
    groups: HashSet<Group>,
    recv: FairQueue,
    /// The peers that have been told the groups.
    greeted: PeerCursor,
}

impl Radio {
    /// Applies the joins and leaves peers have sent, and forgets peers that
    /// are gone. Anything else peers send is discarded.
    pub fn sync(&mut self, peers: &mut PeerList) {
        let members = &mut self.members;
        // Peers that are gone are forgotten below, with those the socket
        // removed.
        self.upstream.drain(peers, |key, message| {
            let Some(message) = message else {
                return;
            };
            match Membership::from_message(&message) {
                Some(Membership::Join(group)) => {
                    members.entry(group).or_default().insert(key);
                }
                Some(Membership::Leave(group)) => {
                    if let Some(joined) = members.get_mut(&group) {
                        joined.remove(&key);
                        if joined.is_empty() {
                            members.remove(&group);
                        }
                    }
                }
                None => {}
            }
        });
        if self.cursor.removed_since(peers) {
            self.members.retain(|_, members| {
                members.retain(|&key| peers.get(key).is_some());
                !members.is_empty()
            });
        }
    }

    /// Sends `body` to every peer that joined `group` and has room. Never
    /// blocks: a peer whose queue is full misses it, as in libzmq. Returns
    /// how many peers it went to.
    pub fn send(&mut self, peers: &mut PeerList, group: &Group, body: Frame) -> usize {
        self.sync(peers);
        let Some(members) = self.members.get(group) else {
            return 0;
        };
        // On the wire: the group, then the body (RFC 48).
        let mut message = Message::new(Frame::copy_from_slice(group.as_slice()));
        message.push(body);
        let mut sent = 0;
        for &key in members {
            if let Some(peer) = peers.get_mut(key)
                && peer.connection.tx.try_send(message.clone()).is_ok()
            {
                sent += 1;
            }
        }
        sent
    }
}

impl Dish {
    /// Joins `group`. Returns false if it was already joined, as libzmq
    /// refuses a second join.
    pub fn join(&mut self, peers: &mut PeerList, group: &Group) -> bool {
        self.greet(peers);
        if !self.groups.insert(group.clone()) {
            return false;
        }
        peers.broadcast(&Membership::Join(group.clone()).to_message());
        true
    }

    /// Leaves `group`. Returns false if it was not joined.
    pub fn leave(&mut self, peers: &mut PeerList, group: &Group) -> bool {
        self.greet(peers);
        if !self.groups.remove(group) {
            return false;
        }
        peers.broadcast(&Membership::Leave(group.clone()).to_message());
        true
    }

    /// Tells peers that arrived since the last call which groups we joined.
    pub fn greet(&mut self, peers: &mut PeerList) {
        for peer in self.greeted.added_since(peers) {
            for group in &self.groups {
                let join = Membership::Join(group.clone()).to_message();
                let _ = peer.connection.tx.force_send(join);
            }
        }
    }

    /// The next message in a joined group, or `Pending` with `cx`
    /// registered on every peer. Messages in other groups, and anything
    /// not a group and a body, are dropped.
    pub fn poll_recv(
        &mut self,
        peers: &mut PeerList,
        cx: &mut TaskContext<'_>,
    ) -> Poll<(Group, Frame)> {
        self.greet(peers);
        loop {
            let Poll::Ready((_, message)) = self.recv.poll_recv(peers, cx) else {
                return Poll::Pending;
            };
            let mut frames = message.into_iter();
            let (Some(name), Some(body), None) = (frames.next(), frames.next(), frames.next())
            else {
                continue;
            };
            if let Ok(group) = Group::new(name)
                && self.groups.contains(&group)
            {
                return Poll::Ready((group, body));
            }
        }
    }
}
