//! RADIO and DISH routing (RFC 48), as libzmq's `radio_t` and `dish_t`:
//! single-part messages sent to a group, matched exactly. DISHes tell
//! their RADIO peers which groups they joined, in the [`Membership`]
//! message form.

use std::collections::{HashMap, HashSet};
use std::task::{Context as TaskContext, Poll};

use zmq_pipe::TryRecvError;
use zmtp::{Frame, Group, Membership, Message};

use crate::FairQueue;
use crate::socket::{PeerKey, PeerList};

/// RADIO's routing: the groups each peer joined, and fan-out to them.
#[derive(Debug, Default)]
pub struct Radio {
    joined: HashMap<PeerKey, HashSet<Group>>,
}

/// DISH's routing: the socket's groups, told to every peer, and filtering
/// of what arrives.
#[derive(Debug, Default)]
pub struct Dish {
    groups: HashSet<Group>,
    recv: FairQueue,
    /// The newest peer that has been told the groups. Keys only grow.
    greeted: Option<PeerKey>,
}

impl Radio {
    /// Applies the joins and leaves peers have sent, and forgets peers that
    /// are gone. Anything else peers send is discarded.
    pub fn sync(&mut self, peers: &mut PeerList) {
        let joined = &mut self.joined;
        peers.retain_mut(|peer| {
            let groups = joined.entry(peer.key()).or_default();
            loop {
                match peer.connection.rx.try_recv() {
                    Ok(message) => match Membership::from_message(&message) {
                        Some(Membership::Join(group)) => {
                            groups.insert(group);
                        }
                        Some(Membership::Leave(group)) => {
                            groups.remove(&group);
                        }
                        None => {}
                    },
                    Err(TryRecvError::Empty) => break true,
                    Err(TryRecvError::Closed) => break false,
                }
            }
        });
        if joined.len() > peers.len() {
            joined.retain(|&key, _| peers.get(key).is_some());
        }
    }

    /// Sends `body` to every peer that joined `group` and has room. Never
    /// blocks: a peer whose queue is full misses it, as in libzmq. Returns
    /// how many peers it went to.
    pub fn send(&mut self, peers: &mut PeerList, group: &Group, body: Frame) -> usize {
        self.sync(peers);
        // On the wire: the group, then the body (RFC 48).
        let mut message = Message::new(Frame::copy_from_slice(group.as_slice()));
        message.push(body);
        let mut sent = 0;
        for peer in peers.iter_mut() {
            let wanted = self
                .joined
                .get(&peer.key())
                .is_some_and(|groups| groups.contains(group));
            if wanted && peer.connection.tx.try_send(message.clone()).is_ok() {
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
        tell_all(peers, &Membership::Join(group.clone()));
        true
    }

    /// Leaves `group`. Returns false if it was not joined.
    pub fn leave(&mut self, peers: &mut PeerList, group: &Group) -> bool {
        self.greet(peers);
        if !self.groups.remove(group) {
            return false;
        }
        tell_all(peers, &Membership::Leave(group.clone()));
        true
    }

    /// Tells peers that arrived since the last call which groups we joined.
    pub fn greet(&mut self, peers: &mut PeerList) {
        for peer in peers.iter_mut() {
            if self.greeted.is_some_and(|greeted| peer.key() <= greeted) {
                continue;
            }
            self.greeted = Some(peer.key());
            for group in &self.groups {
                let join = Membership::Join(group.clone()).to_message();
                let _ = peer.connection.tx.try_send(join);
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

/// Sends a membership change to every peer. A DISH's queues towards its
/// peers are unbounded, so nothing is dropped.
fn tell_all(peers: &mut PeerList, change: &Membership) {
    let message = change.to_message();
    for peer in peers.iter_mut() {
        let _ = peer.connection.tx.try_send(message.clone());
    }
}
