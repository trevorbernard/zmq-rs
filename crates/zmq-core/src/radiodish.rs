//! RADIO and DISH routing (RFC 48), as libzmq's `radio_t` and `dish_t`:
//! single-part messages sent to a group, matched exactly. DISHes tell
//! their RADIO peers which groups they joined, in the [`Membership`]
//! message form.

use std::collections::{HashMap, HashSet};
use std::task::{Context as TaskContext, Poll};

use zmtp::{Frame, Group, Membership, Message};

use crate::FairQueue;
use crate::peers::{PeerCursor, PeerList, PeerSet};

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

#[cfg(test)]
mod tests {
    use super::*;
    use crate::test_support::*;

    fn group(name: &'static str) -> Group {
        Group::new(name).unwrap()
    }

    fn join(session: &mut FakeSession, name: &'static str) {
        session
            .to_socket
            .try_send(Membership::Join(group(name)).to_message())
            .unwrap();
    }

    fn leave(session: &mut FakeSession, name: &'static str) {
        session
            .to_socket
            .try_send(Membership::Leave(group(name)).to_message())
            .unwrap();
    }

    fn on_air(name: &'static str, body: &'static str) -> Message {
        let mut message = msg(name);
        message.push(Frame::from(body));
        message
    }

    #[test]
    fn a_message_goes_to_its_groups_members_only() {
        let (a, mut sa) = connection(10);
        let (b, mut sb) = connection(10);
        let mut peers = list(vec![a, b]);
        let mut radio = Radio::default();
        join(&mut sa, "news");
        join(&mut sb, "sport");
        assert_eq!(
            radio.send(&mut peers, &group("news"), Frame::from("rain")),
            1
        );
        assert_eq!(
            radio.send(&mut peers, &group("chess"), Frame::from("e4")),
            0
        );
        assert_eq!(drain(&mut sa), [on_air("news", "rain")]);
        assert_eq!(drain(&mut sb), []);
    }

    #[test]
    fn a_group_is_forgotten_once_its_last_member_leaves() {
        let (a, mut sa) = connection(10);
        let (b, mut sb) = connection(10);
        let mut peers = list(vec![a, b]);
        let mut radio = Radio::default();
        join(&mut sa, "news");
        join(&mut sb, "news");
        leave(&mut sa, "never joined");
        assert_eq!(radio.send(&mut peers, &group("news"), Frame::from("1")), 2);

        leave(&mut sa, "news");
        assert_eq!(radio.send(&mut peers, &group("news"), Frame::from("2")), 1);
        assert_eq!(radio.members.len(), 1);
        leave(&mut sb, "news");
        assert_eq!(radio.send(&mut peers, &group("news"), Frame::from("3")), 0);
        assert!(radio.members.is_empty());
        assert_eq!(drain(&mut sa), [on_air("news", "1")]);
        assert_eq!(drain(&mut sb), [on_air("news", "1"), on_air("news", "2")]);
    }

    #[test]
    fn what_is_not_a_join_or_a_leave_is_discarded() {
        let (a, mut sa) = connection(10);
        let mut peers = list(vec![a]);
        let mut radio = Radio::default();
        sa.to_socket.try_send(msg("not a membership")).unwrap();
        join(&mut sa, "news");
        assert_eq!(
            radio.send(&mut peers, &group("news"), Frame::from("rain")),
            1
        );
        assert_eq!(radio.members.len(), 1);
    }

    #[test]
    fn a_departed_peer_is_forgotten_as_a_member() {
        let (a, mut sa) = connection(10);
        let (b, mut sb) = connection(10);
        let mut peers = list(vec![a, b]);
        let mut radio = Radio::default();
        join(&mut sa, "news");
        join(&mut sb, "news");
        join(&mut sb, "sport");
        radio.sync(&mut peers);
        assert_eq!(radio.members.len(), 2);

        drop(sb);
        assert_eq!(
            radio.send(&mut peers, &group("news"), Frame::from("rain")),
            1
        );
        assert_eq!(peers.len(), 1, "the closed peer is removed");
        assert_eq!(
            radio.members.keys().collect::<Vec<_>>(),
            [&group("news")],
            "a group with no members left is forgotten"
        );
        assert_eq!(radio.members[&group("news")].len(), 1);
        assert_eq!(drain(&mut sa), [on_air("news", "rain")]);
    }
}
