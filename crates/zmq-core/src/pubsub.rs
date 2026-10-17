//! PUB and SUB routing (RFC 29), as in libzmq's `xpub_t` and `xsub_t`.
//!
//! Subscribers send their topics upstream as [`Subscription`] messages; a
//! publisher keeps each peer's topics and sends a message only to peers
//! subscribed to a prefix of its first frame. Subscribers filter too, so
//! publishers that do not (ZMTP 1.0) are harmless.

use std::collections::HashMap;
use std::task::{Context as TaskContext, Poll};

use zmq_pipe::TryRecvError;
use zmtp::{Frame, Message, Subscription};

use crate::FairQueue;
use crate::socket::{PeerKey, PeerList};

/// Topics and how many times each was subscribed, matched by prefix.
#[derive(Debug, Clone, Default)]
pub struct Topics {
    counts: HashMap<Box<[u8]>, usize>,
    /// The longest topic's length, so matching tries no longer prefixes.
    longest: usize,
}

impl Topics {
    /// Adds a subscription to `topic`. Returns whether it is new.
    pub fn add(&mut self, topic: &[u8]) -> bool {
        if let Some(count) = self.counts.get_mut(topic) {
            *count += 1;
            return false;
        }
        self.counts.insert(topic.into(), 1);
        self.longest = self.longest.max(topic.len());
        true
    }

    /// Removes one subscription to `topic`. Returns whether that was the
    /// last, so the topic is gone.
    pub fn remove(&mut self, topic: &[u8]) -> bool {
        let Some(count) = self.counts.get_mut(topic) else {
            return false;
        };
        *count -= 1;
        if *count > 0 {
            return false;
        }
        self.counts.remove(topic);
        if topic.len() == self.longest {
            self.longest = self
                .counts
                .keys()
                .map(|topic| topic.len())
                .max()
                .unwrap_or(0);
        }
        true
    }

    /// Whether `topic` is subscribed.
    pub fn contains(&self, topic: &[u8]) -> bool {
        self.counts.contains_key(topic)
    }

    /// Whether a message whose first frame is `data` is wanted: some topic
    /// is a prefix of it.
    pub fn matches(&self, data: &[u8]) -> bool {
        !self.counts.is_empty()
            && (0..=self.longest.min(data.len())).any(|len| self.counts.contains_key(&data[..len]))
    }

    /// The subscribed topics, in no particular order.
    pub fn iter(&self) -> impl Iterator<Item = &[u8]> {
        self.counts.keys().map(|topic| &**topic)
    }

    /// Whether nothing is subscribed.
    pub fn is_empty(&self) -> bool {
        self.counts.is_empty()
    }
}

/// PUB's routing: each peer's topics, and fan-out to the peers that match.
#[derive(Debug, Default)]
pub struct Publisher {
    subscribers: HashMap<PeerKey, PeerTopics>,
}

#[derive(Debug, Default)]
struct PeerTopics {
    topics: Topics,
    /// Whether its handshake has been looked at for an implicit
    /// subscription.
    settled: bool,
}

impl Publisher {
    /// Applies the subscriptions peers have sent, and forgets peers that
    /// are gone. Anything else peers send is discarded.
    pub fn sync(&mut self, peers: &mut PeerList) {
        let subscribers = &mut self.subscribers;
        peers.retain_mut(|peer| {
            let entry = subscribers.entry(peer.key()).or_default();
            if !entry.settled
                && let Some(established) = peer.connection.peer()
            {
                entry.settled = true;
                // A ZMTP 1.0 subscriber filters for itself and never says
                // what it wants, so it gets everything.
                if established.implicit_subscription {
                    entry.topics.add(b"");
                }
            }
            loop {
                match peer.connection.rx.try_recv() {
                    Ok(message) => entry.apply(&message),
                    Err(TryRecvError::Empty) => break true,
                    Err(TryRecvError::Closed) => break false,
                }
            }
        });
        if subscribers.len() > peers.len() {
            subscribers.retain(|&key, _| peers.get(key).is_some());
        }
    }

    /// Sends `message` to every peer subscribed to it that has room.
    /// Never blocks: a peer whose queue is full misses the message, as in
    /// libzmq. Returns how many peers it went to.
    pub fn send(&mut self, peers: &mut PeerList, message: Message) -> usize {
        self.sync(peers);
        let topic = message.split_first().0.as_slice();
        let mut sent = 0;
        for peer in peers.iter_mut() {
            let wanted = self
                .subscribers
                .get(&peer.key())
                .is_some_and(|entry| entry.topics.matches(topic));
            if wanted && peer.connection.tx.try_send(message.clone()).is_ok() {
                sent += 1;
            }
        }
        sent
    }
}

impl PeerTopics {
    /// Applies a subscription from the peer. A peer's topics are a set, as
    /// in libzmq's XPUB: subscribing twice is one subscription.
    fn apply(&mut self, message: &Message) {
        match Subscription::from_message(message) {
            Some(Subscription::Subscribe(topic)) => {
                if !self.topics.contains(&topic) {
                    self.topics.add(&topic);
                }
            }
            Some(Subscription::Cancel(topic)) => {
                self.topics.remove(&topic);
            }
            None => {}
        }
    }
}

/// SUB's routing: the socket's topics, sent to every peer, and filtering
/// of what arrives.
#[derive(Debug, Default)]
pub struct Subscriber {
    topics: Topics,
    recv: FairQueue,
    /// The newest peer that has been sent the topics. Keys only grow.
    greeted: Option<PeerKey>,
}

impl Subscriber {
    /// Subscribes to messages starting with `topic`. Subscribing to a topic
    /// again needs as many [`unsubscribe`](Self::unsubscribe)s to undo.
    pub fn subscribe(&mut self, peers: &mut PeerList, topic: &[u8]) {
        self.greet(peers);
        self.topics.add(topic);
        // Sent every time, as libzmq does: publishers keep one per topic.
        tell_all(
            peers,
            &Subscription::Subscribe(Frame::copy_from_slice(topic)),
        );
    }

    /// Removes one subscription to `topic`. Returns whether there was one.
    pub fn unsubscribe(&mut self, peers: &mut PeerList, topic: &[u8]) -> bool {
        self.greet(peers);
        if !self.topics.contains(topic) {
            return false;
        }
        if self.topics.remove(topic) {
            tell_all(peers, &Subscription::Cancel(Frame::copy_from_slice(topic)));
        }
        true
    }

    /// Sends the topics to peers that arrived since the last call.
    pub fn greet(&mut self, peers: &mut PeerList) {
        for peer in peers.iter_mut() {
            if self.greeted.is_some_and(|greeted| peer.key() <= greeted) {
                continue;
            }
            self.greeted = Some(peer.key());
            for topic in self.topics.iter() {
                let subscription = Subscription::Subscribe(Frame::copy_from_slice(topic));
                let _ = peer.connection.tx.try_send(subscription.to_message());
            }
        }
    }

    /// The next message matching a topic, or `Pending` with `cx` registered
    /// on every peer. Messages matching no topic are dropped.
    pub fn poll_recv(&mut self, peers: &mut PeerList, cx: &mut TaskContext<'_>) -> Poll<Message> {
        self.greet(peers);
        loop {
            let Poll::Ready((_, message)) = self.recv.poll_recv(peers, cx) else {
                return Poll::Pending;
            };
            if self.topics.matches(message.split_first().0) {
                return Poll::Ready(message);
            }
        }
    }

    /// The subscribed topics.
    pub fn topics(&self) -> &Topics {
        &self.topics
    }
}

/// Sends `subscription` to every peer. A SUB socket's queues towards its
/// peers are unbounded, so nothing is dropped.
fn tell_all(peers: &mut PeerList, subscription: &Subscription) {
    let message = subscription.to_message();
    for peer in peers.iter_mut() {
        let _ = peer.connection.tx.try_send(message.clone());
    }
}

#[cfg(test)]
mod tests {
    use super::*;
    use crate::test_support::*;

    fn subscribe(topic: &'static str) -> Message {
        Subscription::Subscribe(Frame::from(topic)).to_message()
    }

    fn cancel(topic: &'static str) -> Message {
        Subscription::Cancel(Frame::from(topic)).to_message()
    }

    #[test]
    fn topics_match_by_prefix() {
        let mut topics = Topics::default();
        assert!(!topics.matches(b"anything"));
        assert!(topics.add(b"news"));
        assert!(topics.matches(b"news"));
        assert!(topics.matches(b"newsflash"));
        assert!(!topics.matches(b"new"));
        assert!(!topics.matches(b"sports"));
        assert!(topics.add(b""));
        assert!(topics.matches(b"sports"));
        assert!(topics.matches(b""));
    }

    #[test]
    fn topics_count_subscriptions() {
        let mut topics = Topics::default();
        assert!(topics.add(b"a"));
        assert!(!topics.add(b"a"));
        assert!(!topics.remove(b"a"), "one subscription left");
        assert!(topics.matches(b"a"));
        assert!(topics.remove(b"a"));
        assert!(!topics.matches(b"a"));
        assert!(!topics.remove(b"a"), "nothing to remove");
        assert!(topics.is_empty());
    }

    #[test]
    fn matching_tracks_the_longest_topic() {
        let mut topics = Topics::default();
        topics.add(b"ab");
        topics.add(b"abcdef");
        topics.remove(b"abcdef");
        topics.add(b"x");
        assert!(topics.matches(b"abz"));
        assert!(!topics.matches(b"a"));
        topics.remove(b"ab");
        assert!(!topics.matches(b"abcdef"));
        assert!(topics.matches(b"xyz"));
    }

    #[test]
    fn publisher_sends_only_to_matching_peers() {
        let (a, mut sa) = connection(10);
        let (b, mut sb) = connection(10);
        let (c, mut sc) = connection(10);
        let mut peers = list(vec![a, b, c]);
        sa.to_socket.try_send(subscribe("news")).unwrap();
        sb.to_socket.try_send(subscribe("")).unwrap();
        let mut publisher = Publisher::default();
        publisher.sync(&mut peers);
        assert_eq!(publisher.send(&mut peers, msg("news: rain")), 2);
        assert_eq!(publisher.send(&mut peers, msg("sports")), 1);
        assert_eq!(drain(&mut sa), [msg("news: rain")]);
        assert_eq!(drain(&mut sb), [msg("news: rain"), msg("sports")]);
        assert_eq!(drain(&mut sc), []);
    }

    #[test]
    fn publisher_reads_subscriptions_as_it_sends() {
        let (a, mut sa) = connection(10);
        let mut peers = list(vec![a]);
        let mut publisher = Publisher::default();
        sa.to_socket.try_send(subscribe("x")).unwrap();
        assert_eq!(publisher.send(&mut peers, msg("x1")), 1);
        sa.to_socket.try_send(cancel("x")).unwrap();
        assert_eq!(publisher.send(&mut peers, msg("x2")), 0);
        assert_eq!(drain(&mut sa), [msg("x1")]);
    }

    #[test]
    fn publisher_keeps_one_subscription_per_topic_and_peer() {
        // As libzmq's XPUB: a peer's repeated subscriptions are one, so a
        // single cancel ends it. Subscribers count, and cancel only once.
        let (a, mut sa) = connection(10);
        let mut peers = list(vec![a]);
        sa.to_socket.try_send(subscribe("x")).unwrap();
        sa.to_socket.try_send(subscribe("x")).unwrap();
        sa.to_socket.try_send(cancel("x")).unwrap();
        let mut publisher = Publisher::default();
        assert_eq!(publisher.send(&mut peers, msg("x")), 0);
    }

    #[test]
    fn publisher_drops_for_full_peers_without_blocking() {
        let (a, mut sa) = connection(1);
        let (b, mut sb) = connection(10);
        let mut peers = list(vec![a, b]);
        sa.to_socket.try_send(subscribe("")).unwrap();
        sb.to_socket.try_send(subscribe("")).unwrap();
        let mut publisher = Publisher::default();
        assert_eq!(publisher.send(&mut peers, msg("1")), 2);
        assert_eq!(publisher.send(&mut peers, msg("2")), 1);
        assert_eq!(drain(&mut sa), [msg("1")]);
        assert_eq!(drain(&mut sb), [msg("1"), msg("2")]);
    }

    #[test]
    fn publisher_ignores_other_messages_and_forgets_closed_peers() {
        let (a, mut sa) = connection(10);
        let (b, mut sb) = connection(10);
        let mut peers = list(vec![a, b]);
        sa.to_socket.try_send(msg("not a subscription")).unwrap();
        sb.to_socket.try_send(subscribe("")).unwrap();
        drop(sb);
        let mut publisher = Publisher::default();
        assert_eq!(publisher.send(&mut peers, msg("x")), 0);
        assert_eq!(peers.len(), 1);
        assert_eq!(drain(&mut sa), []);
    }

    #[test]
    fn publisher_subscribes_legacy_peers_to_everything() {
        let (a, mut sa) = connection(10);
        let mut peers = list(vec![a]);
        let mut publisher = Publisher::default();
        assert_eq!(publisher.send(&mut peers, msg("early")), 0);
        sa.establish_legacy_subscriber();
        assert_eq!(publisher.send(&mut peers, msg("x")), 1);
        assert_eq!(drain(&mut sa), [msg("x")]);
    }

    #[test]
    fn subscriber_tells_every_peer_including_later_ones() {
        let (a, mut sa) = connection(10);
        let mut peers = list(vec![a]);
        let mut subscriber = Subscriber::default();
        subscriber.subscribe(&mut peers, b"x");
        let (b, mut sb) = connection(10);
        peers.push(b);
        subscriber.greet(&mut peers);
        subscriber.greet(&mut peers);
        assert_eq!(drain(&mut sa), [subscribe("x")]);
        assert_eq!(drain(&mut sb), [subscribe("x")], "greeted once");
    }

    #[test]
    fn subscriber_cancels_only_its_last_subscription() {
        let (a, mut sa) = connection(10);
        let mut peers = list(vec![a]);
        let mut subscriber = Subscriber::default();
        subscriber.subscribe(&mut peers, b"x");
        subscriber.subscribe(&mut peers, b"x");
        assert!(subscriber.unsubscribe(&mut peers, b"x"));
        assert!(subscriber.unsubscribe(&mut peers, b"x"));
        assert!(!subscriber.unsubscribe(&mut peers, b"x"));
        assert_eq!(
            drain(&mut sa),
            [subscribe("x"), subscribe("x"), cancel("x")],
            "as libzmq: every subscribe goes upstream, a cancel only once"
        );
    }

    #[test]
    fn subscriber_filters_what_arrives() {
        let (a, mut sa) = connection(10);
        let mut peers = list(vec![a]);
        let mut subscriber = Subscriber::default();
        subscriber.subscribe(&mut peers, b"news");
        for text in ["sports", "news: rain", "weather"] {
            sa.to_socket.try_send(msg(text)).unwrap();
        }
        let (_, waker) = waker();
        let mut cx = TaskContext::from_waker(&waker);
        assert_eq!(
            subscriber.poll_recv(&mut peers, &mut cx),
            Poll::Ready(msg("news: rain"))
        );
        assert_eq!(subscriber.poll_recv(&mut peers, &mut cx), Poll::Pending);
    }
}
