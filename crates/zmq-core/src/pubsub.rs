//! PUB and SUB routing (RFC 29), as in libzmq's `xpub_t` and `xsub_t`.
//!
//! Subscribers send their topics upstream as [`Subscription`] messages; a
//! publisher keeps each peer's topics and sends a message only to peers
//! subscribed to a prefix of its first frame. Subscribers filter too, so
//! publishers that do not (ZMTP 1.0) are harmless.

use std::collections::{HashMap, VecDeque};
use std::task::{Context as TaskContext, Poll, Waker};

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

/// What an XPUB socket hears from its peers. As in libzmq, a topic's
/// subscriptions are reported once across all peers: `Subscribe` when the
/// first peer subscribes, `Cancel` when the last one cancels or goes away.
#[derive(Debug, Clone, PartialEq, Eq)]
pub enum XPubEvent {
    /// The first subscription to this topic.
    Subscribe(Frame),
    /// The last subscription to this topic is gone.
    Cancel(Frame),
    /// Anything else a peer sent, such as an XSUB's own messages.
    Message(Message),
}

/// PUB's and XPUB's routing: each peer's topics, and fan-out to the peers
/// that match.
#[derive(Debug, Default)]
pub struct Publisher {
    subscribers: HashMap<PeerKey, PeerTopics>,
    /// How many peers subscribe to each topic.
    everyone: Topics,
    /// What XPUB has yet to receive. `None` for PUB, which discards it.
    events: Option<VecDeque<XPubEvent>>,
}

#[derive(Debug, Default)]
struct PeerTopics {
    /// A set, as in libzmq's XPUB: subscribing twice is one subscription.
    topics: Topics,
    /// Whether its handshake has been looked at for an implicit
    /// subscription.
    settled: bool,
}

impl Publisher {
    /// A publisher that reports what its peers send as [`XPubEvent`]s, for
    /// XPUB. A plain PUB discards it.
    pub fn with_events() -> Self {
        Self {
            events: Some(VecDeque::new()),
            ..Self::default()
        }
    }

    /// The next event, or `Pending` with `cx` registered on every peer.
    /// Always `Pending` for a publisher made without events.
    pub fn poll_event(
        &mut self,
        peers: &mut PeerList,
        cx: &mut TaskContext<'_>,
    ) -> Poll<XPubEvent> {
        self.drain(peers, cx);
        match self.events.as_mut().and_then(VecDeque::pop_front) {
            Some(event) => Poll::Ready(event),
            None => Poll::Pending,
        }
    }

    /// Applies the subscriptions peers have sent, and forgets peers that
    /// are gone.
    pub fn sync(&mut self, peers: &mut PeerList) {
        self.drain(peers, &mut TaskContext::from_waker(Waker::noop()));
    }

    /// Reads everything peers have sent, registering `cx` with each.
    fn drain(&mut self, peers: &mut PeerList, cx: &mut TaskContext<'_>) {
        let Self {
            subscribers,
            everyone,
            events,
        } = self;
        let mut changes = Changes { everyone, events };
        peers.retain_mut(|peer| {
            let entry = subscribers.entry(peer.key()).or_default();
            if !entry.settled
                && let Some(established) = peer.connection.peer()
            {
                entry.settled = true;
                // A ZMTP 1.0 subscriber filters for itself and never says
                // what it wants, so it gets everything.
                if established.implicit_subscription {
                    changes.subscribe(entry, Frame::new());
                }
            }
            loop {
                match peer.connection.rx.poll_recv(cx) {
                    Poll::Ready(Some(message)) => match Subscription::from_message(&message) {
                        Some(Subscription::Subscribe(topic)) => changes.subscribe(entry, topic),
                        Some(Subscription::Cancel(topic)) => changes.cancel(entry, topic),
                        None => changes.message(message),
                    },
                    Poll::Pending => break true,
                    Poll::Ready(None) => {
                        if let Some(entry) = subscribers.remove(&peer.key()) {
                            changes.forget(entry);
                        }
                        break false;
                    }
                }
            }
        });
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

/// Applies what one peer sent to the topics of all peers, noting events.
struct Changes<'a> {
    everyone: &'a mut Topics,
    events: &'a mut Option<VecDeque<XPubEvent>>,
}

impl Changes<'_> {
    fn subscribe(&mut self, peer: &mut PeerTopics, topic: Frame) {
        if peer.topics.contains(&topic) {
            return;
        }
        peer.topics.add(&topic);
        if self.everyone.add(&topic) {
            self.event(XPubEvent::Subscribe(topic));
        }
    }

    fn cancel(&mut self, peer: &mut PeerTopics, topic: Frame) {
        if peer.topics.remove(&topic) && self.everyone.remove(&topic) {
            self.event(XPubEvent::Cancel(topic));
        }
    }

    fn message(&mut self, message: Message) {
        self.event(XPubEvent::Message(message));
    }

    /// A peer is gone: its subscriptions end with it.
    fn forget(&mut self, peer: PeerTopics) {
        for topic in peer.topics.iter() {
            if self.everyone.remove(topic) {
                self.event(XPubEvent::Cancel(Frame::copy_from_slice(topic)));
            }
        }
    }

    fn event(&mut self, event: XPubEvent) {
        if let Some(events) = self.events {
            events.push_back(event);
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

    /// Sends `message` to every peer, as XSUB does. A subscription in the
    /// message form subscribes or unsubscribes instead, as in libzmq, so
    /// the topics stay counted.
    pub fn send_upstream(&mut self, peers: &mut PeerList, message: Message) {
        match Subscription::from_message(&message) {
            Some(Subscription::Subscribe(topic)) => self.subscribe(peers, &topic),
            Some(Subscription::Cancel(topic)) => {
                self.unsubscribe(peers, &topic);
            }
            None => {
                self.greet(peers);
                for peer in peers.iter_mut() {
                    let _ = peer.connection.tx.try_send(message.clone());
                }
            }
        }
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
    use std::sync::atomic::Ordering;

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

    fn next_event(publisher: &mut Publisher, peers: &mut PeerList) -> Option<XPubEvent> {
        match publisher.poll_event(peers, &mut TaskContext::from_waker(Waker::noop())) {
            Poll::Ready(event) => Some(event),
            Poll::Pending => None,
        }
    }

    fn events(publisher: &mut Publisher, peers: &mut PeerList) -> Vec<XPubEvent> {
        std::iter::from_fn(|| next_event(publisher, peers)).collect()
    }

    fn subscribed(topic: &'static str) -> XPubEvent {
        XPubEvent::Subscribe(Frame::from(topic))
    }

    fn cancelled(topic: &'static str) -> XPubEvent {
        XPubEvent::Cancel(Frame::from(topic))
    }

    #[test]
    fn xpub_reports_a_topics_first_subscription_and_last_cancel() {
        let (a, mut sa) = connection(10);
        let (b, mut sb) = connection(10);
        let mut peers = list(vec![a, b]);
        let mut publisher = Publisher::with_events();
        sa.to_socket.try_send(subscribe("x")).unwrap();
        sb.to_socket.try_send(subscribe("x")).unwrap();
        sa.to_socket.try_send(subscribe("x")).unwrap();
        assert_eq!(events(&mut publisher, &mut peers), [subscribed("x")]);
        sa.to_socket.try_send(cancel("x")).unwrap();
        assert_eq!(events(&mut publisher, &mut peers), []);
        sb.to_socket.try_send(cancel("x")).unwrap();
        assert_eq!(events(&mut publisher, &mut peers), [cancelled("x")]);
    }

    #[test]
    fn xpub_ignores_cancels_for_topics_never_subscribed() {
        let (a, mut sa) = connection(10);
        let mut peers = list(vec![a]);
        let mut publisher = Publisher::with_events();
        sa.to_socket.try_send(cancel("x")).unwrap();
        assert_eq!(events(&mut publisher, &mut peers), []);
    }

    #[test]
    fn a_peer_cannot_cancel_another_peers_subscription() {
        let (a, mut sa) = connection(10);
        let (b, mut sb) = connection(10);
        let mut peers = list(vec![a, b]);
        let mut publisher = Publisher::with_events();
        sa.to_socket.try_send(subscribe("x")).unwrap();
        sb.to_socket.try_send(cancel("x")).unwrap();
        assert_eq!(events(&mut publisher, &mut peers), [subscribed("x")]);
        assert_eq!(publisher.send(&mut peers, msg("x")), 1);
        assert_eq!(drain(&mut sa), [msg("x")]);
    }

    #[test]
    fn xpub_cancels_the_topics_of_a_peer_that_goes_away() {
        let (a, mut sa) = connection(10);
        let (b, mut sb) = connection(10);
        let mut peers = list(vec![a, b]);
        let mut publisher = Publisher::with_events();
        sa.to_socket.try_send(subscribe("mine")).unwrap();
        sa.to_socket.try_send(subscribe("shared")).unwrap();
        sb.to_socket.try_send(subscribe("shared")).unwrap();
        assert_eq!(events(&mut publisher, &mut peers).len(), 2);
        drop(sa);
        assert_eq!(events(&mut publisher, &mut peers), [cancelled("mine")]);
        assert_eq!(peers.len(), 1);
        assert_eq!(publisher.send(&mut peers, msg("shared")), 1);
        assert_eq!(drain(&mut sb), [msg("shared")]);
    }

    #[test]
    fn xpub_receives_other_messages_and_pub_discards_them() {
        let (a, mut sa) = connection(10);
        let mut peers = list(vec![a]);
        let mut xpub = Publisher::with_events();
        sa.to_socket.try_send(msg("upstream")).unwrap();
        assert_eq!(
            events(&mut xpub, &mut peers),
            [XPubEvent::Message(msg("upstream"))]
        );
        let (b, mut sb) = connection(10);
        let mut peers = list(vec![b]);
        let mut publisher = Publisher::default();
        sb.to_socket.try_send(msg("upstream")).unwrap();
        sb.to_socket.try_send(subscribe("x")).unwrap();
        assert_eq!(events(&mut publisher, &mut peers), []);
        assert_eq!(publisher.send(&mut peers, msg("x")), 1);
    }

    #[test]
    fn xpub_events_queued_while_sending_are_kept() {
        let (a, mut sa) = connection(10);
        let mut peers = list(vec![a]);
        let mut publisher = Publisher::with_events();
        sa.to_socket.try_send(subscribe("x")).unwrap();
        assert_eq!(publisher.send(&mut peers, msg("x")), 1);
        assert_eq!(events(&mut publisher, &mut peers), [subscribed("x")]);
    }

    #[test]
    fn xpub_waits_for_events() {
        let (a, mut sa) = connection(10);
        let mut peers = list(vec![a]);
        let mut publisher = Publisher::with_events();
        let (count, waker) = waker();
        let mut cx = TaskContext::from_waker(&waker);
        assert_eq!(publisher.poll_event(&mut peers, &mut cx), Poll::Pending);
        sa.to_socket.try_send(subscribe("x")).unwrap();
        assert!(
            count.0.load(Ordering::SeqCst) > 0,
            "a subscription wakes it"
        );
        assert_eq!(
            publisher.poll_event(&mut peers, &mut cx),
            Poll::Ready(subscribed("x"))
        );
    }

    #[test]
    fn xpub_reports_a_legacy_peers_implicit_subscription() {
        let (a, sa) = connection(10);
        let mut peers = list(vec![a]);
        let mut publisher = Publisher::with_events();
        sa.establish_legacy_subscriber();
        assert_eq!(events(&mut publisher, &mut peers), [subscribed("")]);
        drop(sa);
        assert_eq!(events(&mut publisher, &mut peers), [cancelled("")]);
    }

    #[test]
    fn xsub_sends_messages_to_every_peer_and_counts_subscriptions() {
        let (a, mut sa) = connection(10);
        let (b, mut sb) = connection(10);
        let mut peers = list(vec![a, b]);
        let mut subscriber = Subscriber::default();
        subscriber.send_upstream(&mut peers, msg("hello"));
        subscriber.send_upstream(&mut peers, subscribe("x"));
        subscriber.send_upstream(&mut peers, subscribe("x"));
        subscriber.send_upstream(&mut peers, cancel("x"));
        subscriber.send_upstream(&mut peers, cancel("never"));
        assert!(subscriber.topics().contains(b"x"));
        let expected = [msg("hello"), subscribe("x"), subscribe("x")];
        assert_eq!(drain(&mut sa), expected);
        assert_eq!(drain(&mut sb), expected);
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
