//! PUB and SUB routing (RFC 29), as in libzmq's `xpub_t` and `xsub_t`.
//!
//! Subscribers send their topics upstream as [`Subscription`] messages; a
//! publisher keeps each peer's topics and sends a message only to peers
//! subscribed to a prefix of its first frame. Subscribers filter too, so
//! publishers that do not (ZMTP 1.0) are harmless.

use std::collections::{BTreeMap, HashMap, HashSet, VecDeque, btree_map};
use std::hash::{BuildHasher, BuildHasherDefault, Hasher, RandomState};
use std::task::{Context as TaskContext, Poll};

use zmq_pipe::TryRecvError;
use zmtp::{Frame, Message, Subscription};

use crate::FairQueue;
use crate::socket::{PeerKey, PeerList};

/// Topics and how many times each was subscribed, matched by prefix.
#[derive(Debug, Clone, Default)]
pub struct Topics(PrefixMap<usize>);

impl Topics {
    /// Adds a subscription to `topic`. Returns whether it is new.
    pub fn add(&mut self, topic: &[u8]) -> bool {
        if let Some(count) = self.0.get_mut(topic) {
            *count += 1;
            return false;
        }
        self.0.insert(topic, 1);
        true
    }

    /// Removes one subscription to `topic`. Returns whether that was the
    /// last, so the topic is gone.
    pub fn remove(&mut self, topic: &[u8]) -> bool {
        let Some(count) = self.0.get_mut(topic) else {
            return false;
        };
        *count -= 1;
        if *count > 0 {
            return false;
        }
        self.0.remove(topic);
        true
    }

    /// Whether `topic` is subscribed.
    pub fn contains(&self, topic: &[u8]) -> bool {
        self.0.get(topic).is_some()
    }

    /// Whether a message whose first frame is `data` is wanted: some topic
    /// is a prefix of it.
    pub fn matches(&self, data: &[u8]) -> bool {
        self.0.prefixes_of(data).next().is_some()
    }

    /// The subscribed topics, in no particular order.
    pub fn iter(&self) -> impl Iterator<Item = &[u8]> {
        self.0.iter().map(|(topic, _)| topic)
    }

    /// Whether nothing is subscribed.
    pub fn is_empty(&self) -> bool {
        self.0.is_empty()
    }
}

/// A map from topics, searched for the topics that are prefixes of a
/// message.
///
/// Peers choose the topics, so a search must not cost more than linear time
/// in the message: hashing every prefix of it afresh would be quadratic in
/// the longest topic. Topics are kept by a hash fed one octet at a time, so
/// one pass over the message yields every prefix's hash.
#[derive(Debug, Clone)]
struct PrefixMap<V> {
    buckets: HashMap<u64, Bucket<V>, BuildHasherDefault<HashedKey>>,
    /// How many topics have each length, so a search looks up only the
    /// prefixes some topic is as long as.
    lengths: BTreeMap<usize, usize>,
    /// Keyed per map, so peers cannot aim topics at one bucket.
    hasher: RandomState,
}

/// The topics sharing a hash, with their values.
type Bucket<V> = Vec<(Box<[u8]>, V)>;

/// Maps and sets keyed by peer. We assign peer keys from a counter, so no
/// peer can choose one, and they need no protection from flooding.
type PeerMap<V> = HashMap<PeerKey, V, BuildHasherDefault<PeerKeyHasher>>;
type PeerSet = HashSet<PeerKey, BuildHasherDefault<PeerKeyHasher>>;

/// Spreads a peer key's bits with one multiplication (Fibonacci hashing).
#[derive(Debug, Clone, Copy, Default)]
struct PeerKeyHasher(u64);

impl Hasher for PeerKeyHasher {
    fn finish(&self) -> u64 {
        self.0
    }

    fn write(&mut self, bytes: &[u8]) {
        for &octet in bytes {
            self.write_u64(self.0.rotate_left(8) ^ u64::from(octet));
        }
    }

    fn write_u64(&mut self, key: u64) {
        self.0 = key.wrapping_mul(0x9E37_79B9_7F4A_7C15);
    }
}

/// Hashes `buckets`' keys, which are already keyed hashes, as themselves.
#[derive(Debug, Clone, Copy, Default)]
struct HashedKey(u64);

impl Hasher for HashedKey {
    fn finish(&self) -> u64 {
        self.0
    }

    fn write(&mut self, bytes: &[u8]) {
        for &octet in bytes {
            self.0 = self.0.rotate_left(8) ^ u64::from(octet);
        }
    }

    fn write_u64(&mut self, key: u64) {
        self.0 = key;
    }
}

impl<V> Default for PrefixMap<V> {
    fn default() -> Self {
        Self {
            buckets: HashMap::default(),
            lengths: BTreeMap::new(),
            hasher: RandomState::new(),
        }
    }
}

impl<V> PrefixMap<V> {
    fn get(&self, topic: &[u8]) -> Option<&V> {
        self.find(self.hash(topic), topic)
    }

    fn get_mut(&mut self, topic: &[u8]) -> Option<&mut V> {
        let hash = self.hash(topic);
        self.buckets
            .get_mut(&hash)?
            .iter_mut()
            .find(|(t, _)| **t == *topic)
            .map(|(_, value)| value)
    }

    /// Adds `topic`, which must be absent.
    fn insert(&mut self, topic: &[u8], value: V) {
        let hash = self.hash(topic);
        self.buckets
            .entry(hash)
            .or_default()
            .push((topic.into(), value));
        *self.lengths.entry(topic.len()).or_default() += 1;
    }

    fn remove(&mut self, topic: &[u8]) -> Option<V> {
        let hash = self.hash(topic);
        let bucket = self.buckets.get_mut(&hash)?;
        let at = bucket.iter().position(|(t, _)| **t == *topic)?;
        let (_, value) = bucket.swap_remove(at);
        if bucket.is_empty() {
            self.buckets.remove(&hash);
        }
        if let btree_map::Entry::Occupied(mut entry) = self.lengths.entry(topic.len()) {
            *entry.get_mut() -= 1;
            if *entry.get() == 0 {
                entry.remove();
            }
        }
        Some(value)
    }

    /// The values of the topics that are prefixes of `data`, shortest first.
    fn prefixes_of<'a>(&'a self, data: &'a [u8]) -> impl Iterator<Item = &'a V> {
        let mut state = self.hasher.build_hasher();
        let mut hashed = 0;
        self.lengths
            .range(..=data.len())
            .filter_map(move |(&len, _)| {
                for &octet in &data[hashed..len] {
                    state.write_u8(octet);
                }
                hashed = len;
                self.find(state.finish(), &data[..len])
            })
    }

    fn iter(&self) -> impl Iterator<Item = (&[u8], &V)> {
        self.buckets
            .values()
            .flatten()
            .map(|(topic, value)| (&**topic, value))
    }

    fn is_empty(&self) -> bool {
        self.buckets.is_empty()
    }

    /// Hashes octet by octet, as `prefixes_of` extends a prefix.
    fn hash(&self, topic: &[u8]) -> u64 {
        let mut state = self.hasher.build_hasher();
        for &octet in topic {
            state.write_u8(octet);
        }
        state.finish()
    }

    fn find(&self, hash: u64, topic: &[u8]) -> Option<&V> {
        self.buckets
            .get(&hash)?
            .iter()
            .find(|(t, _)| **t == *topic)
            .map(|(_, value)| value)
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

/// Which subscriptions and cancels an XPUB reports.
#[derive(Debug, Clone, Copy, PartialEq, Eq, PartialOrd, Ord, Default)]
pub enum Verbosity {
    /// A topic's first subscription and last cancel across all peers.
    #[default]
    Changes,
    /// Every subscription, even to topics already subscribed, and the
    /// last cancel (libzmq's `ZMQ_XPUB_VERBOSE`).
    Subscriptions,
    /// Every subscription and every cancel, including those of a peer
    /// that goes away (libzmq's `ZMQ_XPUB_VERBOSER`).
    Everything,
}

/// PUB's and XPUB's routing: each peer's topics, and fan-out to the peers
/// that match.
#[derive(Debug, Default)]
pub struct Publisher {
    subscribers: PeerMap<PeerTopics>,
    /// The peers subscribed to each topic, so that one pass over a message
    /// finds every peer it matches, however many there are.
    index: PeerIndex,
    /// The peers the message being sent matches, kept for its capacity.
    matched: PeerSet,
    /// What XPUB has yet to receive, and from which peer. `None` for PUB,
    /// which discards it.
    events: Option<VecDeque<(Option<PeerKey>, XPubEvent)>>,
    verbosity: Verbosity,
    /// Whether peers' subscriptions wait for [`apply`](Self::apply).
    manual: bool,
    /// Whether messages go to the peers they do not match instead.
    invert: bool,
}

#[derive(Debug, Default)]
struct PeerTopics {
    /// A set, as in libzmq's XPUB: subscribing twice is one subscription.
    topics: Topics,
    /// In manual mode, what the peer asked for, as a set.
    requested: Topics,
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

    /// A publisher whose peers' subscriptions are only reported, each
    /// with the peer that sent it, until [`apply`](Self::apply) decides
    /// them (libzmq's `ZMQ_XPUB_MANUAL`).
    pub fn manual() -> Self {
        Self {
            manual: true,
            ..Self::with_events()
        }
    }

    /// Which subscriptions and cancels become events.
    pub fn set_verbosity(&mut self, verbosity: Verbosity) {
        self.verbosity = verbosity;
    }

    /// Whether messages go to the peers subscribed to no prefix of them
    /// instead (libzmq's `ZMQ_INVERT_MATCHING`).
    pub fn set_invert_matching(&mut self, on: bool) {
        self.invert = on;
    }

    /// The next event, or `Pending` with `cx` registered on every peer.
    /// Always `Pending` for a publisher made without events.
    pub fn poll_event(
        &mut self,
        peers: &mut PeerList,
        cx: &mut TaskContext<'_>,
    ) -> Poll<XPubEvent> {
        self.poll_request(peers, cx).map(|(_, event)| event)
    }

    /// The next event and the peer it came from, if it is still here, or
    /// `Pending` with `cx` registered on every peer.
    pub fn poll_request(
        &mut self,
        peers: &mut PeerList,
        cx: &mut TaskContext<'_>,
    ) -> Poll<(Option<PeerKey>, XPubEvent)> {
        self.drain(peers, Some(cx));
        match self.events.as_mut().and_then(VecDeque::pop_front) {
            Some(request) => Poll::Ready(request),
            None => Poll::Pending,
        }
    }

    /// Subscribes or unsubscribes the peer with `key`, for a manual XPUB.
    /// Returns whether that peer is still here.
    pub fn apply(&mut self, key: PeerKey, subscription: Subscription) -> bool {
        let Some(entry) = self.subscribers.get_mut(&key) else {
            return false;
        };
        match subscription {
            Subscription::Subscribe(topic) => {
                if !entry.topics.contains(&topic) {
                    entry.topics.add(&topic);
                    join(&mut self.index, &topic, key);
                }
            }
            Subscription::Cancel(topic) => {
                if entry.topics.remove(&topic) {
                    leave(&mut self.index, &topic, key);
                }
            }
        }
        true
    }

    /// Sends `message` to the peer with `key` alone, if it wants it and has
    /// room (libzmq's `ZMQ_XPUB_MANUAL_LAST_VALUE`). Returns whether it was
    /// queued.
    pub fn send_to(&mut self, peers: &mut PeerList, key: PeerKey, message: Message) -> bool {
        self.wants(key, &message)
            && peers
                .get_mut(key)
                .is_some_and(|peer| peer.connection.tx.try_send(message).is_ok())
    }

    /// Applies the subscriptions peers have sent, and forgets peers that
    /// are gone.
    pub fn sync(&mut self, peers: &mut PeerList) {
        self.drain(peers, None);
    }

    /// Reads everything peers have sent, registering `cx`, if given, with
    /// each. Without one, nothing is registered, so a waker registered
    /// earlier stays in place.
    fn drain(&mut self, peers: &mut PeerList, mut cx: Option<&mut TaskContext<'_>>) {
        let Self {
            subscribers,
            index,
            events,
            verbosity,
            manual,
            ..
        } = self;
        let mut changes = Changes {
            index,
            events,
            verbosity: *verbosity,
            manual: *manual,
            from: None,
        };
        peers.retain_mut(|peer| {
            let entry = subscribers.entry(peer.key()).or_default();
            changes.from = Some(peer.key());
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
                let next = match cx.as_deref_mut() {
                    Some(cx) => peer.connection.rx.poll_recv(cx),
                    None => match peer.connection.rx.try_recv() {
                        Ok(message) => Poll::Ready(Some(message)),
                        Err(TryRecvError::Empty) => Poll::Pending,
                        Err(TryRecvError::Closed) => Poll::Ready(None),
                    },
                };
                match next {
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
        self.match_peers(&message);
        let mut sent = 0;
        for peer in peers.iter_mut() {
            if self.wanted(peer.key()) && peer.connection.tx.try_send(message.clone()).is_ok() {
                sent += 1;
            }
        }
        sent
    }

    /// Sends the message in `slot` to every peer subscribed to it once all
    /// of them have room, as libzmq's `ZMQ_XPUB_NODROP`, returning how many
    /// it went to. Until then `Pending`, with `cx` registered.
    pub fn poll_send_all(
        &mut self,
        peers: &mut PeerList,
        slot: &mut Option<Message>,
        cx: &mut TaskContext<'_>,
    ) -> Poll<usize> {
        self.drain(peers, Some(cx));
        let Some(message) = slot.take() else {
            return Poll::Ready(0);
        };
        self.match_peers(&message);
        let mut full = false;
        for peer in peers.iter_mut() {
            if self.wanted(peer.key()) && peer.connection.tx.poll_ready(cx).is_pending() {
                full = true;
            }
        }
        if full {
            *slot = Some(message);
            return Poll::Pending;
        }
        Poll::Ready(self.send(peers, message))
    }

    /// Whether the peer with `key` should get `message`.
    fn wants(&self, key: PeerKey, message: &Message) -> bool {
        let topic = message.split_first().0.as_slice();
        self.subscribers
            .get(&key)
            .is_some_and(|entry| entry.topics.matches(topic) != self.invert)
    }

    /// Finds the peers subscribed to a prefix of `message`, for
    /// [`wanted`](Self::wanted).
    fn match_peers(&mut self, message: &Message) {
        self.matched.clear();
        let topic = message.split_first().0.as_slice();
        for peers in self.index.prefixes_of(topic) {
            self.matched.extend(peers);
        }
    }

    /// Whether the peer with `key` should get the message last given to
    /// [`match_peers`](Self::match_peers).
    fn wanted(&self, key: PeerKey) -> bool {
        self.subscribers.contains_key(&key) && self.matched.contains(&key) != self.invert
    }
}

type PeerIndex = PrefixMap<PeerSet>;

/// Adds `key` to the peers subscribed to `topic`. Returns whether it is
/// the first.
fn join(index: &mut PeerIndex, topic: &[u8], key: PeerKey) -> bool {
    match index.get_mut(topic) {
        Some(peers) => {
            peers.insert(key);
            false
        }
        None => {
            index.insert(topic, PeerSet::from_iter([key]));
            true
        }
    }
}

/// Removes `key` from the peers subscribed to `topic`. Returns whether it
/// was the last.
fn leave(index: &mut PeerIndex, topic: &[u8], key: PeerKey) -> bool {
    let Some(peers) = index.get_mut(topic) else {
        return false;
    };
    peers.remove(&key);
    if !peers.is_empty() {
        return false;
    }
    index.remove(topic);
    true
}

/// Applies what one peer sent to the topics of all peers, noting events.
struct Changes<'a> {
    index: &'a mut PeerIndex,
    events: &'a mut Option<VecDeque<(Option<PeerKey>, XPubEvent)>>,
    verbosity: Verbosity,
    manual: bool,
    /// The peer being read.
    from: Option<PeerKey>,
}

impl Changes<'_> {
    fn subscribe(&mut self, peer: &mut PeerTopics, topic: Frame) {
        if self.manual {
            if !peer.requested.contains(&topic) {
                peer.requested.add(&topic);
            }
            self.event(XPubEvent::Subscribe(topic));
            return;
        }
        let first = !peer.topics.contains(&topic) && {
            peer.topics.add(&topic);
            join(self.index, &topic, self.key())
        };
        if first || self.verbosity >= Verbosity::Subscriptions {
            self.event(XPubEvent::Subscribe(topic));
        }
    }

    fn cancel(&mut self, peer: &mut PeerTopics, topic: Frame) {
        if self.manual {
            peer.requested.remove(&topic);
            self.event(XPubEvent::Cancel(topic));
            return;
        }
        if !peer.topics.remove(&topic) {
            return;
        }
        if leave(self.index, &topic, self.key()) || self.verbosity == Verbosity::Everything {
            self.event(XPubEvent::Cancel(topic));
        }
    }

    fn message(&mut self, message: Message) {
        self.event(XPubEvent::Message(message));
    }

    /// A peer is gone: its subscriptions end with it.
    fn forget(&mut self, peer: PeerTopics) {
        let key = self.key();
        if self.manual {
            for topic in peer.topics.iter() {
                leave(self.index, topic, key);
            }
            for topic in peer.requested.iter() {
                self.event(XPubEvent::Cancel(Frame::copy_from_slice(topic)));
            }
            return;
        }
        for topic in peer.topics.iter() {
            if leave(self.index, topic, key) || self.verbosity == Verbosity::Everything {
                self.event(XPubEvent::Cancel(Frame::copy_from_slice(topic)));
            }
        }
    }

    fn key(&self) -> PeerKey {
        self.from.expect("set before each peer is read")
    }

    fn event(&mut self, event: XPubEvent) {
        if let Some(events) = self.events {
            events.push_back((self.from, event));
        }
    }
}

/// SUB's routing: the socket's topics, sent to every peer, and filtering
/// of what arrives.
#[derive(Debug, Default)]
pub struct Subscriber {
    topics: Topics,
    /// Whether wanted messages are those matching no topic.
    invert: bool,
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
            if self.topics.matches(message.split_first().0) != self.invert {
                return Poll::Ready(message);
            }
        }
    }

    /// Whether messages matching no topic are received instead (libzmq's
    /// `ZMQ_INVERT_MATCHING`).
    pub fn set_invert_matching(&mut self, on: bool) {
        self.invert = on;
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
mod properties;

#[cfg(test)]
mod tests {
    use super::*;
    use crate::test_support::*;
    use std::sync::atomic::Ordering;
    use std::task::Waker;

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

    // Hashing each prefix afresh took 7 s for the miss alone, optimized:
    // 2^18 prefixes of up to 2^18 octets.
    #[test]
    fn a_long_topic_matches_in_linear_time() {
        let mut topics = Topics::default();
        let long = vec![b'a'; 1 << 18];
        topics.add(&long);
        assert!(topics.matches(&long));
        let mut miss = long.clone();
        *miss.last_mut().unwrap() = b'b';
        assert!(!topics.matches(&miss));
        assert!(topics.remove(&long));
        assert!(!topics.matches(&long));
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
    fn sending_keeps_the_waker_waiting_for_events() {
        let (a, mut sa) = connection(10);
        let mut peers = list(vec![a]);
        let mut publisher = Publisher::with_events();
        let (count, waker) = waker();
        let mut cx = TaskContext::from_waker(&waker);
        assert_eq!(publisher.poll_event(&mut peers, &mut cx), Poll::Pending);
        publisher.send(&mut peers, msg("x"));
        sa.to_socket.try_send(subscribe("x")).unwrap();
        assert!(
            count.0.load(Ordering::SeqCst) > 0,
            "a subscription after a send still wakes it"
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
