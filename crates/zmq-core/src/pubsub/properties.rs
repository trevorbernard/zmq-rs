//! Properties of subscription matching and XPUB's reporting, checked
//! against naive models over generated traffic from several peers.

use std::collections::{BTreeMap, BTreeSet, VecDeque};
use std::task::Waker;

use proptest::prelude::*;

use super::*;
use crate::Peer;
use crate::test_support::*;

/// Short topics over a small alphabet, so that prefixes and repeats are
/// common.
fn topic() -> impl Strategy<Value = Vec<u8>> {
    prop::collection::vec(prop::sample::select(b"ab".to_vec()), 0..3)
}

fn data() -> impl Strategy<Value = Vec<u8>> {
    prop::collection::vec(prop::sample::select(b"abc".to_vec()), 0..4)
}

fn naive_match<'a>(mut topics: impl Iterator<Item = &'a Vec<u8>>, data: &[u8]) -> bool {
    topics.any(|topic| data.starts_with(topic))
}

#[derive(Debug, Clone)]
enum TopicsOp {
    Add(Vec<u8>),
    Remove(Vec<u8>),
    Match(Vec<u8>),
}

fn topics_op() -> impl Strategy<Value = TopicsOp> {
    prop_oneof![
        topic().prop_map(TopicsOp::Add),
        topic().prop_map(TopicsOp::Remove),
        data().prop_map(TopicsOp::Match),
    ]
}

#[derive(Debug, Clone, Copy)]
enum Mode {
    Pub,
    XPub(Verbosity),
    Manual,
}

fn mode() -> impl Strategy<Value = Mode> {
    prop_oneof![
        Just(Mode::Pub),
        Just(Mode::XPub(Verbosity::Changes)),
        Just(Mode::XPub(Verbosity::Subscriptions)),
        Just(Mode::XPub(Verbosity::Everything)),
        Just(Mode::Manual),
    ]
}

/// What a peer sends upstream.
#[derive(Debug, Clone)]
enum Upstream {
    Subscribe(Vec<u8>),
    Cancel(Vec<u8>),
    /// Anything that is not a subscription: several frames, or one not
    /// starting with 0 or 1.
    Other(Vec<Vec<u8>>),
}

impl Upstream {
    fn message(&self) -> Message {
        match self {
            Self::Subscribe(topic) => {
                Subscription::Subscribe(Frame::from(topic.clone())).to_message()
            }
            Self::Cancel(topic) => Subscription::Cancel(Frame::from(topic.clone())).to_message(),
            Self::Other(frames) => {
                Message::try_from(frames.iter().cloned().map(Frame::from).collect::<Vec<_>>())
                    .expect("at least one frame")
            }
        }
    }
}

#[derive(Debug, Clone)]
enum Op {
    Send(usize, Upstream),
    /// The peer's session goes away.
    Close(usize),
    /// The peer turns out to be a ZMTP 1.0 subscriber.
    Legacy(usize),
    Publish(Vec<u8>),
    Poll,
    /// A manual XPUB subscribes or unsubscribes a peer.
    Apply(usize, bool, Vec<u8>),
    /// A manual XPUB sends to one peer.
    SendTo(usize, Vec<u8>),
}

fn op(peers: usize, mode: Mode) -> BoxedStrategy<Op> {
    let peer = 0..peers;
    let other = prop_oneof![
        prop::collection::vec(any::<u8>(), 0..3).prop_map(|frame| vec![frame]),
        (2u8.., prop::collection::vec(any::<u8>(), 0..3)).prop_map(|(kind, mut frame)| {
            frame.insert(0, kind);
            vec![frame]
        }),
        prop::collection::vec(prop::collection::vec(any::<u8>(), 0..3), 2..3),
    ]
    .prop_filter("not a subscription", |frames| {
        frames.len() > 1 || !matches!(frames[0].first(), Some(0 | 1))
    });
    let upstream = prop_oneof![
        4 => topic().prop_map(Upstream::Subscribe),
        3 => topic().prop_map(Upstream::Cancel),
        1 => other.prop_map(Upstream::Other),
    ];
    let common = prop_oneof![
        8 => (peer.clone(), upstream).prop_map(|(i, sent)| Op::Send(i, sent)),
        1 => peer.clone().prop_map(Op::Close),
        1 => peer.clone().prop_map(Op::Legacy),
        3 => data().prop_map(Op::Publish),
        2 => Just(Op::Poll),
    ];
    match mode {
        Mode::Manual => prop_oneof![
            6 => common,
            2 => (peer.clone(), any::<bool>(), topic()).prop_map(|(i, on, t)| Op::Apply(i, on, t)),
            1 => (peer, data()).prop_map(|(i, data)| Op::SendTo(i, data)),
        ]
        .boxed(),
        _ => common.boxed(),
    }
}

fn scenario() -> impl Strategy<Value = (Mode, bool, usize, Vec<Op>)> {
    (mode(), any::<bool>(), 1usize..4).prop_flat_map(|(mode, invert, peers)| {
        (
            Just(mode),
            Just(invert),
            Just(peers),
            prop::collection::vec(op(peers, mode), 0..40),
        )
    })
}

#[derive(Debug, Default)]
struct ModelPeer {
    /// Sent and not yet read by the publisher.
    queued: Vec<Upstream>,
    topics: BTreeSet<Vec<u8>>,
    requested: BTreeSet<Vec<u8>>,
    /// The publisher has read from it, so it has an entry.
    known: bool,
    closed: bool,
    legacy: bool,
    settled: bool,
    /// Closed and noticed: out of the list.
    gone: bool,
    /// Waiting in `Model::turn` to be read.
    in_turn: bool,
}

/// RFC 29's PUB and XPUB as `docs/rfc-compliance.md` describes them.
#[derive(Debug)]
struct Model {
    mode: Mode,
    invert: bool,
    peers: Vec<ModelPeer>,
    /// Peers to read, in the order they woke: new ones, then each as it
    /// sends or closes after being read through.
    turn: VecDeque<usize>,
    /// How many peers subscribe to each topic.
    everyone: BTreeMap<Vec<u8>, usize>,
    events: Vec<(usize, XPubEvent)>,
}

impl Model {
    fn verbosity(&self) -> Verbosity {
        match self.mode {
            Mode::XPub(verbosity) => verbosity,
            _ => Verbosity::Changes,
        }
    }

    fn event(&mut self, from: usize, event: XPubEvent) {
        if !matches!(self.mode, Mode::Pub) {
            self.events.push((from, event));
        }
    }

    /// The peer sent something or closed: it is read again.
    fn wake(&mut self, i: usize) {
        if !self.peers[i].in_turn {
            self.peers[i].in_turn = true;
            self.turn.push_back(i);
        }
    }

    /// What reading peers does: ZMTP 1.0 peers are settled first, then each
    /// peer that woke is read through, in the order it woke. Scenarios send
    /// fewer messages than a drain batch, so one pass reads them all.
    fn sync(&mut self) {
        for i in 0..self.peers.len() {
            if self.peers[i].gone {
                continue;
            }
            self.peers[i].known = true;
            if self.peers[i].legacy && !self.peers[i].settled {
                self.peers[i].settled = true;
                self.subscribe(i, Vec::new());
            }
        }
        while let Some(i) = self.turn.pop_front() {
            self.peers[i].in_turn = false;
            for sent in std::mem::take(&mut self.peers[i].queued) {
                match sent {
                    Upstream::Subscribe(topic) => self.subscribe(i, topic),
                    Upstream::Cancel(topic) => self.cancel(i, topic),
                    Upstream::Other(frames) => {
                        self.event(i, XPubEvent::Message(Upstream::Other(frames).message()));
                    }
                }
            }
            if self.peers[i].closed {
                self.peers[i].gone = true;
                self.forget(i);
            }
        }
    }

    fn subscribe(&mut self, i: usize, topic: Vec<u8>) {
        if let Mode::Manual = self.mode {
            self.peers[i].requested.insert(topic.clone());
            self.event(i, XPubEvent::Subscribe(Frame::from(topic)));
            return;
        }
        let first = self.peers[i].topics.insert(topic.clone()) && {
            let count = self.everyone.entry(topic.clone()).or_default();
            *count += 1;
            *count == 1
        };
        if first || self.verbosity() >= Verbosity::Subscriptions {
            self.event(i, XPubEvent::Subscribe(Frame::from(topic)));
        }
    }

    fn cancel(&mut self, i: usize, topic: Vec<u8>) {
        if let Mode::Manual = self.mode {
            self.peers[i].requested.remove(&topic);
            self.event(i, XPubEvent::Cancel(Frame::from(topic)));
            return;
        }
        if self.peers[i].topics.remove(&topic) {
            self.drop_subscriber(i, topic);
        }
    }

    /// One fewer peer subscribes to `topic`.
    fn drop_subscriber(&mut self, i: usize, topic: Vec<u8>) {
        let count = self.everyone.get_mut(&topic).expect("subscribed");
        *count -= 1;
        let last = *count == 0;
        if last {
            self.everyone.remove(&topic);
        }
        if last || self.verbosity() == Verbosity::Everything {
            self.event(i, XPubEvent::Cancel(Frame::from(topic)));
        }
    }

    fn forget(&mut self, i: usize) {
        if let Mode::Manual = self.mode {
            for topic in std::mem::take(&mut self.peers[i].requested) {
                self.event(i, XPubEvent::Cancel(Frame::from(topic)));
            }
            return;
        }
        for topic in std::mem::take(&mut self.peers[i].topics) {
            self.drop_subscriber(i, topic);
        }
    }

    fn wants(&self, i: usize, data: &[u8]) -> bool {
        let peer = &self.peers[i];
        peer.known && !peer.gone && naive_match(peer.topics.iter(), data) != self.invert
    }
}

/// Events in order, except that a run of cancels from one peer is sorted:
/// a departing peer's topics are cancelled in no particular order.
fn normalized(mut events: Vec<(PeerKey, XPubEvent)>) -> Vec<(PeerKey, XPubEvent)> {
    let cancel_from = |event: &(PeerKey, XPubEvent), key: PeerKey| {
        event.0 == key && matches!(event.1, XPubEvent::Cancel(_))
    };
    let mut start = 0;
    while start < events.len() {
        let key = events[start].0;
        let end = start
            + events[start..]
                .iter()
                .take_while(|e| cancel_from(e, key))
                .count();
        events[start..end].sort_by(|a, b| match (&a.1, &b.1) {
            (XPubEvent::Cancel(a), XPubEvent::Cancel(b)) => a.as_slice().cmp(b.as_slice()),
            _ => unreachable!("a run of cancels"),
        });
        start = end.max(start + 1);
    }
    events
}

fn take_events(publisher: &mut Publisher, peers: &mut PeerList) -> Vec<(PeerKey, XPubEvent)> {
    let mut cx = TaskContext::from_waker(Waker::noop());
    std::iter::from_fn(|| match publisher.poll_request(peers, &mut cx) {
        Poll::Ready(request) => Some(request),
        Poll::Pending => None,
    })
    .collect()
}

proptest! {
    #![proptest_config(ProptestConfig::with_cases(1024))]

    /// `Topics` is a counted set of topics matched by prefix.
    #[test]
    fn topics_match_a_counted_set(ops in prop::collection::vec(topics_op(), 0..60)) {
        let mut topics = Topics::default();
        let mut model: BTreeMap<Vec<u8>, usize> = BTreeMap::new();
        for op in ops {
            match op {
                TopicsOp::Add(topic) => {
                    let count = model.entry(topic.clone()).or_default();
                    *count += 1;
                    prop_assert_eq!(topics.add(&topic), *count == 1);
                }
                TopicsOp::Remove(topic) => {
                    let last = match model.get_mut(&topic) {
                        None => false,
                        Some(count) => {
                            *count -= 1;
                            *count == 0
                        }
                    };
                    if last {
                        model.remove(&topic);
                    }
                    prop_assert_eq!(topics.remove(&topic), last);
                }
                TopicsOp::Match(data) => {
                    prop_assert_eq!(topics.matches(&data), naive_match(model.keys(), &data));
                }
            }
            prop_assert_eq!(topics.is_empty(), model.is_empty());
            let mut listed: Vec<&[u8]> = topics.iter().collect();
            listed.sort();
            prop_assert_eq!(listed, model.keys().map(Vec::as_slice).collect::<Vec<_>>());
            for topic in [&b""[..], b"a", b"b", b"ab", b"ba"] {
                prop_assert_eq!(topics.contains(topic), model.contains_key(topic));
            }
        }
    }

    /// A publisher's events and fan-out follow its peers' subscriptions as
    /// the model says, whatever order peers subscribe, cancel, leave and
    /// publish in, for PUB, each XPUB verbosity, and manual XPUB.
    #[test]
    fn publishers_follow_the_model((mode, invert, count, ops) in scenario()) {
        let (connections, mut sessions): (Vec<_>, Vec<_>) =
            (0..count).map(|_| connection(10_000)).map(|(c, s)| (c, Some(s))).unzip();
        let mut peers = list(connections);
        let keys: Vec<PeerKey> = peers.iter().map(Peer::key).collect();
        let mut publisher = match mode {
            Mode::Pub => Publisher::default(),
            Mode::XPub(verbosity) => {
                let mut publisher = Publisher::with_events();
                publisher.set_verbosity(verbosity);
                publisher
            }
            Mode::Manual => Publisher::manual(),
        };
        publisher.set_invert_matching(invert);
        let mut model = Model {
            mode,
            invert,
            peers: (0..count)
                .map(|_| ModelPeer {
                    in_turn: true,
                    ..ModelPeer::default()
                })
                .collect(),
            turn: (0..count).collect(),
            everyone: BTreeMap::new(),
            events: Vec::new(),
        };
        let expected_events = |model: &mut Model| {
            let events = std::mem::take(&mut model.events);
            normalized(events.into_iter().map(|(i, event)| (keys[i], event)).collect())
        };

        for op in ops.into_iter().chain([Op::Poll]) {
            match op {
                Op::Send(i, sent) => {
                    let Some(session) = &mut sessions[i] else { continue };
                    session.to_socket.try_send(sent.message()).expect("room");
                    model.peers[i].queued.push(sent);
                    model.wake(i);
                }
                Op::Close(i) => {
                    if sessions[i].take().is_some() {
                        model.peers[i].closed = true;
                        model.wake(i);
                    }
                }
                Op::Legacy(i) => {
                    let Some(session) = &sessions[i] else { continue };
                    session.establish_legacy_subscriber();
                    model.peers[i].legacy = true;
                }
                Op::Publish(data) => {
                    let sent = publisher.send(&mut peers, Message::new(Frame::from(data.clone())));
                    model.sync();
                    let wanted: Vec<usize> = (0..count).filter(|&i| model.wants(i, &data)).collect();
                    prop_assert_eq!(sent, wanted.len());
                    for (i, session) in sessions.iter_mut().enumerate() {
                        let Some(session) = session else { continue };
                        let expected = if wanted.contains(&i) {
                            vec![Message::new(Frame::from(data.clone()))]
                        } else {
                            Vec::new()
                        };
                        prop_assert_eq!(drain(session), expected, "peer {}", i);
                    }
                }
                Op::Poll => {
                    let events = normalized(take_events(&mut publisher, &mut peers));
                    model.sync();
                    prop_assert_eq!(events, expected_events(&mut model));
                }
                Op::Apply(i, on, topic) => {
                    let subscription = if on {
                        Subscription::Subscribe(Frame::from(topic.clone()))
                    } else {
                        Subscription::Cancel(Frame::from(topic.clone()))
                    };
                    let peer = &mut model.peers[i];
                    let present = peer.known && !peer.gone;
                    if present {
                        if on {
                            peer.topics.insert(topic);
                        } else {
                            peer.topics.remove(&topic);
                        }
                    }
                    prop_assert_eq!(publisher.apply(keys[i], subscription), present);
                }
                Op::SendTo(i, data) => {
                    let message = Message::new(Frame::from(data.clone()));
                    let queued = publisher.send_to(&mut peers, keys[i], message.clone());
                    let expected = sessions[i].is_some() && model.wants(i, &data);
                    prop_assert_eq!(queued, expected);
                    if let Some(session) = &mut sessions[i] {
                        let expected = if queued { vec![message] } else { Vec::new() };
                        prop_assert_eq!(drain(session), expected);
                    }
                }
            }
        }
    }
}
