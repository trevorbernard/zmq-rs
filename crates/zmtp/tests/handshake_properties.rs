//! Properties of version negotiation and the NULL handshake over generated
//! peers: both ends agree on what was negotiated however the traffic is
//! split, a downgrade happens exactly when it is allowed, and no octet of
//! the peer's traffic is lost or read twice.

use proptest::prelude::*;
use zmtp::{
    Decoder, DecoderConfig, Encoder, Established, Frame, Greeting, GreetingConfig, GreetingError,
    HandshakeError, Identity, Incoming, LegacyPeers, Mechanism, Message, NullHandshake,
    PeerGreeting, Role, SocketType, Version,
};

fn socket_type() -> impl Strategy<Value = SocketType> {
    prop::sample::select(SocketType::ALL.to_vec())
}

/// Up to 255 octets, never starting with the reserved `0x00`.
fn identity() -> impl Strategy<Value = Identity> {
    prop::collection::vec(1u8..=255, 0..=255)
        .prop_map(|octets| Identity::new(octets).expect("valid identity"))
}

fn mechanism() -> impl Strategy<Value = Mechanism> {
    prop_oneof![
        Just(Mechanism::Null {
            legacy_peers: LegacyPeers::Allow
        }),
        Just(Mechanism::Null {
            legacy_peers: LegacyPeers::Refuse
        }),
        Just(Mechanism::Plain(Role::Client)),
        Just(Mechanism::Plain(Role::Server)),
        Just(Mechanism::Curve(Role::Client)),
        Just(Mechanism::Curve(Role::Server)),
    ]
}

fn name(mechanism: Mechanism) -> &'static [u8] {
    match mechanism {
        Mechanism::Null { .. } => b"NULL",
        Mechanism::Plain(_) => b"PLAIN",
        Mechanism::Curve(_) => b"CURVE",
    }
}

fn as_server(mechanism: Mechanism) -> bool {
    matches!(
        mechanism,
        Mechanism::Plain(Role::Server) | Mechanism::Curve(Role::Server)
    )
}

/// Chunk sizes for delivering traffic, cycled.
fn chunks() -> impl Strategy<Value = Vec<usize>> {
    prop::collection::vec(prop_oneof![3 => 1usize..8, 1 => 8usize..200], 1..12)
}

/// The order traffic moves in: which way, and how much, cycled.
fn schedule() -> impl Strategy<Value = Vec<(bool, usize)>> {
    prop::collection::vec(
        (any::<bool>(), prop_oneof![3 => 1usize..8, 1 => 8usize..200]),
        1..16,
    )
}

fn message(frames: &[Vec<u8>]) -> Message {
    Message::try_from(
        frames
            .iter()
            .map(|f| Frame::copy_from_slice(f))
            .collect::<Vec<_>>(),
    )
    .expect("at least one frame")
}

fn messages() -> impl Strategy<Value = Vec<Vec<Vec<u8>>>> {
    prop::collection::vec(
        prop::collection::vec(prop::collection::vec(any::<u8>(), 0..300), 1..3),
        0..4,
    )
}

/// One end of a connection between two of our own sockets: a greeting, then
/// optionally the NULL handshake, then messages.
struct End {
    socket_type: SocketType,
    identity: Identity,
    greeting: Greeting,
    handshake: Option<NullHandshake>,
    decoder: Option<Decoder>,
    /// Octets written and not yet delivered to the other end.
    outbox: Vec<u8>,
    /// Octets delivered and not yet taken by the greeting.
    inbox: Vec<u8>,
    run_handshake: bool,
    peer: Option<Result<PeerGreeting, GreetingError>>,
    established: Option<Result<Established, HandshakeError>>,
    /// Messages to send once established.
    to_send: Vec<Message>,
    /// What arrived after the handshake.
    after: Vec<Incoming>,
}

impl End {
    fn new(
        socket_type: SocketType,
        identity: Identity,
        mechanism: Mechanism,
        run_handshake: bool,
        to_send: Vec<Message>,
    ) -> Self {
        let mut greeting = Greeting::new(GreetingConfig {
            socket_type,
            identity: identity.clone(),
            mechanism,
        });
        let mut outbox = Vec::new();
        greeting.write_output(&mut outbox);
        Self {
            socket_type,
            identity,
            greeting,
            handshake: None,
            decoder: None,
            outbox,
            inbox: Vec::new(),
            run_handshake,
            peer: None,
            established: None,
            to_send,
            after: Vec::new(),
        }
    }

    fn receive(&mut self, octets: &[u8]) {
        if matches!(self.peer, Some(Err(_))) || matches!(self.established, Some(Err(_))) {
            return;
        }
        if self.peer.is_none() {
            self.inbox.extend_from_slice(octets);
            match self.greeting.feed(&self.inbox) {
                Ok(feed) => {
                    self.greeting.write_output(&mut self.outbox);
                    let rest = self.inbox.split_off(feed.consumed);
                    self.inbox = rest;
                    let Some(peer) = feed.peer else { return };
                    self.peer = Some(Ok(peer.clone()));
                    if !self.run_handshake {
                        return;
                    }
                    let start = NullHandshake::start(
                        self.socket_type,
                        &self.identity,
                        &peer,
                        &mut self.outbox,
                    );
                    let handshake = start.expect("ZMTP 3.x peers are never refused at start");
                    self.handshake = Some(handshake);
                    let mut decoder = Decoder::new(peer.version(), DecoderConfig::default());
                    decoder.extend_from_slice(&std::mem::take(&mut self.inbox));
                    self.decoder = Some(decoder);
                }
                Err(err) => {
                    self.peer = Some(Err(err));
                    return;
                }
            }
        } else if let Some(decoder) = &mut self.decoder {
            decoder.extend_from_slice(octets);
        }
        self.decode();
    }

    fn decode(&mut self) {
        let Some(decoder) = &mut self.decoder else {
            return;
        };
        while let Some(incoming) = decoder.decode().expect("well-formed traffic") {
            match self.handshake.take() {
                Some(handshake) => {
                    let result = handshake.receive(incoming, &mut self.outbox);
                    if result.is_ok() {
                        let encoder = Encoder::new(Version::V3_1);
                        for message in self.to_send.drain(..) {
                            encoder.encode(&message, &mut self.outbox);
                        }
                    }
                    let failed = result.is_err();
                    self.established = Some(result);
                    if failed {
                        return;
                    }
                }
                None => self.after.push(incoming),
            }
        }
    }
}

/// Moves traffic both ways in the order `schedule` gives until neither end
/// has anything left to send.
fn connect(a: &mut End, b: &mut End, schedule: &[(bool, usize)]) {
    let mut steps = schedule.iter().cycle();
    while !(a.outbox.is_empty() && b.outbox.is_empty()) {
        let &(a_to_b, size) = steps.next().expect("cycled");
        let a_to_b = if a.outbox.is_empty() {
            false
        } else if b.outbox.is_empty() {
            true
        } else {
            a_to_b
        };
        let (from, to) = if a_to_b {
            (&mut *a, &mut *b)
        } else {
            (&mut *b, &mut *a)
        };
        let n = size.min(from.outbox.len());
        let chunk: Vec<u8> = from.outbox.drain(..n).collect();
        to.receive(&chunk);
    }
}

/// A peer that is not one of ours, as its octets on the wire.
#[derive(Debug, Clone)]
enum Foreign {
    /// ZMTP 1.0 without a greeting: its identity frame, then a message.
    Unversioned { identity: Vec<u8>, then: Vec<u8> },
    /// A 12-octet ZMTP 1.0 (revision 0) or 2.0 (revision 1 or 2) greeting.
    Legacy {
        padding: [u8; 8],
        revision: u8,
        socket_type: u8,
    },
    /// A ZMTP 3.x greeting.
    Zmtp3 {
        minor: u8,
        mechanism: Mechanism,
        then: Vec<u8>,
    },
}

fn foreign() -> impl Strategy<Value = Foreign> {
    prop_oneof![
        // Short identities and long ones, whose frame starts with 0xFF.
        (
            prop::collection::vec(any::<u8>(), 0..300),
            prop::collection::vec(any::<u8>(), 0..20)
        )
            .prop_map(|(identity, then)| Foreign::Unversioned { identity, then }),
        (
            any::<[u8; 8]>(),
            0u8..=2,
            prop_oneof![0u8..=10, any::<u8>()]
        )
            .prop_map(|(padding, revision, socket_type)| Foreign::Legacy {
                padding,
                revision,
                socket_type
            }),
        (
            0u8..=2,
            mechanism(),
            prop::collection::vec(any::<u8>(), 0..20)
        )
            .prop_map(|(minor, mechanism, then)| Foreign::Zmtp3 {
                minor,
                mechanism,
                then
            }),
    ]
}

impl Foreign {
    fn octets(&self) -> Vec<u8> {
        match self {
            Self::Unversioned { identity, then } => {
                let encoder = Encoder::new(Version::V1);
                let mut out = Vec::new();
                encoder.encode(&message(std::slice::from_ref(identity)), &mut out);
                encoder.encode(&message(std::slice::from_ref(then)), &mut out);
                out
            }
            Self::Legacy {
                padding,
                revision,
                socket_type,
            } => {
                let mut out = vec![0xFF];
                out.extend_from_slice(padding);
                out.extend_from_slice(&[0x7F, *revision, *socket_type]);
                out
            }
            Self::Zmtp3 {
                minor,
                mechanism,
                then,
            } => {
                let mut out = vec![0xFF, 0, 0, 0, 0, 0, 0, 0, 1, 0x7F, 3, *minor];
                let mut field = [0u8; 20];
                field[..name(*mechanism).len()].copy_from_slice(name(*mechanism));
                out.extend_from_slice(&field);
                out.push(u8::from(as_server(*mechanism)));
                out.extend_from_slice(&[0; 31]);
                out.extend_from_slice(then);
                out
            }
        }
    }
}

proptest! {
    #![proptest_config(ProptestConfig::with_cases(512))]

    /// Two of our sockets agree on ZMTP 3.1 and on each other's role when
    /// their mechanisms match, however the greetings are split. When they
    /// don't, neither succeeds: an end that refuses closes without sending
    /// the rest of its greeting, as the engine does, so the other end may
    /// never see enough to refuse too.
    #[test]
    fn our_greetings_agree(
        (a_type, b_type) in (socket_type(), socket_type()),
        (a_id, b_id) in (identity(), identity()),
        (a_mech, b_mech) in (mechanism(), mechanism()),
        schedule in schedule(),
    ) {
        let mut a = End::new(a_type, a_id, a_mech, false, Vec::new());
        let mut b = End::new(b_type, b_id, b_mech, false, Vec::new());
        connect(&mut a, &mut b, &schedule);
        if name(a_mech) == name(b_mech) {
            let expect = |m| Some(Ok(PeerGreeting::Zmtp3 { major: 3, minor: 1, as_server: as_server(m) }));
            prop_assert_eq!(&a.peer, &expect(b_mech));
            prop_assert_eq!(&b.peer, &expect(a_mech));
            prop_assert!(a.inbox.is_empty() && b.inbox.is_empty(), "nothing after the greeting was taken");
        } else {
            let refused = |peer: &Option<Result<PeerGreeting, GreetingError>>| {
                matches!(peer, Some(Err(GreetingError::MechanismMismatch { .. })))
            };
            prop_assert!(refused(&a.peer) || refused(&b.peer), "nobody refused: {:?} {:?}", a.peer, b.peer);
            for peer in [&a.peer, &b.peer] {
                prop_assert!(peer.is_none() || refused(peer), "expected a mismatch, got {:?}", peer);
            }
        }
    }

    /// Against any older or newer peer we speak its version, or refuse
    /// exactly when a downgrade is not allowed, and never take an octet
    /// past its greeting: what an unversioned ZMTP 1.0 peer sent is
    /// replayed whole.
    #[test]
    fn foreign_greetings_negotiate_or_refuse(
        our_type in socket_type(),
        our_id in identity(),
        our_mech in mechanism(),
        peer in foreign(),
        chunks in chunks(),
    ) {
        let input = peer.octets();
        let mut greeting = Greeting::new(GreetingConfig {
            socket_type: our_type,
            identity: our_id.clone(),
            mechanism: our_mech,
        });
        let mut ours = Vec::new();
        let mut consumed = 0;
        let mut sizes = chunks.iter().cycle();
        let outcome = loop {
            greeting.write_output(&mut ours);
            if consumed == input.len() {
                break None;
            }
            let end = (consumed + sizes.next().expect("cycled")).min(input.len());
            let fed = &input[consumed..end];
            match greeting.feed(fed) {
                Ok(feed) => {
                    prop_assert!(feed.consumed <= fed.len());
                    consumed += feed.consumed;
                    if let Some(peer) = feed.peer {
                        greeting.write_output(&mut ours);
                        break Some(Ok(peer));
                    }
                    prop_assert_eq!(feed.consumed, fed.len(), "all input is taken until the greeting ends");
                }
                Err(err) => break Some(Err(err)),
            }
        };
        let downgrade = our_mech == Mechanism::Null { legacy_peers: LegacyPeers::Allow }
            && our_type.legacy_code().is_some();
        match &peer {
            Foreign::Unversioned { identity, then } => {
                if !downgrade {
                    prop_assert_eq!(outcome, Some(Err(GreetingError::LegacyPeerRefused)));
                    return Ok(());
                }
                let Some(Ok(PeerGreeting::Zmtp1Unversioned { replay })) = outcome else {
                    return Err(TestCaseError::fail(format!("expected ZMTP 1.0, got {outcome:?}")));
                };
                prop_assert_eq!(&*replay, &input[..consumed], "replay is exactly what was taken");
                let mut decoder = Decoder::new(Version::V1, DecoderConfig::default());
                decoder.extend_from_slice(&replay);
                decoder.extend_from_slice(&input[consumed..]);
                prop_assert_eq!(decoder.decode(), Ok(Some(Incoming::Message(message(std::slice::from_ref(identity))))));
                prop_assert_eq!(decoder.decode(), Ok(Some(Incoming::Message(message(std::slice::from_ref(then))))));
                let mut expected = vec![0xFF];
                expected.extend_from_slice(&(our_id.as_bytes().len() as u64 + 1).to_be_bytes());
                expected.push(0x7F);
                expected.extend_from_slice(our_id.as_bytes());
                prop_assert_eq!(ours, expected, "our identity frame follows the pseudo-signature");
            }
            Foreign::Legacy { revision, socket_type, .. } => {
                if !downgrade {
                    prop_assert_eq!(outcome, Some(Err(GreetingError::LegacyPeerRefused)));
                    return Ok(());
                }
                let expected = match revision {
                    0 => PeerGreeting::Zmtp1 { socket_type: *socket_type },
                    _ => PeerGreeting::Zmtp2 { socket_type: *socket_type },
                };
                prop_assert_eq!(outcome, Some(Ok(expected)));
                prop_assert_eq!(consumed, 12);
                prop_assert_eq!(ours.len(), 12);
                prop_assert_eq!(ours[10], 3, "we announce ZMTP 3");
                prop_assert_eq!(Some(ours[11]), our_type.legacy_code());
            }
            Foreign::Zmtp3 { minor, mechanism, then } => {
                if name(*mechanism) != name(our_mech) {
                    prop_assert!(matches!(outcome, Some(Err(GreetingError::MechanismMismatch { .. }))), "expected a mismatch, got {:?}", outcome);
                    return Ok(());
                }
                let peer = PeerGreeting::Zmtp3 { major: 3, minor: *minor, as_server: as_server(*mechanism) };
                prop_assert_eq!(peer.version(), if *minor == 0 { Version::V3_0 } else { Version::V3_1 });
                prop_assert_eq!(outcome, Some(Ok(peer)));
                prop_assert_eq!(consumed, input.len() - then.len(), "the greeting is 64 octets");
                prop_assert_eq!(ours.len(), 64);
            }
        }
    }

    /// A NULL handshake between two of our sockets succeeds on each side
    /// exactly when that side accepts the other's type, each learns the
    /// other's type and identity, and messages sent straight after the
    /// handshake arrive whole, however the traffic is split.
    #[test]
    fn null_handshakes_agree(
        (a_type, b_type) in (socket_type(), socket_type()),
        (a_id, b_id) in (identity(), identity()),
        (a_msgs, b_msgs) in (messages(), messages()),
        schedule in schedule(),
    ) {
        let null = Mechanism::Null { legacy_peers: LegacyPeers::Allow };
        let a_sent: Vec<Message> = a_msgs.iter().map(|m| message(m)).collect();
        let b_sent: Vec<Message> = b_msgs.iter().map(|m| message(m)).collect();
        let mut a = End::new(a_type, a_id.clone(), null, true, a_sent.clone());
        let mut b = End::new(b_type, b_id.clone(), null, true, b_sent.clone());
        connect(&mut a, &mut b, &schedule);
        for (end, peer_type, peer_id) in [(&a, b_type, &b_id), (&b, a_type, &a_id)] {
            let result = end.established.as_ref().expect("the handshake finished");
            if end.socket_type.accepts(peer_type) {
                let established = result.as_ref().expect("compatible peers are accepted");
                prop_assert_eq!(established.version, Version::V3_1);
                prop_assert_eq!(established.peer_socket_type, Some(peer_type));
                let expected_id = if peer_type.sends_identity() { peer_id.as_bytes() } else { &[] };
                prop_assert_eq!(&established.peer_identity[..], expected_id);
            } else {
                prop_assert_eq!(result, &Err(HandshakeError::IncompatiblePeer { peer: peer_type }));
            }
        }
        if a_type.accepts(b_type) && b_type.accepts(a_type) {
            let incoming = |sent: &[Message]| sent.iter().cloned().map(Incoming::Message).collect::<Vec<_>>();
            prop_assert_eq!(&a.after, &incoming(&b_sent));
            prop_assert_eq!(&b.after, &incoming(&a_sent));
        }
    }
}
