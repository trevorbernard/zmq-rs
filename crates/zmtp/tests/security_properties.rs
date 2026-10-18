//! Properties of the PLAIN and CURVE handshakes between two of our sockets:
//! the outcome on each side follows from the credentials, the ZAP verdict
//! and the socket types alone, however the traffic is split; and CURVE
//! traffic after the handshake arrives intact or not at all.

use proptest::prelude::*;
use zmtp::{
    Authenticated, ClientStep, Credential, CurveClient, CurveClientStep, CurveCodec, CurveError,
    CurveServer, CurveServerConfig, CurveServerStep, DecodeError, Decoder, DecoderConfig, Encoder,
    ErrorReason, Established, Frame, HandshakeError, Identity, Incoming, Keypair, Message,
    Metadata, PeerGreeting, PlainClient, PlainCredentials, PlainServer, PlainServerConfig,
    PropertyName, SecretKey, ServerStep, SocketType, Version, ZapCredentials, ZapFailure, ZapReply,
    ZapRequest,
};

fn socket_type() -> impl Strategy<Value = SocketType> {
    prop::sample::select(SocketType::ALL.to_vec())
}

/// Up to 255 octets, never starting with the reserved `0x00`.
fn identity() -> impl Strategy<Value = Identity> {
    prop::collection::vec(1u8..=255, 0..=255)
        .prop_map(|octets| Identity::new(octets).expect("valid identity"))
}

/// The order traffic moves in: which way, and how much, cycled.
fn schedule() -> impl Strategy<Value = Vec<(bool, usize)>> {
    prop::collection::vec(
        (any::<bool>(), prop_oneof![3 => 1usize..8, 1 => 8usize..200]),
        1..16,
    )
}

/// Half of them pairs that accept each other.
fn socket_types() -> impl Strategy<Value = (SocketType, SocketType)> {
    prop_oneof![
        (socket_type(), socket_type()),
        (socket_type(), socket_type())
            .prop_filter("compatible", |&(a, b)| a.accepts(b) && b.accepts(a)),
    ]
}

fn credential() -> impl Strategy<Value = Credential> {
    prop::collection::vec(any::<u8>(), 0..=255)
        .prop_map(|octets| Credential::new(octets).expect("at most 255 octets"))
}

fn keypair() -> impl Strategy<Value = Keypair> {
    any::<[u8; 32]>().prop_map(|secret| Keypair::from_secret(SecretKey::from_bytes(secret)))
}

#[derive(Debug)]
enum Security {
    Plain(PlainCredentials),
    Curve {
        client: Keypair,
        server: Keypair,
        /// The client trusts some other server key.
        wrong_server_key: bool,
    },
}

fn security() -> impl Strategy<Value = Security> {
    prop_oneof![
        (credential(), credential()).prop_map(|(username, password)| {
            Security::Plain(PlainCredentials { username, password })
        }),
        (keypair(), keypair(), prop::bool::weighted(0.1)).prop_map(
            |(client, server, wrong_server_key)| Security::Curve {
                client,
                server,
                wrong_server_key,
            }
        ),
    ]
}

fn metadata() -> impl Strategy<Value = Metadata> {
    let name = prop::sample::select(vec!["X-Role", "X-Tenant", "User-Id"]);
    prop::collection::vec((name, prop::collection::vec(any::<u8>(), 0..40)), 0..3).prop_map(
        |properties| {
            let mut metadata = Metadata::new();
            for (name, value) in properties {
                let name = PropertyName::new(name).expect("valid name");
                metadata.insert(name, value).expect("short value");
            }
            metadata
        },
    )
}

fn verdict() -> impl Strategy<Value = ZapReply> {
    prop_oneof![
        5 => (prop::collection::vec(any::<u8>(), 0..20), metadata()).prop_map(
            |(user_id, metadata)| ZapReply::Ok { user_id: Frame::from(user_id), metadata }
        ),
        1 => Just(ZapReply::TemporaryFailure),
        1 => Just(ZapReply::AuthenticationFailure),
        1 => Just(ZapReply::InternalError),
    ]
}

/// What a side sends once established.
#[derive(Debug, Clone)]
enum Item {
    Message(Vec<Vec<u8>>),
    Command(Vec<u8>),
}

impl Item {
    fn incoming(&self) -> Incoming {
        match self {
            Self::Message(frames) => Incoming::Message(
                Message::try_from(
                    frames
                        .iter()
                        .map(|f| Frame::copy_from_slice(f))
                        .collect::<Vec<_>>(),
                )
                .expect("at least one frame"),
            ),
            Self::Command(body) => Incoming::Command(Frame::copy_from_slice(body)),
        }
    }
}

fn items() -> impl Strategy<Value = Vec<Item>> {
    let frame = || prop::collection::vec(any::<u8>(), 0..300);
    prop::collection::vec(
        prop_oneof![
            4 => prop::collection::vec(frame(), 1..4).prop_map(Item::Message),
            1 => frame().prop_map(Item::Command),
        ],
        0..5,
    )
}

enum Handshake {
    PlainClient(PlainClient),
    PlainServer(PlainServer),
    CurveClient(CurveClient),
    CurveServer(CurveServer),
}

/// One end of a connection after the greeting.
struct End {
    decoder: Decoder,
    encoder: Encoder,
    handshake: Option<Handshake>,
    /// The verdict a server's authenticator gives.
    verdict: Option<ZapReply>,
    zap: Option<ZapRequest>,
    established: Option<Result<Established, HandshakeError>>,
    codec: Option<CurveCodec>,
    /// Octets written and not yet delivered to the other end.
    outbox: Vec<u8>,
    to_send: Vec<Item>,
    /// What arrived after the handshake.
    after: Vec<Incoming>,
    curve_error: Option<CurveError>,
    decode_error: Option<DecodeError>,
}

impl End {
    fn new(version: Version, verdict: Option<ZapReply>, to_send: Vec<Item>) -> Self {
        Self {
            decoder: Decoder::new(version, DecoderConfig::default()),
            encoder: Encoder::new(version),
            handshake: None,
            verdict,
            zap: None,
            established: None,
            codec: None,
            outbox: Vec::new(),
            to_send,
            after: Vec::new(),
            curve_error: None,
            decode_error: None,
        }
    }

    fn stopped(&self) -> bool {
        matches!(self.established, Some(Err(_)))
            || self.curve_error.is_some()
            || self.decode_error.is_some()
    }

    fn receive(&mut self, octets: &[u8]) {
        if self.stopped() {
            return;
        }
        self.decoder.extend_from_slice(octets);
        loop {
            let incoming = match self.decoder.decode() {
                Ok(Some(incoming)) => incoming,
                Ok(None) => return,
                Err(err) => {
                    self.decode_error = Some(err);
                    return;
                }
            };
            if let Some(handshake) = self.handshake.take() {
                self.step(handshake, incoming);
            } else if let Some(codec) = &mut self.codec {
                match codec.decode(incoming) {
                    Ok(Some(incoming)) => self.after.push(incoming),
                    Ok(None) => {}
                    Err(err) => self.curve_error = Some(err),
                }
            } else {
                self.after.push(incoming);
            }
            if self.stopped() {
                return;
            }
        }
    }

    fn step(&mut self, handshake: Handshake, incoming: Incoming) {
        let out = &mut self.outbox;
        let result = match handshake {
            Handshake::PlainClient(client) => match client.receive(incoming, out) {
                Ok(ClientStep::Continue(client)) => Ok(Some(Handshake::PlainClient(client))),
                Ok(ClientStep::Established(established)) => Err(Ok((established, None))),
                Err(err) => Err(Err(err)),
            },
            Handshake::PlainServer(server) => match server.receive(incoming, out) {
                Ok(ServerStep::Authenticate(pending, request)) => {
                    self.zap = Some(request);
                    let verdict = self.verdict.take().expect("asked ZAP once");
                    match pending.reply(verdict, out) {
                        Ok(server) => Ok(Some(Handshake::PlainServer(server))),
                        Err(err) => Err(Err(err)),
                    }
                }
                Ok(ServerStep::Established(established)) => Err(Ok((established, None))),
                Err(err) => Err(Err(err)),
            },
            Handshake::CurveClient(client) => match client.receive(incoming, out) {
                Ok(CurveClientStep::Continue(client)) => Ok(Some(Handshake::CurveClient(client))),
                Ok(CurveClientStep::Established(established, codec)) => {
                    Err(Ok((established, Some(codec))))
                }
                Err(err) => Err(Err(err)),
            },
            Handshake::CurveServer(server) => match server.receive(incoming, out) {
                Ok(CurveServerStep::Continue(server)) => Ok(Some(Handshake::CurveServer(server))),
                Ok(CurveServerStep::Authenticate(pending, request)) => {
                    self.zap = Some(request);
                    let verdict = self.verdict.take().expect("asked ZAP once");
                    match pending.reply(verdict, out) {
                        Ok((established, codec)) => Err(Ok((established, Some(codec)))),
                        Err(err) => Err(Err(err)),
                    }
                }
                Err(err) => Err(Err(err)),
            },
        };
        match result {
            Ok(handshake) => self.handshake = handshake,
            Err(Ok((established, codec))) => {
                self.established = Some(Ok(established));
                self.codec = codec;
                for item in std::mem::take(&mut self.to_send) {
                    self.send(&item);
                }
            }
            Err(Err(err)) => self.established = Some(Err(err)),
        }
    }

    fn send(&mut self, item: &Item) {
        let out = &mut self.outbox;
        match (&mut self.codec, item.incoming()) {
            (Some(codec), Incoming::Message(message)) => codec
                .encode_message(&message, self.encoder, out)
                .expect("nonces left"),
            (Some(codec), Incoming::Command(body)) => codec
                .encode_command(&body, self.encoder, out)
                .expect("nonces left"),
            (None, Incoming::Message(message)) => self.encoder.encode(&message, out),
            (None, Incoming::Command(body)) => self
                .encoder
                .encode_command(&body, out)
                .expect("ZMTP 3.x carries commands"),
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

struct Sides {
    client_type: SocketType,
    server_type: SocketType,
    client_id: Identity,
    server_id: Identity,
}

/// A client and a server that have exchanged ZMTP 3 greetings, the client's
/// HELLO written.
fn start(
    sides: &Sides,
    security: &Security,
    minor: u8,
    verdict: ZapReply,
    (client_sends, server_sends): (Vec<Item>, Vec<Item>),
) -> (End, End) {
    let greeting = |as_server| PeerGreeting::Zmtp3 {
        major: 3,
        minor,
        as_server,
    };
    let version = greeting(true).version();
    let mut client = End::new(version, None, client_sends);
    let mut server = End::new(version, Some(verdict), server_sends);
    match security {
        Security::Plain(credentials) => {
            let start = PlainClient::start(
                sides.client_type,
                sides.client_id.clone(),
                credentials,
                &greeting(true),
                &mut client.outbox,
            );
            client.handshake = Some(Handshake::PlainClient(start.expect("ZMTP 3 peer")));
            let config = PlainServerConfig {
                socket_type: sides.server_type,
                identity: sides.server_id.clone(),
                domain: Frame::from("domain"),
                peer_address: "127.0.0.1".to_owned(),
            };
            let start = PlainServer::start(config, &greeting(false));
            server.handshake = Some(Handshake::PlainServer(start.expect("ZMTP 3 peer")));
        }
        Security::Curve {
            client: client_keys,
            server: server_keys,
            wrong_server_key,
        } => {
            let server_key = if *wrong_server_key {
                client_keys.public()
            } else {
                server_keys.public()
            };
            let start = CurveClient::start(
                sides.client_type,
                sides.client_id.clone(),
                client_keys,
                server_key,
                &greeting(true),
                &mut client.outbox,
            );
            client.handshake = Some(Handshake::CurveClient(start.expect("ZMTP 3 peer")));
            let config = CurveServerConfig {
                socket_type: sides.server_type,
                identity: sides.server_id.clone(),
                domain: Frame::from("domain"),
                peer_address: "127.0.0.1".to_owned(),
            };
            let start = CurveServer::start(config, server_keys, &greeting(false));
            server.handshake = Some(Handshake::CurveServer(start.expect("ZMTP 3 peer")));
        }
    }
    (client, server)
}

#[derive(Debug, Clone, Copy, PartialEq, Eq)]
enum Tampering {
    Replay,
    Reorder,
    /// Flip these bits of one octet.
    Flip(u8),
}

fn tampering() -> impl Strategy<Value = Tampering> {
    prop_oneof![
        1 => Just(Tampering::Replay),
        1 => Just(Tampering::Reorder),
        2 => (1u8..=255).prop_map(Tampering::Flip),
    ]
}

/// What one boxed frame carries.
#[derive(Debug, Clone)]
enum Boxed {
    Part { payload: Vec<u8>, more: bool },
    Command(Vec<u8>),
}

/// The frames `items` are boxed as, in order.
fn boxed(items: &[Item]) -> Vec<Boxed> {
    items
        .iter()
        .flat_map(|item| match item {
            Item::Message(frames) => frames
                .iter()
                .enumerate()
                .map(|(i, payload)| Boxed::Part {
                    payload: payload.clone(),
                    more: i + 1 < frames.len(),
                })
                .collect(),
            Item::Command(body) => vec![Boxed::Command(body.clone())],
        })
        .collect()
}

/// What a receiver delivers from boxed frames arriving in this order, each
/// with its nonce: nonces must rise, and a command may not interrupt a
/// multipart message.
fn model(frames: &[(u64, &Boxed)]) -> (Vec<Incoming>, Option<CurveError>) {
    let mut delivered = Vec::new();
    let mut last = None;
    let mut partial = Vec::new();
    for &(nonce, boxed) in frames {
        if last.is_some_and(|last| nonce <= last) {
            return (delivered, Some(CurveError::StaleNonce));
        }
        last = Some(nonce);
        match boxed {
            Boxed::Command(_) if !partial.is_empty() => {
                return (delivered, Some(CurveError::Malformed));
            }
            Boxed::Command(body) => {
                delivered.push(Incoming::Command(Frame::copy_from_slice(body)));
            }
            Boxed::Part { payload, more } => {
                partial.push(Frame::copy_from_slice(payload));
                if !more {
                    let message = Message::try_from(std::mem::take(&mut partial));
                    delivered.push(Incoming::Message(message.expect("a frame")));
                }
            }
        }
    }
    (delivered, None)
}

fn reason(reason: &'static str) -> HandshakeError {
    HandshakeError::PeerError(ErrorReason::new(reason).expect("short reason"))
}

/// Checks what `end` learned about its peer.
fn check_peer(
    established: &Established,
    version: Version,
    peer_type: SocketType,
    peer_id: &Identity,
) -> Result<(), TestCaseError> {
    prop_assert_eq!(established.version, version);
    prop_assert_eq!(established.peer_socket_type, Some(peer_type));
    let expected_id = if peer_type.sends_identity() {
        peer_id.as_slice()
    } else {
        &[]
    };
    prop_assert_eq!(&established.peer_identity[..], expected_id);
    prop_assert_eq!(
        established.metadata.get("Socket-Type").map(|f| &f[..]),
        Some(peer_type.name().as_bytes())
    );
    Ok(())
}

proptest! {
    #![proptest_config(ProptestConfig::with_cases(512))]

    /// Each side's outcome follows from the credentials, the ZAP verdict
    /// and the socket types, however the traffic is split: ZAP sees exactly
    /// what the client presented; a refusal reaches the client as the
    /// matching ERROR, or as silence for a temporary failure; each side
    /// learns the other's type and identity, and the server who ZAP said
    /// the client is; and what each sends afterwards arrives whole.
    #[test]
    fn secure_handshakes_agree(
        (client_type, server_type) in socket_types(),
        (client_id, server_id) in (identity(), identity()),
        security in security(),
        minor in 0u8..=1,
        verdict in verdict(),
        traffic in (items(), items()),
        schedule in schedule(),
    ) {
        let sides = Sides { client_type, server_type, client_id, server_id };
        let (client_sent, server_sent) = traffic.clone();
        let (mut client, mut server) = start(&sides, &security, minor, verdict.clone(), traffic);
        connect(&mut client, &mut server, &schedule);
        let version = if minor == 0 { Version::V3_0 } else { Version::V3_1 };

        if let Security::Curve { wrong_server_key: true, .. } = security {
            prop_assert_eq!(&server.established, &Some(Err(HandshakeError::Cryptographic)));
            prop_assert!(client.established.is_none(), "the server closes without a word");
            prop_assert_eq!(&server.zap, &None);
            return Ok(());
        }

        let credentials = match &security {
            Security::Plain(credentials) => ZapCredentials::Plain(credentials.clone()),
            Security::Curve { client, .. } => ZapCredentials::Curve(client.public()),
        };
        let expected_zap = ZapRequest {
            domain: Frame::from("domain"),
            address: "127.0.0.1".to_owned(),
            identity: Frame::copy_from_slice(sides.server_id.as_slice()),
            credentials,
        };
        prop_assert_eq!(server.zap.as_ref(), Some(&expected_zap));

        let (user_id, metadata) = match verdict {
            ZapReply::Ok { user_id, metadata } => (user_id, metadata),
            ZapReply::TemporaryFailure => {
                let failure = HandshakeError::Authentication(ZapFailure::Temporary);
                prop_assert_eq!(server.established, Some(Err(failure)));
                prop_assert!(client.established.is_none(), "temporary failures close silently");
                return Ok(());
            }
            ZapReply::AuthenticationFailure => {
                let failure = HandshakeError::Authentication(ZapFailure::Denied);
                prop_assert_eq!(server.established, Some(Err(failure)));
                prop_assert_eq!(client.established, Some(Err(reason("400"))));
                return Ok(());
            }
            ZapReply::InternalError => {
                let failure = HandshakeError::Authentication(ZapFailure::Internal);
                prop_assert_eq!(server.established, Some(Err(failure)));
                prop_assert_eq!(client.established, Some(Err(reason("500"))));
                return Ok(());
            }
        };

        if !server_type.accepts(client_type) {
            let refused = HandshakeError::IncompatiblePeer { peer: client_type };
            prop_assert_eq!(server.established, Some(Err(refused)));
            prop_assert_eq!(client.established, Some(Err(reason("incompatible socket type"))));
            return Ok(());
        }
        let on_server = server.established.as_ref().expect("finished").as_ref().expect("accepted");
        check_peer(on_server, version, client_type, &sides.client_id)?;
        prop_assert_eq!(on_server.authenticated.as_ref(), Some(&Authenticated { user_id, metadata }));

        let on_client = client.established.as_ref().expect("finished");
        if !client_type.accepts(server_type) {
            prop_assert_eq!(on_client, &Err(HandshakeError::IncompatiblePeer { peer: server_type }));
            return Ok(());
        }
        let on_client = on_client.as_ref().expect("accepted");
        check_peer(on_client, version, server_type, &sides.server_id)?;
        prop_assert_eq!(&on_client.authenticated, &None);

        prop_assert_eq!(client.curve_error, None);
        prop_assert_eq!((client.decode_error, server.decode_error), (None, None));
        prop_assert_eq!(server.curve_error, None);
        let incoming = |sent: &[Item]| sent.iter().map(Item::incoming).collect::<Vec<_>>();
        prop_assert_eq!(&client.after, &incoming(&server_sent));
        prop_assert_eq!(&server.after, &incoming(&client_sent));
    }

    /// After a CURVE handshake, a replayed or reordered frame is delivered
    /// exactly as far as RFC 26's rules allow and then stops the connection,
    /// and an altered one never delivers anything that was not sent: the
    /// receiver delivers some of what was sent, in order.
    #[test]
    fn curve_traffic_arrives_intact_or_not_at_all(
        (client_keys, server_keys) in (keypair(), keypair()),
        minor in 0u8..=1,
        sent in items().prop_filter("something to tamper with", |items| !items.is_empty()),
        tampering in tampering(),
        (first, second) in (any::<prop::sample::Index>(), any::<prop::sample::Index>()),
        schedule in schedule(),
    ) {
        let sides = Sides {
            client_type: SocketType::Dealer,
            server_type: SocketType::Router,
            client_id: Identity::default(),
            server_id: Identity::default(),
        };
        let security = Security::Curve { client: client_keys, server: server_keys, wrong_server_key: false };
        let verdict = ZapReply::Ok { user_id: Frame::new(), metadata: Metadata::new() };
        let (mut client, mut server) = start(&sides, &security, minor, verdict, (Vec::new(), Vec::new()));
        connect(&mut client, &mut server, &schedule);
        prop_assert!(matches!(server.established, Some(Ok(_))), "{:?}", server.established);
        prop_assert!(matches!(client.established, Some(Ok(_))), "{:?}", client.established);

        // The boxed frames the client sends, each a MESSAGE on the wire.
        for item in &sent {
            client.send(item);
        }
        let version = if minor == 0 { Version::V3_0 } else { Version::V3_1 };
        let mut wire = Decoder::new(version, DecoderConfig::default());
        wire.extend_from_slice(&std::mem::take(&mut client.outbox));
        let mut frames = Vec::new();
        while let Some(frame) = wire.decode().expect("well-formed traffic") {
            frames.push(frame);
        }

        let boxed = boxed(&sent);
        prop_assert_eq!(frames.len(), boxed.len(), "one MESSAGE per frame");
        let mut frames: Vec<(u64, &Boxed, Incoming)> = (0..)
            .zip(&boxed)
            .zip(frames)
            .map(|((nonce, boxed), frame)| (nonce, boxed, frame))
            .collect();
        let (i, j) = (first.index(frames.len()), second.index(frames.len()));
        match tampering {
            Tampering::Replay => frames.insert(i, frames[i].clone()),
            Tampering::Reorder => {
                prop_assume!(i != j);
                frames.swap(i, j);
            }
            Tampering::Flip(_) => {}
        }
        let mut stream = Vec::new();
        let mut bodies = Vec::new();
        for (_, _, frame) in &frames {
            let Incoming::Message(message) = frame else {
                return Err(TestCaseError::fail("MESSAGE is sent as a data frame"));
            };
            client.encoder.encode(message, &mut stream);
            bodies.push(stream.len() - message.split_first().0.len()..stream.len());
        }
        let mut flipped_body = false;
        if let Tampering::Flip(mask) = tampering {
            let at = first.index(stream.len());
            stream[at] ^= mask;
            flipped_body = bodies.iter().any(|body| body.contains(&at));
        }

        server.receive(&stream);
        if let Tampering::Replay | Tampering::Reorder = tampering {
            let order: Vec<(u64, &Boxed)> = frames.iter().map(|&(nonce, boxed, _)| (nonce, boxed)).collect();
            let (delivered, error) = model(&order);
            prop_assert_eq!(&server.after, &delivered);
            prop_assert_eq!(server.curve_error, error);
            prop_assert!(error.is_some(), "a replayed or reordered stream was accepted");
            return Ok(());
        }
        let expected: Vec<Incoming> = sent.iter().map(Item::incoming).collect();
        prop_assert!(server.after.len() <= expected.len());
        prop_assert_eq!(&server.after[..], &expected[..server.after.len()]);
        let stopped = server.curve_error.is_some() || server.decode_error.is_some();
        // A flip outside the boxes may go unnoticed: the reserved flag bits
        // are ignored and a COMMAND-flagged MESSAGE is accepted.
        if flipped_body {
            prop_assert!(stopped, "an altered box was opened");
        }
    }
}
