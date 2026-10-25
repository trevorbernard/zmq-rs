#![no_main]

use libfuzzer_sys::arbitrary::{self, Arbitrary};
use libfuzzer_sys::fuzz_target;
use zmtp::{
    CurveClient, CurveClientStep, CurveCodec, CurveServer, CurveServerConfig, CurveServerStep,
    Decoder, Encoder, Established, Frame, Identity, Incoming, Message, Metadata, PeerGreeting,
    SocketType, Version, ZapReply,
};
use zmtp_fuzz::curve;

// A real CURVE client and server shake hands in memory, then trade what
// the input says, so the fuzzer reaches the boxed traffic that raw octets
// never get past the authenticators to. Checks that the handshake succeeds
// exactly when it should, and that whatever the codec delivers is exactly
// what was sent.

#[derive(Arbitrary, Debug)]
struct Session {
    client_type: u8,
    server_type: u8,
    identity: Vec<u8>,
    zap: Zap,
    ops: Vec<Op>,
}

#[derive(Arbitrary, Debug)]
enum Zap {
    Ok(Vec<u8>),
    TemporaryFailure,
    AuthenticationFailure,
    InternalError,
}

#[derive(Arbitrary, Debug, Clone, Copy)]
enum Direction {
    ToServer,
    ToClient,
}

#[derive(Arbitrary, Debug)]
enum Op {
    /// A message, which must arrive as sent.
    Message(Direction, Vec<Vec<u8>>),
    /// A boxed command, which must arrive as sent.
    Command(Direction, Vec<u8>),
    /// A message with an octet changed on the wire: dropped, or the
    /// connection closes, but never delivered changed.
    Tampered(Direction, Vec<Vec<u8>>, u16, u8),
    /// What was sent that many sends ago, again: refused.
    Replayed(Direction, u8),
    /// Octets onto the wire, past the codec.
    Raw(Direction, Vec<u8>),
}

/// One end: the codec it boxes with, and what it has read from the other.
struct End {
    codec: CurveCodec,
    decoder: Decoder,
    sent: Vec<Vec<u8>>,
    /// Raw octets or a tampered frame may have left the decoder mid-frame,
    /// so what follows is no longer checked.
    clean: bool,
    closed: bool,
}

const V3_1: Version = Version::V3_1;

fn socket_type(octet: u8) -> SocketType {
    SocketType::ALL[usize::from(octet) % SocketType::ALL.len()]
}

fn greeting(as_server: bool) -> PeerGreeting {
    PeerGreeting::Zmtp3 {
        major: 3,
        minor: 1,
        as_server,
    }
}

fn decoder() -> Decoder {
    Decoder::new(V3_1, zmtp_fuzz::decoder_config())
}

/// The commands in `out`, which the handshake writes whole.
fn commands(out: &[u8]) -> Vec<Incoming> {
    let mut decoder = decoder();
    decoder.extend_from_slice(out);
    zmtp_fuzz::drain(&mut decoder)
}

fn handshake(
    session: &Session,
    client_type: SocketType,
    server_type: SocketType,
    identity: &Identity,
) -> Option<((Established, CurveCodec), (Established, CurveCodec))> {
    let mut hello = Vec::new();
    let client = CurveClient::start(
        client_type,
        identity.clone(),
        &curve::client_keys(),
        curve::server_public(),
        &greeting(true),
        &mut hello,
    )
    .expect("HELLO");
    let config = CurveServerConfig {
        socket_type: server_type,
        identity: Identity::default(),
        domain: Frame::new(),
        peer_address: String::new(),
    };
    let server = CurveServer::start(config, &curve::server_keys(), &greeting(false)).unwrap();
    let mut welcome = Vec::new();
    let [hello] = commands(&hello).try_into().expect("one HELLO");
    let Ok(CurveServerStep::Continue(server)) = server.receive(hello, &mut welcome) else {
        panic!("our HELLO refused");
    };
    let mut initiate = Vec::new();
    let [welcome] = commands(&welcome).try_into().expect("one WELCOME");
    let Ok(CurveClientStep::Continue(client)) = client.receive(welcome, &mut initiate) else {
        panic!("our WELCOME refused");
    };
    let mut ready = Vec::new();
    let [initiate] = commands(&initiate).try_into().expect("one INITIATE");
    let Ok(CurveServerStep::Authenticate(pending, _)) = server.receive(initiate, &mut ready) else {
        panic!("our INITIATE refused");
    };
    let reply = match &session.zap {
        Zap::Ok(user_id) => ZapReply::Ok {
            user_id: Frame::from(user_id.clone()),
            metadata: Metadata::new(),
        },
        Zap::TemporaryFailure => ZapReply::TemporaryFailure,
        Zap::AuthenticationFailure => ZapReply::AuthenticationFailure,
        Zap::InternalError => ZapReply::InternalError,
    };
    let server = pending.reply(reply, &mut ready).ok()?;
    let [ready] = commands(&ready).try_into().expect("one READY");
    let Ok(CurveClientStep::Established(established, codec)) =
        client.receive(ready, &mut Vec::new())
    else {
        return None;
    };
    Some(((established, codec), server))
}

/// Delivers `wire` to `to`: the items it decodes, or `None` once the
/// connection must close.
fn deliver(to: &mut End, wire: &[u8]) -> Option<Vec<Incoming>> {
    to.decoder.extend_from_slice(wire);
    let items = curve::drain(&mut to.decoder, &mut to.codec).ok();
    to.closed = items.is_none();
    items
}

fn message(frames: &[Vec<u8>]) -> Option<Message> {
    let frames: Vec<Frame> = frames.iter().cloned().map(Frame::from).collect();
    Message::try_from(frames).ok()
}

fn encode(from: &mut End, item: &Incoming) -> Vec<u8> {
    let mut wire = Vec::new();
    match item {
        Incoming::Message(message) => {
            from.codec
                .encode_message(message, Encoder::new(V3_1), &mut wire)
        }
        Incoming::Command(body) => from
            .codec
            .encode_command(body, Encoder::new(V3_1), &mut wire),
    }
    .expect("a nonce left");
    from.sent.push(wire.clone());
    wire
}

fn run(session: Session) {
    let client_type = socket_type(session.client_type);
    let server_type = socket_type(session.server_type);
    let Ok(identity) = Identity::new(session.identity.clone()) else {
        return;
    };
    let should_connect = matches!(session.zap, Zap::Ok(_))
        && client_type.accepts(server_type)
        && server_type.accepts(client_type);
    let Some(((at_client, client), (at_server, server))) =
        handshake(&session, client_type, server_type, &identity)
    else {
        assert!(
            !should_connect,
            "{client_type:?} to {server_type:?} refused"
        );
        return;
    };
    assert!(
        should_connect,
        "{client_type:?} to {server_type:?} accepted"
    );
    assert_eq!(at_client.peer_socket_type, Some(server_type));
    assert_eq!(at_server.peer_socket_type, Some(client_type));
    let expected_identity = if client_type.sends_identity() {
        identity.as_bytes()
    } else {
        b""
    };
    assert_eq!(at_server.peer_identity.as_ref(), expected_identity);
    let Zap::Ok(user_id) = &session.zap else {
        unreachable!()
    };
    let authenticated = at_server.authenticated.expect("ZAP asked");
    assert_eq!(authenticated.user_id.as_ref(), user_id.as_slice());

    let end = |codec| End {
        codec,
        decoder: decoder(),
        sent: Vec::new(),
        clean: true,
        closed: false,
    };
    let mut ends = [end(client), end(server)];
    for op in session.ops {
        let direction = match op {
            Op::Message(d, _) | Op::Command(d, _) | Op::Tampered(d, ..) => d,
            Op::Replayed(d, _) | Op::Raw(d, _) => d,
        };
        let [client, server] = &mut ends;
        let (from, to) = match direction {
            Direction::ToServer => (client, server),
            Direction::ToClient => (server, client),
        };
        if to.closed {
            continue;
        }
        match op {
            Op::Message(_, frames) => {
                let Some(message) = message(&frames) else {
                    continue;
                };
                let item = Incoming::Message(message);
                let wire = encode(from, &item);
                let delivered = deliver(to, &wire);
                if to.clean {
                    assert_eq!(delivered, Some(vec![item]));
                }
            }
            Op::Command(_, body) => {
                let item = Incoming::Command(Frame::from(body));
                let wire = encode(from, &item);
                let delivered = deliver(to, &wire);
                if to.clean {
                    assert_eq!(delivered, Some(vec![item]));
                }
            }
            Op::Tampered(_, frames, offset, xor) => {
                let Some(message) = message(&frames) else {
                    continue;
                };
                let item = Incoming::Message(message);
                let mut wire = encode(from, &item);
                let at = usize::from(offset) % wire.len();
                wire[at] ^= xor.max(1);
                if let Some(delivered) = deliver(to, &wire) {
                    // Framing flags are not boxed: a MESSAGE sent as a
                    // command still opens, unchanged.
                    assert!(delivered.iter().all(|got| *got == item), "{delivered:?}");
                }
                to.clean = false;
            }
            Op::Replayed(_, back) => {
                let Some(index) = from.sent.len().checked_sub(1 + usize::from(back)) else {
                    continue;
                };
                let wire = from.sent[index].clone();
                let delivered = deliver(to, &wire);
                if to.clean {
                    assert_eq!(delivered, None, "a replay accepted");
                }
            }
            Op::Raw(_, octets) => {
                let _ = deliver(to, &octets);
                to.clean = false;
            }
        }
    }
}

fuzz_target!(|session: Session| run(session));
