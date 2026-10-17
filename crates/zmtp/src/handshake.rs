//! The NULL security handshake (RFC 23, 37), and the identity exchange that
//! replaces it for ZMTP 1.0 and 2.0 peers (RFC 13, 15).
//!
//! The handshake works on decoded [`Incoming`] items rather than octets, so
//! messages a peer sends straight after its READY stay in the decoder.

use std::fmt;

use crate::{
    Command, CommandError, Encoder, ErrorReason, Frame, Identity, Incoming, Message, Metadata,
    PeerGreeting, PropertyName, SocketType, Version,
};

/// The NULL handshake for one connection, after the greeting.
#[derive(Debug)]
#[must_use]
pub struct NullHandshake {
    socket_type: SocketType,
    version: Version,
    legacy_peer_type: Option<SocketType>,
    implicit_subscription: bool,
}

/// A connection ready to carry messages.
#[derive(Debug, Clone, PartialEq, Eq)]
pub struct Established {
    /// The protocol version both sides speak.
    pub version: Version,
    /// The peer's socket type, when it announced one. Unversioned ZMTP 1.0
    /// peers and READY commands without `Socket-Type` do not.
    pub peer_socket_type: Option<SocketType>,
    /// The identity the peer announced, possibly empty.
    pub peer_identity: Frame,
    /// The peer's READY properties. Empty for ZMTP 1.0 and 2.0 peers.
    pub metadata: Metadata,
    /// We are PUB or XPUB and the peer speaks unversioned ZMTP 1.0, which
    /// never sends subscriptions: treat it as subscribed to everything, as
    /// libzmq does.
    pub implicit_subscription: bool,
}

/// Why the handshake failed. Flush any output already written, then close
/// the connection.
#[derive(Debug, Clone, PartialEq, Eq)]
pub enum HandshakeError {
    /// The peer's socket type cannot talk to ours.
    IncompatiblePeer { peer: SocketType },
    /// The peer's READY named a socket type we do not know.
    UnknownSocketType(Frame),
    /// The peer's ZMTP 1.0/2.0 greeting carried an unknown socket type.
    UnknownLegacySocketType(u8),
    /// The peer sent ERROR. Do not reconnect with the same configuration.
    PeerError(ErrorReason),
    /// The peer sent a command other than READY or ERROR.
    UnexpectedCommand(Frame),
    /// The peer sent a message before completing the handshake.
    UnexpectedMessage,
    /// The peer sent a command we could not parse.
    MalformedCommand(CommandError),
    /// The peer's identity is multipart or longer than 255 octets.
    InvalidPeerIdentity,
}

/// Sent to a ZMTP 3.x peer whose socket type we reject.
const REJECTION: &str = "incompatible socket type";
const MAX_IDENTITY: usize = u8::MAX as usize;

impl NullHandshake {
    /// Starts the handshake with a peer whose greeting is `peer`, writing our
    /// READY, or our identity frame for a ZMTP 1.0/2.0 peer, to `out`.
    ///
    /// A ZMTP 1.0/2.0 peer's socket type is known from its greeting, so an
    /// incompatible one is refused here, silently as RFC 15 asks.
    pub fn start(
        socket_type: SocketType,
        identity: &Identity,
        peer: &PeerGreeting,
        out: &mut Vec<u8>,
    ) -> Result<Self, HandshakeError> {
        let version = peer.version();
        let encoder = Encoder::new(version);
        let legacy_peer_type = match *peer {
            PeerGreeting::Zmtp1 { socket_type: code }
            | PeerGreeting::Zmtp2 { socket_type: code } => {
                let peer_type = SocketType::from_legacy_code(code)
                    .ok_or(HandshakeError::UnknownLegacySocketType(code))?;
                if !socket_type.accepts(peer_type) {
                    return Err(HandshakeError::IncompatiblePeer { peer: peer_type });
                }
                Some(peer_type)
            }
            PeerGreeting::Zmtp1Unversioned { .. } | PeerGreeting::Zmtp3 { .. } => None,
        };
        let identity = Frame::copy_from_slice(identity.as_slice());
        match peer {
            PeerGreeting::Zmtp3 { .. } => {
                let mut metadata = Metadata::new();
                metadata.push(
                    PropertyName::from_static("Socket-Type"),
                    Frame::from(socket_type.name()),
                );
                if socket_type.sends_identity() {
                    metadata.push(PropertyName::from_static("Identity"), identity);
                }
                write_command(&encoder, &Command::Ready(metadata), out);
            }
            // The greeting already sent our identity frame.
            PeerGreeting::Zmtp1Unversioned { .. } => {}
            PeerGreeting::Zmtp1 { .. } | PeerGreeting::Zmtp2 { .. } => {
                encoder.encode(&Message::new(identity), out);
            }
        }
        let implicit_subscription = matches!(peer, PeerGreeting::Zmtp1Unversioned { .. })
            && matches!(socket_type, SocketType::Pub | SocketType::XPub);
        Ok(Self {
            socket_type,
            version,
            legacy_peer_type,
            implicit_subscription,
        })
    }

    /// Handles the next item from the peer. Writes an ERROR to `out` when we
    /// reject the peer.
    pub fn receive(
        self,
        incoming: Incoming,
        out: &mut Vec<u8>,
    ) -> Result<Established, HandshakeError> {
        match (self.version, incoming) {
            (Version::V3_0 | Version::V3_1, Incoming::Command(body)) => {
                match Command::parse(&body).map_err(HandshakeError::MalformedCommand)? {
                    Command::Ready(metadata) => self.ready(metadata, out),
                    Command::Error(reason) => Err(HandshakeError::PeerError(reason)),
                    other => Err(HandshakeError::UnexpectedCommand(Frame::copy_from_slice(
                        other.name(),
                    ))),
                }
            }
            (Version::V3_0 | Version::V3_1, Incoming::Message(_)) => {
                Err(HandshakeError::UnexpectedMessage)
            }
            (Version::V1 | Version::V2, Incoming::Message(identity)) => {
                let (first, rest) = identity.split_first();
                if !rest.is_empty() || first.len() > MAX_IDENTITY {
                    return Err(HandshakeError::InvalidPeerIdentity);
                }
                Ok(Established {
                    version: self.version,
                    peer_socket_type: self.legacy_peer_type,
                    peer_identity: first.clone(),
                    metadata: Metadata::new(),
                    implicit_subscription: self.implicit_subscription,
                })
            }
            // ZMTP 1.0/2.0 decoders never produce commands.
            (Version::V1 | Version::V2, Incoming::Command(_)) => {
                Err(HandshakeError::UnexpectedMessage)
            }
        }
    }

    fn ready(self, metadata: Metadata, out: &mut Vec<u8>) -> Result<Established, HandshakeError> {
        let peer_socket_type = match metadata.get("Socket-Type") {
            None => None,
            Some(name) => {
                let rejected = match SocketType::from_name(name) {
                    Some(peer) if self.socket_type.accepts(peer) => Ok(peer),
                    Some(peer) => Err(HandshakeError::IncompatiblePeer { peer }),
                    None => Err(HandshakeError::UnknownSocketType(name.clone())),
                };
                match rejected {
                    Ok(peer) => Some(peer),
                    Err(err) => {
                        let error = Command::Error(ErrorReason::from_static(REJECTION));
                        write_command(&Encoder::new(self.version), &error, out);
                        return Err(err);
                    }
                }
            }
        };
        let peer_identity = metadata.get("Identity").cloned().unwrap_or_default();
        if peer_identity.len() > MAX_IDENTITY {
            return Err(HandshakeError::InvalidPeerIdentity);
        }
        Ok(Established {
            version: self.version,
            peer_socket_type,
            peer_identity,
            metadata,
            implicit_subscription: false,
        })
    }
}

fn write_command(encoder: &Encoder, command: &Command, out: &mut Vec<u8>) {
    let mut body = Vec::new();
    command.encode(&mut body);
    let written = encoder.encode_command(&body, out);
    debug_assert!(written.is_ok(), "only ZMTP 3.x handshakes send commands");
}

impl fmt::Display for HandshakeError {
    fn fmt(&self, f: &mut fmt::Formatter<'_>) -> fmt::Result {
        match self {
            Self::IncompatiblePeer { peer } => {
                write!(f, "peer socket type {} is incompatible", peer.name())
            }
            Self::UnknownSocketType(name) => {
                write!(f, "peer socket type \"{}\" is unknown", name.escape_ascii())
            }
            Self::UnknownLegacySocketType(code) => {
                write!(f, "peer socket type code {code} is unknown")
            }
            Self::PeerError(reason) => write!(
                f,
                "peer rejected the handshake: {}",
                reason.as_slice().escape_ascii()
            ),
            Self::UnexpectedCommand(name) => write!(
                f,
                "unexpected {} command during handshake",
                name.escape_ascii()
            ),
            Self::UnexpectedMessage => {
                f.write_str("message received before the handshake completed")
            }
            Self::MalformedCommand(err) => write!(f, "{err}"),
            Self::InvalidPeerIdentity => {
                f.write_str("peer identity is multipart or longer than 255 octets")
            }
        }
    }
}

impl std::error::Error for HandshakeError {}

#[cfg(test)]
mod tests {
    use super::*;
    use crate::Replay;

    const V3_1: PeerGreeting = PeerGreeting::Zmtp3 {
        major: 3,
        minor: 1,
        as_server: false,
    };

    fn me() -> Identity {
        Identity::new(&b"me"[..]).unwrap()
    }

    fn start(socket_type: SocketType, peer: &PeerGreeting) -> (NullHandshake, Vec<u8>) {
        let mut out = Vec::new();
        let handshake = NullHandshake::start(socket_type, &me(), peer, &mut out).unwrap();
        (handshake, out)
    }

    fn metadata(properties: &[(&str, &str)]) -> Metadata {
        let mut metadata = Metadata::new();
        for (name, value) in properties {
            let name = PropertyName::new(name.to_string()).unwrap();
            metadata.insert(name, value.to_string()).unwrap();
        }
        metadata
    }

    fn command_frame(command: Command) -> Vec<u8> {
        let mut body = Vec::new();
        command.encode(&mut body);
        let mut out = Vec::new();
        Encoder::new(Version::V3_1)
            .encode_command(&body, &mut out)
            .unwrap();
        out
    }

    fn incoming(command: Command) -> Incoming {
        let mut body = Vec::new();
        command.encode(&mut body);
        Incoming::Command(Frame::from(body))
    }

    fn peer_ready(properties: &[(&str, &str)]) -> Incoming {
        incoming(Command::Ready(metadata(properties)))
    }

    fn rejection() -> Vec<u8> {
        command_frame(Command::Error(
            ErrorReason::new("incompatible socket type").unwrap(),
        ))
    }

    // ---- ZMTP 3.x ----

    #[test]
    fn sends_ready_with_socket_type_and_identity() {
        let (_, out) = start(SocketType::Dealer, &V3_1);
        let expected = metadata(&[("Socket-Type", "DEALER"), ("Identity", "me")]);
        assert_eq!(out, command_frame(Command::Ready(expected)));
    }

    #[test]
    fn non_routing_types_send_no_identity() {
        let (_, out) = start(SocketType::Pub, &V3_1);
        let expected = metadata(&[("Socket-Type", "PUB")]);
        assert_eq!(out, command_frame(Command::Ready(expected)));
    }

    #[test]
    fn peer_ready_establishes_the_connection() {
        let (handshake, _) = start(SocketType::Dealer, &V3_1);
        let mut out = Vec::new();
        let ready = peer_ready(&[
            ("Socket-Type", "ROUTER"),
            ("Identity", "pid"),
            ("X-Foo", "bar"),
        ]);
        let established = handshake.receive(ready, &mut out).unwrap();
        assert_eq!(established.version, Version::V3_1);
        assert_eq!(established.peer_socket_type, Some(SocketType::Router));
        assert_eq!(established.peer_identity, Frame::from("pid"));
        assert_eq!(established.metadata.get("x-foo"), Some(&Frame::from("bar")));
        assert!(!established.implicit_subscription);
        assert!(out.is_empty());
    }

    #[test]
    fn zmtp_3_0_peer_keeps_its_version() {
        let peer = PeerGreeting::Zmtp3 {
            major: 3,
            minor: 0,
            as_server: false,
        };
        let (handshake, _) = start(SocketType::Sub, &peer);
        let established = handshake
            .receive(peer_ready(&[("Socket-Type", "PUB")]), &mut Vec::new())
            .unwrap();
        assert_eq!(established.version, Version::V3_0);
    }

    #[test]
    fn missing_socket_type_is_accepted_like_libzmq() {
        let (handshake, _) = start(SocketType::Dealer, &V3_1);
        let established = handshake.receive(peer_ready(&[]), &mut Vec::new()).unwrap();
        assert_eq!(established.peer_socket_type, None);
        assert_eq!(established.peer_identity, Frame::new());
    }

    #[test]
    fn incompatible_peer_gets_an_error_command() {
        let (handshake, _) = start(SocketType::Dealer, &V3_1);
        let mut out = Vec::new();
        assert_eq!(
            handshake.receive(peer_ready(&[("Socket-Type", "PUB")]), &mut out),
            Err(HandshakeError::IncompatiblePeer {
                peer: SocketType::Pub
            })
        );
        assert_eq!(out, rejection());
    }

    #[test]
    fn unknown_socket_type_gets_an_error_command() {
        let (handshake, _) = start(SocketType::Dealer, &V3_1);
        let mut out = Vec::new();
        assert_eq!(
            handshake.receive(peer_ready(&[("Socket-Type", "STREAM")]), &mut out),
            Err(HandshakeError::UnknownSocketType(Frame::from("STREAM")))
        );
        assert_eq!(out, rejection());
    }

    #[test]
    fn peer_error_fails_without_reply() {
        let (handshake, _) = start(SocketType::Dealer, &V3_1);
        let reason = ErrorReason::new("400").unwrap();
        let mut out = Vec::new();
        assert_eq!(
            handshake.receive(incoming(Command::Error(reason.clone())), &mut out),
            Err(HandshakeError::PeerError(reason))
        );
        assert!(out.is_empty());
    }

    #[test]
    fn message_before_ready_fails() {
        let (handshake, _) = start(SocketType::Dealer, &V3_1);
        let msg = Incoming::Message(Message::new("early"));
        assert_eq!(
            handshake.receive(msg, &mut Vec::new()),
            Err(HandshakeError::UnexpectedMessage)
        );
    }

    #[test]
    fn other_commands_before_ready_fail() {
        let (handshake, _) = start(SocketType::Dealer, &V3_1);
        let ping = incoming(Command::Subscribe(Frame::from("x")));
        assert_eq!(
            handshake.receive(ping, &mut Vec::new()),
            Err(HandshakeError::UnexpectedCommand(Frame::from("SUBSCRIBE")))
        );
    }

    #[test]
    fn malformed_ready_fails() {
        let (handshake, _) = start(SocketType::Dealer, &V3_1);
        let bad = Incoming::Command(Frame::from(&b"\x05READY\x01"[..]));
        assert_eq!(
            handshake.receive(bad, &mut Vec::new()),
            Err(HandshakeError::MalformedCommand(CommandError::Truncated {
                command: "READY"
            }))
        );
    }

    #[test]
    fn oversized_peer_identity_fails() {
        let (handshake, _) = start(SocketType::Router, &V3_1);
        let long = "x".repeat(256);
        let ready = peer_ready(&[("Socket-Type", "DEALER"), ("Identity", &long)]);
        assert_eq!(
            handshake.receive(ready, &mut Vec::new()),
            Err(HandshakeError::InvalidPeerIdentity)
        );
    }

    // ---- ZMTP 1.0 and 2.0 ----

    #[test]
    fn zmtp_2_exchanges_identity_frames() {
        let peer = PeerGreeting::Zmtp2 { socket_type: 6 };
        let (handshake, out) = start(SocketType::Dealer, &peer);
        assert_eq!(out, b"\x00\x02me", "identity as a ZMTP 2.0 frame");
        let established = handshake
            .receive(Incoming::Message(Message::new("pid")), &mut Vec::new())
            .unwrap();
        assert_eq!(
            established,
            Established {
                version: Version::V2,
                peer_socket_type: Some(SocketType::Router),
                peer_identity: Frame::from("pid"),
                metadata: Metadata::new(),
                implicit_subscription: false,
            }
        );
    }

    #[test]
    fn zmtp_2_incompatible_peer_is_refused_silently() {
        let mut out = Vec::new();
        let peer = PeerGreeting::Zmtp2 { socket_type: 1 };
        assert_eq!(
            NullHandshake::start(SocketType::Dealer, &me(), &peer, &mut out).unwrap_err(),
            HandshakeError::IncompatiblePeer {
                peer: SocketType::Pub
            }
        );
        assert!(out.is_empty(), "ZMTP 2.0 has no ERROR command");
    }

    #[test]
    fn zmtp_2_unknown_socket_type_is_refused() {
        let peer = PeerGreeting::Zmtp2 { socket_type: 42 };
        assert_eq!(
            NullHandshake::start(SocketType::Dealer, &me(), &peer, &mut Vec::new()).unwrap_err(),
            HandshakeError::UnknownLegacySocketType(42)
        );
    }

    #[test]
    fn libzmq_xpub_code_is_accepted() {
        let peer = PeerGreeting::Zmtp2 { socket_type: 9 };
        let (handshake, _) = start(SocketType::Sub, &peer);
        let established = handshake
            .receive(
                Incoming::Message(Message::new(Frame::new())),
                &mut Vec::new(),
            )
            .unwrap();
        assert_eq!(established.peer_socket_type, Some(SocketType::XPub));
    }

    #[test]
    fn unversioned_zmtp_1_sent_identity_in_the_greeting() {
        let peer = PeerGreeting::Zmtp1Unversioned {
            replay: Replay::default(),
        };
        let (handshake, out) = start(SocketType::Dealer, &peer);
        assert!(out.is_empty());
        let established = handshake
            .receive(Incoming::Message(Message::new("pid")), &mut Vec::new())
            .unwrap();
        assert_eq!(established.version, Version::V1);
        assert_eq!(established.peer_socket_type, None);
        assert!(!established.implicit_subscription);
    }

    #[test]
    fn publisher_subscribes_unversioned_zmtp_1_peers_to_everything() {
        let peer = PeerGreeting::Zmtp1Unversioned {
            replay: Replay::default(),
        };
        for socket_type in [SocketType::Pub, SocketType::XPub] {
            let (handshake, _) = start(socket_type, &peer);
            let established = handshake
                .receive(
                    Incoming::Message(Message::new(Frame::new())),
                    &mut Vec::new(),
                )
                .unwrap();
            assert!(established.implicit_subscription, "{socket_type:?}");
        }
    }

    #[test]
    fn zmtp_1_with_revision_sends_a_v1_identity_frame() {
        let peer = PeerGreeting::Zmtp1 { socket_type: 6 };
        let (handshake, out) = start(SocketType::Dealer, &peer);
        assert_eq!(out, b"\x03\x00me");
        let established = handshake
            .receive(Incoming::Message(Message::new("pid")), &mut Vec::new())
            .unwrap();
        assert_eq!(established.peer_socket_type, Some(SocketType::Router));
        assert!(!established.implicit_subscription);
    }

    #[test]
    fn legacy_identity_must_be_one_short_frame() {
        let peer = PeerGreeting::Zmtp2 { socket_type: 6 };
        let mut multipart = Message::new("a");
        multipart.push("b");
        let long = Message::new(vec![b'x'; 256]);
        for identity in [multipart, long] {
            let (handshake, _) = start(SocketType::Dealer, &peer);
            assert_eq!(
                handshake.receive(Incoming::Message(identity), &mut Vec::new()),
                Err(HandshakeError::InvalidPeerIdentity)
            );
        }
    }

    #[test]
    fn error_messages() {
        assert_eq!(
            HandshakeError::IncompatiblePeer {
                peer: SocketType::Pub
            }
            .to_string(),
            "peer socket type PUB is incompatible"
        );
        assert_eq!(
            HandshakeError::PeerError(ErrorReason::new("400").unwrap()).to_string(),
            "peer rejected the handshake: 400"
        );
        assert_eq!(
            HandshakeError::UnexpectedCommand(Frame::from("PING")).to_string(),
            "unexpected PING command during handshake"
        );
    }
}
