//! ZMTP greeting and version negotiation (RFC 13, 15, 23, 37).
//!
//! We always use the RFC 37 strategy that detects both ZMTP 1.0 and 2.0
//! peers, as libzmq does: send a 10-octet pseudo-signature that is also a
//! valid ZMTP 1.0 identity-frame header, then reveal the rest of our greeting
//! one step at a time as the peer reveals its version.

use std::fmt;
use std::ops::Deref;

use crate::{Identity, Mechanism, SocketType};

/// Version negotiation for one connection, as a sans-IO state machine.
///
/// Feed it the octets read from the peer with [`feed`](Self::feed) and send
/// whatever [`write_output`](Self::write_output) produces, until `feed`
/// reports the [`PeerGreeting`].
#[derive(Debug)]
pub struct Greeting {
    socket_type: SocketType,
    identity: Identity,
    mechanism: Mechanism,
    output: Vec<u8>,
    received: [u8; GREETING_LEN],
    len: usize,
    expected: usize,
    outcome: Option<Result<PeerGreeting, GreetingError>>,
}

/// Configuration for a [`Greeting`].
#[derive(Debug, Clone)]
pub struct GreetingConfig {
    pub socket_type: SocketType,
    pub identity: Identity,
    pub mechanism: Mechanism,
}

/// The result of one [`Greeting::feed`] call.
#[derive(Debug, Clone, PartialEq, Eq)]
#[must_use]
pub struct Feed {
    /// Octets taken from the input. Anything after them belongs to the peer's
    /// first frame.
    pub consumed: usize,
    /// The peer's greeting, once complete.
    pub peer: Option<PeerGreeting>,
}

/// What the peer revealed in its greeting.
#[derive(Debug, Clone, PartialEq, Eq)]
pub enum PeerGreeting {
    /// A ZMTP 1.0 peer that sent no greeting. `replay` holds the octets read
    /// so far: the start of the peer's identity frame, to be decoded first.
    /// Our own identity has already been queued for sending.
    Zmtp1Unversioned { replay: Replay },
    /// ZMTP 1.0 framing after a 12-octet greeting with revision 0 (libzmq).
    Zmtp1 { socket_type: u8 },
    /// ZMTP 2.0 (revision 1 or 2). `socket_type` is the raw RFC 15 octet.
    Zmtp2 { socket_type: u8 },
    /// ZMTP 3.x.
    Zmtp3 {
        major: u8,
        minor: u8,
        /// Whether the peer claims the server role of PLAIN or CURVE.
        as_server: bool,
    },
}

/// The protocol version both sides will speak.
#[derive(Debug, Clone, Copy, PartialEq, Eq, Hash)]
pub enum Version {
    V1,
    V2,
    V3_0,
    V3_1,
}

/// Octets of a ZMTP 1.0 peer's identity frame read during the greeting.
#[derive(Clone, PartialEq, Eq, Default)]
pub struct Replay {
    buf: [u8; SIGNATURE_LEN],
    len: usize,
}

/// Why version negotiation failed. The connection must be closed.
#[derive(Debug, Clone, PartialEq, Eq)]
pub enum GreetingError {
    /// The peer speaks ZMTP 1.0 or 2.0 and we may not downgrade: either the
    /// mechanism is not NULL, legacy peers are refused, or our socket type
    /// did not exist before ZMTP 3.x.
    LegacyPeerRefused,
    /// The peer announced a different security mechanism.
    MechanismMismatch {
        /// The peer's mechanism name, NUL-padded as on the wire.
        peer: [u8; MECHANISM_LEN],
    },
}

/// `FF`, eight padding octets, `7F`.
const SIGNATURE_LEN: usize = 10;
const REVISION_POS: usize = 10;
/// Signature, revision and socket type.
const V2_GREETING_LEN: usize = 12;
const MECHANISM_POS: usize = 12;
const MECHANISM_LEN: usize = 20;
const AS_SERVER_POS: usize = 32;
const FILLER_LEN: usize = 31;
const GREETING_LEN: usize = 64;
const MAJOR: u8 = 3;
const MINOR: u8 = 1;

impl Greeting {
    /// Starts negotiation and queues our pseudo-signature for sending.
    pub fn new(config: GreetingConfig) -> Self {
        let GreetingConfig {
            socket_type,
            identity,
            mechanism,
        } = config;
        // A ZMTP 1.0 peer reads this as the long-form header of our identity
        // frame: length (identity plus flags octet), then flags.
        let mut output = Vec::with_capacity(GREETING_LEN);
        output.push(0xFF);
        output.extend_from_slice(&(identity.as_bytes().len() as u64 + 1).to_be_bytes());
        output.push(0x7F);
        Self {
            socket_type,
            identity,
            mechanism,
            output,
            received: [0; GREETING_LEN],
            len: 0,
            expected: GREETING_LEN,
            outcome: None,
        }
    }

    /// Consumes greeting octets from `input`, never reading past the end of
    /// the peer's greeting.
    pub fn feed(&mut self, input: &[u8]) -> Result<Feed, GreetingError> {
        let mut consumed = 0;
        while self.outcome.is_none() && consumed < input.len() {
            let target = self.checkpoint();
            let n = (target - self.len).min(input.len() - consumed);
            self.received[self.len..][..n].copy_from_slice(&input[consumed..][..n]);
            self.len += n;
            consumed += n;
            if self.len == target {
                self.outcome = self.reached_checkpoint();
            }
        }
        match &self.outcome {
            None => Ok(Feed {
                consumed,
                peer: None,
            }),
            Some(Ok(peer)) => Ok(Feed {
                consumed,
                peer: Some(peer.clone()),
            }),
            Some(Err(err)) => Err(err.clone()),
        }
    }

    /// Appends the octets we need to send to `out`.
    pub fn write_output(&mut self, out: &mut Vec<u8>) {
        out.append(&mut self.output);
    }

    /// The next length at which the received octets can be interpreted.
    fn checkpoint(&self) -> usize {
        match self.len {
            0 => 1,
            1..SIGNATURE_LEN => SIGNATURE_LEN,
            SIGNATURE_LEN => REVISION_POS + 1,
            _ => self.expected,
        }
    }

    fn reached_checkpoint(&mut self) -> Option<Result<PeerGreeting, GreetingError>> {
        match self.len {
            // A ZMTP 1.0 peer starts with a short frame length instead.
            1 => (self.received[0] != 0xFF).then(|| self.unversioned()),
            // A ZMTP 1.0 long frame length is followed by a flags octet with
            // the low bit clear; the signature ends in 0x7F.
            SIGNATURE_LEN if self.received[SIGNATURE_LEN - 1] & 1 == 0 => Some(self.unversioned()),
            SIGNATURE_LEN => {
                self.output.push(MAJOR);
                None
            }
            len if len == REVISION_POS + 1 => self.revision_received().err().map(Err),
            _ => Some(self.complete()),
        }
    }

    fn unversioned(&mut self) -> Result<PeerGreeting, GreetingError> {
        self.legacy_socket_type()?;
        // The pseudo-signature already carried our identity frame header.
        self.output.extend_from_slice(self.identity.as_bytes());
        let mut replay = Replay {
            buf: [0; SIGNATURE_LEN],
            len: self.len,
        };
        replay.buf[..self.len].copy_from_slice(&self.received[..self.len]);
        Ok(PeerGreeting::Zmtp1Unversioned { replay })
    }

    fn revision_received(&mut self) -> Result<(), GreetingError> {
        if self.received[REVISION_POS] < MAJOR {
            let code = self.legacy_socket_type()?;
            self.output.push(code);
            self.expected = V2_GREETING_LEN;
        } else {
            self.output.push(MINOR);
            self.output
                .extend_from_slice(&self.mechanism.greeting_name());
            self.output.push(u8::from(self.mechanism.is_server()));
            self.output.extend_from_slice(&[0; FILLER_LEN]);
            self.expected = GREETING_LEN;
        }
        Ok(())
    }

    fn complete(&self) -> Result<PeerGreeting, GreetingError> {
        let after_revision = self.received[REVISION_POS + 1];
        match self.received[REVISION_POS] {
            0 => Ok(PeerGreeting::Zmtp1 {
                socket_type: after_revision,
            }),
            1 | 2 => Ok(PeerGreeting::Zmtp2 {
                socket_type: after_revision,
            }),
            major => {
                let mut peer = [0; MECHANISM_LEN];
                peer.copy_from_slice(&self.received[MECHANISM_POS..][..MECHANISM_LEN]);
                if peer != self.mechanism.greeting_name() {
                    return Err(GreetingError::MechanismMismatch { peer });
                }
                Ok(PeerGreeting::Zmtp3 {
                    major,
                    minor: after_revision,
                    as_server: self.received[AS_SERVER_POS] != 0,
                })
            }
        }
    }

    /// Our RFC 15 socket type, if we may talk to a ZMTP 1.0 or 2.0 peer.
    fn legacy_socket_type(&self) -> Result<u8, GreetingError> {
        match self.socket_type.legacy_code() {
            Some(code) if self.mechanism.allows_legacy_peers() => Ok(code),
            _ => Err(GreetingError::LegacyPeerRefused),
        }
    }
}

impl PeerGreeting {
    /// The protocol version to speak with this peer.
    pub fn version(&self) -> Version {
        match self {
            Self::Zmtp1Unversioned { .. } | Self::Zmtp1 { .. } => Version::V1,
            Self::Zmtp2 { .. } => Version::V2,
            Self::Zmtp3 {
                major: 3, minor: 0, ..
            } => Version::V3_0,
            Self::Zmtp3 { .. } => Version::V3_1,
        }
    }
}

impl Deref for Replay {
    type Target = [u8];

    fn deref(&self) -> &[u8] {
        &self.buf[..self.len]
    }
}

impl fmt::Debug for Replay {
    fn fmt(&self, f: &mut fmt::Formatter<'_>) -> fmt::Result {
        write!(f, "Replay(b\"{}\")", self.escape_ascii())
    }
}

impl fmt::Display for GreetingError {
    fn fmt(&self, f: &mut fmt::Formatter<'_>) -> fmt::Result {
        match self {
            Self::LegacyPeerRefused => {
                f.write_str("peer speaks ZMTP 1.0 or 2.0 and downgrade is not allowed")
            }
            Self::MechanismMismatch { peer } => {
                let name = peer.split(|&b| b == 0).next().unwrap_or_default();
                write!(
                    f,
                    "peer requested security mechanism \"{}\"",
                    name.escape_ascii()
                )
            }
        }
    }
}

impl std::error::Error for GreetingError {}

#[cfg(test)]
mod tests {
    use super::*;
    use crate::{LegacyPeers, Role};

    const NULL: Mechanism = Mechanism::Null {
        legacy_peers: LegacyPeers::Allow,
    };

    fn greeting(socket_type: SocketType, mechanism: Mechanism) -> Greeting {
        Greeting::new(GreetingConfig {
            socket_type,
            identity: Identity::new(&b"abc"[..]).unwrap(),
            mechanism,
        })
    }

    fn output(g: &mut Greeting) -> Vec<u8> {
        let mut out = Vec::new();
        g.write_output(&mut out);
        out
    }

    /// A full ZMTP 3.x greeting as libzmq sends it.
    fn v3_greeting(major: u8, minor: u8, mechanism: &[u8], as_server: bool) -> Vec<u8> {
        let mut g = vec![0xFF, 0, 0, 0, 0, 0, 0, 0, 1, 0x7F, major, minor];
        let mut name = [0u8; 20];
        name[..mechanism.len()].copy_from_slice(mechanism);
        g.extend_from_slice(&name);
        g.push(u8::from(as_server));
        g.extend_from_slice(&[0; 31]);
        g
    }

    fn null_name() -> [u8; 20] {
        let mut name = [0u8; 20];
        name[..4].copy_from_slice(b"NULL");
        name
    }

    #[test]
    fn starts_with_pseudo_signature_carrying_identity_length() {
        let mut g = greeting(SocketType::Dealer, NULL);
        // 0xFF, u64 BE (identity length + 1), 0x7F: also a ZMTP 1.0 frame header.
        assert_eq!(output(&mut g), [0xFF, 0, 0, 0, 0, 0, 0, 0, 4, 0x7F]);
        assert!(output(&mut g).is_empty(), "output is drained");
    }

    #[test]
    fn negotiates_zmtp_3_1_step_by_step() {
        let mut g = greeting(SocketType::Dealer, NULL);
        let peer = v3_greeting(3, 1, b"NULL", false);
        let _ = output(&mut g);

        let feed = g.feed(&peer[..10]).unwrap();
        assert_eq!((feed.consumed, feed.peer), (10, None));
        assert_eq!(output(&mut g), [3], "major version after the signature");

        let feed = g.feed(&peer[10..11]).unwrap();
        assert_eq!((feed.consumed, feed.peer), (1, None));
        let mut rest = vec![1];
        rest.extend_from_slice(&null_name());
        rest.extend_from_slice(&[0; 32]);
        assert_eq!(output(&mut g), rest, "minor, mechanism, as-server, filler");

        let feed = g.feed(&peer[11..]).unwrap();
        assert_eq!(feed.consumed, 53);
        let peer = feed.peer.unwrap();
        assert_eq!(
            peer,
            PeerGreeting::Zmtp3 {
                major: 3,
                minor: 1,
                as_server: false
            }
        );
        assert_eq!(peer.version(), Version::V3_1);
    }

    #[test]
    fn complete_output_is_a_64_octet_greeting() {
        let mut g = greeting(SocketType::Dealer, NULL);
        let peer = v3_greeting(3, 1, b"NULL", false);
        g.feed(&peer).unwrap().peer.unwrap();
        let out = output(&mut g);
        assert_eq!(out.len(), 64);
        assert_eq!(out[10..12], [3, 1]);
    }

    #[test]
    fn never_reads_past_the_greeting() {
        let mut g = greeting(SocketType::Dealer, NULL);
        let mut input = v3_greeting(3, 1, b"NULL", false);
        input.extend_from_slice(b"first frame");
        let feed = g.feed(&input).unwrap();
        assert_eq!(feed.consumed, 64);
        assert!(feed.peer.is_some());
    }

    #[test]
    fn accepts_one_octet_at_a_time() {
        let mut g = greeting(SocketType::Dealer, NULL);
        let peer = v3_greeting(3, 1, b"NULL", false);
        let mut done = None;
        for (i, octet) in peer.iter().enumerate() {
            let feed = g.feed(std::slice::from_ref(octet)).unwrap();
            assert_eq!(feed.consumed, 1);
            assert_eq!(feed.peer.is_some(), i == 63, "complete only at the end");
            done = feed.peer;
        }
        assert_eq!(done.unwrap().version(), Version::V3_1);
    }

    #[test]
    fn minor_zero_selects_zmtp_3_0() {
        let mut g = greeting(SocketType::Sub, NULL);
        let peer = g
            .feed(&v3_greeting(3, 0, b"NULL", false))
            .unwrap()
            .peer
            .unwrap();
        assert_eq!(peer.version(), Version::V3_0);
    }

    #[test]
    fn higher_versions_use_zmtp_3_1() {
        let mut g = greeting(SocketType::Sub, NULL);
        let peer = g
            .feed(&v3_greeting(4, 0, b"NULL", false))
            .unwrap()
            .peer
            .unwrap();
        assert_eq!(peer.version(), Version::V3_1);
    }

    #[test]
    fn signature_padding_is_not_validated() {
        let mut g = greeting(SocketType::Dealer, NULL);
        let mut peer = v3_greeting(3, 1, b"NULL", false);
        peer[1..9].copy_from_slice(&[0xAB; 8]);
        assert!(g.feed(&peer).unwrap().peer.is_some());
    }

    #[test]
    fn mechanism_mismatch_fails() {
        let mut g = greeting(SocketType::Dealer, NULL);
        let mut name = [0u8; 20];
        name[..5].copy_from_slice(b"PLAIN");
        assert_eq!(
            g.feed(&v3_greeting(3, 1, b"PLAIN", true)),
            Err(GreetingError::MechanismMismatch { peer: name })
        );
    }

    #[test]
    fn sends_mechanism_name_and_role() {
        let cases = [
            (Mechanism::Plain(Role::Server), &b"PLAIN"[..], 1),
            (Mechanism::Plain(Role::Client), b"PLAIN", 0),
            (Mechanism::Curve(Role::Server), b"CURVE", 1),
            (Mechanism::Curve(Role::Client), b"CURVE", 0),
            (NULL, b"NULL", 0),
        ];
        for (mechanism, name, as_server) in cases {
            let mut g = greeting(SocketType::Dealer, mechanism);
            let _ = g.feed(&v3_greeting(3, 1, name, as_server == 0)).unwrap();
            let out = output(&mut g);
            assert_eq!(&out[12..12 + name.len()], name, "{mechanism:?}");
            assert!(out[12 + name.len()..32].iter().all(|&b| b == 0));
            assert_eq!(out[32], as_server, "{mechanism:?}");
        }
    }

    #[test]
    fn peer_as_server_is_reported_not_validated() {
        let mut g = greeting(SocketType::Dealer, Mechanism::Plain(Role::Client));
        let peer = g
            .feed(&v3_greeting(3, 1, b"PLAIN", false))
            .unwrap()
            .peer
            .unwrap();
        assert_eq!(
            peer,
            PeerGreeting::Zmtp3 {
                major: 3,
                minor: 1,
                as_server: false
            }
        );
    }

    #[test]
    fn zmtp_2_peer_gets_our_socket_type() {
        let mut g = greeting(SocketType::Dealer, NULL);
        let _ = output(&mut g);
        let peer_greeting = [0xFF, 0, 0, 0, 0, 0, 0, 0, 1, 0x7F, 1, 6];
        let feed = g.feed(&peer_greeting).unwrap();
        assert_eq!(feed.consumed, 12);
        assert_eq!(feed.peer, Some(PeerGreeting::Zmtp2 { socket_type: 6 }));
        assert_eq!(feed.peer.unwrap().version(), Version::V2);
        assert_eq!(output(&mut g), [3, 5], "major, then DEALER's RFC 15 code");
    }

    #[test]
    fn revision_2_is_zmtp_2_per_rfc_37() {
        let mut g = greeting(SocketType::Dealer, NULL);
        let peer = g
            .feed(&[0xFF, 0, 0, 0, 0, 0, 0, 0, 1, 0x7F, 2, 6])
            .unwrap()
            .peer;
        assert_eq!(peer, Some(PeerGreeting::Zmtp2 { socket_type: 6 }));
    }

    #[test]
    fn revision_0_is_zmtp_1_framing() {
        let mut g = greeting(SocketType::Dealer, NULL);
        let peer = g
            .feed(&[0xFF, 0, 0, 0, 0, 0, 0, 0, 1, 0x7F, 0, 6])
            .unwrap()
            .peer
            .unwrap();
        assert_eq!(peer, PeerGreeting::Zmtp1 { socket_type: 6 });
        assert_eq!(peer.version(), Version::V1);
    }

    #[test]
    fn xpub_announces_itself_as_pub_to_legacy_peers() {
        let mut g = greeting(SocketType::XPub, NULL);
        let _ = output(&mut g);
        let _ = g.feed(&[0xFF, 0, 0, 0, 0, 0, 0, 0, 1, 0x7F, 1, 2]).unwrap();
        assert_eq!(output(&mut g), [3, 1]);
    }

    #[test]
    fn unversioned_peer_with_short_identity_length() {
        let mut g = greeting(SocketType::Dealer, NULL);
        let _ = output(&mut g);
        // ZMTP 1.0 identity frame: length 4 (flags + "xyz"), flags 0, "xyz".
        let feed = g.feed(b"\x04\x00xyz").unwrap();
        assert_eq!(feed.consumed, 1);
        let peer = feed.peer.unwrap();
        assert_eq!(peer.version(), Version::V1);
        let PeerGreeting::Zmtp1Unversioned { replay } = peer else {
            panic!("expected unversioned peer, got {peer:?}");
        };
        assert_eq!(&*replay, b"\x04");
        assert_eq!(output(&mut g), b"abc", "rest of our identity frame");
    }

    #[test]
    fn unversioned_peer_with_long_identity_length() {
        let mut g = greeting(SocketType::Dealer, NULL);
        let _ = output(&mut g);
        // Long length (0xFF + u64), flags octet with the low bit clear.
        let input = [0xFF, 0, 0, 0, 0, 0, 0, 1, 0, 0x00, b'x'];
        let feed = g.feed(&input).unwrap();
        assert_eq!(feed.consumed, 10);
        let Some(PeerGreeting::Zmtp1Unversioned { replay }) = feed.peer else {
            panic!("expected unversioned peer");
        };
        assert_eq!(&*replay, &input[..10]);
        assert_eq!(output(&mut g), b"abc");
    }

    #[test]
    fn legacy_peers_can_be_refused_for_null() {
        let refuse = Mechanism::Null {
            legacy_peers: LegacyPeers::Refuse,
        };
        let mut g = greeting(SocketType::Dealer, refuse);
        assert_eq!(
            g.feed(&[0xFF, 0, 0, 0, 0, 0, 0, 0, 1, 0x7F, 1]),
            Err(GreetingError::LegacyPeerRefused)
        );
        let mut g = greeting(SocketType::Dealer, refuse);
        assert_eq!(g.feed(b"\x04"), Err(GreetingError::LegacyPeerRefused));
    }

    #[test]
    fn secure_mechanisms_refuse_legacy_peers() {
        let mut g = greeting(SocketType::Dealer, Mechanism::Curve(Role::Client));
        assert_eq!(
            g.feed(&[0xFF, 0, 0, 0, 0, 0, 0, 0, 1, 0x7F, 1]),
            Err(GreetingError::LegacyPeerRefused)
        );
        let mut g = greeting(SocketType::Dealer, Mechanism::Plain(Role::Server));
        assert_eq!(g.feed(b"\x04"), Err(GreetingError::LegacyPeerRefused));
    }

    #[test]
    fn thread_safe_types_refuse_legacy_peers() {
        let mut g = greeting(SocketType::Server, NULL);
        assert_eq!(
            g.feed(&[0xFF, 0, 0, 0, 0, 0, 0, 0, 1, 0x7F, 1]),
            Err(GreetingError::LegacyPeerRefused)
        );
    }

    #[test]
    fn feeding_after_completion_consumes_nothing() {
        let mut g = greeting(SocketType::Dealer, NULL);
        let first = g.feed(&v3_greeting(3, 1, b"NULL", false)).unwrap().peer;
        let again = g.feed(b"more").unwrap();
        assert_eq!(
            again,
            Feed {
                consumed: 0,
                peer: first
            }
        );
    }

    #[test]
    fn failure_is_sticky() {
        let mut g = greeting(SocketType::Dealer, Mechanism::Curve(Role::Client));
        let err = g.feed(b"\x04").unwrap_err();
        assert_eq!(g.feed(&v3_greeting(3, 1, b"CURVE", true)), Err(err));
        assert_eq!(
            GreetingError::LegacyPeerRefused.to_string(),
            "peer speaks ZMTP 1.0 or 2.0 and downgrade is not allowed"
        );
    }

    #[test]
    fn mismatch_error_names_the_peer_mechanism() {
        let mut peer = [0u8; 20];
        peer[..5].copy_from_slice(b"CURVE");
        assert_eq!(
            GreetingError::MechanismMismatch { peer }.to_string(),
            "peer requested security mechanism \"CURVE\""
        );
    }
}
