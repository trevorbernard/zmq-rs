//! Per-socket options, fixed when the socket is created.

use std::fmt;
use std::num::NonZeroUsize;

use std::time::Duration;

use zmq_core::{Heartbeat, Linger, Reconnect, Security};
use zmq_pipe::Hwm;
use zmtp::{Frame, Identity};

/// How a socket presents itself and buffers messages. Given when the socket
/// is created, so it applies to every connection the socket makes.
#[derive(Debug, Clone)]
pub struct SocketOptions {
    /// The identity announced to ROUTER peers (`ZMQ_ROUTING_ID`). Empty by
    /// default, in which case a ROUTER generates one for us.
    pub identity: Identity,
    /// Messages queued per peer towards it (`ZMQ_SNDHWM`, default 1000).
    pub send_hwm: Hwm,
    /// Messages queued per peer from it (`ZMQ_RCVHWM`, default 1000).
    pub recv_hwm: Hwm,
    /// How long queued messages are still delivered after the socket is
    /// dropped (`ZMQ_LINGER`, default forever). Dropping the context waits
    /// that long for them.
    pub linger: Linger,
    /// How connections retry (`ZMQ_RECONNECT_IVL`, default 100 ms).
    pub reconnect: Reconnect,
    /// Whether a TCP or IPC connection becomes a peer only once its
    /// handshake completes, and stops being one whenever it breaks,
    /// discarding what was queued on it (`ZMQ_IMMEDIATE`, default off). Off,
    /// messages queue on it from [`connect`](crate::Socket::connect) on and
    /// across reconnections. `inproc` and PEER's `connect_peer` ignore it.
    pub immediate: bool,
    /// The largest message accepted from peers, counting all its frames
    /// (`ZMQ_MAXMSGSIZE`). A peer that sends a larger one is disconnected.
    pub max_message_size: Option<u64>,
    /// The security mechanism (`ZMQ_PLAIN_SERVER` and friends), NULL by
    /// default.
    pub security: Security,
    /// ZMTP 3.1 heartbeats (`ZMQ_HEARTBEAT_IVL` and friends), off by
    /// default.
    pub heartbeat: Option<Heartbeat>,
    /// How long a handshake may take (`ZMQ_HANDSHAKE_IVL`, default 30 s).
    pub handshake_timeout: Option<Duration>,
}

impl Default for SocketOptions {
    /// libzmq's defaults.
    fn default() -> Self {
        let hwm = Hwm::Messages(NonZeroUsize::new(1000).unwrap_or(NonZeroUsize::MIN));
        Self {
            identity: Identity::default(),
            send_hwm: hwm,
            recv_hwm: hwm,
            linger: Linger::default(),
            reconnect: Reconnect::default(),
            immediate: false,
            max_message_size: None,
            security: Security::default(),
            heartbeat: None,
            handshake_timeout: Some(Duration::from_secs(30)),
        }
    }
}

/// A peer's address on a ROUTER socket: the identity it announced, or one
/// the ROUTER generated for it. 1 to 255 octets.
#[derive(Clone, PartialEq, Eq, Hash)]
pub struct RoutingId(Frame);

/// A routing id must be 1 to 255 octets.
#[derive(Debug, Clone, Copy, PartialEq, Eq)]
pub struct InvalidRoutingId;

impl RoutingId {
    /// Validates `id`: 1 to 255 octets.
    pub fn new(id: impl Into<Frame>) -> Result<Self, InvalidRoutingId> {
        let id = id.into();
        if id.is_empty() || id.len() > usize::from(u8::MAX) {
            return Err(InvalidRoutingId);
        }
        Ok(Self(id))
    }

    /// The routing id's octets.
    pub fn as_bytes(&self) -> &[u8] {
        &self.0
    }

    /// A routing id the table already validated.
    pub(crate) fn trusted(id: Frame) -> Self {
        Self(id)
    }

    pub(crate) fn into_frame(self) -> Frame {
        self.0
    }
}

impl fmt::Debug for RoutingId {
    fn fmt(&self, f: &mut fmt::Formatter<'_>) -> fmt::Result {
        write!(f, "RoutingId(b\"{}\")", self.0.escape_ascii())
    }
}

impl fmt::Display for InvalidRoutingId {
    fn fmt(&self, f: &mut fmt::Formatter<'_>) -> fmt::Result {
        f.write_str("a routing id must be 1 to 255 octets")
    }
}

impl std::error::Error for InvalidRoutingId {}

#[cfg(test)]
mod tests {
    use super::*;

    #[test]
    fn routing_ids_are_1_to_255_octets() {
        assert_eq!(RoutingId::new("a").unwrap().as_bytes(), b"a");
        assert_eq!(RoutingId::new(vec![0; 255]).unwrap().as_bytes().len(), 255);
        assert_eq!(RoutingId::new(Frame::new()), Err(InvalidRoutingId));
        assert_eq!(RoutingId::new(vec![1; 256]), Err(InvalidRoutingId));
    }

    #[test]
    fn defaults_match_libzmq() {
        let options = SocketOptions::default();
        let thousand = Hwm::Messages(NonZeroUsize::new(1000).unwrap());
        assert_eq!(options.send_hwm, thousand);
        assert_eq!(options.recv_hwm, thousand);
        assert_eq!(options.linger, Linger::Forever);
        assert_eq!(options.identity, Identity::default());
    }

    #[test]
    fn debug_shows_the_octets() {
        assert_eq!(
            format!("{:?}", RoutingId::new("a\x00").unwrap()),
            r#"RoutingId(b"a\x00")"#
        );
    }
}
