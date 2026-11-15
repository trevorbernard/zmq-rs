//! Per-socket options, given when the socket is created.

use std::fmt;
use std::num::NonZeroUsize;

use std::time::Duration;

use zmq_core::{Heartbeat, Linger, MessageProperties, Reconnect, Security, TcpKeepalive};
use zmq_pipe::Hwm;
use zmtp::{Credential, Frame, Identity};

use crate::Error;
use crate::endpoint::{Target, connect_target};

/// How a socket presents itself and buffers messages. Given when the socket
/// is created, so it applies to every connection the socket makes.
///
/// Each option can be changed later with the socket's method of the same
/// name prefixed with `set_`, such as
/// [`set_linger`](crate::Socket::set_linger), as libzmq's `zmq_setsockopt`
/// on a live socket: the linger and the high-water marks change for the
/// connections the socket has, every other option for binds and connects
/// made afterwards.
///
/// Starts from libzmq's defaults; each method sets one option and returns
/// the options, so they chain:
///
/// ```
/// use std::time::Duration;
/// use zmq::{Context, Dealer, Identity, Linger, SocketOptions};
///
/// let options = SocketOptions::new()
///     .identity(Identity::new("worker-1")?)
///     .linger(Linger::For(Duration::ZERO));
/// let context = Context::new()?;
/// let worker = context.socket_with::<Dealer>(options);
/// # drop(worker);
/// # Ok::<(), Box<dyn std::error::Error>>(())
/// ```
#[derive(Debug, Clone)]
pub struct SocketOptions {
    pub(crate) identity: Identity,
    pub(crate) send_hwm: Hwm,
    pub(crate) recv_hwm: Hwm,
    /// `None` is the socket type's default.
    pub(crate) linger: Option<Linger>,
    pub(crate) reconnect: Reconnect,
    pub(crate) immediate: bool,
    pub(crate) max_message_size: Option<u64>,
    pub(crate) message_properties: MessageProperties,
    pub(crate) security: Security,
    pub(crate) heartbeat: Option<Heartbeat>,
    pub(crate) tcp_keepalive: Option<TcpKeepalive>,
    pub(crate) type_of_service: u8,
    pub(crate) bind_to_device: Option<String>,
    pub(crate) socks_proxy: Option<SocksProxy>,
    /// `None` or zero is no limit.
    pub(crate) handshake_timeout: Option<Duration>,
}

impl Default for SocketOptions {
    /// libzmq's defaults.
    fn default() -> Self {
        let hwm = Hwm::Messages(NonZeroUsize::new(1000).unwrap_or(NonZeroUsize::MIN));
        Self {
            identity: Identity::default(),
            send_hwm: hwm,
            recv_hwm: hwm,
            linger: None,
            reconnect: Reconnect::default(),
            immediate: false,
            max_message_size: None,
            message_properties: MessageProperties::default(),
            security: Security::default(),
            heartbeat: None,
            tcp_keepalive: None,
            type_of_service: 0,
            bind_to_device: None,
            socks_proxy: None,
            handshake_timeout: Some(Duration::from_secs(30)),
        }
    }
}

impl SocketOptions {
    /// libzmq's defaults, as [`SocketOptions::default`].
    #[must_use]
    pub fn new() -> Self {
        Self::default()
    }

    /// The identity announced to ROUTER peers (`ZMQ_ROUTING_ID`). Empty by
    /// default, in which case a ROUTER generates one for us.
    #[must_use]
    pub fn identity(mut self, identity: Identity) -> Self {
        self.identity = identity;
        self
    }

    /// Messages queued per peer towards it (`ZMQ_SNDHWM`, default 1000).
    #[must_use]
    pub fn send_hwm(mut self, hwm: impl Into<Hwm>) -> Self {
        self.send_hwm = hwm.into();
        self
    }

    /// Messages queued per peer from it (`ZMQ_RCVHWM`, default 1000).
    #[must_use]
    pub fn recv_hwm(mut self, hwm: impl Into<Hwm>) -> Self {
        self.recv_hwm = hwm.into();
        self
    }

    /// How long queued messages are still delivered after the socket is
    /// dropped (`ZMQ_LINGER`). Dropping the context waits that long for
    /// them. The default is the socket type's, as libzmq's: forever, but
    /// not at all for SUB, XSUB and DISH, whose subscriptions and joins a
    /// peer that never comes would otherwise hold up the context.
    #[must_use]
    pub fn linger(mut self, linger: Linger) -> Self {
        self.linger = Some(linger);
        self
    }

    /// How connections retry (`ZMQ_RECONNECT_IVL`, default 100 ms).
    #[must_use]
    pub fn reconnect(mut self, reconnect: Reconnect) -> Self {
        self.reconnect = reconnect;
        self
    }

    /// Whether a TCP or IPC connection becomes a peer only once its
    /// handshake completes, and stops being one whenever it breaks,
    /// discarding what was queued on it (`ZMQ_IMMEDIATE`, default off). Off,
    /// messages queue on it from [`connect`](crate::Socket::connect) on and
    /// across reconnections. `inproc` and PEER's `connect_peer` ignore it.
    #[must_use]
    pub fn immediate(mut self, immediate: bool) -> Self {
        self.immediate = immediate;
        self
    }

    /// The largest message accepted from peers, in octets, counting all its
    /// frames (`ZMQ_MAXMSGSIZE`). A peer that sends a larger one is
    /// disconnected. No limit by default.
    #[must_use]
    pub fn max_message_size(mut self, octets: u64) -> Self {
        self.max_message_size = Some(octets);
        self
    }

    /// Whether received messages carry their connection's properties, read
    /// with [`Message::property`](crate::Message::property) as libzmq's
    /// `zmq_msg_gets`. On by default, as in libzmq.
    #[must_use]
    pub fn message_properties(mut self, message_properties: MessageProperties) -> Self {
        self.message_properties = message_properties;
        self
    }

    /// The security mechanism (`ZMQ_PLAIN_SERVER` and friends), NULL by
    /// default.
    #[must_use]
    pub fn security(mut self, security: Security) -> Self {
        self.security = security;
        self
    }

    /// ZMTP 3.1 heartbeats (`ZMQ_HEARTBEAT_IVL` and friends), off by
    /// default.
    #[must_use]
    pub fn heartbeat(mut self, heartbeat: Heartbeat) -> Self {
        self.heartbeat = Some(heartbeat);
        self
    }

    /// TCP keepalive probes on TCP connections (`ZMQ_TCP_KEEPALIVE` and
    /// friends), off by default, leaving the system's setting. They find a
    /// peer that vanished without closing, as when a NAT or load balancer
    /// drops an idle connection.
    #[must_use]
    pub fn tcp_keepalive(mut self, keepalive: TcpKeepalive) -> Self {
        self.tcp_keepalive = Some(keepalive);
        self
    }

    /// The IP type of service, or IPv6 traffic class, of what TCP
    /// connections send (`ZMQ_TOS`), set before they connect or listen.
    /// Zero, the default, leaves the system's.
    #[must_use]
    pub fn type_of_service(mut self, type_of_service: u8) -> Self {
        self.type_of_service = type_of_service;
        self
    }

    /// The network interface TCP connections and listeners are bound to
    /// (`ZMQ_BINDTODEVICE`, Linux's `SO_BINDTODEVICE`), none by default. A
    /// bind fails, and a connect keeps retrying, where the system does not
    /// support or permit it.
    #[must_use]
    pub fn bind_to_device(mut self, device: impl Into<String>) -> Self {
        self.bind_to_device = Some(device.into());
        self
    }

    /// The SOCKS5 proxy TCP connections are made through
    /// (`ZMQ_SOCKS_PROXY`), none by default.
    #[must_use]
    pub fn socks_proxy(mut self, proxy: SocksProxy) -> Self {
        self.socks_proxy = Some(proxy);
        self
    }

    /// How long a handshake may take (`ZMQ_HANDSHAKE_IVL`, default 30 s).
    /// Zero means no limit, as libzmq's 0 does.
    #[must_use]
    pub fn handshake_timeout(mut self, timeout: Duration) -> Self {
        self.handshake_timeout = Some(timeout);
        self
    }
}

/// A SOCKS5 proxy (RFC 1928) for a socket's TCP connections, as libzmq's
/// `ZMQ_SOCKS_PROXY`. The proxy is asked to connect to each endpoint, a host
/// name being left for it to resolve; `ipc` and `inproc` connects, and
/// binds, do not go through it.
///
/// ```
/// let proxy = zmq::SocksProxy::new("proxy.example:1080")?;
/// let options = zmq::SocketOptions::new().socks_proxy(proxy);
/// # Ok::<(), zmq::Error>(())
/// ```
#[derive(Debug, Clone, PartialEq, Eq)]
pub struct SocksProxy(pub(crate) zmq_core::SocksProxy);

impl SocksProxy {
    /// The proxy at `address`: `host:port`, as libzmq takes it, without
    /// `tcp://`. The host is resolved for each connection attempt, as an
    /// endpoint's is. `source;host:port` reaches the proxy from `source`,
    /// as an endpoint's source does.
    pub fn new(address: &str) -> Result<Self, Error> {
        let invalid = || Error::InvalidEndpoint(address.to_owned());
        let (host, port, source) = match connect_target(&format!("tcp://{address}")) {
            Ok(Target::Tcp(addr)) => (addr.ip().to_string(), addr.port(), None),
            Ok(Target::TcpHost { host, port, source }) => (host, port, source),
            _ => return Err(invalid()),
        };
        if port == 0 {
            return Err(invalid());
        }
        Ok(Self(zmq_core::SocksProxy {
            host,
            port,
            source,
            username: Credential::default(),
            password: Credential::default(),
        }))
    }

    /// Authenticates to the proxy with a user name and password (RFC
    /// 1929), as libzmq's draft `ZMQ_SOCKS_USERNAME` and
    /// `ZMQ_SOCKS_PASSWORD`. As in libzmq, an empty user name means no
    /// authentication, whatever the password.
    #[cfg(feature = "draft")]
    #[must_use]
    pub fn with_credentials(mut self, username: Credential, password: Credential) -> Self {
        self.0.username = username;
        self.0.password = password;
        self
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
}

/// The routing id's octets as a frame, without a copy: to put it back in
/// front of a message, as a broker does to remember where a request came
/// from.
impl From<RoutingId> for Frame {
    fn from(id: RoutingId) -> Self {
        id.0
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
        assert_eq!(options.linger, None);
        assert_eq!(options.identity, Identity::default());
        assert_eq!(options.socks_proxy, None);
    }

    #[test]
    fn message_properties_are_attached_by_default_as_in_libzmq() {
        assert_eq!(
            SocketOptions::default().message_properties,
            MessageProperties::Attach
        );
    }

    #[test]
    fn debug_shows_the_octets() {
        assert_eq!(
            format!("{:?}", RoutingId::new("a\x00").unwrap()),
            r#"RoutingId(b"a\x00")"#
        );
    }
}
