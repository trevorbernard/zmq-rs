//! Endpoint strings, as libzmq writes them: `tcp://host:port` and
//! `inproc://name`.

use std::fmt;
use std::io;
use std::net::{IpAddr, Ipv4Addr, SocketAddr, ToSocketAddrs};

/// Why binding or connecting failed.
#[derive(Debug)]
#[non_exhaustive]
pub enum Error {
    /// The endpoint is not `transport://address`, or the address is
    /// malformed for its transport.
    InvalidEndpoint(String),
    /// The transport is not supported (yet).
    UnsupportedTransport(String),
    /// The operating system refused, e.g. the address is in use.
    Io(io::Error),
}

/// Where a socket is bound, as [`Socket::bind`](crate::Socket::bind)
/// reports it. Displays as an endpoint string another socket can connect
/// to.
#[derive(Debug, Clone, PartialEq, Eq, Hash)]
pub enum Endpoint {
    /// A TCP address, with the actual port when bound to `*`.
    Tcp(SocketAddr),
    /// An `inproc` name, reachable from sockets of the same context.
    Inproc(String),
}

impl Endpoint {
    /// The TCP address, for a TCP endpoint.
    pub fn tcp_addr(&self) -> Option<SocketAddr> {
        match self {
            Self::Tcp(addr) => Some(*addr),
            Self::Inproc(_) => None,
        }
    }
}

impl From<SocketAddr> for Endpoint {
    fn from(addr: SocketAddr) -> Self {
        Self::Tcp(addr)
    }
}

impl fmt::Display for Endpoint {
    fn fmt(&self, f: &mut fmt::Formatter<'_>) -> fmt::Result {
        match self {
            Self::Tcp(addr) => write!(f, "tcp://{addr}"),
            Self::Inproc(name) => write!(f, "inproc://{name}"),
        }
    }
}

/// What a bind or connect is aimed at.
pub(crate) enum Target {
    Tcp(SocketAddr),
    Inproc(String),
}

/// Parses `endpoint` for binding.
pub(crate) fn bind_target(endpoint: &str) -> Result<Target, Error> {
    match inproc_name(endpoint)? {
        Some(name) => Ok(Target::Inproc(name)),
        None => bind_addr(endpoint).map(Target::Tcp),
    }
}

/// Parses `endpoint` for connecting.
pub(crate) fn connect_target(endpoint: &str) -> Result<Target, Error> {
    match inproc_name(endpoint)? {
        Some(name) => Ok(Target::Inproc(name)),
        None => connect_addr(endpoint).map(Target::Tcp),
    }
}

/// The name of an `inproc://name` endpoint, which must not be empty.
fn inproc_name(endpoint: &str) -> Result<Option<String>, Error> {
    match endpoint.strip_prefix("inproc://") {
        Some("") => Err(invalid(endpoint)),
        Some(name) => Ok(Some(name.to_owned())),
        None => Ok(None),
    }
}

/// The address to bind for `endpoint`. `*` means every interface or, as the
/// port, any free port.
pub(crate) fn bind_addr(endpoint: &str) -> Result<SocketAddr, Error> {
    let (host, port) = tcp_host_port(endpoint)?;
    let port = match port {
        "*" => 0,
        port => parse_port(port, endpoint)?,
    };
    if host == "*" {
        return Ok(SocketAddr::from((Ipv4Addr::UNSPECIFIED, port)));
    }
    resolve(host, port, endpoint)
}

/// The address to connect to for `endpoint`, resolving host names.
pub(crate) fn connect_addr(endpoint: &str) -> Result<SocketAddr, Error> {
    let (host, port) = tcp_host_port(endpoint)?;
    if host == "*" || port == "*" {
        return Err(invalid(endpoint));
    }
    resolve(host, parse_port(port, endpoint)?, endpoint)
}

/// Splits `tcp://host:port`, removing brackets around an IPv6 host.
fn tcp_host_port(endpoint: &str) -> Result<(&str, &str), Error> {
    let (transport, address) = endpoint
        .split_once("://")
        .ok_or_else(|| invalid(endpoint))?;
    if transport != "tcp" {
        return Err(Error::UnsupportedTransport(transport.to_owned()));
    }
    let (host, port) = address.rsplit_once(':').ok_or_else(|| invalid(endpoint))?;
    let host = host
        .strip_prefix('[')
        .and_then(|host| host.strip_suffix(']'))
        .unwrap_or(host);
    if host.is_empty() || port.is_empty() {
        return Err(invalid(endpoint));
    }
    Ok((host, port))
}

fn parse_port(port: &str, endpoint: &str) -> Result<u16, Error> {
    port.parse().map_err(|_| invalid(endpoint))
}

fn resolve(host: &str, port: u16, endpoint: &str) -> Result<SocketAddr, Error> {
    if let Ok(ip) = host.parse::<IpAddr>() {
        return Ok(SocketAddr::new(ip, port));
    }
    (host, port)
        .to_socket_addrs()
        .map_err(Error::Io)?
        .next()
        .ok_or_else(|| invalid(endpoint))
}

fn invalid(endpoint: &str) -> Error {
    Error::InvalidEndpoint(endpoint.to_owned())
}

impl fmt::Display for Error {
    fn fmt(&self, f: &mut fmt::Formatter<'_>) -> fmt::Result {
        match self {
            Self::InvalidEndpoint(endpoint) => write!(f, "invalid endpoint \"{endpoint}\""),
            Self::UnsupportedTransport(transport) => {
                write!(f, "unsupported transport \"{transport}\"")
            }
            Self::Io(err) => write!(f, "{err}"),
        }
    }
}

impl std::error::Error for Error {
    fn source(&self) -> Option<&(dyn std::error::Error + 'static)> {
        match self {
            Self::Io(err) => Some(err),
            _ => None,
        }
    }
}

#[cfg(test)]
mod tests {
    use super::*;

    fn addr(s: &str) -> SocketAddr {
        s.parse().unwrap()
    }

    #[test]
    fn bind_wildcards() {
        assert_eq!(bind_addr("tcp://*:5555").unwrap(), addr("0.0.0.0:5555"));
        assert_eq!(bind_addr("tcp://127.0.0.1:*").unwrap(), addr("127.0.0.1:0"));
        assert_eq!(bind_addr("tcp://*:*").unwrap(), addr("0.0.0.0:0"));
        assert_eq!(bind_addr("tcp://[::1]:7").unwrap(), addr("[::1]:7"));
    }

    #[test]
    fn connect_addresses() {
        assert_eq!(
            connect_addr("tcp://127.0.0.1:5555").unwrap(),
            addr("127.0.0.1:5555")
        );
        assert_eq!(
            connect_addr("tcp://[::1]:5555").unwrap(),
            addr("[::1]:5555")
        );
        assert!(
            connect_addr("tcp://localhost:5555")
                .unwrap()
                .ip()
                .is_loopback()
        );
    }

    #[test]
    fn connect_rejects_wildcards() {
        for endpoint in ["tcp://*:5555", "tcp://127.0.0.1:*"] {
            assert!(
                matches!(connect_addr(endpoint), Err(Error::InvalidEndpoint(_))),
                "{endpoint}"
            );
        }
    }

    #[test]
    fn malformed_endpoints() {
        for endpoint in [
            "127.0.0.1:5555",
            "tcp://127.0.0.1",
            "tcp://127.0.0.1:port",
            "tcp://:5",
        ] {
            assert!(
                matches!(bind_addr(endpoint), Err(Error::InvalidEndpoint(_))),
                "{endpoint}"
            );
        }
    }

    #[test]
    fn inproc_endpoints() {
        assert!(matches!(bind_target("inproc://work"), Ok(Target::Inproc(n)) if n == "work"));
        assert!(matches!(connect_target("inproc://a/b"), Ok(Target::Inproc(n)) if n == "a/b"));
        assert!(matches!(
            bind_target("inproc://"),
            Err(Error::InvalidEndpoint(_))
        ));
        assert_eq!(Endpoint::Inproc("x".into()).to_string(), "inproc://x");
        assert_eq!(
            Endpoint::Tcp(addr("127.0.0.1:5")).to_string(),
            "tcp://127.0.0.1:5"
        );
    }

    #[test]
    fn other_transports_are_unsupported() {
        assert!(matches!(
            bind_addr("ipc:///tmp/x"),
            Err(Error::UnsupportedTransport(t)) if t == "ipc"
        ));
    }

    #[test]
    fn messages() {
        assert_eq!(
            Error::InvalidEndpoint("tcp://x".into()).to_string(),
            "invalid endpoint \"tcp://x\""
        );
        assert_eq!(
            Error::UnsupportedTransport("ipc".into()).to_string(),
            "unsupported transport \"ipc\""
        );
    }
}
