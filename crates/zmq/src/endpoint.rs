//! Endpoint strings, as libzmq writes them: `tcp://host:port`,
//! `ipc://path`, `ipc://@name` (Linux, Android) and `inproc://name`.

use std::fmt;
use std::io;
use std::net::{IpAddr, Ipv4Addr, SocketAddr};
use std::path::PathBuf;

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
    /// The socket is not bound or connected there, for
    /// [`Socket::unbind`](crate::Socket::unbind) and
    /// [`Socket::disconnect`](crate::Socket::disconnect).
    UnknownEndpoint(String),
}

/// Where a socket is bound, as [`Socket::bind`](crate::Socket::bind)
/// reports it. Displays as an endpoint string another socket can connect
/// to.
#[derive(Debug, Clone, PartialEq, Eq, Hash)]
pub enum Endpoint {
    /// A TCP address, with the actual port when bound to `*`.
    Tcp(SocketAddr),
    /// A Unix domain socket's path, for `ipc://`. Unix only.
    Ipc(PathBuf),
    /// A name in the abstract socket namespace, without the leading `@`,
    /// for `ipc://@name`. Linux and Android only.
    IpcAbstract(String),
    /// An `inproc` name, reachable from sockets of the same context.
    Inproc(String),
}

impl Endpoint {
    /// The TCP address, for a TCP endpoint.
    #[must_use]
    pub fn tcp_addr(&self) -> Option<SocketAddr> {
        match self {
            Self::Tcp(addr) => Some(*addr),
            Self::Ipc(_) | Self::IpcAbstract(_) | Self::Inproc(_) => None,
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
            Self::Ipc(path) => write!(f, "ipc://{}", path.display()),
            Self::IpcAbstract(name) => write!(f, "ipc://@{name}"),
            Self::Inproc(name) => write!(f, "inproc://{name}"),
        }
    }
}

/// What a bind or connect is aimed at.
#[derive(Debug, Clone, PartialEq, Eq)]
pub(crate) enum Target {
    Tcp(SocketAddr),
    /// A host name to connect to, resolved for each connection attempt, as
    /// libzmq resolves it.
    TcpHost {
        host: String,
        port: u16,
    },
    Ipc(PathBuf),
    /// `ipc://*`: a fresh path in a new temporary directory.
    IpcAnywhere,
    /// `ipc://@name`: `name` in Linux's abstract socket namespace.
    #[cfg(any(target_os = "linux", target_os = "android"))]
    IpcAbstract(String),
    Inproc(String),
}

impl Target {
    /// The endpoint aimed at; `None` for `ipc://*`, which names no path
    /// until bound.
    pub(crate) fn into_endpoint(self) -> Option<Endpoint> {
        match self {
            Self::Tcp(addr) => Some(Endpoint::Tcp(addr)),
            Self::TcpHost { .. } => None,
            Self::Ipc(path) => Some(Endpoint::Ipc(path)),
            Self::IpcAnywhere => None,
            #[cfg(any(target_os = "linux", target_os = "android"))]
            Self::IpcAbstract(name) => Some(Endpoint::IpcAbstract(name)),
            Self::Inproc(name) => Some(Endpoint::Inproc(name)),
        }
    }

    /// The address a TCP target resolves to now, looking a host name up.
    pub(crate) fn resolve(&self) -> Option<SocketAddr> {
        match self {
            Self::Tcp(addr) => Some(*addr),
            Self::TcpHost { host, port } => zmq_core::resolve_tcp(host, *port).ok(),
            _ => None,
        }
    }

    /// Whether a connection made to `self` is one `wanted`, as given to
    /// `disconnect`, names: the same target, or one that resolves to the
    /// same address, `wanted_addr` being what `wanted` resolves to.
    pub(crate) fn names(&self, wanted: &Self, wanted_addr: Option<SocketAddr>) -> bool {
        self == wanted || (wanted_addr.is_some() && self.resolve() == wanted_addr)
    }
}

/// Parses `endpoint` for binding.
pub(crate) fn bind_target(endpoint: &str) -> Result<Target, Error> {
    match local_target(endpoint)? {
        Some(target) => Ok(target),
        None => bind_addr(endpoint).map(Target::Tcp),
    }
}

/// Parses `endpoint` for connecting.
pub(crate) fn connect_target(endpoint: &str) -> Result<Target, Error> {
    match local_target(endpoint)? {
        Some(Target::IpcAnywhere) => Err(invalid(endpoint)),
        Some(target) => Ok(target),
        None => connect_addr(endpoint),
    }
}

/// An `ipc://path` or `inproc://name` endpoint, whose path or name must not
/// be empty. On Linux and Android, `ipc://@name` is an abstract name, where
/// `*` is a literal name and not a wildcard, as in libzmq. Elsewhere it is a
/// relative path.
fn local_target(endpoint: &str) -> Result<Option<Target>, Error> {
    if let Some(name) = endpoint.strip_prefix("inproc://") {
        return match name {
            "" => Err(invalid(endpoint)),
            name => Ok(Some(Target::Inproc(name.to_owned()))),
        };
    }
    if let Some(path) = endpoint.strip_prefix("ipc://") {
        if !cfg!(unix) {
            return Err(Error::UnsupportedTransport("ipc".to_owned()));
        }
        #[cfg(any(target_os = "linux", target_os = "android"))]
        if let Some(name) = path.strip_prefix('@') {
            return match name {
                "" => Err(invalid(endpoint)),
                name => Ok(Some(Target::IpcAbstract(name.to_owned()))),
            };
        }
        return match path {
            "" => Err(invalid(endpoint)),
            "*" => Ok(Some(Target::IpcAnywhere)),
            path => Ok(Some(Target::Ipc(PathBuf::from(path)))),
        };
    }
    Ok(None)
}

/// The address to bind for `endpoint`. `*` means every interface or, as the
/// port, any free port.
fn bind_addr(endpoint: &str) -> Result<SocketAddr, Error> {
    let (host, port) = tcp_host_port(endpoint)?;
    let port = match port {
        "*" => 0,
        port => parse_port(port, endpoint)?,
    };
    if host == "*" {
        return Ok(SocketAddr::from((Ipv4Addr::UNSPECIFIED, port)));
    }
    if let Ok(ip) = host.parse::<IpAddr>() {
        return Ok(SocketAddr::new(ip, port));
    }
    // As libzmq's bind, which resolves at once and fails with EINVAL.
    zmq_core::resolve_tcp(host, port).map_err(|_| invalid(endpoint))
}

/// What to connect to for `endpoint`. A host name is resolved only when
/// connecting, as libzmq defers it, so it must only look like one.
fn connect_addr(endpoint: &str) -> Result<Target, Error> {
    let (host, port) = tcp_host_port(endpoint)?;
    if host == "*" || port == "*" {
        return Err(invalid(endpoint));
    }
    let port = parse_port(port, endpoint)?;
    if let Ok(ip) = host.parse::<IpAddr>() {
        return Ok(Target::Tcp(SocketAddr::new(ip, port)));
    }
    // An IPv6 address with a scope, such as `fe80::1%eth0`, which only the
    // system resolver parses. It is numeric, so nothing is looked up.
    if host.contains(':') {
        return zmq_core::resolve_tcp(host, port)
            .map(Target::Tcp)
            .map_err(|_| invalid(endpoint));
    }
    if !is_host_name(host) {
        return Err(invalid(endpoint));
    }
    Ok(Target::TcpHost {
        host: host.to_owned(),
        port,
    })
}

/// Whether `host` looks like a host name: letters, digits, `.`, `-` and
/// `_`, starting with a letter or digit, as libzmq checks a connect's.
fn is_host_name(host: &str) -> bool {
    host.starts_with(|c: char| c.is_ascii_alphanumeric())
        && host
            .chars()
            .all(|c| c.is_ascii_alphanumeric() || matches!(c, '.' | '-' | '_'))
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
            // The io error is the source, so reporters that walk sources
            // print it once.
            Self::Io(_) => f.write_str("the operating system refused"),
            Self::UnknownEndpoint(endpoint) => {
                write!(f, "not bound or connected at \"{endpoint}\"")
            }
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
mod properties;

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
            Target::Tcp(addr("127.0.0.1:5555"))
        );
        assert_eq!(
            connect_addr("tcp://[::1]:5555").unwrap(),
            Target::Tcp(addr("[::1]:5555"))
        );
        // Resolved only when connecting, as libzmq does.
        assert_eq!(
            connect_addr("tcp://no-such-host.invalid:5555").unwrap(),
            Target::TcpHost {
                host: "no-such-host.invalid".into(),
                port: 5555
            }
        );
    }

    #[test]
    fn connect_host_names_must_look_like_one() {
        for endpoint in [
            "tcp://-lead:1",
            "tcp://sp ace:1",
            "tcp://a/b:1",
            "tcp://a!b:1",
        ] {
            assert!(
                matches!(connect_addr(endpoint), Err(Error::InvalidEndpoint(_))),
                "{endpoint}"
            );
        }
        assert!(connect_addr("tcp://under_score-1.example:1").is_ok());
    }

    #[test]
    fn binding_a_host_that_does_not_resolve_is_invalid() {
        assert!(matches!(
            bind_addr("tcp://no-such-host.invalid:5555"),
            Err(Error::InvalidEndpoint(_))
        ));
        assert_eq!(bind_addr("tcp://localhost:7").unwrap(), addr("127.0.0.1:7"));
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
    fn ipc_endpoints() {
        assert!(
            matches!(bind_target("ipc:///tmp/x"), Ok(Target::Ipc(p)) if p == std::path::Path::new("/tmp/x"))
        );
        assert!(
            matches!(connect_target("ipc://rel/y"), Ok(Target::Ipc(p)) if p == std::path::Path::new("rel/y"))
        );
        assert!(matches!(bind_target("ipc://*"), Ok(Target::IpcAnywhere)));
        assert!(matches!(
            connect_target("ipc://*"),
            Err(Error::InvalidEndpoint(_))
        ));
        assert!(matches!(
            bind_target("ipc://"),
            Err(Error::InvalidEndpoint(_))
        ));
        assert_eq!(Endpoint::Ipc("/tmp/x".into()).to_string(), "ipc:///tmp/x");
    }

    #[cfg(any(target_os = "linux", target_os = "android"))]
    #[test]
    fn abstract_ipc_endpoints() {
        assert!(matches!(bind_target("ipc://@feed"), Ok(Target::IpcAbstract(n)) if n == "feed"));
        assert!(matches!(connect_target("ipc://@a/b"), Ok(Target::IpcAbstract(n)) if n == "a/b"));
        assert!(matches!(bind_target("ipc://@*"), Ok(Target::IpcAbstract(n)) if n == "*"));
        assert!(matches!(connect_target("ipc://@*"), Ok(Target::IpcAbstract(n)) if n == "*"));
        assert!(matches!(
            bind_target("ipc://@"),
            Err(Error::InvalidEndpoint(_))
        ));
        assert_eq!(
            Endpoint::IpcAbstract("feed".into()).to_string(),
            "ipc://@feed"
        );
    }

    #[cfg(all(unix, not(any(target_os = "linux", target_os = "android"))))]
    #[test]
    fn at_names_are_paths_off_linux() {
        assert!(
            matches!(bind_target("ipc://@feed"), Ok(Target::Ipc(p)) if p == std::path::Path::new("@feed"))
        );
    }

    #[test]
    fn other_transports_are_unsupported() {
        assert!(matches!(
            bind_target("pgm://eth0;239.192.1.1:5555"),
            Err(Error::UnsupportedTransport(t)) if t == "pgm"
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
        let refused = Error::Io(io::Error::from(io::ErrorKind::AddrInUse));
        assert_eq!(refused.to_string(), "the operating system refused");
        let source = std::error::Error::source(&refused).map(ToString::to_string);
        assert_eq!(
            source,
            Some(io::Error::from(io::ErrorKind::AddrInUse).to_string())
        );
    }
}
