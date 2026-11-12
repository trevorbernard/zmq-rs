//! The byte streams connections run over: TCP, and on Unix, the Unix domain
//! sockets behind `ipc://` endpoints, including Linux's abstract names.
//!
//! On Unix a connection is held as the standard library's stream, made by
//! mio's non-blocking connect or accept and registered by its descriptor:
//! the standard streams can read into uninitialized memory (see
//! [`Stream::read_appending`]), and mio's do not offer that. Elsewhere
//! mio's streams are used as they are.

use std::fmt;
use std::io::{self, Read, Write};
use std::net::{IpAddr, SocketAddr, SocketAddrV6, ToSocketAddrs};
use std::time::Duration;

use mio::event::Source;
use mio::net::TcpListener;
use mio::{Interest, Registry, Token};

use crate::TcpKeepalive;

#[cfg(any(target_os = "linux", target_os = "android"))]
pub(crate) use ipc::bind_ipc_abstract;
#[cfg(unix)]
pub(crate) use ipc::{IpcCleanup, bind_ipc, bind_ipc_anywhere};
#[cfg(unix)]
use mio::net::UnixListener;
#[cfg(unix)]
use mio::unix::SourceFd;
#[cfg(unix)]
use std::os::fd::AsRawFd;
#[cfg(unix)]
use std::path::PathBuf;

/// A connection's TCP stream.
#[cfg(unix)]
type TcpStream = std::net::TcpStream;
#[cfg(not(unix))]
type TcpStream = mio::net::TcpStream;

#[cfg(unix)]
type UnixStream = std::os::unix::net::UnixStream;

/// Resolves `host`, preferring an IPv4 address as libzmq does with
/// `ZMQ_IPV6` off, its default. Otherwise `localhost` may resolve to `::1`
/// and never reach a `tcp://*:port` listener, which binds IPv4 only. Blocks
/// while the name is looked up.
pub fn resolve_tcp(host: &str, port: u16) -> io::Result<SocketAddr> {
    let addrs: Vec<SocketAddr> = (host, port).to_socket_addrs()?.collect();
    addrs
        .iter()
        .find(|addr| addr.is_ipv4())
        .or_else(|| addrs.first())
        .copied()
        .ok_or_else(|| io::Error::new(io::ErrorKind::NotFound, format!("{host} has no address")))
}

/// The address of the network interface named `name`, as libzmq binds
/// `tcp://eth0:port` and connects from `tcp://eth0:port;...`: an IPv4 one
/// if it has one, as for host names. `None` if there is no such interface
/// or it has no address.
pub fn interface_addr(name: &str, port: u16) -> Option<SocketAddr> {
    let interfaces: Vec<_> = if_addrs::get_if_addrs()
        .ok()?
        .into_iter()
        .filter(|interface| interface.name == name)
        .collect();
    let interface = interfaces
        .iter()
        .find(|interface| interface.ip().is_ipv4())
        .or_else(|| interfaces.first())?;
    Some(match interface.ip() {
        IpAddr::V4(ip) => SocketAddr::from((ip, port)),
        // A link-local address means nothing without its interface.
        IpAddr::V6(ip) => {
            SocketAddr::V6(SocketAddrV6::new(ip, port, 0, interface.index.unwrap_or(0)))
        }
    })
}

/// What a TCP socket is given before it connects or listens, as libzmq's
/// `tcp_open_socket` sets it. The default sets nothing.
#[derive(Debug, Clone, Default, PartialEq, Eq)]
pub struct TcpTuning {
    /// The IP type of service, or IPv6 traffic class, of what the socket
    /// sends (`ZMQ_TOS`). Zero leaves the system's.
    pub type_of_service: u8,
    /// The network interface the socket is bound to (`ZMQ_BINDTODEVICE`,
    /// Linux's `SO_BINDTODEVICE`). A connect or bind fails where that is
    /// not supported or not permitted.
    pub bind_to_device: Option<String>,
    /// How long a blocking receive busy-polls the device before sleeping
    /// (`ZMQ_BUSY_POLL`, Linux's `SO_BUSY_POLL`). Ignored where that is
    /// not supported or not permitted.
    pub busy_poll: Option<Duration>,
}

impl TcpTuning {
    fn is_default(&self) -> bool {
        *self == Self::default()
    }

    /// Sets the type of service, on a socket of either family: one of the
    /// two options fails on each, as libzmq expects of the IPv6 one.
    fn set_type_of_service(&self, socket: &socket2::Socket) {
        if self.type_of_service == 0 {
            return;
        }
        let tos = u32::from(self.type_of_service);
        let _ = socket.set_tos_v4(tos);
        #[cfg(unix)]
        let _ = socket.set_tclass_v6(tos);
    }

    /// Applies everything to a socket yet to connect or listen. Only
    /// binding to the device can fail, as in libzmq.
    fn apply(&self, socket: &socket2::Socket) -> io::Result<()> {
        self.set_type_of_service(socket);
        if let Some(device) = &self.bind_to_device {
            #[cfg(any(target_os = "linux", target_os = "android"))]
            socket.bind_device(Some(device.as_bytes()))?;
            #[cfg(not(any(target_os = "linux", target_os = "android")))]
            {
                let _ = device;
                return Err(io::ErrorKind::Unsupported.into());
            }
        }
        #[cfg(target_os = "linux")]
        if let Some(busy_poll) = self.busy_poll {
            let micros = u32::try_from(busy_poll.as_micros()).unwrap_or(u32::MAX);
            if micros > 0 {
                let _ = socket.set_busy_poll(micros);
            }
        }
        Ok(())
    }
}

/// A non-blocking TCP socket for `addr`'s family, tuned.
fn tcp_socket(addr: SocketAddr, tuning: &TcpTuning) -> io::Result<socket2::Socket> {
    let socket = socket2::Socket::new(
        socket2::Domain::for_address(addr),
        socket2::Type::STREAM,
        Some(socket2::Protocol::TCP),
    )?;
    socket.set_nonblocking(true)?;
    tuning.apply(&socket)?;
    Ok(socket)
}

/// Listens on `addr`, as mio's listener does, tuned first.
pub(crate) fn listen_tcp(addr: SocketAddr, tuning: &TcpTuning) -> io::Result<TcpListener> {
    if tuning.is_default() {
        return TcpListener::bind(addr);
    }
    let socket = tcp_socket(addr, tuning)?;
    #[cfg(unix)]
    socket.set_reuse_address(true)?;
    socket.bind(&addr.into())?;
    socket.listen(1024)?;
    Ok(TcpListener::from_std(socket.into()))
}

/// Starts a non-blocking connection to `addr`, tuned first, from `source`
/// if given, as libzmq does for `tcp://source;host:port`: reusing the
/// address, so that one source port can reach several servers.
fn connect_tcp(
    source: Option<SocketAddr>,
    addr: SocketAddr,
    tuning: &TcpTuning,
) -> io::Result<TcpStream> {
    if source.is_none() && tuning.is_default() {
        return mio::net::TcpStream::connect(addr).map(Into::into);
    }
    let socket = tcp_socket(addr, tuning)?;
    if let Some(source) = source {
        socket.set_reuse_address(true)?;
        socket.bind(&source.into())?;
    }
    match socket.connect(&addr.into()) {
        Ok(()) => {}
        Err(err) if in_progress(&err) => {}
        Err(err) => return Err(err),
    }
    let stream = std::net::TcpStream::from(socket);
    #[cfg(not(unix))]
    let stream = mio::net::TcpStream::from_std(stream);
    Ok(stream)
}

/// Whether a non-blocking connect failed only because it is under way.
fn in_progress(err: &io::Error) -> bool {
    #[cfg(unix)]
    if err.raw_os_error() == Some(libc::EINPROGRESS) {
        return true;
    }
    err.kind() == io::ErrorKind::WouldBlock
}

/// Nothing to clean up without `ipc://`.
#[cfg(not(unix))]
#[derive(Debug)]
pub(crate) enum IpcCleanup {}

/// Where a connecting session connects, and reconnects.
#[derive(Debug, Clone)]
pub(crate) enum Address {
    Tcp(SocketAddr),
    /// A host name or address, resolved for each attempt as libzmq
    /// resolves it, so a reconnect follows a name to a new address. Made
    /// from `source` if given, as libzmq's `tcp://source;host:port`.
    TcpHost {
        host: String,
        port: u16,
        source: Option<SocketAddr>,
    },
    #[cfg(unix)]
    Ipc(PathBuf),
    /// A name in Linux's abstract socket namespace, for `ipc://@name`.
    #[cfg(any(target_os = "linux", target_os = "android"))]
    IpcAbstract(String),
}

impl fmt::Display for Address {
    /// The endpoint string, as libzmq writes it.
    fn fmt(&self, f: &mut fmt::Formatter<'_>) -> fmt::Result {
        match self {
            Self::Tcp(addr) => write!(f, "tcp://{addr}"),
            Self::TcpHost { host, port, source } => {
                f.write_str("tcp://")?;
                if let Some(source) = source {
                    write!(f, "{source};")?;
                }
                if host.contains(':') {
                    write!(f, "[{host}]:{port}")
                } else {
                    write!(f, "{host}:{port}")
                }
            }
            #[cfg(unix)]
            Self::Ipc(path) => write!(f, "ipc://{}", path.display()),
            #[cfg(any(target_os = "linux", target_os = "android"))]
            Self::IpcAbstract(name) => write!(f, "ipc://@{name}"),
        }
    }
}

/// One connection's byte stream.
#[derive(Debug)]
pub(crate) enum Stream {
    Tcp(TcpStream),
    #[cfg(unix)]
    Unix(UnixStream),
}

/// A listening socket.
#[derive(Debug)]
pub(crate) enum StreamListener {
    Tcp(TcpListener),
    #[cfg(unix)]
    Unix(UnixListener),
}

/// Forwards to the inner stream or listener.
macro_rules! delegate {
    ($self:ident, $inner:ident => $call:expr) => {
        match $self {
            Self::Tcp($inner) => $call,
            #[cfg(unix)]
            Self::Unix($inner) => $call,
        }
    };
}

impl Stream {
    /// Starts a non-blocking connection to `address`, a TCP one tuned
    /// first.
    pub(crate) fn connect(address: &Address, tuning: &TcpTuning) -> io::Result<Self> {
        match address {
            Address::Tcp(addr) => connect_tcp(None, *addr, tuning).map(Self::Tcp),
            Address::TcpHost { host, port, source } => {
                let addr = resolve_tcp(host, *port)?;
                connect_tcp(*source, addr, tuning).map(Self::Tcp)
            }
            #[cfg(unix)]
            Address::Ipc(path) => mio::net::UnixStream::connect(path).map(|s| Self::Unix(s.into())),
            #[cfg(any(target_os = "linux", target_os = "android"))]
            Address::IpcAbstract(name) => {
                mio::net::UnixStream::connect_addr(&ipc::abstract_addr(name)?)
                    .map(|s| Self::Unix(s.into()))
            }
        }
    }

    /// Appends up to `limit` octets to `buf`, as many as have arrived.
    /// Fails with `WouldBlock` if none have; `Ok(0)` is the end of the
    /// stream.
    pub(crate) fn read_appending(&mut self, buf: &mut Vec<u8>, limit: usize) -> io::Result<usize> {
        let before = buf.len();
        // A workaround for reading into uninitialized memory, which
        // `Read::read` cannot do: its buffer must be initialized, so a
        // fresh one costs a memset of every octet before the kernel
        // overwrites them. `Read::read_buf`, which takes uninitialized
        // memory, is unstable (rust-lang/rust#78485). `read_to_end` is
        // stable and reads into `buf`'s spare capacity through the
        // stream's own `read_buf`, which the standard TCP and Unix streams
        // implement without zeroing. `take` stops it at `limit`. On a
        // non-blocking stream it ends in `WouldBlock` once the socket is
        // drained, keeping what it read. Revisit once `read_buf` is stable.
        let result = delegate!(self, stream => (&*stream).take(limit as u64).read_to_end(buf));
        let read = buf.len() - before;
        match result {
            Err(err) if err.kind() == io::ErrorKind::WouldBlock && read > 0 => Ok(read),
            Err(err) => Err(err),
            Ok(_) => Ok(read),
        }
    }

    /// Whether a non-blocking connect has completed. Fails if it failed.
    pub(crate) fn finish_connect(&self) -> io::Result<bool> {
        let (error, connected) = match self {
            Self::Tcp(stream) => (stream.take_error()?, stream.peer_addr().map(drop)),
            #[cfg(unix)]
            Self::Unix(stream) => (stream.take_error()?, stream.peer_addr().map(drop)),
        };
        if let Some(err) = error {
            return Err(err);
        }
        match connected {
            Ok(()) => Ok(true),
            Err(err) if err.kind() == io::ErrorKind::NotConnected => Ok(false),
            Err(err) => Err(err),
        }
    }

    /// Sets a TCP stream's options, connecting or accepted, as libzmq's
    /// `tune_tcp_socket` and `tune_tcp_keepalives`: Nagle off, as batching
    /// happens in the encoder, and keepalive probes if asked for. An
    /// accepted stream is given the type of service here, as libzmq's
    /// listener gives it. Failures leave the system's defaults.
    pub(crate) fn tune(&self, keepalive: Option<&TcpKeepalive>, tuning: &TcpTuning) {
        let Self::Tcp(stream) = self else {
            return;
        };
        let _ = stream.set_nodelay(true);
        tuning.set_type_of_service(&socket2::SockRef::from(stream));
        let Some(keepalive) = keepalive else {
            return;
        };
        let mut params = socket2::TcpKeepalive::new();
        if let Some(idle) = keepalive.idle {
            params = params.with_time(idle);
        }
        #[cfg(any(
            target_os = "linux",
            target_os = "android",
            target_os = "macos",
            target_os = "ios",
            target_os = "freebsd",
            target_os = "windows",
        ))]
        {
            if let Some(interval) = keepalive.interval {
                params = params.with_interval(interval);
            }
            if let Some(count) = keepalive.count {
                params = params.with_retries(count);
            }
        }
        let _ = socket2::SockRef::from(stream).set_tcp_keepalive(&params);
    }

    /// The peer's address as ZAP reports it: its IP for TCP, and empty for
    /// IPC, as in libzmq.
    pub(crate) fn zap_address(&self) -> String {
        match self {
            Self::Tcp(stream) => stream
                .peer_addr()
                .map(|addr| addr.ip().to_string())
                .unwrap_or_default(),
            #[cfg(unix)]
            Self::Unix(_) => String::new(),
        }
    }
}

impl StreamListener {
    /// Takes the next waiting connection.
    pub(crate) fn accept(&self) -> io::Result<Stream> {
        match self {
            Self::Tcp(listener) => listener
                .accept()
                .map(|(stream, _)| Stream::Tcp(stream.into())),
            #[cfg(unix)]
            Self::Unix(listener) => listener
                .accept()
                .map(|(stream, _)| Stream::Unix(stream.into())),
        }
    }
}

impl Read for Stream {
    fn read(&mut self, buf: &mut [u8]) -> io::Result<usize> {
        delegate!(self, stream => stream.read(buf))
    }
}

impl Write for Stream {
    fn write(&mut self, buf: &[u8]) -> io::Result<usize> {
        delegate!(self, stream => stream.write(buf))
    }

    // Forwarded: the default writes only the first slice.
    fn write_vectored(&mut self, bufs: &[io::IoSlice<'_>]) -> io::Result<usize> {
        delegate!(self, stream => stream.write_vectored(bufs))
    }

    fn flush(&mut self) -> io::Result<()> {
        delegate!(self, stream => stream.flush())
    }
}

/// Registered by descriptor on Unix, where the streams are the standard
/// library's.
#[cfg(unix)]
impl Source for Stream {
    fn register(
        &mut self,
        registry: &Registry,
        token: Token,
        interests: Interest,
    ) -> io::Result<()> {
        let fd = delegate!(self, stream => stream.as_raw_fd());
        SourceFd(&fd).register(registry, token, interests)
    }

    fn reregister(
        &mut self,
        registry: &Registry,
        token: Token,
        interests: Interest,
    ) -> io::Result<()> {
        let fd = delegate!(self, stream => stream.as_raw_fd());
        SourceFd(&fd).reregister(registry, token, interests)
    }

    fn deregister(&mut self, registry: &Registry) -> io::Result<()> {
        let fd = delegate!(self, stream => stream.as_raw_fd());
        SourceFd(&fd).deregister(registry)
    }
}

#[cfg(not(unix))]
impl Source for Stream {
    fn register(
        &mut self,
        registry: &Registry,
        token: Token,
        interests: Interest,
    ) -> io::Result<()> {
        delegate!(self, stream => stream.register(registry, token, interests))
    }

    fn reregister(
        &mut self,
        registry: &Registry,
        token: Token,
        interests: Interest,
    ) -> io::Result<()> {
        delegate!(self, stream => stream.reregister(registry, token, interests))
    }

    fn deregister(&mut self, registry: &Registry) -> io::Result<()> {
        delegate!(self, stream => stream.deregister(registry))
    }
}

impl Source for StreamListener {
    fn register(
        &mut self,
        registry: &Registry,
        token: Token,
        interests: Interest,
    ) -> io::Result<()> {
        delegate!(self, listener => listener.register(registry, token, interests))
    }

    fn reregister(
        &mut self,
        registry: &Registry,
        token: Token,
        interests: Interest,
    ) -> io::Result<()> {
        delegate!(self, listener => listener.reregister(registry, token, interests))
    }

    fn deregister(&mut self, registry: &Registry) -> io::Result<()> {
        delegate!(self, listener => listener.deregister(registry))
    }
}

#[cfg(unix)]
mod ipc {
    //! Binding `ipc://` paths, and removing them again.

    use std::fs;
    use std::io;
    use std::os::unix::fs::{FileTypeExt, MetadataExt};
    use std::path::{Path, PathBuf};
    use std::sync::atomic::{AtomicU64, Ordering};

    use mio::net::UnixListener;

    /// Removes a bound socket's file when its listener closes, and the
    /// directory a wildcard bind made for it. Only the file this bind
    /// created is removed: if something else has replaced it since, it
    /// stays.
    #[derive(Debug)]
    pub(crate) struct IpcCleanup {
        path: PathBuf,
        /// The file's device and inode when we created it.
        identity: (u64, u64),
        temp_dir: Option<PathBuf>,
    }

    impl Drop for IpcCleanup {
        fn drop(&mut self) {
            if fs::symlink_metadata(&self.path)
                .is_ok_and(|metadata| (metadata.dev(), metadata.ino()) == self.identity)
            {
                let _ = fs::remove_file(&self.path);
            }
            if let Some(dir) = &self.temp_dir {
                let _ = fs::remove_dir(dir);
            }
        }
    }

    /// Binds a Unix domain socket at `path`.
    ///
    /// libzmq unlinks whatever is at `path` first. We remove only a socket
    /// file nobody is listening on, as a crashed process leaves behind; a
    /// live socket or any other file is `AddrInUse`.
    pub(crate) fn bind_ipc(path: &Path) -> io::Result<(UnixListener, IpcCleanup)> {
        if let Ok(metadata) = fs::symlink_metadata(path) {
            let stale = metadata.file_type().is_socket()
                && std::os::unix::net::UnixStream::connect(path)
                    .is_err_and(|err| err.kind() == io::ErrorKind::ConnectionRefused);
            if !stale {
                return Err(io::Error::new(
                    io::ErrorKind::AddrInUse,
                    format!("{} is in use", path.display()),
                ));
            }
            fs::remove_file(path)?;
        }
        let listener = UnixListener::bind(path)?;
        let metadata = fs::symlink_metadata(path)?;
        let cleanup = IpcCleanup {
            path: path.to_owned(),
            identity: (metadata.dev(), metadata.ino()),
            temp_dir: None,
        };
        Ok((listener, cleanup))
    }

    /// `name` in the abstract namespace. Fails if it is too long for
    /// `sun_path`.
    #[cfg(any(target_os = "linux", target_os = "android"))]
    pub(crate) fn abstract_addr(name: &str) -> io::Result<std::os::unix::net::SocketAddr> {
        #[cfg(target_os = "android")]
        use std::os::android::net::SocketAddrExt;
        #[cfg(target_os = "linux")]
        use std::os::linux::net::SocketAddrExt;
        std::os::unix::net::SocketAddr::from_abstract_name(name)
    }

    /// Binds `name` in the abstract namespace, for `ipc://@name`. There is
    /// no file, so nothing is stale and nothing needs removing: the kernel
    /// refuses a live name and frees it when the listener closes.
    #[cfg(any(target_os = "linux", target_os = "android"))]
    pub(crate) fn bind_ipc_abstract(name: &str) -> io::Result<UnixListener> {
        UnixListener::bind_addr(&abstract_addr(name)?)
    }

    /// Binds a socket at a fresh path in a new temporary directory, for an
    /// `ipc://*` endpoint, as libzmq does.
    pub(crate) fn bind_ipc_anywhere() -> io::Result<(UnixListener, IpcCleanup, PathBuf)> {
        static NEXT: AtomicU64 = AtomicU64::new(0);
        let base = std::env::temp_dir();
        let dir = loop {
            let dir = base.join(format!(
                "zmq-{}-{}",
                std::process::id(),
                NEXT.fetch_add(1, Ordering::Relaxed)
            ));
            match fs::create_dir(&dir) {
                Ok(()) => break dir,
                Err(err) if err.kind() == io::ErrorKind::AlreadyExists => {}
                Err(err) => return Err(err),
            }
        };
        let path = dir.join("socket");
        match bind_ipc(&path) {
            Ok((listener, mut cleanup)) => {
                cleanup.temp_dir = Some(dir);
                Ok((listener, cleanup, path))
            }
            Err(err) => {
                let _ = fs::remove_dir(&dir);
                Err(err)
            }
        }
    }
}

#[cfg(all(test, unix))]
mod tests {
    use super::*;
    use std::time::Duration;

    fn pair() -> (Stream, UnixStream) {
        let (ours, theirs) = UnixStream::pair().unwrap();
        ours.set_nonblocking(true).unwrap();
        (Stream::Unix(ours), theirs)
    }

    #[test]
    fn read_appending_stops_at_the_limit_and_keeps_what_it_read() {
        let (mut stream, mut peer) = pair();
        peer.write_all(&[1; 100]).unwrap();
        let mut buf = vec![9];
        assert_eq!(stream.read_appending(&mut buf, 60).unwrap(), 60);
        assert_eq!(buf.len(), 61);
        assert_eq!(
            stream.read_appending(&mut buf, 60).unwrap(),
            40,
            "then what has arrived"
        );
        assert_eq!(buf, [&[9][..], &[1; 100]].concat());
        let err = stream.read_appending(&mut buf, 60).unwrap_err();
        assert_eq!(err.kind(), io::ErrorKind::WouldBlock, "nothing more yet");
        drop(peer);
        assert_eq!(
            stream.read_appending(&mut buf, 60).unwrap(),
            0,
            "end of stream"
        );
    }

    #[test]
    fn tuning_turns_nagle_off_and_keepalive_on() {
        let listener = std::net::TcpListener::bind("127.0.0.1:0").unwrap();
        let stream = Stream::Tcp(TcpStream::connect(listener.local_addr().unwrap()).unwrap());
        let Stream::Tcp(tcp) = &stream else {
            unreachable!("made as TCP");
        };
        stream.tune(
            Some(&TcpKeepalive {
                idle: Some(Duration::from_secs(30)),
                interval: Some(Duration::from_secs(5)),
                count: Some(4),
            }),
            &TcpTuning::default(),
        );
        let socket = socket2::SockRef::from(tcp);
        assert!(socket.tcp_nodelay().unwrap());
        assert!(socket.keepalive().unwrap());
        assert_eq!(
            socket.tcp_keepalive_time().unwrap(),
            Duration::from_secs(30)
        );
        assert_eq!(
            socket.tcp_keepalive_interval().unwrap(),
            Duration::from_secs(5)
        );
        assert_eq!(socket.tcp_keepalive_retries().unwrap(), 4);
    }

    fn serviced(type_of_service: u8) -> TcpTuning {
        TcpTuning {
            type_of_service,
            ..TcpTuning::default()
        }
    }

    #[test]
    fn a_connection_and_its_listener_carry_the_type_of_service() {
        let listener = listen_tcp("127.0.0.1:0".parse().unwrap(), &serviced(0x28)).unwrap();
        assert_eq!(socket2::SockRef::from(&listener).tos_v4().unwrap(), 0x28);

        let address = Address::Tcp(listener.local_addr().unwrap());
        let Stream::Tcp(tcp) = Stream::connect(&address, &serviced(0x58)).unwrap() else {
            unreachable!("made as TCP");
        };
        assert_eq!(socket2::SockRef::from(&tcp).tos_v4().unwrap(), 0x58);
    }

    #[cfg(unix)]
    #[test]
    fn an_accepted_stream_is_given_the_type_of_service() {
        let listener = std::net::TcpListener::bind("127.0.0.1:0").unwrap();
        let _client = TcpStream::connect(listener.local_addr().unwrap()).unwrap();
        let (accepted, _) = listener.accept().unwrap();
        let stream = Stream::Tcp(accepted);
        stream.tune(None, &serviced(0x28));
        let Stream::Tcp(tcp) = &stream else {
            unreachable!("made as TCP");
        };
        assert_eq!(socket2::SockRef::from(tcp).tos_v4().unwrap(), 0x28);
    }

    #[test]
    fn an_ipv6_connection_carries_the_traffic_class() {
        let Ok(listener) = listen_tcp("[::1]:0".parse().unwrap(), &serviced(0x28)) else {
            return; // no IPv6 here
        };
        assert_eq!(socket2::SockRef::from(&listener).tclass_v6().unwrap(), 0x28);
    }

    #[test]
    fn untuned_sockets_keep_the_systems_type_of_service() {
        let listener = listen_tcp("127.0.0.1:0".parse().unwrap(), &TcpTuning::default()).unwrap();
        assert_eq!(socket2::SockRef::from(&listener).tos_v4().unwrap(), 0);
    }

    #[cfg(any(target_os = "linux", target_os = "android"))]
    #[test]
    fn a_socket_is_bound_to_its_device() {
        let tuning = TcpTuning {
            bind_to_device: Some("lo".to_owned()),
            ..TcpTuning::default()
        };
        let listener = match listen_tcp("127.0.0.1:0".parse().unwrap(), &tuning) {
            Ok(listener) => listener,
            // Before Linux 5.7 this takes CAP_NET_RAW.
            Err(err) if err.kind() == io::ErrorKind::PermissionDenied => return,
            Err(err) => panic!("{err}"),
        };
        assert_eq!(
            socket2::SockRef::from(&listener).device().unwrap(),
            Some(b"lo".to_vec())
        );

        let address = Address::Tcp(listener.local_addr().unwrap());
        let Stream::Tcp(tcp) = Stream::connect(&address, &tuning).unwrap() else {
            unreachable!("made as TCP");
        };
        assert_eq!(
            socket2::SockRef::from(&tcp).device().unwrap(),
            Some(b"lo".to_vec())
        );
    }

    #[cfg(any(target_os = "linux", target_os = "android"))]
    #[test]
    fn a_device_that_does_not_exist_fails_the_bind_and_the_connect() {
        let tuning = TcpTuning {
            bind_to_device: Some("no-such-device".to_owned()),
            ..TcpTuning::default()
        };
        let addr = "127.0.0.1:0".parse().unwrap();
        assert!(listen_tcp(addr, &tuning).is_err());
        assert!(Stream::connect(&Address::Tcp(addr), &tuning).is_err());
    }

    /// Raising it takes CAP_NET_ADMIN, so only that it is harmless is
    /// checked, and that it took if it was permitted.
    #[cfg(target_os = "linux")]
    #[test]
    fn busy_polling_is_set_where_permitted() {
        let tuning = TcpTuning {
            busy_poll: Some(Duration::from_micros(50)),
            ..TcpTuning::default()
        };
        let listener = listen_tcp("127.0.0.1:0".parse().unwrap(), &tuning).unwrap();
        let set = socket2::SockRef::from(&listener).busy_poll().unwrap();
        assert!(set == 50 || set == 0, "{set}");
    }

    #[test]
    fn keepalive_stays_off_unless_asked_for() {
        let listener = std::net::TcpListener::bind("127.0.0.1:0").unwrap();
        let stream = Stream::Tcp(TcpStream::connect(listener.local_addr().unwrap()).unwrap());
        let Stream::Tcp(tcp) = &stream else {
            unreachable!("made as TCP");
        };
        stream.tune(None, &TcpTuning::default());
        let socket = socket2::SockRef::from(tcp);
        assert!(socket.tcp_nodelay().unwrap());
        assert!(!socket.keepalive().unwrap());
    }
}
