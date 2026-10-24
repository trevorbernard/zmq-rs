//! The byte streams connections run over: TCP, and on Unix, the Unix domain
//! sockets behind `ipc://` endpoints, including Linux's abstract names.
//!
//! On Unix a connection is held as the standard library's stream, made by
//! mio's non-blocking connect or accept and registered by its descriptor:
//! the standard streams can read into uninitialized memory (see
//! [`Stream::read_appending`]), and mio's do not offer that. Elsewhere
//! mio's streams are used as they are.

use std::io::{self, Read, Write};
use std::net::SocketAddr;

use mio::event::Source;
use mio::net::TcpListener;
use mio::{Interest, Registry, Token};

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

/// Nothing to clean up without `ipc://`.
#[cfg(not(unix))]
#[derive(Debug)]
pub(crate) enum IpcCleanup {}

/// Where a connecting session connects, and reconnects.
#[derive(Debug, Clone)]
pub(crate) enum Address {
    Tcp(SocketAddr),
    #[cfg(unix)]
    Ipc(PathBuf),
    /// A name in Linux's abstract socket namespace, for `ipc://@name`.
    #[cfg(any(target_os = "linux", target_os = "android"))]
    IpcAbstract(String),
}

impl std::fmt::Display for Address {
    /// The endpoint string, as libzmq writes it.
    fn fmt(&self, f: &mut std::fmt::Formatter<'_>) -> std::fmt::Result {
        match self {
            Self::Tcp(addr) => write!(f, "tcp://{addr}"),
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
    /// Starts a non-blocking connection to `address`.
    pub(crate) fn connect(address: &Address) -> io::Result<Self> {
        match address {
            Address::Tcp(addr) => mio::net::TcpStream::connect(*addr).map(|s| Self::Tcp(s.into())),
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
            Ok(()) => {
                // libzmq disables Nagle; batching happens in the encoder.
                if let Self::Tcp(stream) = self {
                    let _ = stream.set_nodelay(true);
                }
                Ok(true)
            }
            Err(err) if err.kind() == io::ErrorKind::NotConnected => Ok(false),
            Err(err) => Err(err),
        }
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
}
