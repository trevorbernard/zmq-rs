//! The byte streams connections run over: TCP, and on Unix, the Unix domain
//! sockets behind `ipc://` endpoints.

use std::io::{self, Read, Write};
use std::net::SocketAddr;

use mio::event::Source;
use mio::net::{TcpListener, TcpStream};
use mio::{Interest, Registry, Token};

#[cfg(unix)]
pub(crate) use ipc::{IpcCleanup, bind_ipc, bind_ipc_anywhere};
#[cfg(unix)]
use mio::net::{UnixListener, UnixStream};
#[cfg(unix)]
use std::path::PathBuf;

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
}

impl std::fmt::Display for Address {
    /// The endpoint string, as libzmq writes it.
    fn fmt(&self, f: &mut std::fmt::Formatter<'_>) -> std::fmt::Result {
        match self {
            Self::Tcp(addr) => write!(f, "tcp://{addr}"),
            #[cfg(unix)]
            Self::Ipc(path) => write!(f, "ipc://{}", path.display()),
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

impl Stream {
    /// Starts a non-blocking connection to `address`.
    pub(crate) fn connect(address: &Address) -> io::Result<Self> {
        match address {
            Address::Tcp(addr) => TcpStream::connect(*addr).map(Self::Tcp),
            #[cfg(unix)]
            Address::Ipc(path) => UnixStream::connect(path).map(Self::Unix),
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
            Self::Tcp(listener) => listener.accept().map(|(stream, _)| Stream::Tcp(stream)),
            #[cfg(unix)]
            Self::Unix(listener) => listener.accept().map(|(stream, _)| Stream::Unix(stream)),
        }
    }
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

impl Read for Stream {
    fn read(&mut self, buf: &mut [u8]) -> io::Result<usize> {
        delegate!(self, stream => stream.read(buf))
    }
}

impl Write for Stream {
    fn write(&mut self, buf: &[u8]) -> io::Result<usize> {
        delegate!(self, stream => stream.write(buf))
    }

    fn flush(&mut self) -> io::Result<()> {
        delegate!(self, stream => stream.flush())
    }
}

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
