//! The ways a socket of ours and one of libzmq's are joined: which of the
//! two binds, and over which transport.

use std::cell::RefCell;
use std::net::TcpListener;
use std::path::PathBuf;
use std::sync::atomic::{AtomicU32, Ordering};

use zmq::SocketType;
use zmq_test_ports::free_port;

use crate::libzmq::{Context, Kind, Socket};

#[derive(Debug, Clone, Copy, PartialEq, Eq)]
pub enum Binds {
    Ours,
    Libzmq,
}

#[derive(Debug, Clone, Copy, PartialEq, Eq)]
pub enum Transport {
    Tcp,
    /// `tcp` over the IPv6 loopback.
    Tcp6,
    Ipc,
    /// A name in Linux's abstract socket namespace.
    IpcAbstract,
}

/// How a socket of ours and one of libzmq's are joined.
#[derive(Debug, Clone, Copy)]
pub struct Link {
    pub binds: Binds,
    pub transport: Transport,
}

thread_local! {
    /// The socket files libzmq was given to bind on this thread.
    static BOUND: RefCell<Vec<PathBuf>> = const { RefCell::new(Vec::new()) };
}

/// Every link this host can make, each with either library binding: IPv6
/// only where its loopback can be bound, `ipc` only on Unix, and abstract
/// names only on Linux.
///
/// The socket files libzmq binds for them are removed when the list is
/// dropped: libzmq leaves the file of a path it was given to bind.
pub fn links() -> Links {
    let mut transports = vec![Transport::Tcp];
    if TcpListener::bind("[::1]:0").is_ok() {
        transports.push(Transport::Tcp6);
    }
    if cfg!(unix) {
        transports.push(Transport::Ipc);
    }
    if cfg!(any(target_os = "linux", target_os = "android")) {
        transports.push(Transport::IpcAbstract);
    }
    Links(
        transports
            .into_iter()
            .flat_map(|transport| {
                [Binds::Ours, Binds::Libzmq].map(|binds| Link { binds, transport })
            })
            .collect(),
    )
}

/// The links of [`links`].
#[derive(Debug)]
pub struct Links(Vec<Link>);

impl Links {
    pub fn iter(&self) -> impl Iterator<Item = Link> + '_ {
        self.0.iter().copied()
    }
}

impl Drop for Links {
    fn drop(&mut self) {
        for path in BOUND.take() {
            let _ = std::fs::remove_file(path);
        }
    }
}

impl Link {
    /// What a socket of ours binds, to be told where it is bound.
    pub fn wildcard(self) -> String {
        match self.transport {
            Transport::Tcp => "tcp://127.0.0.1:*".to_owned(),
            Transport::Tcp6 => "tcp://[::1]:*".to_owned(),
            Transport::Ipc => "ipc://*".to_owned(),
            // An abstract name has no wildcard.
            Transport::IpcAbstract => self.free(),
        }
    }

    /// An endpoint nothing is bound to, for libzmq to bind.
    pub fn free(self) -> String {
        static NEXT: AtomicU32 = AtomicU32::new(0);
        let name = || {
            format!(
                "zmq-interop-link-{}-{}",
                std::process::id(),
                NEXT.fetch_add(1, Ordering::Relaxed)
            )
        };
        match self.transport {
            Transport::Tcp => format!("tcp://{}", free_port()),
            Transport::Tcp6 => format!("tcp://[::1]:{}", free_port().port()),
            Transport::Ipc => {
                let path = std::env::temp_dir().join(name());
                let endpoint = format!("ipc://{}", path.display());
                BOUND.with_borrow_mut(|bound| bound.push(path));
                endpoint
            }
            Transport::IpcAbstract => format!("ipc://@{}", name()),
        }
    }

    /// A socket of libzmq's, able to use this link's transport.
    pub fn theirs(self, kind: Kind) -> Socket {
        self.theirs_in(&Context::new(), kind)
    }

    /// As [`theirs`](Self::theirs), in `context`.
    pub fn theirs_in(self, context: &Context, kind: Kind) -> Socket {
        let socket = Socket::in_context(context, kind);
        if self.transport == Transport::Tcp6 {
            socket.set_ipv6();
        }
        socket
    }

    /// Joins `ours` and `theirs`, neither bound nor connected yet.
    pub fn join<T: SocketType>(self, ours: &mut zmq::Socket<T>, theirs: &Socket) {
        match self.binds {
            Binds::Ours => {
                let bound = ours.bind(self.wildcard()).unwrap();
                theirs.connect_str(&bound.to_string());
            }
            Binds::Libzmq => {
                let endpoint = self.free();
                theirs.bind(&endpoint);
                ours.connect(&endpoint).unwrap();
            }
        }
    }
}
