//! The context: the I/O threads every connection runs on.

use std::fmt;
use std::io;
use std::net::SocketAddr;
use std::num::NonZeroUsize;
use std::sync::atomic::{AtomicUsize, Ordering};
use std::sync::{Arc, Mutex, PoisonError};

use std::sync::mpsc;
use std::task::Poll;
use std::time::{Duration, Instant};

use zmq_pipe::{Hwm, Reader, TryRecvError, Writer, pipe};
use zmtp::{Established, Identity, Message, SocketType};

use mio::net::TcpListener;

use crate::io_thread::{ConnectRequest, IoCommand, IoThread, ListenRequest};
use crate::{Reconnect, Security};

/// libzmq's default ZMQ_HANDSHAKE_IVL.
const DEFAULT_HANDSHAKE_TIMEOUT: Duration = Duration::from_secs(30);

/// libzmq's default ZMQ_SNDHWM and ZMQ_RCVHWM.
const DEFAULT_HWM: NonZeroUsize = NonZeroUsize::new(1000).unwrap();

/// Owns the I/O threads. Cloning shares them; they stop when the last clone
/// is dropped, closing every connection on them.
#[derive(Debug, Clone)]
pub struct Context {
    inner: Arc<Inner>,
}

#[derive(Debug)]
struct Inner {
    /// Dropping these stops and joins the threads.
    io_threads: Vec<IoThread>,
    /// Round-robin choice of I/O thread for new connections.
    next: AtomicUsize,
}

/// How a connection presents itself and buffers messages.
#[derive(Debug, Clone)]
pub struct ConnectionConfig {
    /// Our socket type, announced in the handshake.
    pub socket_type: SocketType,
    /// Our identity, announced to ROUTER peers.
    pub identity: Identity,
    /// Messages queued towards the peer before senders must wait.
    pub send_hwm: Hwm,
    /// Messages queued from the peer before we stop reading from it.
    pub recv_hwm: Hwm,
    /// The largest frame accepted from the peer.
    pub max_frame_size: Option<u64>,
    /// How a connecting session retries. Accepted connections never do.
    pub reconnect: Reconnect,
    /// How long queued messages keep being delivered after the connection's
    /// sender is dropped.
    pub linger: Linger,
    /// The security mechanism, NULL by default.
    pub security: Security,
    /// ZMTP 3.1 heartbeats, off by default.
    pub heartbeat: Option<Heartbeat>,
    /// How long the greeting and security handshake may take before the
    /// connection is dropped (`ZMQ_HANDSHAKE_IVL`, default 30 s).
    pub handshake_timeout: Option<Duration>,
}

/// ZMTP 3.1 heartbeats: libzmq's `ZMQ_HEARTBEAT_IVL`, `ZMQ_HEARTBEAT_TIMEOUT`
/// and `ZMQ_HEARTBEAT_TTL`.
#[derive(Debug, Clone, Copy, PartialEq, Eq)]
pub struct Heartbeat {
    /// How often to send a PING once the handshake is done.
    pub interval: Duration,
    /// How long to wait for any traffic after a PING before giving up on the
    /// connection.
    pub timeout: Duration,
    /// How long the peer should wait for traffic from us before giving up,
    /// announced in each PING. Zero announces none.
    pub ttl: Duration,
}

impl Heartbeat {
    /// A PING every `interval`, timing out after the same interval and
    /// announcing no TTL, like libzmq's defaults.
    pub fn every(interval: Duration) -> Self {
        Self {
            interval,
            timeout: interval,
            ttl: Duration::ZERO,
        }
    }
}

/// How long a closed connection keeps delivering the messages queued on it,
/// like libzmq's `ZMQ_LINGER`.
#[derive(Debug, Clone, Copy, PartialEq, Eq, Default)]
pub enum Linger {
    /// Until every message is sent, or the context is dropped. libzmq's
    /// default (-1).
    #[default]
    Forever,
    /// At most this long. `Duration::ZERO` discards queued messages at once.
    For(Duration),
}

/// [`Context::shutdown`] gave up on connections that were still delivering,
/// or still disconnected, when its timeout ran out.
#[derive(Debug, Clone, Copy, PartialEq, Eq)]
pub struct ShutdownTimedOut {
    /// How many connections were cut off.
    pub abandoned: usize,
}

/// One connection's message pipes, as seen from the socket.
///
/// Messages sent on `tx` are queued straight away and delivered once the
/// handshake completes. When the connection ends, both pipes close.
#[derive(Debug)]
pub struct Connection {
    /// Messages to the peer.
    pub tx: Writer<Message>,
    /// Messages from the peer.
    pub rx: Reader<Message>,
    pub(crate) peer: PeerSlot,
}

/// What the most recent handshake established, shared by a session and its
/// connection. Written before any message from that handshake is delivered.
pub(crate) type PeerSlot = Arc<Mutex<Option<Established>>>;

impl Connection {
    /// What the most recent handshake with the peer established, such as
    /// its identity and socket type. `None` until the first handshake
    /// completes; replaced after each reconnection.
    pub fn peer(&self) -> Option<Established> {
        self.peer
            .lock()
            .unwrap_or_else(PoisonError::into_inner)
            .clone()
    }
}

/// A bound TCP port. Each peer that connects and completes the handshake
/// arrives as a [`Connection`]; peers that fail the handshake never appear.
/// Dropping the listener closes the port.
#[derive(Debug)]
pub struct Listener {
    local_addr: SocketAddr,
    connections: Reader<Connection>,
}

impl Listener {
    /// The bound address, with the actual port when binding to port 0.
    pub fn local_addr(&self) -> SocketAddr {
        self.local_addr
    }

    /// The next connection, if one is waiting.
    pub fn try_accept(&mut self) -> Result<Connection, TryRecvError> {
        self.connections.try_recv()
    }

    /// The next connection, or `None` once the listener has closed.
    pub fn poll_accept(&mut self, cx: &mut std::task::Context<'_>) -> Poll<Option<Connection>> {
        self.connections.poll_recv(cx)
    }

    /// The next connection, waiting for one to arrive.
    pub async fn accept(&mut self) -> Option<Connection> {
        self.connections.recv().await
    }

    /// The next connection, blocking the thread until one arrives.
    pub fn accept_blocking(&mut self) -> Option<Connection> {
        self.connections.recv_blocking()
    }
}

impl ConnectionConfig {
    /// libzmq's defaults: empty identity, HWM 1000 each way, no frame limit.
    pub fn new(socket_type: SocketType) -> Self {
        let hwm = Hwm::Messages(DEFAULT_HWM);
        Self {
            socket_type,
            identity: Identity::default(),
            send_hwm: hwm,
            recv_hwm: hwm,
            max_frame_size: None,
            reconnect: Reconnect::default(),
            linger: Linger::default(),
            security: Security::default(),
            heartbeat: None,
            handshake_timeout: Some(DEFAULT_HANDSHAKE_TIMEOUT),
        }
    }
}

impl Context {
    /// A context with one I/O thread, like libzmq's default.
    pub fn new() -> io::Result<Self> {
        Self::with_io_threads(NonZeroUsize::MIN)
    }

    /// A context with `threads` I/O threads.
    pub fn with_io_threads(threads: NonZeroUsize) -> io::Result<Self> {
        let io_threads = (0..threads.get())
            .map(IoThread::spawn)
            .collect::<io::Result<Vec<_>>>()?;
        Ok(Self {
            inner: Arc::new(Inner {
                io_threads,
                next: AtomicUsize::new(0),
            }),
        })
    }

    /// Binds a TCP port now, so errors such as an address in use are
    /// reported here, and accepts peers on an I/O thread. Accepted
    /// connections use `config`.
    pub fn bind(&self, addr: SocketAddr, config: ConnectionConfig) -> io::Result<Listener> {
        let listener = TcpListener::bind(addr)?;
        let local_addr = listener.local_addr()?;
        let (writer, connections) = pipe(Hwm::Unlimited);
        self.io_thread()
            .send(IoCommand::Listen(Box::new(ListenRequest {
                listener,
                config,
                connections: writer,
            })));
        Ok(Listener {
            local_addr,
            connections,
        })
    }

    /// Connects to a TCP peer in the background and returns the pipes for
    /// talking to it.
    pub fn connect(&self, addr: SocketAddr, config: ConnectionConfig) -> Connection {
        let (tx, outbound) = pipe(config.send_hwm);
        let (inbound, rx) = pipe(config.recv_hwm);
        let peer = PeerSlot::default();
        self.io_thread()
            .send(IoCommand::Connect(Box::new(ConnectRequest {
                addr,
                config,
                inbound,
                outbound,
                peer: peer.clone(),
            })));
        Connection { tx, rx, peer }
    }

    /// Closes every listener and connection, lets connections deliver their
    /// queued messages for up to `timeout` (or less, per their [`Linger`]),
    /// then stops the I/O threads. Affects every clone of this context.
    ///
    /// Dropping the last clone without calling this closes everything at
    /// once, discarding queued messages.
    pub fn shutdown(&self, timeout: Duration) -> Result<(), ShutdownTimedOut> {
        let deadline = Instant::now() + timeout;
        let reports: Vec<mpsc::Receiver<usize>> = self
            .inner
            .io_threads
            .iter()
            .map(|thread| {
                let (done, report) = mpsc::channel();
                thread.send(IoCommand::Shutdown { deadline, done });
                report
            })
            .collect();
        let mut abandoned = 0;
        for (thread, report) in self.inner.io_threads.iter().zip(reports) {
            // A thread already stopped by an earlier shutdown reports nothing.
            abandoned += report.recv().unwrap_or(0);
            thread.join();
        }
        if abandoned == 0 {
            Ok(())
        } else {
            Err(ShutdownTimedOut { abandoned })
        }
    }

    /// The next I/O thread, round robin.
    fn io_thread(&self) -> &IoThread {
        let threads = &self.inner.io_threads;
        &threads[self.inner.next.fetch_add(1, Ordering::Relaxed) % threads.len()]
    }
}

impl fmt::Display for ShutdownTimedOut {
    fn fmt(&self, f: &mut fmt::Formatter<'_>) -> fmt::Result {
        write!(
            f,
            "shutdown timed out with {} connections unfinished",
            self.abandoned
        )
    }
}

impl std::error::Error for ShutdownTimedOut {}
