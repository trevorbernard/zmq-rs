//! The context: the I/O threads every connection runs on.

use std::io;
use std::net::SocketAddr;
use std::num::NonZeroUsize;
use std::sync::Arc;
use std::sync::atomic::{AtomicUsize, Ordering};

use zmq_pipe::{Hwm, Reader, Writer, pipe};
use zmtp::{Identity, Message, SocketType};

use crate::io_thread::{ConnectRequest, IoCommand, IoThread};

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

    /// Connects to a TCP peer in the background and returns the pipes for
    /// talking to it.
    pub fn connect(&self, addr: SocketAddr, config: ConnectionConfig) -> Connection {
        let (tx, outbound) = pipe(config.send_hwm);
        let (inbound, rx) = pipe(config.recv_hwm);
        let threads = &self.inner.io_threads;
        let index = self.inner.next.fetch_add(1, Ordering::Relaxed) % threads.len();
        threads[index].send(IoCommand::Connect(Box::new(ConnectRequest {
            addr,
            config,
            inbound,
            outbound,
        })));
        Connection { tx, rx }
    }
}
