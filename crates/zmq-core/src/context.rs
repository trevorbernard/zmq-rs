//! The context: the I/O threads every connection runs on.

use std::collections::HashMap;
use std::fmt;
use std::io;
use std::net::SocketAddr;
use std::num::NonZeroUsize;
use std::sync::atomic::{AtomicUsize, Ordering};
use std::sync::{Arc, Mutex, PoisonError};

use std::sync::mpsc;
use std::task::Poll;
use std::time::{Duration, Instant};

use zmq_pipe::{Hwm, Reader, TryRecvError, Writer, pipe, pipe_conflated};
use zmtp::{Established, Frame, Identity, Message, Metadata, SocketType, Version};

use mio::net::TcpListener;
#[cfg(unix)]
use std::path::{Path, PathBuf};

use crate::io_thread::{
    ConnectPipes, ConnectRequest, IoCommand, IoThread, ListenRequest, SessionPipes,
};
use crate::transport::{Address, IpcCleanup, StreamListener};
use crate::{EventKind, MonitorSink, Reconnect, Security};

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
    /// The `inproc` names bound in this context, and connections waiting
    /// for a name to be bound.
    inproc: Mutex<InprocRegistry>,
}

#[derive(Debug, Default)]
struct InprocRegistry {
    bound: HashMap<String, InprocBinding>,
    pending: HashMap<String, Vec<PendingConnect>>,
}

/// A bound `inproc` name. Free again once its listener is dropped.
#[derive(Debug)]
struct InprocBinding {
    config: ConnectionConfig,
    connections: Writer<Connection>,
}

/// A connection made before its name was bound. Its pipes already carry
/// the connector's messages; binding hands `server` to the listener.
#[derive(Debug)]
struct PendingConnect {
    config: ConnectionConfig,
    server: Connection,
    client: PeerSlot,
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
    /// Keep only the latest message from the peer, ignoring `recv_hwm`
    /// (`ZMQ_CONFLATE`).
    pub conflate_inbound: bool,
    /// Keep only the latest message towards the peer, ignoring `send_hwm`
    /// (`ZMQ_CONFLATE`).
    pub conflate_outbound: bool,
    /// The largest frame accepted from the peer.
    pub max_frame_size: Option<u64>,
    /// How a connecting session retries. Accepted connections never do.
    pub reconnect: Reconnect,
    /// Whether a TCP or IPC connection reaches the socket only once its
    /// handshake completes, and leaves it whenever it breaks, discarding
    /// what was queued on it (`ZMQ_IMMEDIATE`). Otherwise its pipes exist,
    /// and queue, from the start and across reconnections.
    pub immediate: bool,
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
    /// Where listeners and connections report [`Event`]s. Shared by every
    /// connection made with a clone of this config.
    ///
    /// [`Event`]: crate::Event
    pub monitor: MonitorSink,
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

    pub(crate) fn into_queue(self) -> Reader<Connection> {
        self.connections
    }
}

/// A bound `ipc://` path: a Unix domain socket. Peers that complete the
/// handshake arrive as [`Connection`]s. Dropping the listener closes the
/// socket and removes its file.
#[cfg(unix)]
#[derive(Debug)]
pub struct IpcListener {
    path: PathBuf,
    connections: Reader<Connection>,
}

#[cfg(unix)]
impl IpcListener {
    /// The bound path.
    pub fn path(&self) -> &Path {
        &self.path
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

    pub(crate) fn into_queue(self) -> Reader<Connection> {
        self.connections
    }
}

/// A bound `ipc://@name`: a Unix domain socket in Linux's abstract
/// namespace. Peers that complete the handshake arrive as [`Connection`]s.
/// Dropping the listener frees the name; there is no file.
#[cfg(any(target_os = "linux", target_os = "android"))]
#[derive(Debug)]
pub struct IpcAbstractListener {
    name: String,
    connections: Reader<Connection>,
}

#[cfg(any(target_os = "linux", target_os = "android"))]
impl IpcAbstractListener {
    /// The bound name, without the leading `@`.
    pub fn name(&self) -> &str {
        &self.name
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

    pub(crate) fn into_queue(self) -> Reader<Connection> {
        self.connections
    }
}

/// An `inproc` name bound in a context. Peers that connect to it with a
/// compatible socket type arrive as [`Connection`]s. Dropping it frees the
/// name.
#[derive(Debug)]
pub struct InprocListener {
    name: String,
    connections: Reader<Connection>,
}

impl InprocListener {
    /// The bound name.
    pub fn name(&self) -> &str {
        &self.name
    }

    /// The next connection, if one is waiting.
    pub fn try_accept(&mut self) -> Result<Connection, TryRecvError> {
        self.connections.try_recv()
    }

    /// The next connection, or `None` once the listener has closed.
    pub fn poll_accept(&mut self, cx: &mut std::task::Context<'_>) -> Poll<Option<Connection>> {
        self.connections.poll_recv(cx)
    }

    /// The next connection, blocking the thread until one arrives.
    pub fn accept_blocking(&mut self) -> Option<Connection> {
        self.connections.recv_blocking()
    }

    pub(crate) fn into_queue(self) -> Reader<Connection> {
        self.connections
    }
}

/// Introduces the two ends of an `inproc` connection to each other, as a
/// handshake would, and returns the server's end. Incompatible socket types
/// are never paired: dropping the server's end closes the connection.
/// One direction of a connection: only the latest message if `conflate`,
/// otherwise up to `hwm`.
pub(crate) fn message_pipe(hwm: Hwm, conflate: bool) -> (Writer<Message>, Reader<Message>) {
    if conflate {
        pipe_conflated()
    } else {
        pipe(hwm)
    }
}

fn pair(
    server_config: &ConnectionConfig,
    client_config: &ConnectionConfig,
    server: Connection,
    client: &PeerSlot,
) -> Option<Connection> {
    if !server_config.socket_type.accepts(client_config.socket_type)
        || !client_config.socket_type.accepts(server_config.socket_type)
    {
        return None;
    }
    *server.peer.lock().unwrap_or_else(PoisonError::into_inner) = Some(introduce(client_config));
    *client.lock().unwrap_or_else(PoisonError::into_inner) = Some(introduce(server_config));
    Some(server)
}

/// What a handshake with a peer configured as `config` would establish.
fn introduce(config: &ConnectionConfig) -> Established {
    Established {
        version: Version::V3_1,
        peer_socket_type: Some(config.socket_type),
        peer_identity: if config.socket_type.sends_identity() {
            Frame::copy_from_slice(config.identity.as_slice())
        } else {
            Frame::new()
        },
        metadata: Metadata::default(),
        authenticated: None,
        implicit_subscription: false,
    }
}

/// The HWM of an `inproc` pipe whose ends have HWMs `a` and `b`.
fn hwm_sum(a: Hwm, b: Hwm) -> Hwm {
    match (a, b) {
        (Hwm::Messages(a), Hwm::Messages(b)) => Hwm::Messages(a.saturating_add(b.get())),
        _ => Hwm::Unlimited,
    }
}

/// Passes a bind's result through, reporting a failure to the monitors.
fn reported<T>(result: io::Result<T>, config: &ConnectionConfig, endpoint: &str) -> io::Result<T> {
    result.inspect_err(|err| {
        config
            .monitor
            .emit(endpoint, EventKind::BindFailed(err.kind()));
    })
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
            conflate_inbound: false,
            conflate_outbound: false,
            max_frame_size: None,
            reconnect: Reconnect::default(),
            immediate: false,
            linger: Linger::default(),
            security: Security::default(),
            heartbeat: None,
            handshake_timeout: Some(DEFAULT_HANDSHAKE_TIMEOUT),
            monitor: MonitorSink::default(),
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
                inproc: Mutex::default(),
            }),
        })
    }

    /// Binds a TCP port now, so errors such as an address in use are
    /// reported here, and accepts peers on an I/O thread. Accepted
    /// connections use `config`.
    pub fn bind(&self, addr: SocketAddr, config: ConnectionConfig) -> io::Result<Listener> {
        let bound = TcpListener::bind(addr).and_then(|listener| {
            let local_addr = listener.local_addr()?;
            Ok((listener, local_addr))
        });
        let (listener, local_addr) = reported(bound, &config, &format!("tcp://{addr}"))?;
        let endpoint = format!("tcp://{local_addr}");
        let connections = self.listen(StreamListener::Tcp(listener), None, endpoint, config);
        Ok(Listener {
            local_addr,
            connections,
        })
    }

    /// Binds a Unix domain socket at `path` for `ipc://`, and accepts peers
    /// on an I/O thread. A socket file nobody is listening on, as a crashed
    /// process leaves behind, is replaced; a live socket or any other file
    /// is `AddrInUse`. The file is removed when the listener closes.
    #[cfg(unix)]
    pub fn bind_ipc(&self, path: &Path, config: ConnectionConfig) -> io::Result<IpcListener> {
        let endpoint = format!("ipc://{}", path.display());
        let (listener, cleanup) = reported(crate::transport::bind_ipc(path), &config, &endpoint)?;
        let connections = self.listen(
            StreamListener::Unix(listener),
            Some(cleanup),
            endpoint,
            config,
        );
        Ok(IpcListener {
            path: path.to_owned(),
            connections,
        })
    }

    /// Binds a Unix domain socket at a fresh path in a new temporary
    /// directory, for `ipc://*`. Both are removed when the listener closes.
    #[cfg(unix)]
    pub fn bind_ipc_anywhere(&self, config: ConnectionConfig) -> io::Result<IpcListener> {
        let (listener, cleanup, path) =
            reported(crate::transport::bind_ipc_anywhere(), &config, "ipc://*")?;
        let endpoint = format!("ipc://{}", path.display());
        let connections = self.listen(
            StreamListener::Unix(listener),
            Some(cleanup),
            endpoint,
            config,
        );
        Ok(IpcListener { path, connections })
    }

    /// Binds `name` in Linux's abstract socket namespace, for
    /// `ipc://@name`, and accepts peers on an I/O thread. A live name is
    /// `AddrInUse`, and a name too long for `sun_path` is `InvalidInput`.
    /// There is no file: the name is free again once the listener closes.
    #[cfg(any(target_os = "linux", target_os = "android"))]
    pub fn bind_ipc_abstract(
        &self,
        name: &str,
        config: ConnectionConfig,
    ) -> io::Result<IpcAbstractListener> {
        let endpoint = format!("ipc://@{name}");
        let listener = reported(
            crate::transport::bind_ipc_abstract(name),
            &config,
            &endpoint,
        )?;
        let connections = self.listen(StreamListener::Unix(listener), None, endpoint, config);
        Ok(IpcAbstractListener {
            name: name.to_owned(),
            connections,
        })
    }

    /// Serves `listener`, bound at `endpoint`, on an I/O thread, returning
    /// its accept queue.
    fn listen(
        &self,
        listener: StreamListener,
        cleanup: Option<IpcCleanup>,
        endpoint: String,
        config: ConnectionConfig,
    ) -> Reader<Connection> {
        config.monitor.emit(&endpoint, EventKind::Listening);
        let (writer, connections) = pipe(Hwm::Unlimited);
        self.io_thread()
            .send(IoCommand::Listen(Box::new(ListenRequest {
                listener,
                config,
                connections: writer,
                cleanup,
                endpoint,
            })));
        connections
    }

    /// Binds the `inproc` `name` in this context. Fails with
    /// `AddrInUse` if a live listener already has it. Connections that were
    /// waiting for the name are paired at once.
    pub fn bind_inproc(&self, name: &str, config: ConnectionConfig) -> io::Result<InprocListener> {
        let mut registry = self.inproc();
        if registry
            .bound
            .get(name)
            .is_some_and(|binding| !binding.connections.is_closed())
        {
            return Err(io::Error::new(
                io::ErrorKind::AddrInUse,
                format!("inproc://{name} is already bound"),
            ));
        }
        let (mut writer, connections) = pipe(Hwm::Unlimited);
        for pending in registry.pending.remove(name).unwrap_or_default() {
            if let Some(server) = pair(&config, &pending.config, pending.server, &pending.client) {
                let _ = writer.try_send(server);
            }
        }
        registry.bound.insert(
            name.to_owned(),
            InprocBinding {
                config,
                connections: writer,
            },
        );
        Ok(InprocListener {
            name: name.to_owned(),
            connections,
        })
    }

    /// Connects to the `inproc` `name` in this context. If nothing is bound
    /// yet, the connection waits for a bind; messages sent meanwhile queue.
    pub fn connect_inproc(&self, name: &str, config: ConnectionConfig) -> Connection {
        let mut registry = self.inproc();
        let binding = registry
            .bound
            .get_mut(name)
            .filter(|binding| !binding.connections.is_closed());
        // Bound: each direction holds what the sender and the receiver
        // would each have buffered, as in libzmq. Otherwise only our side
        // is known.
        let (send_hwm, recv_hwm) = match &binding {
            Some(binding) => (
                hwm_sum(config.send_hwm, binding.config.recv_hwm),
                hwm_sum(config.recv_hwm, binding.config.send_hwm),
            ),
            None => (config.send_hwm, config.recv_hwm),
        };
        let (conflate_out, conflate_in) = match &binding {
            Some(binding) => (
                config.conflate_outbound || binding.config.conflate_inbound,
                config.conflate_inbound || binding.config.conflate_outbound,
            ),
            None => (config.conflate_outbound, config.conflate_inbound),
        };
        let (tx, server_rx) = message_pipe(send_hwm, conflate_out);
        let (server_tx, rx) = message_pipe(recv_hwm, conflate_in);
        let client = Connection {
            tx,
            rx,
            peer: PeerSlot::default(),
        };
        let server = Connection {
            tx: server_tx,
            rx: server_rx,
            peer: PeerSlot::default(),
        };
        match binding {
            Some(binding) => {
                if let Some(server) = pair(&binding.config, &config, server, &client.peer) {
                    let _ = binding.connections.try_send(server);
                }
            }
            None => {
                let pending = &mut registry.pending.entry(name.to_owned()).or_default();
                // Forget connections given up on while waiting.
                pending.retain(|pending| !pending.server.rx.is_closed());
                pending.push(PendingConnect {
                    config,
                    server,
                    client: client.peer.clone(),
                });
            }
        }
        client
    }

    fn inproc(&self) -> std::sync::MutexGuard<'_, InprocRegistry> {
        self.inner
            .inproc
            .lock()
            .unwrap_or_else(PoisonError::into_inner)
    }

    /// Connects to a TCP peer in the background and returns the pipes for
    /// talking to it.
    pub fn connect(&self, addr: SocketAddr, config: ConnectionConfig) -> Connection {
        self.connect_to(Address::Tcp(addr), config)
    }

    /// Connects to the Unix domain socket at `path` in the background, for
    /// `ipc://`. Like a TCP connection, it retries until something listens
    /// there, and reconnects if the connection breaks.
    #[cfg(unix)]
    pub fn connect_ipc(&self, path: &Path, config: ConnectionConfig) -> Connection {
        self.connect_to(Address::Ipc(path.to_owned()), config)
    }

    /// Connects to `name` in Linux's abstract socket namespace in the
    /// background, for `ipc://@name`, retrying and reconnecting like
    /// [`connect_ipc`](Self::connect_ipc).
    #[cfg(any(target_os = "linux", target_os = "android"))]
    pub fn connect_ipc_abstract(&self, name: &str, config: ConnectionConfig) -> Connection {
        self.connect_to(Address::IpcAbstract(name.to_owned()), config)
    }

    pub(crate) fn connect_to(&self, addr: Address, config: ConnectionConfig) -> Connection {
        let (connection, pipes) = SessionPipes::new(&config);
        self.io_thread()
            .send(IoCommand::Connect(Box::new(ConnectRequest {
                addr,
                config,
                pipes: ConnectPipes::Now(pipes),
            })));
        connection
    }

    /// Connects to `addr` in the background, as `ZMQ_IMMEDIATE`: each time
    /// a handshake completes, a new [`Connection`] arrives on the returned
    /// queue, and it closes when that connection breaks. The queue's
    /// reader going away ends the session.
    pub(crate) fn connect_immediate(
        &self,
        addr: Address,
        config: ConnectionConfig,
    ) -> Reader<Connection> {
        let (writer, connections) = pipe(Hwm::Unlimited);
        self.io_thread()
            .send(IoCommand::Connect(Box::new(ConnectRequest {
                addr,
                config,
                pipes: ConnectPipes::Immediate(writer),
            })));
        connections
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

#[cfg(test)]
mod tests {
    use super::*;

    fn config(socket_type: SocketType) -> ConnectionConfig {
        ConnectionConfig::new(socket_type)
    }

    #[test]
    fn inproc_peers_learn_each_others_type_and_identity() {
        let context = Context::new().unwrap();
        let mut listener = context
            .bind_inproc("ids", config(SocketType::Router))
            .unwrap();
        let client = context.connect_inproc(
            "ids",
            ConnectionConfig {
                identity: Identity::new("me").unwrap(),
                ..config(SocketType::Dealer)
            },
        );
        let server = listener.try_accept().expect("paired at once");
        let seen = server.peer().unwrap();
        assert_eq!(seen.peer_socket_type, Some(SocketType::Dealer));
        assert_eq!(seen.peer_identity, Frame::from("me"));
        let seen = client.peer().unwrap();
        assert_eq!(seen.peer_socket_type, Some(SocketType::Router));
    }

    #[test]
    fn a_pending_connect_of_the_wrong_type_is_closed_by_the_bind() {
        let context = Context::new().unwrap();
        let client = context.connect_inproc("late", config(SocketType::Push));
        let mut listener = context
            .bind_inproc("late", config(SocketType::Push))
            .unwrap();
        assert!(listener.try_accept().is_err());
        assert!(client.tx.is_closed());
        assert_eq!(client.peer(), None);
    }

    #[test]
    fn a_pending_connect_given_up_on_is_forgotten() {
        let context = Context::new().unwrap();
        drop(context.connect_inproc("gone", config(SocketType::Push)));
        let _kept = context.connect_inproc("gone", config(SocketType::Push));
        assert_eq!(context.inproc().pending["gone"].len(), 1);
    }

    #[test]
    fn unlimited_on_either_side_makes_the_inproc_pipe_unlimited() {
        let two = Hwm::Messages(NonZeroUsize::new(2).unwrap());
        let five = Hwm::Messages(NonZeroUsize::new(5).unwrap());
        assert_eq!(
            hwm_sum(two, Hwm::Messages(NonZeroUsize::new(3).unwrap())),
            five
        );
        assert_eq!(hwm_sum(two, Hwm::Unlimited), Hwm::Unlimited);
        assert_eq!(hwm_sum(Hwm::Unlimited, two), Hwm::Unlimited);
    }
}
