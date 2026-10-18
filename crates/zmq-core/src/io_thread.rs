//! I/O threads: one mio event loop each, driven by socket readiness, pipe
//! wakeups and commands from other threads.

use std::cmp::Reverse;
use std::collections::{BinaryHeap, HashMap};
use std::io;
use std::sync::atomic::{AtomicBool, Ordering};
use std::sync::{Arc, Mutex, PoisonError, mpsc};
use std::task::{Context, Wake, Waker};
use std::thread::{self, JoinHandle};
use std::time::Instant;

use mio::{Events, Interest, Token};
use zmq_pipe::{Reader, Writer};
use zmtp::Message;

use crate::context::{PeerSlot, message_pipe};
use crate::engine::{Engine, EngineError, Pipes};
use crate::reconnect::Retry;
use crate::transport::{Address, IpcCleanup, Stream, StreamListener};
use crate::{Connection, ConnectionConfig, EventKind, Linger, Topics};

/// Token of the waker that reports commands and pipe wakeups.
const WAKER: Token = Token(0);

/// Work sent to an I/O thread.
pub(crate) enum IoCommand {
    /// Connect and serve the pipes in the request.
    Connect(Box<ConnectRequest>),
    /// Accept peers on a bound listener.
    Listen(Box<ListenRequest>),
    /// Close listeners, let connections deliver what they have queued until
    /// `deadline`, or for as long as their linger allows if `None`, then
    /// exit and report how many were cut off.
    Shutdown {
        deadline: Option<Instant>,
        done: mpsc::Sender<usize>,
    },
    /// Close every connection and listener at once, and exit.
    Stop,
}

/// A connection to start: where to, as whom, and the session's pipe ends.
pub(crate) struct ConnectRequest {
    pub(crate) addr: Address,
    pub(crate) config: ConnectionConfig,
    pub(crate) pipes: ConnectPipes,
}

/// The pipes a connecting session serves.
pub(crate) enum ConnectPipes {
    /// The same pipes for the session's whole life.
    Now(SessionPipes),
    /// New pipes for each completed handshake, delivered here
    /// (`ZMQ_IMMEDIATE`).
    Immediate(Writer<Connection>),
}

/// A bound listener to serve, and where to deliver accepted connections.
pub(crate) struct ListenRequest {
    pub(crate) listener: StreamListener,
    pub(crate) config: ConnectionConfig,
    pub(crate) connections: Writer<Connection>,
    /// What to remove once the listener closes.
    pub(crate) cleanup: Option<IpcCleanup>,
    /// The bound endpoint, for monitor events.
    pub(crate) endpoint: String,
}

/// A running I/O thread. Dropping it stops and joins the thread.
#[derive(Debug)]
pub(crate) struct IoThread {
    commands: mpsc::Sender<IoCommand>,
    waker: Arc<mio::Waker>,
    thread: Mutex<Option<JoinHandle<()>>>,
}

impl IoThread {
    pub(crate) fn spawn(index: usize) -> io::Result<Self> {
        let poll = mio::Poll::new()?;
        let waker = Arc::new(mio::Waker::new(poll.registry(), WAKER)?);
        let (commands, command_rx) = mpsc::channel();
        let (ready, ready_rx) = mpsc::channel();
        let event_loop = EventLoop {
            poll,
            waker: waker.clone(),
            commands: command_rx,
            ready,
            ready_rx,
            sessions: HashMap::new(),
            listeners: HashMap::new(),
            timers: BinaryHeap::new(),
            shutdown: None,
            next_id: WAKER.0 + 1,
        };
        let thread = thread::Builder::new()
            .name(format!("zmq-io-{index}"))
            .spawn(move || event_loop.run())?;
        Ok(Self {
            commands,
            waker,
            thread: Mutex::new(Some(thread)),
        })
    }

    /// Hands `command` to the thread. If the thread is gone, the command and
    /// any pipes or sockets in it are dropped, which closes them.
    pub(crate) fn send(&self, command: IoCommand) {
        if self.commands.send(command).is_ok() {
            let _ = self.waker.wake();
        }
    }

    /// Waits for the thread to exit, if it has not been joined already.
    pub(crate) fn join(&self) {
        let thread = self
            .thread
            .lock()
            .unwrap_or_else(PoisonError::into_inner)
            .take();
        if let Some(thread) = thread {
            let _ = thread.join();
        }
    }
}

impl Drop for IoThread {
    fn drop(&mut self) {
        self.send(IoCommand::Stop);
        self.join();
    }
}

struct EventLoop {
    poll: mio::Poll,
    waker: Arc<mio::Waker>,
    commands: mpsc::Receiver<IoCommand>,
    /// Ids whose pipes woke them, sent by [`TokenWaker`].
    ready: mpsc::Sender<usize>,
    ready_rx: mpsc::Receiver<usize>,
    sessions: HashMap<usize, Session>,
    listeners: HashMap<usize, ListenerEntry>,
    /// Deadlines, such as reconnection attempts. A timer for something that
    /// has since changed state is harmless: driving it finds nothing to do.
    timers: BinaryHeap<Reverse<(Instant, usize)>>,
    /// Set once shutdown starts: when it must end, and where to report.
    shutdown: Option<(Option<Instant>, mpsc::Sender<usize>)>,
    /// Ids are never reused, so a late wakeup for something closed cannot
    /// reach a newcomer.
    next_id: usize,
}

impl EventLoop {
    fn run(mut self) {
        let mut events = Events::with_capacity(256);
        let mut due = Vec::new();
        loop {
            let timeout = self
                .timers
                .peek()
                .map(|Reverse((at, _))| at.saturating_duration_since(Instant::now()));
            if let Err(err) = self.poll.poll(&mut events, timeout) {
                if err.kind() == io::ErrorKind::Interrupted {
                    continue;
                }
                return;
            }
            due.extend(
                events
                    .iter()
                    .map(|event| event.token())
                    .filter(|&token| token != WAKER)
                    .map(|token| token.0),
            );
            let now = Instant::now();
            while let Some(&Reverse((at, id))) = self.timers.peek()
                && at <= now
            {
                self.timers.pop();
                due.push(id);
            }
            loop {
                match self.commands.try_recv() {
                    // Once shutting down, new work is dropped, closing its pipes.
                    Ok(IoCommand::Connect(_) | IoCommand::Listen(_)) if self.shutdown.is_some() => {
                    }
                    Ok(IoCommand::Connect(request)) => due.extend(self.connect(*request)),
                    Ok(IoCommand::Listen(request)) => due.extend(self.listen(*request)),
                    Ok(IoCommand::Shutdown { deadline, done }) => {
                        due.extend(self.start_shutdown(deadline, done));
                    }
                    Ok(IoCommand::Stop) | Err(mpsc::TryRecvError::Disconnected) => return,
                    Err(mpsc::TryRecvError::Empty) => break,
                }
            }
            while let Ok(id) = self.ready_rx.try_recv() {
                let waker = match (self.sessions.get(&id), self.listeners.get(&id)) {
                    (Some(session), _) => &session.waker,
                    (None, Some(listener)) => &listener.waker,
                    (None, None) => continue,
                };
                // Cleared before driving, so a wakeup during the drive
                // queues the id again.
                waker.queued.store(false, Ordering::Release);
                due.push(id);
            }
            for id in due.drain(..) {
                if self.sessions.contains_key(&id) {
                    self.drive_session(id);
                } else if self.listeners.contains_key(&id) {
                    self.drive_listener(id);
                }
            }
            if let Some((deadline, done)) = &self.shutdown {
                let finished = self.sessions.is_empty();
                if finished || deadline.is_some_and(|deadline| Instant::now() >= deadline) {
                    let _ = done.send(self.sessions.len());
                    return;
                }
            }
        }
    }

    /// Closes the listeners and starts closing every session: each delivers
    /// what it has queued, within its linger and the shutdown deadline.
    fn start_shutdown(
        &mut self,
        deadline: Option<Instant>,
        done: mpsc::Sender<usize>,
    ) -> Vec<usize> {
        for (_, mut entry) in self.listeners.drain() {
            let _ = self.poll.registry().deregister(&mut entry.listener);
            entry
                .config
                .monitor
                .emit(&entry.endpoint, EventKind::Closed);
        }
        let now = Instant::now();
        let mut due = Vec::with_capacity(self.sessions.len());
        for (&id, session) in &mut self.sessions {
            // Sessions still open at the shutdown deadline count as cut off,
            // so only a shorter linger gets a deadline of its own.
            let linger = match session.config.linger {
                Linger::Forever => None,
                Linger::For(linger) => {
                    Some(now + linger).filter(|&at| deadline.is_none_or(|deadline| at < deadline))
                }
            };
            let at = match session.closing {
                Some(Closing {
                    deadline: Some(existing),
                }) => Some(existing),
                _ => linger,
            };
            session.closing = Some(Closing { deadline: at });
            if let Some(at) = at {
                self.timers.push(Reverse((at, id)));
            }
            due.push(id);
        }
        // Wake at the deadline even if no session has a timer of its own.
        if let Some(deadline) = deadline {
            self.timers.push(Reverse((deadline, WAKER.0)));
        }
        self.shutdown = Some((deadline, done));
        due
    }

    fn take_id(&mut self) -> usize {
        let id = self.next_id;
        self.next_id += 1;
        id
    }

    fn token_waker(&self, id: usize) -> Arc<TokenWaker> {
        Arc::new(TokenWaker {
            id,
            queued: AtomicBool::new(false),
            ready: self.ready.clone(),
            mio: self.waker.clone(),
        })
    }

    /// Adds a session that connects to `addr`; driving it makes the first
    /// attempt straight away.
    fn connect(&mut self, request: ConnectRequest) -> Option<usize> {
        let ConnectRequest {
            addr,
            config,
            pipes,
        } = request;
        let endpoint = addr.to_string();
        let (pipes, immediate) = match pipes {
            ConnectPipes::Now(pipes) => (pipes, None),
            ConnectPipes::Immediate(connections) => {
                let (pending, pipes) = SessionPipes::new(&config);
                let immediate = Immediate {
                    connections,
                    pending: Some(pending),
                };
                (pipes, Some(immediate))
            }
        };
        let id = self.add_session(Origin::Connect(addr), None, config, pipes, endpoint)?;
        if let Some(session) = self.sessions.get_mut(&id) {
            session.immediate = immediate;
        }
        Some(id)
    }

    fn listen(&mut self, request: ListenRequest) -> Option<usize> {
        let ListenRequest {
            mut listener,
            config,
            connections,
            cleanup,
            endpoint,
        } = request;
        let id = self.take_id();
        self.poll
            .registry()
            .register(&mut listener, Token(id), Interest::READABLE)
            .ok()?;
        let waker = self.token_waker(id);
        let entry = ListenerEntry {
            listener,
            config,
            connections,
            _cleanup: cleanup,
            endpoint,
            task_waker: Waker::from(waker.clone()),
            waker,
        };
        self.listeners.insert(id, entry);
        Some(id)
    }

    fn add_session(
        &mut self,
        origin: Origin,
        stream: Option<Stream>,
        config: ConnectionConfig,
        pipes: SessionPipes,
        endpoint: String,
    ) -> Option<usize> {
        let SessionPipes {
            inbound,
            outbound,
            peer,
        } = pipes;
        let id = self.take_id();
        let waker = self.token_waker(id);
        let mut session = Session {
            engine: None,
            retry: Retry::new(config.reconnect),
            reconnect_at: None,
            closing: None,
            scheduled: None,
            origin,
            config,
            inbound,
            outbound,
            peer,
            subscriptions: Topics::default(),
            immediate: None,
            endpoint,
            progress: Progress::default(),
            task_waker: Waker::from(waker.clone()),
            waker,
        };
        if let Some(stream) = stream {
            session.engine = Some(self.start_engine(id, stream, &session.config)?);
        }
        self.sessions.insert(id, session);
        Some(id)
    }

    fn start_engine(
        &self,
        id: usize,
        mut stream: Stream,
        config: &ConnectionConfig,
    ) -> Option<Engine> {
        self.poll
            .registry()
            .register(
                &mut stream,
                Token(id),
                Interest::READABLE | Interest::WRITABLE,
            )
            .ok()?;
        Some(Engine::new(stream, config.clone()))
    }

    fn drive_session(&mut self, id: usize) {
        let Some(session) = self.sessions.get_mut(&id) else {
            return;
        };
        let now = Instant::now();
        let mut cx = Context::from_waker(&session.task_waker);
        // The socket is gone while it holds none of our pipes. Checked even
        // when closing, as a shutdown may start closing us before the
        // socket's drop is seen here.
        if let Some(immediate) = &mut session.immediate
            && immediate.connections.poll_closed(&mut cx).is_ready()
            && immediate.pending.is_some()
        {
            self.close_session(id);
            return;
        }
        if session.closing.is_none() {
            // The socket dropped its sender: deliver what is queued, within
            // the linger, then close.
            if session.outbound.poll_closed(&mut cx).is_ready() {
                let deadline = match session.config.linger {
                    Linger::For(linger) if linger.is_zero() => {
                        self.close_session(id);
                        return;
                    }
                    Linger::For(linger) => Some(now + linger),
                    Linger::Forever => None,
                };
                if let Some(at) = deadline {
                    self.timers.push(Reverse((at, id)));
                }
                session.closing = Some(Closing { deadline });
            }
        }
        if session
            .closing
            .as_ref()
            .and_then(|closing| closing.deadline)
            .is_some_and(|at| at <= now)
        {
            self.close_session(id);
            return;
        }
        // Nothing left to deliver and no connection to deliver on, as
        // libzmq ends a session whose pipe has terminated: lingering would
        // reconnect forever for nothing.
        let established = session.engine.as_ref().is_some_and(Engine::is_established);
        if session.closing.is_some() && !established && session.outbound.is_drained() {
            self.close_session(id);
            return;
        }
        if session.engine.is_none() {
            if session.reconnect_at.is_some_and(|at| at > now) {
                return;
            }
            session.reconnect_at = None;
            let Origin::Connect(address) = &session.origin else {
                // Accepted sessions always have an engine until they end.
                self.close_session(id);
                return;
            };
            let address = address.clone();
            let config = session.config.clone();
            let engine = Stream::connect(&address)
                .ok()
                .and_then(|stream| self.start_engine(id, stream, &config));
            let Some(session) = self.sessions.get_mut(&id) else {
                return;
            };
            match engine {
                Some(engine) => session.engine = Some(engine),
                None => {
                    self.retry_later(id);
                    return;
                }
            }
        }
        let Some(session) = self.sessions.get_mut(&id) else {
            return;
        };
        let result = session.drive();
        session.report_progress();
        match result {
            Ok(()) => {}
            Err(err) => {
                session.report_failure(&err);
                if let Some(mut engine) = session.engine.take() {
                    let _ = self.poll.registry().deregister(engine.stream());
                }
                // libzmq reconnects after connection errors but not after
                // protocol errors such as a failed handshake.
                let reconnects =
                    matches!(session.origin, Origin::Connect(_)) && err.is_connection_error();
                if reconnects {
                    if session.immediate.is_some() {
                        // The socket's pipes close, discarding what it
                        // queued; the next connection gets new ones.
                        session.renew_pipes();
                    }
                    self.retry_later(id);
                } else {
                    self.close_session(id);
                }
                return;
            }
        }
        session.schedule_deadline(id, &mut self.timers);
        // A closing session is finished once everything queued is written.
        // Shutdown also finishes sessions whose socket is still open.
        let caught_up = session.engine.as_ref().is_some_and(Engine::caught_up);
        if caught_up && (session.outbound.is_closed() || self.shutdown.is_some()) {
            self.close_session(id);
            return;
        }
        let established = session.engine.as_ref().is_some_and(Engine::is_established);
        if !established {
            return;
        }
        session.retry.reset();
        if let Some(immediate) = &mut session.immediate
            && let Some(connection) = immediate.pending.take()
            && immediate.connections.try_send(connection).is_err()
        {
            self.close_session(id);
            return;
        }
        // An accepted connection reaches its listener's owner only once the
        // handshake has succeeded, as in libzmq.
        if let Origin::Accepted(delivery) = &mut session.origin
            && let Some(Delivery {
                listener,
                connection,
            }) = delivery.take()
        {
            let delivered = self
                .listeners
                .get_mut(&listener)
                .is_some_and(|entry| entry.connections.try_send(connection).is_ok());
            if !delivered {
                self.close_session(id);
            }
        }
    }

    /// Schedules the session's next connection attempt, or closes it if its
    /// policy gives up.
    fn retry_later(&mut self, id: usize) {
        let Some(session) = self.sessions.get_mut(&id) else {
            return;
        };
        match session.retry.next_delay() {
            Some(delay) => {
                session.config.monitor.emit(
                    &session.endpoint,
                    EventKind::ConnectRetried { interval: delay },
                );
                let at = Instant::now() + delay;
                session.reconnect_at = Some(at);
                self.timers.push(Reverse((at, id)));
            }
            None => self.close_session(id),
        }
    }

    fn close_session(&mut self, id: usize) {
        if let Some(session) = self.sessions.remove(&id)
            && let Some(mut engine) = session.engine
        {
            let _ = self.poll.registry().deregister(engine.stream());
        }
    }

    fn drive_listener(&mut self, id: usize) {
        let Some(entry) = self.listeners.get_mut(&id) else {
            return;
        };
        let mut cx = Context::from_waker(&entry.task_waker);
        if entry.connections.poll_closed(&mut cx).is_ready() {
            // The Listener was dropped: stop listening.
            if let Some(mut entry) = self.listeners.remove(&id) {
                let _ = self.poll.registry().deregister(&mut entry.listener);
                entry
                    .config
                    .monitor
                    .emit(&entry.endpoint, EventKind::Closed);
            }
            return;
        }
        let config = entry.config.clone();
        let endpoint = entry.endpoint.clone();
        let mut accepted = Vec::new();
        loop {
            match entry.listener.accept() {
                Ok(stream) => {
                    config.monitor.emit(&endpoint, EventKind::Accepted);
                    accepted.push(stream);
                }
                Err(err) if err.kind() == io::ErrorKind::Interrupted => {}
                Err(err) if err.kind() == io::ErrorKind::WouldBlock => break,
                // A transient error such as too many open files: try again
                // on the next readiness event.
                Err(err) => {
                    config
                        .monitor
                        .emit(&endpoint, EventKind::AcceptFailed(err.kind()));
                    break;
                }
            }
        }
        for stream in accepted {
            let (connection, pipes) = SessionPipes::new(&config);
            let origin = Origin::Accepted(Some(Delivery {
                listener: id,
                connection,
            }));
            if let Some(session) = self.add_session(
                origin,
                Some(stream),
                config.clone(),
                pipes,
                endpoint.clone(),
            ) {
                self.drive_session(session);
            }
        }
    }
}

/// A peer's pipes and, while connected, its engine. A connecting session
/// outlives individual connections: its pipes, and any messages queued in
/// them, survive until it gives up or is closed. Dropping it closes the
/// stream and both pipes.
struct Session {
    engine: Option<Engine>,
    origin: Origin,
    config: ConnectionConfig,
    retry: Retry,
    /// When the next connection attempt is due, while disconnected.
    reconnect_at: Option<Instant>,
    /// Set once the socket has dropped its sender, or shutdown has begun.
    closing: Option<Closing>,
    /// The engine deadline a timer is already set for.
    scheduled: Option<Instant>,
    inbound: Writer<Message>,
    outbound: Reader<Message>,
    /// Shared with the socket's [`Connection`].
    peer: PeerSlot,
    /// Subscriptions sent so far, replayed after reconnecting.
    subscriptions: Topics,
    /// Set for `ZMQ_IMMEDIATE` connections.
    immediate: Option<Immediate>,
    /// What monitor events call this connection.
    endpoint: String,
    /// How far the current engine has got, as monitors have been told.
    progress: Progress,
    waker: Arc<TokenWaker>,
    task_waker: Waker,
}

/// How far a session's current engine has got.
#[derive(Debug, Default, Clone, Copy)]
struct Progress {
    connected: bool,
    established: bool,
}

/// The session's ends of a connection, and the slot for its peer.
pub(crate) struct SessionPipes {
    inbound: Writer<Message>,
    outbound: Reader<Message>,
    peer: PeerSlot,
}

impl SessionPipes {
    /// New pipes for a connection configured as `config`: the socket's
    /// ends, and the session's.
    pub(crate) fn new(config: &ConnectionConfig) -> (Connection, Self) {
        let (tx, outbound) = message_pipe(config.send_hwm, config.conflate_outbound);
        let (inbound, rx) = message_pipe(config.recv_hwm, config.conflate_inbound);
        let peer = PeerSlot::default();
        let connection = Connection {
            tx,
            rx,
            peer: peer.clone(),
        };
        let pipes = Self {
            inbound,
            outbound,
            peer,
        };
        (connection, pipes)
    }
}

/// A `ZMQ_IMMEDIATE` session's way to the socket.
struct Immediate {
    /// Where each established connection's pipes go.
    connections: Writer<Connection>,
    /// The socket's ends of the current pipes, until the handshake on them
    /// completes.
    pending: Option<Connection>,
}

/// A session delivering its last queued messages.
struct Closing {
    /// When to give up; `None` lingers until done.
    deadline: Option<Instant>,
}

/// How a session came about, which decides whether it reconnects.
enum Origin {
    /// We connected to this address, and reconnect to it.
    Connect(Address),
    /// A listener accepted it. It never reconnects, and its pipes go to the
    /// listener's owner once the handshake succeeds.
    Accepted(Option<Delivery>),
}

struct Delivery {
    listener: usize,
    connection: Connection,
}

impl Session {
    /// Replaces the pipes with new ones for the next connection, closing
    /// those the socket holds.
    fn renew_pipes(&mut self) {
        let (connection, pipes) = SessionPipes::new(&self.config);
        self.inbound = pipes.inbound;
        self.outbound = pipes.outbound;
        self.peer = pipes.peer;
        // The socket greets the new pipes as a new peer.
        self.subscriptions = Topics::default();
        if let Some(immediate) = &mut self.immediate {
            immediate.pending = Some(connection);
        }
    }

    /// Sets a timer for the engine's next deadline unless one is already
    /// set for it or earlier, so busy connections do not flood the heap.
    fn schedule_deadline(&mut self, id: usize, timers: &mut BinaryHeap<Reverse<(Instant, usize)>>) {
        let Some(deadline) = self.engine.as_ref().and_then(Engine::next_deadline) else {
            return;
        };
        let now = Instant::now();
        if self
            .scheduled
            .is_some_and(|scheduled| scheduled > now && scheduled <= deadline)
        {
            return;
        }
        self.scheduled = Some(deadline);
        timers.push(Reverse((deadline, id)));
    }

    /// Tells monitors what the engine has newly achieved.
    fn report_progress(&mut self) {
        let Some(engine) = &self.engine else {
            return;
        };
        let monitor = &self.config.monitor;
        if !self.progress.connected && engine.is_connected() {
            self.progress.connected = true;
            // Accepted connections were reported by their listener.
            if matches!(self.origin, Origin::Connect(_)) {
                monitor.emit(&self.endpoint, EventKind::Connected);
            }
        }
        if !self.progress.established && engine.is_established() {
            self.progress.established = true;
            monitor.emit(&self.endpoint, EventKind::HandshakeSucceeded);
        }
    }

    /// Tells monitors the engine has failed: a connection that was up is
    /// disconnected, after a failed handshake if it never completed one. A
    /// connection attempt that never connected reports nothing here.
    fn report_failure(&mut self, err: &EngineError) {
        let progress = std::mem::take(&mut self.progress);
        if !progress.connected {
            return;
        }
        let monitor = &self.config.monitor;
        if !progress.established {
            monitor.emit(
                &self.endpoint,
                EventKind::HandshakeFailed(err.handshake_failure()),
            );
        }
        monitor.emit(&self.endpoint, EventKind::Disconnected);
    }

    fn drive(&mut self) -> Result<(), EngineError> {
        let Some(engine) = &mut self.engine else {
            return Ok(());
        };
        let mut cx = Context::from_waker(&self.task_waker);
        let mut pipes = Pipes {
            inbound: &mut self.inbound,
            outbound: &mut self.outbound,
            peer: &self.peer,
            subscriptions: &mut self.subscriptions,
        };
        engine.drive(&mut pipes, &mut cx)
    }
}

/// A listening socket and the queue its accepted connections go to.
struct ListenerEntry {
    listener: StreamListener,
    config: ConnectionConfig,
    connections: Writer<Connection>,
    /// Dropped with the entry, removing an `ipc://` socket's file.
    _cleanup: Option<IpcCleanup>,
    /// The bound endpoint, for monitor events.
    endpoint: String,
    waker: Arc<TokenWaker>,
    task_waker: Waker,
}

/// Wakes a session or listener when one of its pipes becomes ready: queues
/// its id once and wakes the event loop.
struct TokenWaker {
    id: usize,
    queued: AtomicBool,
    ready: mpsc::Sender<usize>,
    mio: Arc<mio::Waker>,
}

impl Wake for TokenWaker {
    fn wake(self: Arc<Self>) {
        self.wake_by_ref();
    }

    fn wake_by_ref(self: &Arc<Self>) {
        if !self.queued.swap(true, Ordering::AcqRel) && self.ready.send(self.id).is_ok() {
            let _ = self.mio.wake();
        }
    }
}
