//! I/O threads: one mio event loop each, driven by socket readiness, pipe
//! wakeups and commands from other threads.

use std::collections::HashMap;
use std::io;
use std::net::SocketAddr;
use std::sync::atomic::{AtomicBool, Ordering};
use std::sync::{Arc, mpsc};
use std::task::{Context, Wake, Waker};
use std::thread::{self, JoinHandle};

use mio::net::{TcpListener, TcpStream};
use mio::{Events, Interest, Token};
use zmq_pipe::{Reader, Writer, pipe};
use zmtp::Message;

use crate::engine::{Engine, EngineError, Pipes};
use crate::{Connection, ConnectionConfig};

/// Token of the waker that reports commands and pipe wakeups.
const WAKER: Token = Token(0);

/// Work sent to an I/O thread.
pub(crate) enum IoCommand {
    /// Connect and serve the pipes in the request.
    Connect(Box<ConnectRequest>),
    /// Accept peers on a bound listener.
    Listen(Box<ListenRequest>),
    /// Close every connection and listener, and exit.
    Stop,
}

/// A connection to start: where to, as whom, and the session's pipe ends.
pub(crate) struct ConnectRequest {
    pub(crate) addr: SocketAddr,
    pub(crate) config: ConnectionConfig,
    pub(crate) inbound: Writer<Message>,
    pub(crate) outbound: Reader<Message>,
}

/// A bound listener to serve, and where to deliver accepted connections.
pub(crate) struct ListenRequest {
    pub(crate) listener: TcpListener,
    pub(crate) config: ConnectionConfig,
    pub(crate) connections: Writer<Connection>,
}

/// A running I/O thread. Dropping it stops and joins the thread.
#[derive(Debug)]
pub(crate) struct IoThread {
    commands: mpsc::Sender<IoCommand>,
    waker: Arc<mio::Waker>,
    thread: Option<JoinHandle<()>>,
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
            next_id: WAKER.0 + 1,
        };
        let thread = thread::Builder::new()
            .name(format!("zmq-io-{index}"))
            .spawn(move || event_loop.run())?;
        Ok(Self {
            commands,
            waker,
            thread: Some(thread),
        })
    }

    /// Hands `command` to the thread. If the thread is gone, the command and
    /// any pipes or sockets in it are dropped, which closes them.
    pub(crate) fn send(&self, command: IoCommand) {
        if self.commands.send(command).is_ok() {
            let _ = self.waker.wake();
        }
    }
}

impl Drop for IoThread {
    fn drop(&mut self) {
        self.send(IoCommand::Stop);
        if let Some(thread) = self.thread.take() {
            let _ = thread.join();
        }
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
    /// Ids are never reused, so a late wakeup for something closed cannot
    /// reach a newcomer.
    next_id: usize,
}

impl EventLoop {
    fn run(mut self) {
        let mut events = Events::with_capacity(256);
        let mut due = Vec::new();
        loop {
            if let Err(err) = self.poll.poll(&mut events, None) {
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
            loop {
                match self.commands.try_recv() {
                    Ok(IoCommand::Connect(request)) => due.extend(self.connect(*request)),
                    Ok(IoCommand::Listen(request)) => due.extend(self.listen(*request)),
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
        }
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

    /// Starts a connection. On failure the pipes are dropped, closing them.
    fn connect(&mut self, request: ConnectRequest) -> Option<usize> {
        let ConnectRequest {
            addr,
            config,
            inbound,
            outbound,
        } = request;
        let stream = TcpStream::connect(addr).ok()?;
        self.add_session(stream, config, inbound, outbound, None)
    }

    fn listen(&mut self, request: ListenRequest) -> Option<usize> {
        let ListenRequest {
            mut listener,
            config,
            connections,
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
            task_waker: Waker::from(waker.clone()),
            waker,
        };
        self.listeners.insert(id, entry);
        Some(id)
    }

    fn add_session(
        &mut self,
        mut stream: TcpStream,
        config: ConnectionConfig,
        inbound: Writer<Message>,
        outbound: Reader<Message>,
        delivery: Option<Delivery>,
    ) -> Option<usize> {
        let id = self.take_id();
        self.poll
            .registry()
            .register(
                &mut stream,
                Token(id),
                Interest::READABLE | Interest::WRITABLE,
            )
            .ok()?;
        let waker = self.token_waker(id);
        let session = Session {
            engine: Engine::new(stream, config),
            inbound,
            outbound,
            delivery,
            task_waker: Waker::from(waker.clone()),
            waker,
        };
        self.sessions.insert(id, session);
        Some(id)
    }

    fn drive_session(&mut self, id: usize) {
        let Some(session) = self.sessions.get_mut(&id) else {
            return;
        };
        if session.drive().is_err() {
            self.close_session(id);
            return;
        }
        // An accepted connection reaches its listener's owner only once the
        // handshake has succeeded, as in libzmq.
        if session.engine.is_established()
            && let Some(Delivery {
                listener,
                connection,
            }) = session.delivery.take()
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

    fn close_session(&mut self, id: usize) {
        if let Some(mut session) = self.sessions.remove(&id) {
            let _ = self.poll.registry().deregister(session.engine.stream());
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
            }
            return;
        }
        let config = entry.config.clone();
        let mut accepted = Vec::new();
        loop {
            match entry.listener.accept() {
                Ok((stream, _peer)) => accepted.push(stream),
                Err(err) if err.kind() == io::ErrorKind::Interrupted => {}
                // WouldBlock, or a transient error such as too many open
                // files: try again on the next readiness event.
                Err(_) => break,
            }
        }
        for stream in accepted {
            let (tx, outbound) = pipe(config.send_hwm);
            let (inbound, rx) = pipe(config.recv_hwm);
            let delivery = Delivery {
                listener: id,
                connection: Connection { tx, rx },
            };
            if let Some(session) =
                self.add_session(stream, config.clone(), inbound, outbound, Some(delivery))
            {
                self.drive_session(session);
            }
        }
    }
}

/// A connection and the pipes it serves. Dropping it closes the stream and
/// both pipes.
struct Session {
    engine: Engine,
    inbound: Writer<Message>,
    outbound: Reader<Message>,
    /// For an accepted connection: the socket's end of the pipes, handed to
    /// the listener once the handshake succeeds.
    delivery: Option<Delivery>,
    waker: Arc<TokenWaker>,
    task_waker: Waker,
}

struct Delivery {
    listener: usize,
    connection: Connection,
}

impl Session {
    fn drive(&mut self) -> Result<(), EngineError> {
        let mut cx = Context::from_waker(&self.task_waker);
        let mut pipes = Pipes {
            inbound: &mut self.inbound,
            outbound: &mut self.outbound,
        };
        self.engine.drive(&mut pipes, &mut cx)
    }
}

/// A listening socket and the queue its accepted connections go to.
struct ListenerEntry {
    listener: TcpListener,
    config: ConnectionConfig,
    connections: Writer<Connection>,
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
