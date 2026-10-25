//! Socket monitoring, as libzmq's `zmq_socket_monitor`: typed events about
//! a socket's listeners and connections, delivered to any number of
//! [`Monitor`]s.

use std::fmt;
use std::io;
use std::num::NonZeroUsize;
use std::sync::{Arc, Mutex, PoisonError};
use std::task::{Context, Poll};
use std::time::Duration;

use zmq_pipe::{Hwm, Reader, RecvTimeoutError, Writer, pipe};
use zmtp::{CurveError, DecodeError, GreetingError, HandshakeError};

/// Events a monitor holds before dropping new ones, as libzmq's monitor
/// socket does at its default HWM.
const MONITOR_HWM: NonZeroUsize = NonZeroUsize::new(1000).unwrap();

/// Something that happened to one of a socket's listeners or connections.
#[derive(Debug, Clone, PartialEq, Eq)]
pub struct Event {
    /// The endpoint it concerns, as an endpoint string: what the socket
    /// connected to, or the bound endpoint for listeners and the
    /// connections they accepted.
    pub endpoint: String,
    pub kind: EventKind,
}

/// What happened. `inproc` connections, which have no listeners, streams
/// or handshakes, report nothing.
#[derive(Debug, Clone, PartialEq, Eq)]
#[non_exhaustive]
pub enum EventKind {
    /// A listener is bound and accepting.
    Listening,
    /// Binding failed.
    BindFailed(io::ErrorKind),
    /// A listener accepted a connection; its handshake follows.
    Accepted,
    /// A listener failed to accept a connection.
    AcceptFailed(io::ErrorKind),
    /// A listener closed.
    Closed,
    /// A connection the socket made is up; its handshake follows.
    Connected,
    /// A connection attempt failed or a connection broke, and the next
    /// attempt follows after `interval`.
    ConnectRetried { interval: Duration },
    /// The handshake completed: messages flow.
    HandshakeSucceeded,
    /// The handshake failed. `Disconnected` follows.
    HandshakeFailed(HandshakeFailure),
    /// A connection that was up has ended.
    Disconnected,
}

/// Why a handshake failed.
#[derive(Debug, Clone, PartialEq, Eq)]
pub enum HandshakeFailure {
    /// Authentication refused the peer, with ZAP status 300 (try later),
    /// 400 (denied) or 500 (internal error). A client learns the status
    /// from the server's ERROR.
    Authentication { status: u16 },
    /// The peer broke the protocol, or the mechanism's rules.
    Protocol(ProtocolError),
    /// The connection broke, or the handshake took too long.
    NoDetail,
}

/// The protocol error that ended a handshake.
#[derive(Debug, Clone, PartialEq, Eq)]
pub enum ProtocolError {
    Greeting(GreetingError),
    Handshake(HandshakeError),
    Decode(DecodeError),
    Curve(CurveError),
}

/// Where a socket's events go: shared by the socket, its listeners and its
/// sessions. Cloning shares it.
#[derive(Clone, Default)]
pub struct MonitorSink {
    monitors: Arc<Mutex<Vec<Writer<Event>>>>,
}

/// Receives a socket's events from the moment it is made. Each monitor
/// hears every event; one that falls 1000 events behind misses new ones
/// until it catches up, as libzmq's does.
#[derive(Debug)]
pub struct Monitor {
    events: Reader<Event>,
}

impl MonitorSink {
    /// A new monitor of everything reported here from now on.
    pub fn monitor(&self) -> Monitor {
        let (writer, events) = pipe(Hwm::Messages(MONITOR_HWM));
        self.lock().push(writer);
        Monitor { events }
    }

    /// Reports an event to every monitor, forgetting monitors that are
    /// gone. Costs a lock and nothing more while nobody monitors.
    pub(crate) fn emit(&self, endpoint: &str, kind: EventKind) {
        let mut monitors = self.lock();
        if monitors.is_empty() {
            return;
        }
        monitors.retain(|monitor| !monitor.is_closed());
        let event = Event {
            endpoint: endpoint.to_owned(),
            kind,
        };
        for monitor in monitors.iter_mut() {
            let _ = monitor.try_send(event.clone());
        }
    }

    /// Ignores poisoning: a panic mid-`retain` or mid-send can at worst
    /// leave a closed monitor in the list, which the next `emit` drops.
    fn lock(&self) -> std::sync::MutexGuard<'_, Vec<Writer<Event>>> {
        self.monitors.lock().unwrap_or_else(PoisonError::into_inner)
    }
}

impl Monitor {
    /// The next event, if one is waiting.
    pub fn try_recv(&mut self) -> Option<Event> {
        self.events.try_recv().ok()
    }

    /// The next event, or `None` once the socket and all its connections
    /// are gone.
    pub fn poll_recv(&mut self, cx: &mut Context<'_>) -> Poll<Option<Event>> {
        self.events.poll_recv(cx)
    }

    /// The next event, waiting for one. `None` once the socket and all its
    /// connections are gone.
    pub async fn recv(&mut self) -> Option<Event> {
        self.events.recv().await
    }

    /// The next event, blocking the thread. `None` once the socket and all
    /// its connections are gone.
    pub fn recv_blocking(&mut self) -> Option<Event> {
        self.events.recv_blocking()
    }

    /// The next event, blocking the thread for at most `timeout`. Fails
    /// with `Disconnected` once the socket and all its connections are gone.
    pub fn recv_timeout(&mut self, timeout: Duration) -> Result<Event, RecvTimeoutError> {
        self.events.recv_timeout(timeout)
    }
}

impl fmt::Debug for MonitorSink {
    fn fmt(&self, f: &mut fmt::Formatter<'_>) -> fmt::Result {
        f.debug_struct("MonitorSink")
            .field("monitors", &self.lock().len())
            .finish()
    }
}
