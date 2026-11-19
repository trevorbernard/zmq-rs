//! Socket monitoring, as libzmq's `zmq_socket_monitor`: typed events about
//! a socket's listeners and connections, delivered to any number of
//! [`Monitor`]s.

use std::fmt;
use std::io;
use std::num::NonZeroUsize;
use std::sync::{Arc, Mutex, MutexGuard, PoisonError};
use std::task::{Context, Poll};
use std::time::Duration;

use zmq_pipe::{Hwm, Reader, RecvTimeoutError, TryRecvError, Writer, pipe};
use zmtp::{CurveError, DecodeError, GreetingError, HandshakeError};

/// Events a monitor holds before dropping new ones, as libzmq's monitor
/// socket does at its default HWM.
const MONITOR_HWM: NonZeroUsize = NonZeroUsize::new(1000).unwrap();

/// Something that happened to one of a socket's listeners or connections.
#[derive(Debug, Clone, PartialEq, Eq)]
#[non_exhaustive]
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
#[non_exhaustive]
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

/// The protocol error that ended a handshake. It displays what the peer
/// got wrong; [`kind`](Self::kind) tells which part of the protocol.
#[derive(Debug, Clone, PartialEq, Eq)]
pub struct ProtocolError(ProtocolDetail);

#[derive(Debug, Clone, PartialEq, Eq)]
pub(crate) enum ProtocolDetail {
    Greeting(GreetingError),
    Handshake(HandshakeError),
    Decode(DecodeError),
    Curve(CurveError),
}

/// Which part of the protocol a [`ProtocolError`] is about.
#[derive(Debug, Clone, Copy, PartialEq, Eq, Hash)]
#[non_exhaustive]
pub enum ProtocolErrorKind {
    /// The greeting: its signature, version or mechanism.
    Greeting,
    /// The mechanism's handshake commands.
    Handshake,
    /// The framing of what the peer sent.
    Decode,
    /// CURVE's own rules.
    Curve,
}

impl ProtocolError {
    pub(crate) fn new(detail: ProtocolDetail) -> Self {
        Self(detail)
    }

    pub fn kind(&self) -> ProtocolErrorKind {
        match self.0 {
            ProtocolDetail::Greeting(_) => ProtocolErrorKind::Greeting,
            ProtocolDetail::Handshake(_) => ProtocolErrorKind::Handshake,
            ProtocolDetail::Decode(_) => ProtocolErrorKind::Decode,
            ProtocolDetail::Curve(_) => ProtocolErrorKind::Curve,
        }
    }

    fn detail(&self) -> &(dyn std::error::Error + 'static) {
        match &self.0 {
            ProtocolDetail::Greeting(err) => err,
            ProtocolDetail::Handshake(err) => err,
            ProtocolDetail::Decode(err) => err,
            ProtocolDetail::Curve(err) => err,
        }
    }
}

impl fmt::Display for ProtocolError {
    fn fmt(&self, f: &mut fmt::Formatter<'_>) -> fmt::Result {
        let part = match self.kind() {
            ProtocolErrorKind::Greeting => "greeting",
            ProtocolErrorKind::Handshake => "handshake",
            ProtocolErrorKind::Decode => "framing",
            ProtocolErrorKind::Curve => "CURVE",
        };
        write!(f, "{part}: {}", self.detail())
    }
}

impl std::error::Error for ProtocolError {
    fn source(&self) -> Option<&(dyn std::error::Error + 'static)> {
        Some(self.detail())
    }
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
///
/// It receives as a socket does: `None` when nothing is waiting or the
/// timeout ran out, and an error, [`MonitorStopped`], when nothing ever
/// will be.
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
    fn lock(&self) -> MutexGuard<'_, Vec<Writer<Event>>> {
        self.monitors.lock().unwrap_or_else(PoisonError::into_inner)
    }
}

/// The monitored socket and all its connections are gone and every event
/// is read, so a [`Monitor`] has nothing more to say, as libzmq's
/// `ZMQ_EVENT_MONITOR_STOPPED`. Drop the monitor.
#[derive(Debug, Clone, Copy, PartialEq, Eq)]
pub struct MonitorStopped;

impl fmt::Display for MonitorStopped {
    fn fmt(&self, f: &mut fmt::Formatter<'_>) -> fmt::Result {
        f.write_str("the monitored socket is gone")
    }
}

impl std::error::Error for MonitorStopped {}

impl Monitor {
    /// The next event, if one is waiting.
    pub fn try_recv(&mut self) -> Result<Option<Event>, MonitorStopped> {
        match self.events.try_recv() {
            Ok(event) => Ok(Some(event)),
            Err(TryRecvError::Empty) => Ok(None),
            Err(TryRecvError::Closed) => Err(MonitorStopped),
        }
    }

    /// The next event, waking `cx` for it if none is waiting.
    pub fn poll_recv(&mut self, cx: &mut Context<'_>) -> Poll<Result<Event, MonitorStopped>> {
        self.events
            .poll_recv(cx)
            .map(|event| event.ok_or(MonitorStopped))
    }

    /// The next event, blocking the thread, as a socket's `recv` does.
    pub fn recv(&mut self) -> Result<Event, MonitorStopped> {
        self.events.recv_blocking().ok_or(MonitorStopped)
    }

    /// The next event, waiting for one without blocking the thread, as a
    /// socket's `recv_async` does.
    pub async fn recv_async(&mut self) -> Result<Event, MonitorStopped> {
        self.events.recv().await.ok_or(MonitorStopped)
    }

    /// The next event, blocking the thread for at most `timeout`: `None` if
    /// it ran out.
    pub fn recv_timeout(&mut self, timeout: Duration) -> Result<Option<Event>, MonitorStopped> {
        match self.events.recv_timeout(timeout) {
            Ok(event) => Ok(Some(event)),
            Err(RecvTimeoutError::Timeout) => Ok(None),
            Err(RecvTimeoutError::Closed) => Err(MonitorStopped),
        }
    }
}

impl fmt::Debug for MonitorSink {
    fn fmt(&self, f: &mut fmt::Formatter<'_>) -> fmt::Result {
        f.debug_struct("MonitorSink")
            .field("monitors", &self.lock().len())
            .finish()
    }
}

#[cfg(test)]
mod tests {
    use super::*;

    #[test]
    fn try_recv_tells_an_empty_monitor_from_a_stopped_one() {
        let sink = MonitorSink::default();
        let mut monitor = sink.monitor();
        assert_eq!(monitor.try_recv(), Ok(None));
        sink.emit("tcp://127.0.0.1:1", EventKind::Closed);
        drop(sink);
        assert!(matches!(monitor.try_recv(), Ok(Some(_))));
        assert_eq!(monitor.try_recv(), Err(MonitorStopped));
    }

    #[test]
    fn recv_timeout_tells_a_quiet_monitor_from_a_stopped_one() {
        let sink = MonitorSink::default();
        let mut monitor = sink.monitor();
        assert_eq!(monitor.recv_timeout(Duration::ZERO), Ok(None));
        drop(sink);
        assert_eq!(monitor.recv_timeout(Duration::ZERO), Err(MonitorStopped));
    }

    #[test]
    fn each_way_of_waiting_gets_the_next_event_then_fails_once_stopped() {
        let sink = MonitorSink::default();
        let mut monitor = sink.monitor();
        for _ in 0..3 {
            sink.emit("tcp://127.0.0.1:1", EventKind::Closed);
        }
        drop(sink);
        let (_, waker) = crate::test_support::waker();
        let mut cx = Context::from_waker(&waker);
        assert!(matches!(monitor.poll_recv(&mut cx), Poll::Ready(Ok(_))));
        assert!(monitor.recv().is_ok());
        {
            let next = std::pin::pin!(monitor.recv_async());
            assert!(matches!(next.poll(&mut cx), Poll::Ready(Ok(_))));
        }
        assert_eq!(monitor.poll_recv(&mut cx), Poll::Ready(Err(MonitorStopped)));
        assert_eq!(monitor.recv(), Err(MonitorStopped));
        {
            let next = std::pin::pin!(monitor.recv_async());
            assert_eq!(next.poll(&mut cx), Poll::Ready(Err(MonitorStopped)));
        }
    }
}
