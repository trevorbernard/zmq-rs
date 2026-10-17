//! Typed sockets: each type has only the operations its pattern allows.

use std::fmt;
use std::future::poll_fn;
use std::marker::PhantomData;
use std::net::SocketAddr;
use std::task::{Context as TaskContext, Poll, Waker};

use zmq_core::{ConnectionConfig, FairQueue, LoadBalancer, Peers};
use zmtp::Message;

use crate::{Error, endpoint, wait};

mod sealed {
    pub trait Sealed {}
}

/// A ZeroMQ socket type. Implemented by [`Push`], [`Pull`] and the other
/// markers; cannot be implemented outside this crate.
pub trait SocketType: sealed::Sealed + 'static {
    /// The type announced in the handshake.
    #[doc(hidden)]
    const KIND: zmtp::SocketType;
    /// How the socket routes messages among its peers.
    #[doc(hidden)]
    type Routing: Default + fmt::Debug + Send;
}

/// Sends messages round robin to its PULL peers (RFC 30). Waits while no
/// peer has room; never drops a message.
#[derive(Debug)]
pub enum Push {}

/// Receives messages fairly from its PUSH peers (RFC 30).
#[derive(Debug)]
pub enum Pull {}

impl sealed::Sealed for Push {}
impl SocketType for Push {
    const KIND: zmtp::SocketType = zmtp::SocketType::Push;
    type Routing = LoadBalancer;
}

impl sealed::Sealed for Pull {}
impl SocketType for Pull {
    const KIND: zmtp::SocketType = zmtp::SocketType::Pull;
    type Routing = FairQueue;
}

/// A socket of type `T`. What it can do depends on `T`: a
/// `Socket<Push>` can only send and a `Socket<Pull>` can only receive.
pub struct Socket<T: SocketType> {
    peers: Peers,
    routing: T::Routing,
    _type: PhantomData<T>,
}

impl<T: SocketType> Socket<T> {
    pub(crate) fn new(context: zmq_core::Context) -> Self {
        Self {
            peers: Peers::new(context, ConnectionConfig::new(T::KIND)),
            routing: T::Routing::default(),
            _type: PhantomData,
        }
    }

    /// Binds `endpoint`, such as `tcp://*:5555` or `tcp://127.0.0.1:*`, and
    /// accepts peers on it. Returns the bound address, with the actual port
    /// when binding to `*`.
    pub fn bind(&mut self, endpoint: &str) -> Result<SocketAddr, Error> {
        let addr = endpoint::bind_addr(endpoint)?;
        self.peers.bind(addr).map_err(Error::Io)
    }

    /// Connects to `endpoint`, such as `tcp://localhost:5555`. Returns at
    /// once; messages queue until the connection is up, and it reconnects if
    /// the connection breaks.
    pub fn connect(&mut self, endpoint: &str) -> Result<(), Error> {
        let addr = endpoint::connect_addr(endpoint)?;
        self.peers.connect(addr);
        Ok(())
    }
}

impl Socket<Push> {
    /// Sends `message` if a peer has room, otherwise hands it back.
    pub fn try_send(&mut self, message: impl Into<Message>) -> Result<(), Message> {
        let mut cx = TaskContext::from_waker(Waker::noop());
        self.poll_send(message.into(), &mut cx)
    }

    /// Sends `message`, blocking while no peer has room.
    pub fn send(&mut self, message: impl Into<Message>) {
        let mut slot = Some(message.into());
        wait::block_on(|cx| self.poll_send_slot(&mut slot, cx));
    }

    /// Sends `message`, waiting while no peer has room.
    pub async fn send_async(&mut self, message: impl Into<Message>) {
        let mut slot = Some(message.into());
        poll_fn(|cx| self.poll_send_slot(&mut slot, cx)).await;
    }

    /// Sends the message in `slot`, putting it back if no peer has room.
    fn poll_send_slot(&mut self, slot: &mut Option<Message>, cx: &mut TaskContext<'_>) -> Poll<()> {
        let Some(message) = slot.take() else {
            return Poll::Ready(());
        };
        match self.poll_send(message, cx) {
            Ok(()) => Poll::Ready(()),
            Err(message) => {
                *slot = Some(message);
                Poll::Pending
            }
        }
    }

    fn poll_send(&mut self, message: Message, cx: &mut TaskContext<'_>) -> Result<(), Message> {
        self.peers.accept(cx);
        self.peers.prune_send_only();
        self.routing
            .poll_send(self.peers.connections_mut(), message, cx)
    }
}

impl Socket<Pull> {
    /// The next message, if one is waiting.
    pub fn try_recv(&mut self) -> Option<Message> {
        let mut cx = TaskContext::from_waker(Waker::noop());
        match self.poll_recv(&mut cx) {
            Poll::Ready(message) => Some(message),
            Poll::Pending => None,
        }
    }

    /// The next message, blocking until one arrives.
    pub fn recv(&mut self) -> Message {
        wait::block_on(|cx| self.poll_recv(cx))
    }

    /// The next message, waiting until one arrives.
    pub async fn recv_async(&mut self) -> Message {
        poll_fn(|cx| self.poll_recv(cx)).await
    }

    fn poll_recv(&mut self, cx: &mut TaskContext<'_>) -> Poll<Message> {
        self.peers.accept(cx);
        self.routing.poll_recv(self.peers.connections_mut(), cx)
    }
}

impl<T: SocketType> fmt::Debug for Socket<T> {
    fn fmt(&self, f: &mut fmt::Formatter<'_>) -> fmt::Result {
        f.debug_struct("Socket")
            .field("type", &T::KIND)
            .field("peers", &self.peers.len())
            .field("routing", &self.routing)
            .finish()
    }
}
