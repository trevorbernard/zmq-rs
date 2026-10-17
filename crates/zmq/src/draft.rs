//! libzmq's draft socket types, behind the `draft` feature: their RFCs
//! are drafts, so their API may change.

use std::fmt;
use std::future::poll_fn;
use std::num::NonZeroU32;
use std::task::{Context as TaskContext, Poll, Waker};

use zmq_core::{FairQueue, LoadBalancer, ServerSendError, ServerTable};
use zmtp::{Frame, Message};

use crate::socket::{Routing, SocketType, sealed};
use crate::{Socket, wait};

/// Sends single-part messages round robin to its SERVER peers, and
/// receives from them fairly (RFC 41). Waits while no peer has room.
///
/// CLIENT and SERVER carry single frames, so a multipart message cannot be
/// sent:
///
/// ```compile_fail,E0277
/// let context = zmq::Context::new().unwrap();
/// let mut client = context.socket::<zmq::Client>();
/// let message = zmq::Message::try_from(vec![zmq::Frame::from("a"), zmq::Frame::from("b")]);
/// client.send(message.unwrap());
/// ```
#[derive(Debug)]
pub enum Client {}

/// Receives single-part messages from its CLIENT peers, each tagged with
/// the [`ClientId`] the server gave that client, and answers a client by
/// its id (RFC 41).
#[derive(Debug)]
pub enum Server {}

/// The id a SERVER gives each CLIENT peer: non-zero, and unique among the
/// server's current clients. libzmq's routing id for SERVER.
#[derive(Debug, Clone, Copy, PartialEq, Eq, Hash, PartialOrd, Ord)]
pub struct ClientId(NonZeroU32);

/// A SERVER could not send to a client: no client has that id, or it has
/// gone. The frame is handed back.
#[derive(Debug, Clone, PartialEq, Eq)]
pub struct NoSuchClient(pub Frame);

/// CLIENT's routing: load-balanced sends, fair-queued receives.
#[doc(hidden)]
#[derive(Debug, Default)]
pub struct ClientRouting {
    send: LoadBalancer,
    recv: FairQueue,
}

impl Routing for ClientRouting {}
impl Routing for ServerTable {}

impl sealed::Sealed for Client {}
impl SocketType for Client {
    const KIND: zmtp::SocketType = zmtp::SocketType::Client;
    type Routing = ClientRouting;
}

impl sealed::Sealed for Server {}
impl SocketType for Server {
    const KIND: zmtp::SocketType = zmtp::SocketType::Server;
    type Routing = ServerTable;
}

impl ClientId {
    /// The id `id`, unless it is zero, which is never a client's.
    pub fn new(id: u32) -> Option<Self> {
        NonZeroU32::new(id).map(Self)
    }

    /// The id as a number.
    pub fn get(self) -> u32 {
        self.0.get()
    }
}

impl Socket<Client> {
    /// Sends `frame` if a server has room, otherwise hands it back.
    pub fn try_send(&mut self, frame: impl Into<Frame>) -> Result<(), Frame> {
        let mut slot = Some(Message::new(frame.into()));
        let _ = self.poll_send_frame(&mut slot, &mut TaskContext::from_waker(Waker::noop()));
        slot.map_or(Ok(()), |message| Err(only_frame(message)))
    }

    /// Sends `frame`, blocking while no server has room.
    pub fn send(&mut self, frame: impl Into<Frame>) {
        let mut slot = Some(Message::new(frame.into()));
        wait::block_on(|cx| self.poll_send_frame(&mut slot, cx));
    }

    /// Sends `frame`, waiting while no server has room.
    pub async fn send_async(&mut self, frame: impl Into<Frame>) {
        let mut slot = Some(Message::new(frame.into()));
        poll_fn(|cx| self.poll_send_frame(&mut slot, cx)).await;
    }

    /// The next message, if one is waiting.
    pub fn try_recv(&mut self) -> Option<Frame> {
        match self.poll_recv_frame(&mut TaskContext::from_waker(Waker::noop())) {
            Poll::Ready(frame) => Some(frame),
            Poll::Pending => None,
        }
    }

    /// The next message, blocking until one arrives.
    pub fn recv(&mut self) -> Frame {
        wait::block_on(|cx| self.poll_recv_frame(cx))
    }

    /// The next message, waiting until one arrives.
    pub async fn recv_async(&mut self) -> Frame {
        poll_fn(|cx| self.poll_recv_frame(cx)).await
    }

    fn poll_send_frame(
        &mut self,
        slot: &mut Option<Message>,
        cx: &mut TaskContext<'_>,
    ) -> Poll<()> {
        let Some(message) = slot.take() else {
            return Poll::Ready(());
        };
        let (peers, routing) = self.parts_accepting(cx);
        match routing.send.poll_send(peers, message, cx) {
            Ok(_) => Poll::Ready(()),
            Err(message) => {
                *slot = Some(message);
                Poll::Pending
            }
        }
    }

    /// The next single-part message. Multipart ones are dropped, as RFC 41
    /// requires.
    fn poll_recv_frame(&mut self, cx: &mut TaskContext<'_>) -> Poll<Frame> {
        let (peers, routing) = self.parts_accepting(cx);
        loop {
            let Poll::Ready((_, message)) = routing.recv.poll_recv(peers, cx) else {
                return Poll::Pending;
            };
            let (frame, rest) = message.split_first();
            if rest.is_empty() {
                return Poll::Ready(frame.clone());
            }
        }
    }
}

impl Socket<Server> {
    /// The next message and the id of the client that sent it, if one is
    /// waiting.
    pub fn try_recv(&mut self) -> Option<(ClientId, Frame)> {
        match self.poll_recv_frame(&mut TaskContext::from_waker(Waker::noop())) {
            Poll::Ready(received) => Some(received),
            Poll::Pending => None,
        }
    }

    /// The next message and its sender's id, blocking until one arrives.
    pub fn recv(&mut self) -> (ClientId, Frame) {
        wait::block_on(|cx| self.poll_recv_frame(cx))
    }

    /// The next message and its sender's id, waiting until one arrives.
    pub async fn recv_async(&mut self) -> (ClientId, Frame) {
        poll_fn(|cx| self.poll_recv_frame(cx)).await
    }

    /// Sends `frame` to the client `to` without waiting.
    pub fn try_send(
        &mut self,
        to: ClientId,
        frame: impl Into<Frame>,
    ) -> Result<(), ServerSendError> {
        let mut cx = TaskContext::from_waker(Waker::noop());
        let (peers, table) = self.parts_accepting(&mut cx);
        table.try_send(peers, to.0, frame.into(), &mut cx)
    }

    /// Sends `frame` to the client `to`, blocking while its queue is full.
    /// Fails if no client has that id, or it goes away meanwhile.
    pub fn send(&mut self, to: ClientId, frame: impl Into<Frame>) -> Result<(), NoSuchClient> {
        let mut slot = Some(frame.into());
        wait::block_on(|cx| self.poll_send_to(to, &mut slot, cx))
    }

    /// Sends `frame` to the client `to`, waiting while its queue is full.
    /// Fails if no client has that id, or it goes away meanwhile.
    pub async fn send_async(
        &mut self,
        to: ClientId,
        frame: impl Into<Frame>,
    ) -> Result<(), NoSuchClient> {
        let mut slot = Some(frame.into());
        poll_fn(|cx| self.poll_send_to(to, &mut slot, cx)).await
    }

    /// Closes the connection to the client `to`. Returns whether there was
    /// one.
    pub fn disconnect(&mut self, to: ClientId) -> bool {
        let mut cx = TaskContext::from_waker(Waker::noop());
        let (peers, table) = self.parts_accepting(&mut cx);
        table.disconnect(peers, to.0)
    }

    fn poll_send_to(
        &mut self,
        to: ClientId,
        slot: &mut Option<Frame>,
        cx: &mut TaskContext<'_>,
    ) -> Poll<Result<(), NoSuchClient>> {
        let frame = slot.take().expect("polled again after completing");
        let (peers, table) = self.parts_accepting(cx);
        match table.try_send(peers, to.0, frame, cx) {
            Ok(()) => Poll::Ready(Ok(())),
            Err(ServerSendError::NoSuchClient(frame)) => Poll::Ready(Err(NoSuchClient(frame))),
            Err(ServerSendError::Full(frame)) => {
                *slot = Some(frame);
                Poll::Pending
            }
        }
    }

    fn poll_recv_frame(&mut self, cx: &mut TaskContext<'_>) -> Poll<(ClientId, Frame)> {
        let (peers, table) = self.parts_accepting(cx);
        table
            .poll_recv(peers, cx)
            .map(|(id, frame)| (ClientId(id), frame))
    }
}

fn only_frame(message: Message) -> Frame {
    message.into_iter().next().expect("a message has a frame")
}

impl fmt::Display for ClientId {
    fn fmt(&self, f: &mut fmt::Formatter<'_>) -> fmt::Result {
        self.0.fmt(f)
    }
}

impl fmt::Display for NoSuchClient {
    fn fmt(&self, f: &mut fmt::Formatter<'_>) -> fmt::Result {
        f.write_str("no client has that id")
    }
}

impl std::error::Error for NoSuchClient {}
