//! libzmq's draft socket types, behind the `draft` feature: their RFCs
//! are drafts, so their API may change.

use std::convert::Infallible;
use std::fmt;
use std::future;
use std::num::NonZeroU32;
use std::task::{Context as TaskContext, Poll};
use std::time::Duration;

use zmq_core::{FairQueue, LoadBalancer, ServerSendError, ServerTable};
use zmtp::{Frame, Group, Message};

use crate::socket::{Receives, Routing, SocketType, WaitsForRoom, noop, sealed};
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
impl Routing for zmq_core::Radio {}

impl Routing for zmq_core::Dish {
    /// Groups are told to a peer as soon as it is connected, as in libzmq.
    fn connected(&mut self, peers: &mut zmq_core::PeerList) {
        self.greet(peers);
    }
}

/// Sends single-part messages to a [`Group`], reaching every DISH peer
/// that joined it (RFC 48). Groups match exactly. Never blocks: a dish
/// whose queue is full misses the message, as in libzmq.
#[derive(Debug)]
pub enum Radio {}

/// Receives what its RADIO peers send to the groups it joined (RFC 48).
/// Joins nothing at first. Its joins are never dropped: its queues towards
/// peers are unbounded, whatever its send HWM.
#[derive(Debug)]
pub enum Dish {}

impl sealed::Sealed for Radio {}
impl SocketType for Radio {
    const KIND: zmtp::SocketType = zmtp::SocketType::Radio;
    type Routing = zmq_core::Radio;
    type Received = Infallible;
}

impl sealed::Sealed for Dish {}
impl SocketType for Dish {
    const KIND: zmtp::SocketType = zmtp::SocketType::Dish;
    type Routing = zmq_core::Dish;
    type Received = (Group, Frame);
}

impl Socket<Radio> {
    /// Sends `body` to every dish in `group` that has room. Never blocks.
    /// Returns how many dishes it was queued for: none if nobody has joined
    /// yet, as joins take a moment to arrive after connecting.
    pub fn send(&mut self, group: &Group, body: impl Into<Frame>) -> usize {
        let mut cx = noop();
        let (peers, radio) = self.parts_accepting(&mut cx);
        radio.send(peers, group, body.into())
    }
}

impl Socket<Dish> {
    /// Joins `group`. Returns false if it was already joined.
    pub fn join(&mut self, group: &Group) -> bool {
        let mut cx = noop();
        let (peers, dish) = self.parts_accepting(&mut cx);
        dish.join(peers, group)
    }

    /// Leaves `group`. Returns false if it was not joined. Messages already
    /// queued for the group are dropped.
    pub fn leave(&mut self, group: &Group) -> bool {
        let mut cx = noop();
        let (peers, dish) = self.parts_accepting(&mut cx);
        dish.leave(peers, group)
    }

    /// The next message and its group, if one is waiting.
    pub fn try_recv(&mut self) -> Option<(Group, Frame)> {
        self.try_recv_next()
    }

    /// The next message and its group, blocking until one arrives.
    pub fn recv(&mut self) -> (Group, Frame) {
        self.recv_next()
    }

    /// The next message and its group, blocking for at most `timeout`;
    /// `None` if none arrives in time.
    pub fn recv_timeout(&mut self, timeout: Duration) -> Option<(Group, Frame)> {
        self.recv_next_timeout(timeout)
    }

    /// The next message and its group, waiting until one arrives.
    pub async fn recv_async(&mut self) -> (Group, Frame) {
        self.recv_next_async().await
    }
}

impl sealed::Sealed for Client {}
impl SocketType for Client {
    const KIND: zmtp::SocketType = zmtp::SocketType::Client;
    type Routing = ClientRouting;
    type Received = Frame;
}

impl sealed::Sealed for Server {}
impl SocketType for Server {
    const KIND: zmtp::SocketType = zmtp::SocketType::Server;
    type Routing = ServerTable;
    type Received = (ClientId, Frame);
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
        self.try_send_message(Message::new(frame))
            .map_err(only_frame)
    }

    /// Sends `frame`, blocking while no server has room.
    pub fn send(&mut self, frame: impl Into<Frame>) {
        self.send_message(Message::new(frame));
    }

    /// Sends `frame`, blocking for at most `timeout` while no server has
    /// room. Hands it back if there is still no room.
    pub fn send_timeout(
        &mut self,
        frame: impl Into<Frame>,
        timeout: Duration,
    ) -> Result<(), Frame> {
        self.send_message_timeout(Message::new(frame), timeout)
            .map_err(only_frame)
    }

    /// Sends `frame`, waiting while no server has room.
    pub async fn send_async(&mut self, frame: impl Into<Frame>) {
        self.send_message_async(Message::new(frame)).await;
    }

    /// The next message, if one is waiting.
    pub fn try_recv(&mut self) -> Option<Frame> {
        self.try_recv_next()
    }

    /// The next message, blocking until one arrives.
    pub fn recv(&mut self) -> Frame {
        self.recv_next()
    }

    /// The next message, blocking for at most `timeout`; `None` if none
    /// arrives in time.
    pub fn recv_timeout(&mut self, timeout: Duration) -> Option<Frame> {
        self.recv_next_timeout(timeout)
    }

    /// The next message, waiting until one arrives.
    pub async fn recv_async(&mut self) -> Frame {
        self.recv_next_async().await
    }
}

/// Sends the message in `slot` to the next peer with room, putting it back
/// and returning `Pending` if none has room.
fn poll_load_balanced(
    peers: &mut zmq_core::PeerList,
    balancer: &mut LoadBalancer,
    slot: &mut Option<Message>,
    cx: &mut TaskContext<'_>,
) -> Poll<()> {
    let Some(message) = slot.take() else {
        return Poll::Ready(());
    };
    match balancer.poll_send(peers, message, cx) {
        Ok(_) => Poll::Ready(()),
        Err(message) => {
            *slot = Some(message);
            Poll::Pending
        }
    }
}

/// The next single-part message, fairly queued. Multipart ones are dropped,
/// as the draft RFCs require.
fn poll_single_part(
    peers: &mut zmq_core::PeerList,
    queue: &mut FairQueue,
    cx: &mut TaskContext<'_>,
) -> Poll<Frame> {
    loop {
        let Poll::Ready((_, message)) = queue.poll_recv(peers, cx) else {
            return Poll::Pending;
        };
        let (frame, rest) = message.split_first();
        if rest.is_empty() {
            return Poll::Ready(frame.clone());
        }
    }
}

/// Sends single-part messages round robin to its GATHER peers (RFC 49), as
/// PUSH does. Waits while no peer has room; never drops a message.
///
/// A SCATTER cannot receive, and a GATHER cannot send:
///
/// ```compile_fail,E0599
/// let context = zmq::Context::new().unwrap();
/// context.socket::<zmq::Scatter>().recv();
/// ```
///
/// ```compile_fail,E0599
/// let context = zmq::Context::new().unwrap();
/// context.socket::<zmq::Gather>().send("upstream");
/// ```
#[derive(Debug)]
pub enum Scatter {}

/// Receives single-part messages fairly from its SCATTER peers (RFC 49), as
/// PULL does. Multipart messages are dropped.
#[derive(Debug)]
pub enum Gather {}

impl sealed::Sealed for Scatter {}
impl SocketType for Scatter {
    const KIND: zmtp::SocketType = zmtp::SocketType::Scatter;
    type Routing = LoadBalancer;
    type Received = Infallible;
}

impl sealed::Sealed for Gather {}
impl SocketType for Gather {
    const KIND: zmtp::SocketType = zmtp::SocketType::Gather;
    type Routing = FairQueue;
    type Received = Frame;
}

impl Socket<Scatter> {
    /// Sends `frame` if a gather has room, otherwise hands it back.
    pub fn try_send(&mut self, frame: impl Into<Frame>) -> Result<(), Frame> {
        self.try_send_message(Message::new(frame))
            .map_err(only_frame)
    }

    /// Sends `frame`, blocking while no gather has room.
    pub fn send(&mut self, frame: impl Into<Frame>) {
        self.send_message(Message::new(frame));
    }

    /// Sends `frame`, blocking for at most `timeout` while no gather has
    /// room. Hands it back if there is still no room.
    pub fn send_timeout(
        &mut self,
        frame: impl Into<Frame>,
        timeout: Duration,
    ) -> Result<(), Frame> {
        self.send_message_timeout(Message::new(frame), timeout)
            .map_err(only_frame)
    }

    /// Sends `frame`, waiting while no gather has room.
    pub async fn send_async(&mut self, frame: impl Into<Frame>) {
        self.send_message_async(Message::new(frame)).await;
    }
}

impl Socket<Gather> {
    /// The next message, if one is waiting.
    pub fn try_recv(&mut self) -> Option<Frame> {
        self.try_recv_next()
    }

    /// The next message, blocking until one arrives.
    pub fn recv(&mut self) -> Frame {
        self.recv_next()
    }

    /// The next message, blocking for at most `timeout`; `None` if none
    /// arrives in time.
    pub fn recv_timeout(&mut self, timeout: Duration) -> Option<Frame> {
        self.recv_next_timeout(timeout)
    }

    /// The next message, waiting until one arrives.
    pub async fn recv_async(&mut self) -> Frame {
        self.recv_next_async().await
    }
}

impl Socket<Server> {
    /// The next message and the id of the client that sent it, if one is
    /// waiting.
    pub fn try_recv(&mut self) -> Option<(ClientId, Frame)> {
        self.try_recv_next()
    }

    /// The next message and its sender's id, blocking until one arrives.
    pub fn recv(&mut self) -> (ClientId, Frame) {
        self.recv_next()
    }

    /// The next message and its sender's id, blocking for at most
    /// `timeout`; `None` if none arrives in time.
    pub fn recv_timeout(&mut self, timeout: Duration) -> Option<(ClientId, Frame)> {
        self.recv_next_timeout(timeout)
    }

    /// The next message and its sender's id, waiting until one arrives.
    pub async fn recv_async(&mut self) -> (ClientId, Frame) {
        self.recv_next_async().await
    }

    /// Sends `frame` to the client `to` without waiting.
    pub fn try_send(
        &mut self,
        to: ClientId,
        frame: impl Into<Frame>,
    ) -> Result<(), ServerSendError> {
        try_send_routed(self, to.0, frame.into())
    }

    /// Sends `frame` to the client `to`, blocking while its queue is full.
    /// Fails if no client has that id, or it goes away meanwhile.
    pub fn send(&mut self, to: ClientId, frame: impl Into<Frame>) -> Result<(), NoSuchClient> {
        let mut slot = Some(frame.into());
        wait::block_on(|cx| poll_send_routed(self, to.0, &mut slot, cx)).map_err(NoSuchClient)
    }

    /// Sends `frame` to the client `to`, blocking for at most `timeout`
    /// while its queue is full. Fails at once if no client has that id, and
    /// with `Full` if there is still no room.
    pub fn send_timeout(
        &mut self,
        to: ClientId,
        frame: impl Into<Frame>,
        timeout: Duration,
    ) -> Result<(), ServerSendError> {
        send_routed_timeout(self, to.0, frame.into(), timeout)
    }

    /// Sends `frame` to the client `to`, waiting while its queue is full.
    /// Fails if no client has that id, or it goes away meanwhile.
    pub async fn send_async(
        &mut self,
        to: ClientId,
        frame: impl Into<Frame>,
    ) -> Result<(), NoSuchClient> {
        let mut slot = Some(frame.into());
        future::poll_fn(|cx| poll_send_routed(self, to.0, &mut slot, cx))
            .await
            .map_err(NoSuchClient)
    }

    /// Closes the connection to the client `to`. Returns whether there was
    /// one.
    pub fn disconnect_client(&mut self, to: ClientId) -> bool {
        disconnect_routed(self, to.0)
    }
}

/// Talks to any number of other PEER sockets, each addressed by the
/// [`PeerId`] this socket gave it (RFC 51), whether it connected or was
/// accepted. [`connect_peer`](Socket::connect_peer) returns the id at
/// once, so a message can be sent before the connection is up.
#[derive(Debug)]
pub enum Peer {}

/// The id a PEER gives each of its peers: non-zero, and unique among its
/// current peers.
#[derive(Debug, Clone, Copy, PartialEq, Eq, Hash, PartialOrd, Ord)]
pub struct PeerId(NonZeroU32);

/// A PEER could not send: no peer has that id, or it has gone. The frame
/// is handed back.
#[derive(Debug, Clone, PartialEq, Eq)]
pub struct NoSuchPeer(pub Frame);

/// Why a PEER could not send without waiting.
#[derive(Debug, Clone, PartialEq, Eq)]
pub enum PeerSendError {
    /// No peer has that id, or it has gone. The frame is handed back.
    NoSuchPeer(Frame),
    /// The peer's queue is full. The frame is handed back.
    Full(Frame),
}

impl sealed::Sealed for Peer {}
impl SocketType for Peer {
    const KIND: zmtp::SocketType = zmtp::SocketType::Peer;
    type Routing = ServerTable;
    type Received = (PeerId, Frame);
}

impl PeerId {
    /// The id `id`, unless it is zero, which is never a peer's.
    pub fn new(id: u32) -> Option<Self> {
        NonZeroU32::new(id).map(Self)
    }

    /// The id as a number.
    pub fn get(self) -> u32 {
        self.0.get()
    }
}

impl Socket<Peer> {
    /// Connects to the PEER at `endpoint` and returns its id. Messages to
    /// it queue until the connection is up.
    pub fn connect_peer(&mut self, endpoint: &str) -> Result<PeerId, crate::Error> {
        let key = self
            .connect_key(endpoint)?
            .expect("PEER never connects with ZMQ_IMMEDIATE");
        let mut cx = noop();
        let (peers, table) = self.parts_accepting(&mut cx);
        let id = table
            .id_of(peers, key)
            .expect("a peer just connected has an id");
        Ok(PeerId(id))
    }

    /// The next message and the id of the peer that sent it, if one is
    /// waiting.
    pub fn try_recv(&mut self) -> Option<(PeerId, Frame)> {
        self.try_recv_next()
    }

    /// The next message and its sender's id, blocking until one arrives.
    pub fn recv(&mut self) -> (PeerId, Frame) {
        self.recv_next()
    }

    /// The next message and its sender's id, blocking for at most
    /// `timeout`; `None` if none arrives in time.
    pub fn recv_timeout(&mut self, timeout: Duration) -> Option<(PeerId, Frame)> {
        self.recv_next_timeout(timeout)
    }

    /// The next message and its sender's id, waiting until one arrives.
    pub async fn recv_async(&mut self) -> (PeerId, Frame) {
        self.recv_next_async().await
    }

    /// Sends `frame` to the peer `to` without waiting.
    pub fn try_send(&mut self, to: PeerId, frame: impl Into<Frame>) -> Result<(), PeerSendError> {
        try_send_routed(self, to.0, frame.into()).map_err(|err| match err {
            ServerSendError::NoSuchClient(frame) => PeerSendError::NoSuchPeer(frame),
            ServerSendError::Full(frame) => PeerSendError::Full(frame),
        })
    }

    /// Sends `frame` to the peer `to`, blocking while its queue is full.
    /// Fails if no peer has that id, or it goes away meanwhile.
    pub fn send(&mut self, to: PeerId, frame: impl Into<Frame>) -> Result<(), NoSuchPeer> {
        let mut slot = Some(frame.into());
        wait::block_on(|cx| poll_send_routed(self, to.0, &mut slot, cx)).map_err(NoSuchPeer)
    }

    /// Sends `frame` to the peer `to`, blocking for at most `timeout` while
    /// its queue is full. Fails at once if no peer has that id, and with
    /// `Full` if there is still no room.
    pub fn send_timeout(
        &mut self,
        to: PeerId,
        frame: impl Into<Frame>,
        timeout: Duration,
    ) -> Result<(), PeerSendError> {
        send_routed_timeout(self, to.0, frame.into(), timeout).map_err(|err| match err {
            ServerSendError::NoSuchClient(frame) => PeerSendError::NoSuchPeer(frame),
            ServerSendError::Full(frame) => PeerSendError::Full(frame),
        })
    }

    /// Sends `frame` to the peer `to`, waiting while its queue is full.
    /// Fails if no peer has that id, or it goes away meanwhile.
    pub async fn send_async(
        &mut self,
        to: PeerId,
        frame: impl Into<Frame>,
    ) -> Result<(), NoSuchPeer> {
        let mut slot = Some(frame.into());
        future::poll_fn(|cx| poll_send_routed(self, to.0, &mut slot, cx))
            .await
            .map_err(NoSuchPeer)
    }

    /// Closes the connection to the peer `to`, as libzmq's
    /// `zmq_disconnect_peer`. Returns whether there was one.
    pub fn disconnect_peer(&mut self, to: PeerId) -> bool {
        disconnect_routed(self, to.0)
    }
}

// SERVER and PEER route the same way, by the ids their table gives peers.

fn poll_routed<T: SocketType<Routing = ServerTable>>(
    socket: &mut Socket<T>,
    cx: &mut TaskContext<'_>,
) -> Poll<(NonZeroU32, Frame)> {
    let (peers, table) = socket.parts_accepting(cx);
    table.poll_recv(peers, cx)
}

fn try_send_routed<T: SocketType<Routing = ServerTable>>(
    socket: &mut Socket<T>,
    to: NonZeroU32,
    frame: Frame,
) -> Result<(), ServerSendError> {
    let mut cx = noop();
    let (peers, table) = socket.parts_accepting(&mut cx);
    table.try_send(peers, to, frame, &mut cx)
}

/// Sends the frame in `slot` to `to`, waiting while it is full. Fails with
/// the frame if there is no such peer.
fn poll_send_routed<T: SocketType<Routing = ServerTable>>(
    socket: &mut Socket<T>,
    to: NonZeroU32,
    slot: &mut Option<Frame>,
    cx: &mut TaskContext<'_>,
) -> Poll<Result<(), Frame>> {
    let frame = slot.take().expect("polled again after completing");
    let (peers, table) = socket.parts_accepting(cx);
    match table.try_send(peers, to, frame, cx) {
        Ok(()) => Poll::Ready(Ok(())),
        Err(ServerSendError::NoSuchClient(frame)) => Poll::Ready(Err(frame)),
        Err(ServerSendError::Full(frame)) => {
            *slot = Some(frame);
            Poll::Pending
        }
    }
}

/// Sends `frame` to `to`, waiting at most `timeout` while it is full.
fn send_routed_timeout<T: SocketType<Routing = ServerTable>>(
    socket: &mut Socket<T>,
    to: NonZeroU32,
    frame: Frame,
    timeout: Duration,
) -> Result<(), ServerSendError> {
    let mut slot = Some(frame);
    match wait::block_on_until(wait::deadline(timeout), |cx| {
        poll_send_routed(socket, to, &mut slot, cx)
    }) {
        Some(Ok(())) => Ok(()),
        Some(Err(frame)) => Err(ServerSendError::NoSuchClient(frame)),
        None => Err(ServerSendError::Full(
            slot.take().expect("kept while pending"),
        )),
    }
}

fn disconnect_routed<T: SocketType<Routing = ServerTable>>(
    socket: &mut Socket<T>,
    to: NonZeroU32,
) -> bool {
    let mut cx = noop();
    let (peers, table) = socket.parts_accepting(&mut cx);
    table.disconnect(peers, to)
}

/// Talks to exactly one other CHANNEL socket, both ways, in single-part
/// messages (RFC 52), as PAIR does: the first peer is the one, and others
/// are turned away until it leaves. Waits while there is no peer or no
/// room; multipart messages received are dropped.
#[derive(Debug)]
pub enum Channel {}

impl sealed::Sealed for Channel {}
impl SocketType for Channel {
    const KIND: zmtp::SocketType = zmtp::SocketType::Channel;
    type Routing = zmq_core::Pair;
    type Received = Frame;
}

impl Socket<Channel> {
    /// Sends `frame` if there is a peer with room, otherwise hands it back.
    pub fn try_send(&mut self, frame: impl Into<Frame>) -> Result<(), Frame> {
        self.try_send_message(Message::new(frame))
            .map_err(only_frame)
    }

    /// Sends `frame`, blocking until there is a peer with room.
    pub fn send(&mut self, frame: impl Into<Frame>) {
        self.send_message(Message::new(frame));
    }

    /// Sends `frame`, blocking for at most `timeout` while there is no peer
    /// with room. Hands it back if there is still no room.
    pub fn send_timeout(
        &mut self,
        frame: impl Into<Frame>,
        timeout: Duration,
    ) -> Result<(), Frame> {
        self.send_message_timeout(Message::new(frame), timeout)
            .map_err(only_frame)
    }

    /// Sends `frame`, waiting until there is a peer with room.
    pub async fn send_async(&mut self, frame: impl Into<Frame>) {
        self.send_message_async(Message::new(frame)).await;
    }

    /// The next message, if one is waiting.
    pub fn try_recv(&mut self) -> Option<Frame> {
        self.try_recv_next()
    }

    /// The next message, blocking until one arrives.
    pub fn recv(&mut self) -> Frame {
        self.recv_next()
    }

    /// The next message, blocking for at most `timeout`; `None` if none
    /// arrives in time.
    pub fn recv_timeout(&mut self, timeout: Duration) -> Option<Frame> {
        self.recv_next_timeout(timeout)
    }

    /// The next message, waiting until one arrives.
    pub async fn recv_async(&mut self) -> Frame {
        self.recv_next_async().await
    }
}

/// The frame of a message a frame send handed back, as it was sent.
fn only_frame(message: Message) -> Frame {
    message.into_parts().0
}

impl fmt::Display for ClientId {
    fn fmt(&self, f: &mut fmt::Formatter<'_>) -> fmt::Result {
        self.0.fmt(f)
    }
}

impl fmt::Display for PeerId {
    fn fmt(&self, f: &mut fmt::Formatter<'_>) -> fmt::Result {
        self.0.fmt(f)
    }
}

impl fmt::Display for NoSuchPeer {
    fn fmt(&self, f: &mut fmt::Formatter<'_>) -> fmt::Result {
        f.write_str("no peer has that id")
    }
}

impl std::error::Error for NoSuchPeer {}

impl fmt::Display for NoSuchClient {
    fn fmt(&self, f: &mut fmt::Formatter<'_>) -> fmt::Result {
        f.write_str("no client has that id")
    }
}

impl std::error::Error for NoSuchClient {}

impl Receives for Dish {
    fn poll_next(socket: &mut Socket<Self>, cx: &mut TaskContext<'_>) -> Poll<(Group, Frame)> {
        let (peers, dish) = socket.parts_accepting(cx);
        dish.poll_recv(peers, cx)
    }
}

impl Receives for Client {
    fn poll_next(socket: &mut Socket<Self>, cx: &mut TaskContext<'_>) -> Poll<Frame> {
        let (peers, routing) = socket.parts_accepting(cx);
        poll_single_part(peers, &mut routing.recv, cx)
    }
}

impl Receives for Server {
    fn poll_next(socket: &mut Socket<Self>, cx: &mut TaskContext<'_>) -> Poll<(ClientId, Frame)> {
        poll_routed(socket, cx).map(|(id, frame)| (ClientId(id), frame))
    }
}

impl Receives for Gather {
    fn poll_next(socket: &mut Socket<Self>, cx: &mut TaskContext<'_>) -> Poll<Frame> {
        let (peers, queue) = socket.parts_accepting(cx);
        poll_single_part(peers, queue, cx)
    }
}

impl Receives for Peer {
    fn poll_next(socket: &mut Socket<Self>, cx: &mut TaskContext<'_>) -> Poll<(PeerId, Frame)> {
        poll_routed(socket, cx).map(|(id, frame)| (PeerId(id), frame))
    }
}

impl Receives for Channel {
    fn poll_next(socket: &mut Socket<Self>, cx: &mut TaskContext<'_>) -> Poll<Frame> {
        let (peers, pair) = socket.parts_accepting(cx);
        loop {
            let Poll::Ready(message) = pair.poll_recv(peers, cx) else {
                return Poll::Pending;
            };
            let (frame, rest) = message.split_first();
            if rest.is_empty() {
                return Poll::Ready(frame.clone());
            }
        }
    }
}

impl WaitsForRoom for Client {
    fn poll_room(socket: &mut Socket<Self>, cx: &mut TaskContext<'_>) -> Poll<()> {
        let (peers, routing) = socket.parts_accepting(cx);
        routing.send.poll_ready(peers, cx)
    }

    fn poll_send(
        socket: &mut Socket<Self>,
        slot: &mut Option<Message>,
        cx: &mut TaskContext<'_>,
    ) -> Poll<()> {
        let (peers, routing) = socket.parts_accepting(cx);
        poll_load_balanced(peers, &mut routing.send, slot, cx)
    }
}

impl WaitsForRoom for Scatter {
    fn poll_room(socket: &mut Socket<Self>, cx: &mut TaskContext<'_>) -> Poll<()> {
        // As `poll_send`: SCATTER discards what peers send and drops those
        // gone.
        socket.prune_send_only();
        let (peers, balancer) = socket.parts_accepting(cx);
        balancer.poll_ready(peers, cx)
    }

    fn poll_send(
        socket: &mut Socket<Self>,
        slot: &mut Option<Message>,
        cx: &mut TaskContext<'_>,
    ) -> Poll<()> {
        // SCATTER never receives: discard anything peers send, and drop
        // peers that are gone.
        socket.prune_send_only();
        let (peers, balancer) = socket.parts_accepting(cx);
        poll_load_balanced(peers, balancer, slot, cx)
    }
}

impl WaitsForRoom for Channel {
    fn poll_room(socket: &mut Socket<Self>, cx: &mut TaskContext<'_>) -> Poll<()> {
        let (peers, pair) = socket.parts_accepting(cx);
        pair.poll_ready(peers, cx)
    }

    fn poll_send(
        socket: &mut Socket<Self>,
        slot: &mut Option<Message>,
        cx: &mut TaskContext<'_>,
    ) -> Poll<()> {
        let Some(message) = slot.take() else {
            return Poll::Ready(());
        };
        let (peers, pair) = socket.parts_accepting(cx);
        match pair.poll_send(peers, message, cx) {
            Ok(()) => Poll::Ready(()),
            Err(message) => {
                *slot = Some(message);
                Poll::Pending
            }
        }
    }
}
