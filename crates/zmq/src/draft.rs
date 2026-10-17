//! libzmq's draft socket types, behind the `draft` feature: their RFCs
//! are drafts, so their API may change.

use std::fmt;
use std::future::poll_fn;
use std::num::NonZeroU32;
use std::task::{Context as TaskContext, Poll, Waker};

use zmq_core::{FairQueue, LoadBalancer, ServerSendError, ServerTable};
use zmtp::{Frame, Group, Message};

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
}

impl sealed::Sealed for Dish {}
impl SocketType for Dish {
    const KIND: zmtp::SocketType = zmtp::SocketType::Dish;
    type Routing = zmq_core::Dish;
}

impl Socket<Radio> {
    /// Sends `body` to every dish in `group` that has room. Never blocks.
    /// Returns how many dishes it was queued for: none if nobody has joined
    /// yet, as joins take a moment to arrive after connecting.
    pub fn send(&mut self, group: &Group, body: impl Into<Frame>) -> usize {
        let mut cx = TaskContext::from_waker(Waker::noop());
        let (peers, radio) = self.parts_accepting(&mut cx);
        radio.send(peers, group, body.into())
    }
}

impl Socket<Dish> {
    /// Joins `group`. Returns false if it was already joined.
    pub fn join(&mut self, group: &Group) -> bool {
        let mut cx = TaskContext::from_waker(Waker::noop());
        let (peers, dish) = self.parts_accepting(&mut cx);
        dish.join(peers, group)
    }

    /// Leaves `group`. Returns false if it was not joined. Messages already
    /// queued for the group are dropped.
    pub fn leave(&mut self, group: &Group) -> bool {
        let mut cx = TaskContext::from_waker(Waker::noop());
        let (peers, dish) = self.parts_accepting(&mut cx);
        dish.leave(peers, group)
    }

    /// The next message and its group, if one is waiting.
    pub fn try_recv(&mut self) -> Option<(Group, Frame)> {
        match self.poll_recv_group(&mut TaskContext::from_waker(Waker::noop())) {
            Poll::Ready(received) => Some(received),
            Poll::Pending => None,
        }
    }

    /// The next message and its group, blocking until one arrives.
    pub fn recv(&mut self) -> (Group, Frame) {
        wait::block_on(|cx| self.poll_recv_group(cx))
    }

    /// The next message and its group, waiting until one arrives.
    pub async fn recv_async(&mut self) -> (Group, Frame) {
        poll_fn(|cx| self.poll_recv_group(cx)).await
    }

    fn poll_recv_group(&mut self, cx: &mut TaskContext<'_>) -> Poll<(Group, Frame)> {
        let (peers, dish) = self.parts_accepting(cx);
        dish.poll_recv(peers, cx)
    }
}

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
        let (peers, routing) = self.parts_accepting(cx);
        poll_load_balanced(peers, &mut routing.send, slot, cx)
    }

    fn poll_recv_frame(&mut self, cx: &mut TaskContext<'_>) -> Poll<Frame> {
        let (peers, routing) = self.parts_accepting(cx);
        poll_single_part(peers, &mut routing.recv, cx)
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
}

impl sealed::Sealed for Gather {}
impl SocketType for Gather {
    const KIND: zmtp::SocketType = zmtp::SocketType::Gather;
    type Routing = FairQueue;
}

impl Socket<Scatter> {
    /// Sends `frame` if a gather has room, otherwise hands it back.
    pub fn try_send(&mut self, frame: impl Into<Frame>) -> Result<(), Frame> {
        let mut slot = Some(Message::new(frame.into()));
        let _ = self.poll_scatter(&mut slot, &mut TaskContext::from_waker(Waker::noop()));
        slot.map_or(Ok(()), |message| Err(only_frame(message)))
    }

    /// Sends `frame`, blocking while no gather has room.
    pub fn send(&mut self, frame: impl Into<Frame>) {
        let mut slot = Some(Message::new(frame.into()));
        wait::block_on(|cx| self.poll_scatter(&mut slot, cx));
    }

    /// Sends `frame`, waiting while no gather has room.
    pub async fn send_async(&mut self, frame: impl Into<Frame>) {
        let mut slot = Some(Message::new(frame.into()));
        poll_fn(|cx| self.poll_scatter(&mut slot, cx)).await;
    }

    fn poll_scatter(&mut self, slot: &mut Option<Message>, cx: &mut TaskContext<'_>) -> Poll<()> {
        // SCATTER never receives: discard anything peers send, and drop
        // peers that are gone.
        self.prune_send_only();
        let (peers, balancer) = self.parts_accepting(cx);
        poll_load_balanced(peers, balancer, slot, cx)
    }
}

impl Socket<Gather> {
    /// The next message, if one is waiting.
    pub fn try_recv(&mut self) -> Option<Frame> {
        match self.poll_gather(&mut TaskContext::from_waker(Waker::noop())) {
            Poll::Ready(frame) => Some(frame),
            Poll::Pending => None,
        }
    }

    /// The next message, blocking until one arrives.
    pub fn recv(&mut self) -> Frame {
        wait::block_on(|cx| self.poll_gather(cx))
    }

    /// The next message, waiting until one arrives.
    pub async fn recv_async(&mut self) -> Frame {
        poll_fn(|cx| self.poll_gather(cx)).await
    }

    fn poll_gather(&mut self, cx: &mut TaskContext<'_>) -> Poll<Frame> {
        let (peers, queue) = self.parts_accepting(cx);
        poll_single_part(peers, queue, cx)
    }
}

impl Socket<Server> {
    /// The next message and the id of the client that sent it, if one is
    /// waiting.
    pub fn try_recv(&mut self) -> Option<(ClientId, Frame)> {
        try_recv_routed(self).map(|(id, frame)| (ClientId(id), frame))
    }

    /// The next message and its sender's id, blocking until one arrives.
    pub fn recv(&mut self) -> (ClientId, Frame) {
        let (id, frame) = wait::block_on(|cx| poll_recv_routed(self, cx));
        (ClientId(id), frame)
    }

    /// The next message and its sender's id, waiting until one arrives.
    pub async fn recv_async(&mut self) -> (ClientId, Frame) {
        let (id, frame) = poll_fn(|cx| poll_recv_routed(self, cx)).await;
        (ClientId(id), frame)
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

    /// Sends `frame` to the client `to`, waiting while its queue is full.
    /// Fails if no client has that id, or it goes away meanwhile.
    pub async fn send_async(
        &mut self,
        to: ClientId,
        frame: impl Into<Frame>,
    ) -> Result<(), NoSuchClient> {
        let mut slot = Some(frame.into());
        poll_fn(|cx| poll_send_routed(self, to.0, &mut slot, cx))
            .await
            .map_err(NoSuchClient)
    }

    /// Closes the connection to the client `to`. Returns whether there was
    /// one.
    pub fn disconnect(&mut self, to: ClientId) -> bool {
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
        let key = self.connect_key(endpoint)?;
        let mut cx = TaskContext::from_waker(Waker::noop());
        let (peers, table) = self.parts_accepting(&mut cx);
        let id = table
            .id_of(peers, key)
            .expect("a peer just connected has an id");
        Ok(PeerId(id))
    }

    /// The next message and the id of the peer that sent it, if one is
    /// waiting.
    pub fn try_recv(&mut self) -> Option<(PeerId, Frame)> {
        try_recv_routed(self).map(|(id, frame)| (PeerId(id), frame))
    }

    /// The next message and its sender's id, blocking until one arrives.
    pub fn recv(&mut self) -> (PeerId, Frame) {
        let (id, frame) = wait::block_on(|cx| poll_recv_routed(self, cx));
        (PeerId(id), frame)
    }

    /// The next message and its sender's id, waiting until one arrives.
    pub async fn recv_async(&mut self) -> (PeerId, Frame) {
        let (id, frame) = poll_fn(|cx| poll_recv_routed(self, cx)).await;
        (PeerId(id), frame)
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

    /// Sends `frame` to the peer `to`, waiting while its queue is full.
    /// Fails if no peer has that id, or it goes away meanwhile.
    pub async fn send_async(
        &mut self,
        to: PeerId,
        frame: impl Into<Frame>,
    ) -> Result<(), NoSuchPeer> {
        let mut slot = Some(frame.into());
        poll_fn(|cx| poll_send_routed(self, to.0, &mut slot, cx))
            .await
            .map_err(NoSuchPeer)
    }

    /// Closes the connection to the peer `to`. Returns whether there was
    /// one.
    pub fn disconnect(&mut self, to: PeerId) -> bool {
        disconnect_routed(self, to.0)
    }
}

// SERVER and PEER route the same way, by the ids their table gives peers.

fn try_recv_routed<T: SocketType<Routing = ServerTable>>(
    socket: &mut Socket<T>,
) -> Option<(NonZeroU32, Frame)> {
    match poll_recv_routed(socket, &mut TaskContext::from_waker(Waker::noop())) {
        Poll::Ready(received) => Some(received),
        Poll::Pending => None,
    }
}

fn poll_recv_routed<T: SocketType<Routing = ServerTable>>(
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
    let mut cx = TaskContext::from_waker(Waker::noop());
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

fn disconnect_routed<T: SocketType<Routing = ServerTable>>(
    socket: &mut Socket<T>,
    to: NonZeroU32,
) -> bool {
    let mut cx = TaskContext::from_waker(Waker::noop());
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
}

impl Socket<Channel> {
    /// Sends `frame` if there is a peer with room, otherwise hands it back.
    pub fn try_send(&mut self, frame: impl Into<Frame>) -> Result<(), Frame> {
        let mut slot = Some(Message::new(frame.into()));
        let _ = self.poll_channel_send(&mut slot, &mut TaskContext::from_waker(Waker::noop()));
        slot.map_or(Ok(()), |message| Err(only_frame(message)))
    }

    /// Sends `frame`, blocking until there is a peer with room.
    pub fn send(&mut self, frame: impl Into<Frame>) {
        let mut slot = Some(Message::new(frame.into()));
        wait::block_on(|cx| self.poll_channel_send(&mut slot, cx));
    }

    /// Sends `frame`, waiting until there is a peer with room.
    pub async fn send_async(&mut self, frame: impl Into<Frame>) {
        let mut slot = Some(Message::new(frame.into()));
        poll_fn(|cx| self.poll_channel_send(&mut slot, cx)).await;
    }

    /// The next message, if one is waiting.
    pub fn try_recv(&mut self) -> Option<Frame> {
        match self.poll_channel_recv(&mut TaskContext::from_waker(Waker::noop())) {
            Poll::Ready(frame) => Some(frame),
            Poll::Pending => None,
        }
    }

    /// The next message, blocking until one arrives.
    pub fn recv(&mut self) -> Frame {
        wait::block_on(|cx| self.poll_channel_recv(cx))
    }

    /// The next message, waiting until one arrives.
    pub async fn recv_async(&mut self) -> Frame {
        poll_fn(|cx| self.poll_channel_recv(cx)).await
    }

    fn poll_channel_send(
        &mut self,
        slot: &mut Option<Message>,
        cx: &mut TaskContext<'_>,
    ) -> Poll<()> {
        let Some(message) = slot.take() else {
            return Poll::Ready(());
        };
        let (peers, pair) = self.parts_accepting(cx);
        match pair.poll_send(peers, message, cx) {
            Ok(()) => Poll::Ready(()),
            Err(message) => {
                *slot = Some(message);
                Poll::Pending
            }
        }
    }

    fn poll_channel_recv(&mut self, cx: &mut TaskContext<'_>) -> Poll<Frame> {
        let (peers, pair) = self.parts_accepting(cx);
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

fn only_frame(message: Message) -> Frame {
    message.into_iter().next().expect("a message has a frame")
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
