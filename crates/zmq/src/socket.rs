//! Typed sockets: each type has only the operations its pattern allows.

use std::fmt;
use std::future::poll_fn;
use std::marker::PhantomData;
use std::task::{Context as TaskContext, Poll, Waker};

use zmq_core::{
    ConnectionConfig, FairQueue, LoadBalancer, Outstanding, Pair as PairRouting, PeerGone,
    PeerList, Peers, Publisher, Received, Replier, Requester, RouteOutcome, RouterSendError,
    RouterTable, Subscriber, XPubEvent,
};
use zmq_pipe::Hwm;
use zmtp::Message;

use crate::endpoint::Target;
use crate::{Endpoint, Error, RoutingId, SocketOptions, endpoint, wait};

pub(crate) mod sealed {
    pub trait Sealed {}
}

impl<T: SocketType> sealed::Sealed for Socket<T> {}

/// A ZeroMQ socket type. Implemented by [`Push`], [`Pull`] and the other
/// markers; cannot be implemented outside this crate.
pub trait SocketType: sealed::Sealed + 'static {
    /// The type announced in the handshake.
    #[doc(hidden)]
    const KIND: zmtp::SocketType;
    /// How the socket routes messages among its peers.
    #[doc(hidden)]
    type Routing: Routing;
}

/// A socket type that can keep only the latest message, as libzmq's
/// `ZMQ_CONFLATE`: PUSH, PULL, PUB, SUB and DEALER.
pub trait Conflatable: SocketType {
    /// Whether messages from peers conflate.
    #[doc(hidden)]
    const INBOUND: bool;
    /// Whether messages towards peers conflate.
    #[doc(hidden)]
    const OUTBOUND: bool;
}

// Subscriptions are never conflated, unlike libzmq, which can lose all but
// the last of them.
impl Conflatable for Push {
    const INBOUND: bool = false;
    const OUTBOUND: bool = true;
}

impl Conflatable for Pull {
    const INBOUND: bool = true;
    const OUTBOUND: bool = false;
}

impl Conflatable for Pub {
    const INBOUND: bool = false;
    const OUTBOUND: bool = true;
}

impl Conflatable for Sub {
    const INBOUND: bool = true;
    const OUTBOUND: bool = false;
}

impl Conflatable for Dealer {
    const INBOUND: bool = true;
    const OUTBOUND: bool = true;
}

impl<T: Conflatable> Socket<T> {
    /// Whether connections bound or connected from now on keep only the
    /// latest message in each queue, as libzmq's `ZMQ_CONFLATE`: a newer
    /// message replaces one not yet delivered, and the high-water marks no
    /// longer apply. Off by default.
    pub fn set_conflate(&mut self, on: bool) {
        let config = self.peers.config_mut();
        config.conflate_inbound = on && T::INBOUND;
        config.conflate_outbound = on && T::OUTBOUND;
    }
}

/// How a socket type routes messages among its peers.
#[doc(hidden)]
pub trait Routing: Default + fmt::Debug + Send {
    /// Called when [`Socket::connect`] has added a peer.
    fn connected(&mut self, _peers: &mut PeerList) {}
}

impl Routing for LoadBalancer {}
impl Routing for FairQueue {}
impl Routing for Requester {}
impl Routing for Replier {}
impl Routing for DealerRouting {}
impl Routing for RouterRouting {}
impl Routing for Publisher {}
impl Routing for XPubRouting {}
impl Routing for PairRouting {}

impl Routing for Subscriber {
    /// Subscriptions go to a peer as soon as it is connected, as in libzmq,
    /// so it publishes to us even before we next receive.
    fn connected(&mut self, peers: &mut PeerList) {
        self.greet(peers);
    }
}

/// Sends each message to every SUB peer subscribed to a prefix of its
/// first frame (RFC 29). Never blocks: a subscriber whose queue is full
/// misses the message, as in libzmq.
#[derive(Debug)]
pub enum Pub {}

/// Receives what its PUB peers publish on the topics it subscribes to
/// (RFC 29). Subscribed to nothing at first, so it receives nothing until
/// [`subscribe`](Socket::subscribe) is called. Its subscriptions are never
/// dropped: its queues towards peers are unbounded, whatever its send HWM.
#[derive(Debug)]
pub enum Sub {}

/// A PUB that also receives: each topic's first subscription and last
/// cancellation across its peers, and whatever XSUB peers send upstream,
/// as [`XPubEvent`]s (RFC 29). Used to build proxies and to see what
/// subscribers want.
#[derive(Debug)]
pub enum XPub {}

/// A SUB that also sends: [`send`](Socket::send) goes to every publisher
/// (RFC 29). Used with [`XPub`] to build proxies.
#[derive(Debug)]
pub enum XSub {}

/// XPUB's routing: a publisher that keeps its peers' events.
#[doc(hidden)]
#[derive(Debug)]
pub struct XPubRouting(Publisher);

impl Default for XPubRouting {
    fn default() -> Self {
        Self(Publisher::with_events())
    }
}

impl sealed::Sealed for XPub {}
impl SocketType for XPub {
    const KIND: zmtp::SocketType = zmtp::SocketType::XPub;
    type Routing = XPubRouting;
}

impl sealed::Sealed for XSub {}
impl SocketType for XSub {
    const KIND: zmtp::SocketType = zmtp::SocketType::XSub;
    type Routing = Subscriber;
}

/// Talks to exactly one other PAIR socket, both ways (RFC 31). The first
/// peer to arrive is the one; others are turned away until its session
/// ends for good. Waits while there is no peer or it has no room; never
/// drops a message.
#[derive(Debug)]
pub enum Pair {}

impl sealed::Sealed for Pair {}
impl SocketType for Pair {
    const KIND: zmtp::SocketType = zmtp::SocketType::Pair;
    type Routing = PairRouting;
}

impl sealed::Sealed for Pub {}
impl SocketType for Pub {
    const KIND: zmtp::SocketType = zmtp::SocketType::Pub;
    type Routing = Publisher;
}

impl sealed::Sealed for Sub {}
impl SocketType for Sub {
    const KIND: zmtp::SocketType = zmtp::SocketType::Sub;
    type Routing = Subscriber;
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

/// Sends round robin to, and receives fairly from, its peers (RFC 28):
/// REP, ROUTER or other DEALERs. Waits while no peer has room.
#[derive(Debug)]
pub enum Dealer {}

/// Receives from its peers tagged with their [`RoutingId`], and sends to a
/// peer by routing id (RFC 28). Sending to an unknown routing id fails
/// rather than dropping the message, as libzmq's `ZMQ_ROUTER_MANDATORY`;
/// [`try_send`](Socket::try_send) and ignoring the error is libzmq's
/// default, which drops it.
#[derive(Debug)]
pub enum Router {}

/// Sends requests and receives their replies in lockstep (RFC 28): each
/// [`request`](Socket::request) goes to the next REP or ROUTER peer with
/// room, and only that peer's reply is accepted.
#[derive(Debug)]
pub enum Req {}

/// Receives requests fairly and answers each with one reply (RFC 28). The
/// [`Request`] returned by [`recv`](Socket::recv) borrows the socket until
/// it is answered or dropped.
#[derive(Debug)]
pub enum Rep {}

impl sealed::Sealed for Req {}
impl SocketType for Req {
    const KIND: zmtp::SocketType = zmtp::SocketType::Req;
    type Routing = Requester;
}

impl sealed::Sealed for Rep {}
impl SocketType for Rep {
    const KIND: zmtp::SocketType = zmtp::SocketType::Rep;
    type Routing = Replier;
}

/// DEALER's routing: load-balanced sends, fair-queued receives.
#[doc(hidden)]
#[derive(Debug, Default)]
pub struct DealerRouting {
    send: LoadBalancer,
    recv: FairQueue,
}

/// ROUTER's routing: fair-queued receives tagged by routing id.
#[doc(hidden)]
#[derive(Debug, Default)]
pub struct RouterRouting {
    recv: FairQueue,
    table: RouterTable,
}

impl sealed::Sealed for Dealer {}
impl SocketType for Dealer {
    const KIND: zmtp::SocketType = zmtp::SocketType::Dealer;
    type Routing = DealerRouting;
}

impl sealed::Sealed for Router {}
impl SocketType for Router {
    const KIND: zmtp::SocketType = zmtp::SocketType::Router;
    type Routing = RouterRouting;
}

/// A socket of type `T`. What it can do depends on `T`: a
/// `Socket<Push>` can only send and a `Socket<Pull>` can only receive.
pub struct Socket<T: SocketType> {
    peers: Peers,
    routing: T::Routing,
    _type: PhantomData<T>,
}

impl<T: SocketType> Socket<T> {
    pub(crate) fn new(context: zmq_core::Context, options: SocketOptions) -> Self {
        let config = ConnectionConfig {
            identity: options.identity,
            // Subscriptions and joins must never be dropped, so SUB, XSUB
            // and DISH queue without limit towards their peers.
            send_hwm: if matches!(
                T::KIND,
                zmtp::SocketType::Sub | zmtp::SocketType::XSub | zmtp::SocketType::Dish
            ) {
                Hwm::Unlimited
            } else {
                options.send_hwm
            },
            recv_hwm: options.recv_hwm,
            linger: options.linger,
            reconnect: options.reconnect,
            max_frame_size: options.max_frame_size,
            security: options.security,
            heartbeat: options.heartbeat,
            handshake_timeout: options.handshake_timeout,
            ..ConnectionConfig::new(T::KIND)
        };
        Self {
            peers: Peers::new(context, config),
            routing: T::Routing::default(),
            _type: PhantomData,
        }
    }

    /// Drops peers that are gone and discards what peers sent, for
    /// patterns that never receive.
    #[cfg(feature = "draft")]
    pub(crate) fn prune_send_only(&mut self) {
        self.peers.prune_send_only();
    }

    /// Takes in peers the socket's listeners accepted, registering `cx`
    /// with them, then lends out the peers and the routing state together.
    #[cfg(feature = "draft")]
    pub(crate) fn parts_accepting(
        &mut self,
        cx: &mut TaskContext<'_>,
    ) -> (&mut PeerList, &mut T::Routing) {
        self.peers.accept(cx);
        (self.peers.list_mut(), &mut self.routing)
    }

    /// A new monitor of this socket's events, as libzmq's
    /// `zmq_socket_monitor`: its listeners binding, accepting and closing,
    /// and its connections connecting, completing or failing handshakes,
    /// and disconnecting. Monitors hear events from when they are made, so
    /// make one before binding or connecting to hear everything.
    pub fn monitor(&self) -> zmq_core::Monitor {
        self.peers.monitor()
    }

    /// Binds `endpoint`, such as `tcp://*:5555`, `tcp://127.0.0.1:*`,
    /// `ipc:///tmp/feed`, `ipc://*` or `inproc://name`, and accepts peers on
    /// it. Returns where it is bound: the actual port for `tcp://…:*`, and
    /// the fresh path for `ipc://*`.
    ///
    /// An `ipc` path's socket file is removed when the socket is dropped. A
    /// leftover socket file nobody listens on is replaced, but a live socket
    /// or any other file is `AddrInUse`.
    pub fn bind(&mut self, endpoint: &str) -> Result<Endpoint, Error> {
        match endpoint::bind_target(endpoint)? {
            Target::Tcp(addr) => self.peers.bind(addr).map(Endpoint::Tcp),
            #[cfg(unix)]
            Target::Ipc(path) => self.peers.bind_ipc(&path).map(|()| Endpoint::Ipc(path)),
            #[cfg(unix)]
            Target::IpcAnywhere => self.peers.bind_ipc_anywhere().map(Endpoint::Ipc),
            #[cfg(not(unix))]
            Target::Ipc(_) | Target::IpcAnywhere => unreachable!("ipc is refused when parsing"),
            Target::Inproc(name) => self
                .peers
                .bind_inproc(&name)
                .map(|()| Endpoint::Inproc(name)),
        }
        .map_err(Error::Io)
    }

    /// Connects to `endpoint`, such as `tcp://localhost:5555`,
    /// `ipc:///tmp/feed` or `inproc://name`. Returns at once; messages queue
    /// until the connection is up, and a TCP or IPC connection retries until
    /// something listens and reconnects if it breaks. An `inproc`
    /// connection waits for its name to be bound in this socket's context,
    /// and ends for good when the bound socket is dropped.
    pub fn connect(&mut self, endpoint: &str) -> Result<(), Error> {
        self.connect_key(endpoint).map(drop)
    }

    /// Connects to `endpoint` and returns the new peer.
    pub(crate) fn connect_key(&mut self, endpoint: &str) -> Result<zmq_core::PeerKey, Error> {
        let key = match endpoint::connect_target(endpoint)? {
            Target::Tcp(addr) => self.peers.connect(addr),
            #[cfg(unix)]
            Target::Ipc(path) => self.peers.connect_ipc(&path),
            #[cfg(not(unix))]
            Target::Ipc(_) => unreachable!("ipc is refused when parsing"),
            Target::IpcAnywhere => unreachable!("refused when parsing"),
            Target::Inproc(name) => self.peers.connect_inproc(&name),
        };
        self.routing.connected(self.peers.list_mut());
        Ok(key)
    }
}

impl Socket<Push> {
    /// Sends `message` if a peer has room, otherwise hands it back.
    pub fn try_send(&mut self, message: impl Into<Message>) -> Result<(), Message> {
        let mut slot = Some(message.into());
        let _ = self.poll_send(&mut slot, &mut TaskContext::from_waker(Waker::noop()));
        slot.map_or(Ok(()), Err)
    }

    /// Sends `message`, blocking while no peer has room.
    pub fn send(&mut self, message: impl Into<Message>) {
        let mut slot = Some(message.into());
        wait::block_on(|cx| self.poll_send(&mut slot, cx));
    }

    /// Sends `message`, waiting while no peer has room.
    pub async fn send_async(&mut self, message: impl Into<Message>) {
        let mut slot = Some(message.into());
        poll_fn(|cx| self.poll_send(&mut slot, cx)).await;
    }

    pub(crate) fn poll_send(
        &mut self,
        slot: &mut Option<Message>,
        cx: &mut TaskContext<'_>,
    ) -> Poll<()> {
        // PUSH never receives: discard anything peers send, and drop peers
        // that are gone.
        self.peers.prune_send_only();
        poll_load_balanced(&mut self.peers, &mut self.routing, slot, cx)
    }
}

impl Socket<Pair> {
    /// Sends `message` if there is a peer with room, otherwise hands it
    /// back.
    pub fn try_send(&mut self, message: impl Into<Message>) -> Result<(), Message> {
        let mut slot = Some(message.into());
        let _ = self.poll_send(&mut slot, &mut TaskContext::from_waker(Waker::noop()));
        slot.map_or(Ok(()), Err)
    }

    /// Sends `message`, blocking until there is a peer with room.
    pub fn send(&mut self, message: impl Into<Message>) {
        let mut slot = Some(message.into());
        wait::block_on(|cx| self.poll_send(&mut slot, cx));
    }

    /// Sends `message`, waiting until there is a peer with room.
    pub async fn send_async(&mut self, message: impl Into<Message>) {
        let mut slot = Some(message.into());
        poll_fn(|cx| self.poll_send(&mut slot, cx)).await;
    }

    /// The next message, if one is waiting.
    pub fn try_recv(&mut self) -> Option<Message> {
        ready(self.poll_recv(&mut TaskContext::from_waker(Waker::noop())))
    }

    /// The next message, blocking until one arrives.
    pub fn recv(&mut self) -> Message {
        wait::block_on(|cx| self.poll_recv(cx))
    }

    /// The next message, waiting until one arrives.
    pub async fn recv_async(&mut self) -> Message {
        poll_fn(|cx| self.poll_recv(cx)).await
    }

    pub(crate) fn poll_send(
        &mut self,
        slot: &mut Option<Message>,
        cx: &mut TaskContext<'_>,
    ) -> Poll<()> {
        self.peers.accept(cx);
        let Some(message) = slot.take() else {
            return Poll::Ready(());
        };
        match self.routing.poll_send(self.peers.list_mut(), message, cx) {
            Ok(()) => Poll::Ready(()),
            Err(message) => {
                *slot = Some(message);
                Poll::Pending
            }
        }
    }

    pub(crate) fn poll_recv(&mut self, cx: &mut TaskContext<'_>) -> Poll<Message> {
        self.peers.accept(cx);
        self.routing.poll_recv(self.peers.list_mut(), cx)
    }
}

impl Socket<Pub> {
    /// Sends `message` to every subscriber that wants it and has room.
    /// Never blocks. Returns how many subscribers it was queued for: none if
    /// nobody has subscribed to it yet, as subscriptions take a moment to
    /// arrive after connecting.
    pub fn send(&mut self, message: impl Into<Message>) -> usize {
        self.peers
            .accept(&mut TaskContext::from_waker(Waker::noop()));
        self.routing.send(self.peers.list_mut(), message.into())
    }
}

impl Socket<Sub> {
    /// Subscribes to messages whose first frame starts with `topic`. The
    /// empty topic matches everything. Subscribing to a topic again needs
    /// another [`unsubscribe`](Self::unsubscribe) to undo.
    pub fn subscribe(&mut self, topic: impl AsRef<[u8]>) {
        self.peers
            .accept(&mut TaskContext::from_waker(Waker::noop()));
        self.routing
            .subscribe(self.peers.list_mut(), topic.as_ref());
    }

    /// Removes one subscription to `topic`. Returns whether there was one.
    /// Messages already queued that no longer match are dropped.
    pub fn unsubscribe(&mut self, topic: impl AsRef<[u8]>) -> bool {
        self.peers
            .accept(&mut TaskContext::from_waker(Waker::noop()));
        self.routing
            .unsubscribe(self.peers.list_mut(), topic.as_ref())
    }

    /// The next message on a subscribed topic, if one is waiting.
    pub fn try_recv(&mut self) -> Option<Message> {
        ready(self.poll_recv(&mut TaskContext::from_waker(Waker::noop())))
    }

    /// The next message on a subscribed topic, blocking until one arrives.
    pub fn recv(&mut self) -> Message {
        wait::block_on(|cx| self.poll_recv(cx))
    }

    /// The next message on a subscribed topic, waiting until one arrives.
    pub async fn recv_async(&mut self) -> Message {
        poll_fn(|cx| self.poll_recv(cx)).await
    }

    pub(crate) fn poll_recv(&mut self, cx: &mut TaskContext<'_>) -> Poll<Message> {
        self.peers.accept(cx);
        self.routing.poll_recv(self.peers.list_mut(), cx)
    }
}

impl Socket<XPub> {
    /// Sends `message` to every subscriber that wants it and has room.
    /// Never blocks. Returns how many subscribers it was queued for.
    pub fn send(&mut self, message: impl Into<Message>) -> usize {
        self.peers
            .accept(&mut TaskContext::from_waker(Waker::noop()));
        self.routing.0.send(self.peers.list_mut(), message.into())
    }

    /// The next event, if one is waiting.
    pub fn try_recv(&mut self) -> Option<XPubEvent> {
        ready(self.poll_recv(&mut TaskContext::from_waker(Waker::noop())))
    }

    /// The next event, blocking until one arrives.
    pub fn recv(&mut self) -> XPubEvent {
        wait::block_on(|cx| self.poll_recv(cx))
    }

    /// The next event, waiting until one arrives.
    pub async fn recv_async(&mut self) -> XPubEvent {
        poll_fn(|cx| self.poll_recv(cx)).await
    }

    pub(crate) fn poll_recv(&mut self, cx: &mut TaskContext<'_>) -> Poll<XPubEvent> {
        self.peers.accept(cx);
        self.routing.0.poll_event(self.peers.list_mut(), cx)
    }
}

impl Socket<XSub> {
    /// Subscribes to messages whose first frame starts with `topic`, as
    /// [`Socket<Sub>::subscribe`] does.
    pub fn subscribe(&mut self, topic: impl AsRef<[u8]>) {
        self.peers
            .accept(&mut TaskContext::from_waker(Waker::noop()));
        self.routing
            .subscribe(self.peers.list_mut(), topic.as_ref());
    }

    /// Removes one subscription to `topic`. Returns whether there was one.
    pub fn unsubscribe(&mut self, topic: impl AsRef<[u8]>) -> bool {
        self.peers
            .accept(&mut TaskContext::from_waker(Waker::noop()));
        self.routing
            .unsubscribe(self.peers.list_mut(), topic.as_ref())
    }

    /// Sends `message` to every publisher. Never blocks. As in libzmq, a
    /// single frame starting with `0x01` or `0x00` is a subscription and
    /// subscribes or unsubscribes to the rest.
    pub fn send(&mut self, message: impl Into<Message>) {
        self.peers
            .accept(&mut TaskContext::from_waker(Waker::noop()));
        self.routing
            .send_upstream(self.peers.list_mut(), message.into());
    }

    /// Passes on what an [`XPub`] heard, as a proxy does: subscriptions
    /// become ours, and messages are sent upstream.
    pub fn forward(&mut self, event: XPubEvent) {
        match event {
            XPubEvent::Subscribe(topic) => self.subscribe(topic),
            XPubEvent::Cancel(topic) => {
                self.unsubscribe(topic);
            }
            XPubEvent::Message(message) => self.send(message),
        }
    }

    /// The next message on a subscribed topic, if one is waiting.
    pub fn try_recv(&mut self) -> Option<Message> {
        ready(self.poll_recv(&mut TaskContext::from_waker(Waker::noop())))
    }

    /// The next message on a subscribed topic, blocking until one arrives.
    pub fn recv(&mut self) -> Message {
        wait::block_on(|cx| self.poll_recv(cx))
    }

    /// The next message on a subscribed topic, waiting until one arrives.
    pub async fn recv_async(&mut self) -> Message {
        poll_fn(|cx| self.poll_recv(cx)).await
    }

    pub(crate) fn poll_recv(&mut self, cx: &mut TaskContext<'_>) -> Poll<Message> {
        self.peers.accept(cx);
        self.routing.poll_recv(self.peers.list_mut(), cx)
    }
}

impl Socket<Pull> {
    /// The next message, if one is waiting.
    pub fn try_recv(&mut self) -> Option<Message> {
        ready(self.poll_recv(&mut TaskContext::from_waker(Waker::noop())))
    }

    /// The next message, blocking until one arrives.
    pub fn recv(&mut self) -> Message {
        wait::block_on(|cx| self.poll_recv(cx))
    }

    /// The next message, waiting until one arrives.
    pub async fn recv_async(&mut self) -> Message {
        poll_fn(|cx| self.poll_recv(cx)).await
    }

    pub(crate) fn poll_recv(&mut self, cx: &mut TaskContext<'_>) -> Poll<Message> {
        poll_fair_queued(&mut self.peers, &mut self.routing, cx)
    }
}

impl Socket<Dealer> {
    /// Sends `message` if a peer has room, otherwise hands it back.
    pub fn try_send(&mut self, message: impl Into<Message>) -> Result<(), Message> {
        let mut slot = Some(message.into());
        let _ = self.poll_send(&mut slot, &mut TaskContext::from_waker(Waker::noop()));
        slot.map_or(Ok(()), Err)
    }

    /// Sends `message`, blocking while no peer has room.
    pub fn send(&mut self, message: impl Into<Message>) {
        let mut slot = Some(message.into());
        wait::block_on(|cx| self.poll_send(&mut slot, cx));
    }

    /// Sends `message`, waiting while no peer has room.
    pub async fn send_async(&mut self, message: impl Into<Message>) {
        let mut slot = Some(message.into());
        poll_fn(|cx| self.poll_send(&mut slot, cx)).await;
    }

    /// The next message, if one is waiting.
    pub fn try_recv(&mut self) -> Option<Message> {
        ready(self.poll_recv(&mut TaskContext::from_waker(Waker::noop())))
    }

    /// The next message, blocking until one arrives.
    pub fn recv(&mut self) -> Message {
        wait::block_on(|cx| self.poll_recv(cx))
    }

    /// The next message, waiting until one arrives.
    pub async fn recv_async(&mut self) -> Message {
        poll_fn(|cx| self.poll_recv(cx)).await
    }

    pub(crate) fn poll_send(
        &mut self,
        slot: &mut Option<Message>,
        cx: &mut TaskContext<'_>,
    ) -> Poll<()> {
        poll_load_balanced(&mut self.peers, &mut self.routing.send, slot, cx)
    }

    pub(crate) fn poll_recv(&mut self, cx: &mut TaskContext<'_>) -> Poll<Message> {
        poll_fair_queued(&mut self.peers, &mut self.routing.recv, cx)
    }
}

impl Socket<Dealer> {
    /// Whether each peer connected or accepted from now on is first sent an
    /// empty message, as libzmq's `ZMQ_PROBE_ROUTER`, so a ROUTER peer
    /// learns our routing id at once. Off by default.
    pub fn set_probe(&mut self, on: bool) {
        self.peers
            .set_greeting(on.then(|| Message::new(zmtp::Frame::new())));
    }
}

impl Socket<Req> {
    /// Tags requests with an id so that a reply to an abandoned request,
    /// such as a dropped [`request_async`](Self::request_async) future, is
    /// recognised and dropped (libzmq's `ZMQ_REQ_CORRELATE`). Off by default.
    ///
    /// Without it, the next request to the same peer may receive the reply
    /// meant for the abandoned one. Peers echo the id as part of the
    /// envelope; a ROUTER application sees it as an extra frame.
    pub fn set_correlation(&mut self, on: bool) {
        self.routing.set_correlate(on);
    }

    /// Sends `message` to the next peer with room and waits for its reply,
    /// blocking the thread. Fails if that peer's connection closes first.
    pub fn request(&mut self, message: impl Into<Message>) -> Result<Message, PeerGone> {
        let mut state = RequestState::Sending(Some(message.into()));
        wait::block_on(|cx| self.poll_request(&mut state, cx))
    }

    /// Sends `message` to the next peer with room and waits for its reply.
    /// If this future is dropped before the reply arrives, the request is
    /// abandoned; see [`set_correlation`](Self::set_correlation).
    pub async fn request_async(
        &mut self,
        message: impl Into<Message>,
    ) -> Result<Message, PeerGone> {
        let mut state = RequestState::Sending(Some(message.into()));
        poll_fn(|cx| self.poll_request(&mut state, cx)).await
    }

    fn poll_request(
        &mut self,
        state: &mut RequestState,
        cx: &mut TaskContext<'_>,
    ) -> Poll<Result<Message, PeerGone>> {
        self.peers.accept(cx);
        let peers = self.peers.list_mut();
        loop {
            match state {
                RequestState::Sending(slot) => {
                    let Some(body) = slot.take() else {
                        return Poll::Ready(Err(PeerGone));
                    };
                    match self.routing.poll_send(peers, body, cx) {
                        Ok(outstanding) => *state = RequestState::Waiting(outstanding),
                        Err(body) => {
                            *slot = Some(body);
                            return Poll::Pending;
                        }
                    }
                }
                RequestState::Waiting(outstanding) => {
                    return self.routing.poll_reply(peers, outstanding, cx);
                }
            }
        }
    }
}

/// Where a request is: waiting for a peer with room, then for the reply.
enum RequestState {
    Sending(Option<Message>),
    Waiting(Outstanding),
}

impl Socket<Rep> {
    /// The next request, if one is waiting.
    pub fn try_recv(&mut self) -> Option<Request<'_>> {
        let received = ready(self.poll_received(&mut TaskContext::from_waker(Waker::noop())))?;
        Some(Request {
            socket: self,
            received,
        })
    }

    /// The next request, blocking until one arrives.
    pub fn recv(&mut self) -> Request<'_> {
        let received = wait::block_on(|cx| self.poll_received(cx));
        Request {
            socket: self,
            received,
        }
    }

    /// The next request, waiting until one arrives.
    pub async fn recv_async(&mut self) -> Request<'_> {
        let received = poll_fn(|cx| self.poll_received(cx)).await;
        Request {
            socket: self,
            received,
        }
    }

    fn poll_received(&mut self, cx: &mut TaskContext<'_>) -> Poll<Received> {
        self.peers.accept(cx);
        self.routing.poll_recv(self.peers.list_mut(), cx)
    }
}

/// A request received by a REP socket. Answer it with
/// [`reply`](Self::reply); until then the socket cannot receive another.
/// Dropping it unanswered discards the request, and the requester waits.
#[must_use = "the requester waits for a reply"]
pub struct Request<'a> {
    socket: &'a mut Socket<Rep>,
    received: Received,
}

impl Request<'_> {
    /// The request's message, without its envelope.
    pub fn body(&self) -> &Message {
        &self.received.body
    }

    /// Answers the request. Never blocks: if the requester is gone or its
    /// queue is full, the reply is dropped and the outcome says so.
    pub fn reply(self, reply: impl Into<Message>) -> RouteOutcome {
        let Received { peer, envelope, .. } = self.received;
        Replier::reply(self.socket.peers.list_mut(), peer, envelope, reply.into())
    }
}

impl fmt::Debug for Request<'_> {
    fn fmt(&self, f: &mut fmt::Formatter<'_>) -> fmt::Result {
        f.debug_struct("Request")
            .field("envelope", &self.received.envelope)
            .field("body", &self.received.body)
            .finish()
    }
}

/// A ROUTER could not send: no peer has that routing id, or it went away
/// while the message waited. The message is handed back.
#[derive(Debug, Clone, PartialEq, Eq)]
pub struct UnknownRoutingId(pub Message);

impl fmt::Display for UnknownRoutingId {
    fn fmt(&self, f: &mut fmt::Formatter<'_>) -> fmt::Result {
        f.write_str("no peer has that routing id")
    }
}

impl std::error::Error for UnknownRoutingId {}

impl Socket<Router> {
    /// Sends `message` to the peer with routing id `to` if it is known and
    /// has room, otherwise hands it back. Never blocks.
    pub fn try_send(
        &mut self,
        to: &RoutingId,
        message: impl Into<Message>,
    ) -> Result<(), RouterSendError> {
        self.peers
            .accept(&mut TaskContext::from_waker(Waker::noop()));
        let RouterRouting { table, .. } = &mut self.routing;
        let peers = self.peers.list_mut();
        table.sync(peers);
        table.try_send(peers, to.as_bytes(), message.into())
    }

    /// Sends `message` to the peer with routing id `to`, blocking while its
    /// queue is full. Fails if no peer has that routing id, or it goes away
    /// meanwhile.
    pub fn send(
        &mut self,
        to: &RoutingId,
        message: impl Into<Message>,
    ) -> Result<(), UnknownRoutingId> {
        let mut slot = Some(message.into());
        wait::block_on(|cx| self.poll_send(to, &mut slot, cx)).map_err(UnknownRoutingId)
    }

    /// Sends `message` to the peer with routing id `to`, waiting while its
    /// queue is full. Fails if no peer has that routing id, or it goes away
    /// meanwhile.
    pub async fn send_async(
        &mut self,
        to: &RoutingId,
        message: impl Into<Message>,
    ) -> Result<(), UnknownRoutingId> {
        let mut slot = Some(message.into());
        poll_fn(|cx| self.poll_send(to, &mut slot, cx))
            .await
            .map_err(UnknownRoutingId)
    }

    fn poll_send(
        &mut self,
        to: &RoutingId,
        slot: &mut Option<Message>,
        cx: &mut TaskContext<'_>,
    ) -> Poll<Result<(), Message>> {
        self.peers.accept(cx);
        let RouterRouting { table, .. } = &mut self.routing;
        let peers = self.peers.list_mut();
        table.sync(peers);
        table.poll_send(peers, to.as_bytes(), slot, cx)
    }

    /// Whether a peer announcing a routing id already in use takes it over,
    /// disconnecting the peer that had it, as libzmq's
    /// `ZMQ_ROUTER_HANDOVER`. Off by default: the newcomer is ignored.
    pub fn set_handover(&mut self, on: bool) {
        self.routing.table.set_handover(on);
    }

    /// Whether each peer connected or accepted from now on is first sent an
    /// empty message, as libzmq's `ZMQ_PROBE_ROUTER`, so a ROUTER peer
    /// learns our routing id at once. Off by default.
    pub fn set_probe(&mut self, on: bool) {
        self.peers
            .set_greeting(on.then(|| Message::new(zmtp::Frame::new())));
    }

    /// The next message and the routing id of the peer that sent it, if
    /// one is waiting.
    pub fn try_recv(&mut self) -> Option<(RoutingId, Message)> {
        ready(self.poll_recv(&mut TaskContext::from_waker(Waker::noop())))
    }

    /// The next message and its sender's routing id, blocking until one
    /// arrives.
    pub fn recv(&mut self) -> (RoutingId, Message) {
        wait::block_on(|cx| self.poll_recv(cx))
    }

    /// The next message and its sender's routing id, waiting until one
    /// arrives.
    pub async fn recv_async(&mut self) -> (RoutingId, Message) {
        poll_fn(|cx| self.poll_recv(cx)).await
    }

    pub(crate) fn poll_recv(&mut self, cx: &mut TaskContext<'_>) -> Poll<(RoutingId, Message)> {
        self.peers.accept(cx);
        let RouterRouting { recv, table } = &mut self.routing;
        let peers = self.peers.list_mut();
        table.sync(peers);
        loop {
            let Poll::Ready((peer, message)) = recv.poll_recv(peers, cx) else {
                return Poll::Pending;
            };
            if table.routing_id(peer).is_none() {
                table.sync(peers);
            }
            // A peer whose identity duplicates another's is ignored, as in
            // libzmq: its messages are dropped.
            if let Some(routing_id) = table.routing_id(peer) {
                return Poll::Ready((RoutingId::trusted(routing_id.clone()), message));
            }
        }
    }
}

/// Sends the message in `slot` to the next peer with room, putting it back
/// and returning `Pending` if none has room. Shared by PUSH and DEALER.
fn poll_load_balanced(
    peers: &mut Peers,
    balancer: &mut LoadBalancer,
    slot: &mut Option<Message>,
    cx: &mut TaskContext<'_>,
) -> Poll<()> {
    let Some(message) = slot.take() else {
        return Poll::Ready(());
    };
    peers.accept(cx);
    match balancer.poll_send(peers.list_mut(), message, cx) {
        Ok(_) => Poll::Ready(()),
        Err(message) => {
            *slot = Some(message);
            Poll::Pending
        }
    }
}

/// The next message from any peer, fairly. Shared by PULL and DEALER.
fn poll_fair_queued(
    peers: &mut Peers,
    queue: &mut FairQueue,
    cx: &mut TaskContext<'_>,
) -> Poll<Message> {
    peers.accept(cx);
    queue
        .poll_recv(peers.list_mut(), cx)
        .map(|(_, message)| message)
}

fn ready<T>(poll: Poll<T>) -> Option<T> {
    match poll {
        Poll::Ready(value) => Some(value),
        Poll::Pending => None,
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
