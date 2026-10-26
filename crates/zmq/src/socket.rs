//! Typed sockets: each type has only the operations its pattern allows.

use std::convert::Infallible;
use std::fmt;
use std::future;
use std::marker::PhantomData;
use std::task::{Context as TaskContext, Poll};
use std::time::Duration;

use zmq_core::{
    ConnectionConfig, EndpointId, FairQueue, LoadBalancer, Outstanding, Pair as PairRouting,
    PeerGone, PeerList, Peers, Publisher, Received, Replier, Requester, RouteOutcome,
    RouterSendError, RouterTable, Subscriber, Verbosity, XPubEvent,
};
use zmq_pipe::Hwm;
use zmtp::{Frame, Message, Subscription};

use crate::endpoint::Target;
use crate::polling::{PollItem, PollReadable, PollWritable};
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
    /// What one receive yields, as a poll holds it until it is taken.
    #[doc(hidden)]
    type Received: Send;

    /// A new socket's routing. Types that share a routing but set it up
    /// differently, as XPUB does a PUB's, say how.
    #[doc(hidden)]
    fn routing() -> Self::Routing {
        Self::Routing::default()
    }
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
impl Routing for PairRouting {}

impl Routing for Subscriber {
    /// Subscriptions go to a peer as soon as it is connected, as in libzmq,
    /// so it publishes to us even before we next receive.
    fn connected(&mut self, peers: &mut PeerList) {
        self.greet(peers);
    }
}

/// A socket type that receives, so its sockets can be
/// [polled](crate::poll) until a receive would not wait, with
/// [`Socket::readable`]. REQ has none: its [`request`](Socket::request)
/// waits for its own reply.
///
/// ```compile_fail
/// # let context = zmq::Context::new().unwrap();
/// let mut req = context.socket::<zmq::Req>();
/// zmq::poll([req.readable()]);
/// ```
pub trait Receives: SocketType + Sized {
    /// The next item from the socket's peers, as the type's receives
    /// produce it. What a poll already holds is the caller's to return.
    #[doc(hidden)]
    fn poll_next(socket: &mut Socket<Self>, cx: &mut TaskContext<'_>) -> Poll<Self::Received>;
}

/// A socket type whose sends wait for a peer with room, so its sockets can
/// be [polled](crate::poll) until a send would not wait, with
/// [`Socket::writable`]. Types whose sends never wait, such as PUB and
/// ROUTER, have none.
///
/// ```compile_fail
/// # let context = zmq::Context::new().unwrap();
/// let mut publisher = context.socket::<zmq::Pub>();
/// zmq::poll([publisher.writable()]);
/// ```
pub trait WaitsForRoom: SocketType + Sized {
    /// Whether a send would succeed now; registers `cx` if not.
    #[doc(hidden)]
    fn poll_room(socket: &mut Socket<Self>, cx: &mut TaskContext<'_>) -> Poll<()>;

    /// Sends the message in `slot` to a peer with room, or puts it back and
    /// returns `Pending`, with `cx` registered.
    #[doc(hidden)]
    fn poll_send(
        socket: &mut Socket<Self>,
        slot: &mut Option<Message>,
        cx: &mut TaskContext<'_>,
    ) -> Poll<()>;
}

// The receives and waiting sends behind each type's own `try_recv`, `recv`,
// `recv_timeout` and `recv_async`, and `try_send`, `send`, `send_timeout` and
// `send_async`. Those are written out per type, so that each documents what
// it takes and returns.

impl<T: Receives> Socket<T> {
    /// What a poll for readability received, or else the next.
    pub(crate) fn poll_recv(&mut self, cx: &mut TaskContext<'_>) -> Poll<T::Received> {
        if let Some(received) = self.take_pending() {
            return Poll::Ready(received);
        }
        T::poll_next(self, cx)
    }

    pub(crate) fn try_recv_next(&mut self) -> Option<T::Received> {
        ready(self.poll_recv(&mut noop()))
    }

    pub(crate) fn recv_next(&mut self) -> T::Received {
        wait::block_on(|cx| self.poll_recv(cx))
    }

    pub(crate) fn recv_next_timeout(&mut self, timeout: Duration) -> Option<T::Received> {
        wait::block_on_until(wait::deadline(timeout), |cx| self.poll_recv(cx))
    }

    pub(crate) async fn recv_next_async(&mut self) -> T::Received {
        future::poll_fn(|cx| self.poll_recv(cx)).await
    }
}

impl<T: WaitsForRoom> Socket<T> {
    pub(crate) fn poll_send(
        &mut self,
        slot: &mut Option<Message>,
        cx: &mut TaskContext<'_>,
    ) -> Poll<()> {
        T::poll_send(self, slot, cx)
    }

    pub(crate) fn try_send_message(&mut self, message: Message) -> Result<(), Message> {
        let mut slot = Some(message);
        let _ = self.poll_send(&mut slot, &mut noop());
        slot.map_or(Ok(()), Err)
    }

    pub(crate) fn send_message(&mut self, message: Message) {
        let mut slot = Some(message);
        wait::block_on(|cx| self.poll_send(&mut slot, cx));
    }

    pub(crate) fn send_message_timeout(
        &mut self,
        message: Message,
        timeout: Duration,
    ) -> Result<(), Message> {
        let mut slot = Some(message);
        wait::block_on_until(wait::deadline(timeout), |cx| self.poll_send(&mut slot, cx));
        slot.map_or(Ok(()), Err)
    }

    pub(crate) async fn send_message_async(&mut self, message: Message) {
        let mut slot = Some(message);
        future::poll_fn(|cx| self.poll_send(&mut slot, cx)).await;
    }
}

impl Receives for Pair {
    fn poll_next(socket: &mut Socket<Self>, cx: &mut TaskContext<'_>) -> Poll<Message> {
        socket.peers.accept(cx);
        socket.routing.poll_recv(socket.peers.list_mut(), cx)
    }
}

impl Receives for Sub {
    fn poll_next(socket: &mut Socket<Self>, cx: &mut TaskContext<'_>) -> Poll<Message> {
        socket.peers.accept(cx);
        socket.routing.poll_recv(socket.peers.list_mut(), cx)
    }
}

impl Receives for XPub {
    fn poll_next(socket: &mut Socket<Self>, cx: &mut TaskContext<'_>) -> Poll<XPubEvent> {
        socket.peers.accept(cx);
        socket.routing.poll_event(socket.peers.list_mut(), cx)
    }
}

impl Receives for ManualXPub {
    fn poll_next(
        socket: &mut Socket<Self>,
        cx: &mut TaskContext<'_>,
    ) -> Poll<(Option<zmq_core::PeerKey>, XPubEvent)> {
        socket.peers.accept(cx);
        socket.routing.poll_request(socket.peers.list_mut(), cx)
    }
}

impl Receives for XSub {
    fn poll_next(socket: &mut Socket<Self>, cx: &mut TaskContext<'_>) -> Poll<Message> {
        socket.peers.accept(cx);
        socket.routing.poll_recv(socket.peers.list_mut(), cx)
    }
}

impl Receives for Pull {
    fn poll_next(socket: &mut Socket<Self>, cx: &mut TaskContext<'_>) -> Poll<Message> {
        poll_fair_queued(&mut socket.peers, &mut socket.routing, cx)
    }
}

impl Receives for Rep {
    fn poll_next(socket: &mut Socket<Self>, cx: &mut TaskContext<'_>) -> Poll<Received> {
        socket.peers.accept(cx);
        socket.routing.poll_recv(socket.peers.list_mut(), cx)
    }
}

impl Receives for Dealer {
    fn poll_next(socket: &mut Socket<Self>, cx: &mut TaskContext<'_>) -> Poll<Message> {
        poll_fair_queued(&mut socket.peers, &mut socket.routing.recv, cx)
    }
}

impl Receives for Router {
    fn poll_next(
        socket: &mut Socket<Self>,
        cx: &mut TaskContext<'_>,
    ) -> Poll<(RoutingId, Message)> {
        socket.peers.accept(cx);
        let RouterRouting { recv, table } = &mut socket.routing;
        let peers = socket.peers.list_mut();
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

impl WaitsForRoom for Push {
    fn poll_room(socket: &mut Socket<Self>, cx: &mut TaskContext<'_>) -> Poll<()> {
        // As `poll_send`: PUSH discards what peers send and drops those gone.
        socket.peers.prune_send_only();
        socket.peers.accept(cx);
        socket.routing.poll_ready(socket.peers.list_mut(), cx)
    }

    fn poll_send(
        socket: &mut Socket<Self>,
        slot: &mut Option<Message>,
        cx: &mut TaskContext<'_>,
    ) -> Poll<()> {
        // PUSH never receives: discard anything peers send, and drop peers
        // that are gone.
        socket.peers.prune_send_only();
        poll_load_balanced(&mut socket.peers, &mut socket.routing, slot, cx)
    }
}

impl WaitsForRoom for Pair {
    fn poll_room(socket: &mut Socket<Self>, cx: &mut TaskContext<'_>) -> Poll<()> {
        socket.peers.accept(cx);
        socket.routing.poll_ready(socket.peers.list_mut(), cx)
    }

    fn poll_send(
        socket: &mut Socket<Self>,
        slot: &mut Option<Message>,
        cx: &mut TaskContext<'_>,
    ) -> Poll<()> {
        socket.peers.accept(cx);
        let Some(message) = slot.take() else {
            return Poll::Ready(());
        };
        match socket
            .routing
            .poll_send(socket.peers.list_mut(), message, cx)
        {
            Ok(()) => Poll::Ready(()),
            Err(message) => {
                *slot = Some(message);
                Poll::Pending
            }
        }
    }
}

impl WaitsForRoom for Dealer {
    fn poll_room(socket: &mut Socket<Self>, cx: &mut TaskContext<'_>) -> Poll<()> {
        socket.peers.accept(cx);
        socket.routing.send.poll_ready(socket.peers.list_mut(), cx)
    }

    fn poll_send(
        socket: &mut Socket<Self>,
        slot: &mut Option<Message>,
        cx: &mut TaskContext<'_>,
    ) -> Poll<()> {
        poll_load_balanced(&mut socket.peers, &mut socket.routing.send, slot, cx)
    }
}

impl<T: Receives> Socket<T> {
    /// This socket, to [`poll`](crate::poll()) until a receive would not
    /// wait. Once the poll reports it, `try_recv` returns at once.
    pub fn readable(&mut self) -> PollItem<'_> {
        PollItem::readable(self)
    }
}

impl<T: WaitsForRoom> Socket<T> {
    /// This socket, to [`poll`](crate::poll()) until a send would not wait.
    /// Once the poll reports it, `try_send` succeeds, unless the peer with
    /// room leaves first.
    pub fn writable(&mut self) -> PollItem<'_> {
        PollItem::writable(self)
    }
}

impl<T: Receives> PollReadable for Socket<T> {
    /// Receives into `pending` rather than peeking: SUB, DISH and others
    /// drop messages as they receive, so a message in a pipe does not mean
    /// a receive would return one.
    fn poll_readable(&mut self, cx: &mut TaskContext<'_>) -> Poll<()> {
        if self.pending.is_none() {
            let Poll::Ready(received) = T::poll_next(self, cx) else {
                return Poll::Pending;
            };
            self.pending = Some(received);
        }
        Poll::Ready(())
    }
}

impl<T: WaitsForRoom> PollWritable for Socket<T> {
    fn poll_writable(&mut self, cx: &mut TaskContext<'_>) -> Poll<()> {
        T::poll_room(self, cx)
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

impl sealed::Sealed for XPub {}
impl SocketType for XPub {
    const KIND: zmtp::SocketType = zmtp::SocketType::XPub;
    type Routing = Publisher;
    type Received = XPubEvent;

    /// A publisher that keeps its peers' events.
    fn routing() -> Publisher {
        Publisher::with_events()
    }
}

/// An XPUB that decides its peers' subscriptions, as libzmq's
/// `ZMQ_XPUB_MANUAL`: each subscription and cancel a peer sends arrives as
/// a [`SubscriptionRequest`] and has no effect until the application acts
/// on it, by accepting it or subscribing that peer to something else. It
/// announces itself as XPUB.
#[derive(Debug)]
pub enum ManualXPub {}

impl sealed::Sealed for ManualXPub {}
impl SocketType for ManualXPub {
    const KIND: zmtp::SocketType = zmtp::SocketType::XPub;
    type Routing = Publisher;
    type Received = (Option<zmq_core::PeerKey>, XPubEvent);

    /// A publisher that leaves subscriptions to the application.
    fn routing() -> Publisher {
        Publisher::manual()
    }
}

impl sealed::Sealed for XSub {}
impl SocketType for XSub {
    const KIND: zmtp::SocketType = zmtp::SocketType::XSub;
    type Routing = Subscriber;
    type Received = Message;

    /// SUB's, delivering every message, as libzmq's XSUB does. Only SUB
    /// filters by topic.
    fn routing() -> Subscriber {
        let mut subscriber = Subscriber::default();
        subscriber.set_deliver_all(true);
        subscriber
    }
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
    type Received = Message;
}

impl sealed::Sealed for Pub {}
impl SocketType for Pub {
    const KIND: zmtp::SocketType = zmtp::SocketType::Pub;
    type Routing = Publisher;
    type Received = Infallible;
}

impl sealed::Sealed for Sub {}
impl SocketType for Sub {
    const KIND: zmtp::SocketType = zmtp::SocketType::Sub;
    type Routing = Subscriber;
    type Received = Message;
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
    type Received = Infallible;
}

impl sealed::Sealed for Pull {}
impl SocketType for Pull {
    const KIND: zmtp::SocketType = zmtp::SocketType::Pull;
    type Routing = FairQueue;
    type Received = Message;
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
    type Received = Infallible;
}

impl sealed::Sealed for Rep {}
impl SocketType for Rep {
    const KIND: zmtp::SocketType = zmtp::SocketType::Rep;
    type Routing = Replier;
    type Received = Received;
}

/// DEALER's routing: load-balanced sends, fair-queued receives.
#[doc(hidden)]
#[derive(Debug, Default)]
pub struct DealerRouting {
    pub(crate) send: LoadBalancer,
    pub(crate) recv: FairQueue,
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
    type Received = Message;
}

impl sealed::Sealed for Router {}
impl SocketType for Router {
    const KIND: zmtp::SocketType = zmtp::SocketType::Router;
    type Routing = RouterRouting;
    type Received = (RoutingId, Message);
}

/// A socket of type `T`. What it can do depends on `T`: a
/// `Socket<Push>` can only send and a `Socket<Pull>` can only receive.
pub struct Socket<T: SocketType> {
    peers: Peers,
    routing: T::Routing,
    /// Where it is bound, as `bind` reported it, for `unbind`.
    bound: Vec<(Endpoint, EndpointId)>,
    /// What it connected to, as written and as resolved, for `disconnect`.
    connected: Vec<(String, Endpoint, EndpointId)>,
    /// What a poll for readability received, which the next receive
    /// returns.
    pending: Option<T::Received>,
    _type: PhantomData<T>,
}

#[cfg(feature = "draft")]
fn is_peer(kind: zmtp::SocketType) -> bool {
    kind == zmtp::SocketType::Peer
}

#[cfg(not(feature = "draft"))]
fn is_peer(_: zmtp::SocketType) -> bool {
    false
}

impl<T: SocketType> Socket<T> {
    /// What a poll for readability received, for the next receive.
    pub(crate) fn take_pending(&mut self) -> Option<T::Received> {
        self.pending.take()
    }

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
            // PEER's connect_peer names the peer at once.
            immediate: options.immediate && !is_peer(T::KIND),
            linger: options.linger,
            reconnect: options.reconnect,
            max_message_size: options.max_message_size,
            security: options.security,
            heartbeat: options.heartbeat,
            tcp_keepalive: options.tcp_keepalive,
            handshake_timeout: options.handshake_timeout,
            ..ConnectionConfig::new(T::KIND)
        };
        Self {
            peers: Peers::new(context, config),
            routing: T::routing(),
            bound: Vec::new(),
            connected: Vec::new(),
            pending: None,
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
    /// `ipc:///tmp/feed`, `ipc://*`, `ipc://@name` (Linux, Android) or
    /// `inproc://name`, and accepts peers on it. Returns where it is bound:
    /// the actual port for `tcp://…:*`, and the fresh path for `ipc://*`.
    ///
    /// Dropping the socket, as [unbinding](Self::unbind), returns once its
    /// addresses are free to bind again, and removes an `ipc` path's socket
    /// file. A leftover socket file nobody listens on is replaced, but a
    /// live socket or any other file is `AddrInUse`. An abstract
    /// `ipc://@name` has no file: a live name is `AddrInUse`.
    ///
    /// Connections already waiting for an `inproc` name join the socket in
    /// the bind, as in libzmq, so a SUB that binds subscribes on them at
    /// once.
    pub fn bind(&mut self, endpoint: &str) -> Result<Endpoint, Error> {
        let (id, bound) = match endpoint::bind_target(endpoint)? {
            Target::Tcp(addr) => self
                .peers
                .bind(addr)
                .map(|(id, addr)| (id, Endpoint::Tcp(addr))),
            #[cfg(unix)]
            Target::Ipc(path) => self
                .peers
                .bind_ipc(&path)
                .map(|id| (id, Endpoint::Ipc(path))),
            #[cfg(unix)]
            Target::IpcAnywhere => self
                .peers
                .bind_ipc_anywhere()
                .map(|(id, path)| (id, Endpoint::Ipc(path))),
            #[cfg(not(unix))]
            Target::Ipc(_) | Target::IpcAnywhere => unreachable!("ipc is refused when parsing"),
            #[cfg(any(target_os = "linux", target_os = "android"))]
            Target::IpcAbstract(name) => self
                .peers
                .bind_ipc_abstract(&name)
                .map(|id| (id, Endpoint::IpcAbstract(name))),
            Target::Inproc(name) => self
                .peers
                .bind_inproc(&name)
                .map(|id| (id, Endpoint::Inproc(name))),
        }
        .map_err(Error::Io)?;
        self.bound.push((bound.clone(), id));
        // Connections already waiting for an inproc name join now, as
        // libzmq attaches them in the bind, so a SUB subscribes on them at
        // once rather than when next used.
        self.peers.accept(&mut noop());
        self.routing.connected(self.peers.list_mut());
        Ok(bound)
    }

    /// Stops listening on `endpoint`, as libzmq's `zmq_unbind`, and closes
    /// the connections accepted there. What those peers sent is discarded;
    /// what was queued to them is still delivered within the
    /// [`linger`](SocketOptions::linger).
    ///
    /// `endpoint` is as [`bind`](Self::bind) took it or as it returned it:
    /// `tcp://*:5555` and `tcp://0.0.0.0:5555` are the same bind, but a
    /// wildcard port or `ipc://*` must be given as bound. Returns once the
    /// address can be bound again. Fails with [`Error::UnknownEndpoint`] if
    /// the socket is not bound there.
    pub fn unbind(&mut self, endpoint: &str) -> Result<(), Error> {
        let wanted = endpoint::bind_target(endpoint)?.into_endpoint();
        let mut found = Vec::new();
        self.bound.retain(|(bound, id)| {
            let keep = Some(bound) != wanted.as_ref();
            if !keep {
                found.push(*id);
            }
            keep
        });
        if found.is_empty() {
            return Err(Error::UnknownEndpoint(endpoint.to_owned()));
        }
        for id in found {
            self.peers.close(id);
        }
        Ok(())
    }

    /// Connects to `endpoint`, such as `tcp://localhost:5555`,
    /// `ipc:///tmp/feed`, `ipc://@name` (Linux, Android) or
    /// `inproc://name`. Returns at once; messages queue until the connection
    /// is up, and a TCP or IPC connection retries until something listens
    /// and reconnects if it breaks. An `inproc`
    /// connection waits for its name to be bound in this socket's context,
    /// and ends for good when the bound socket is dropped.
    pub fn connect(&mut self, endpoint: &str) -> Result<(), Error> {
        self.connect_key(endpoint).map(drop)
    }

    /// Connects to `endpoint` and returns the new peer, unless
    /// [`immediate`](SocketOptions::immediate) defers it.
    pub(crate) fn connect_key(
        &mut self,
        endpoint: &str,
    ) -> Result<Option<zmq_core::PeerKey>, Error> {
        let target = endpoint::connect_target(endpoint)?;
        let resolved = target
            .clone()
            .into_endpoint()
            .expect("ipc://* is refused when parsing");
        let (id, key) = match target {
            Target::Tcp(addr) => self.peers.connect(addr),
            #[cfg(unix)]
            Target::Ipc(path) => self.peers.connect_ipc(&path),
            #[cfg(not(unix))]
            Target::Ipc(_) => unreachable!("ipc is refused when parsing"),
            Target::IpcAnywhere => unreachable!("refused when parsing"),
            #[cfg(any(target_os = "linux", target_os = "android"))]
            Target::IpcAbstract(name) => self.peers.connect_ipc_abstract(&name),
            Target::Inproc(name) => {
                let (id, key) = self.peers.connect_inproc(&name);
                (id, Some(key))
            }
        };
        self.connected.push((endpoint.to_owned(), resolved, id));
        self.routing.connected(self.peers.list_mut());
        Ok(key)
    }

    /// Stops connecting to `endpoint`, as libzmq's `zmq_disconnect`: closes
    /// each connection made to it, so they no longer reconnect. What those
    /// peers sent is discarded; what was queued to them is still delivered
    /// within the [`linger`](SocketOptions::linger), if they are connected.
    ///
    /// `endpoint` is as [`connect`](Self::connect) took it, or resolves to
    /// the same address. Fails with [`Error::UnknownEndpoint`] if the socket
    /// has no connection to it.
    pub fn disconnect(&mut self, endpoint: &str) -> Result<(), Error> {
        let as_written = self
            .connected
            .iter()
            .any(|(written, ..)| written == endpoint);
        // Resolved only when not given as written, sparing a lookup.
        let resolved = if as_written {
            None
        } else {
            endpoint::connect_target(endpoint)?.into_endpoint()
        };
        let mut found = Vec::new();
        self.connected.retain(|(written, connected, id)| {
            let keep = if as_written {
                written != endpoint
            } else {
                Some(connected) != resolved.as_ref()
            };
            if !keep {
                found.push(*id);
            }
            keep
        });
        if found.is_empty() {
            return Err(Error::UnknownEndpoint(endpoint.to_owned()));
        }
        for id in found {
            self.peers.close(id);
        }
        Ok(())
    }
}

impl Socket<Push> {
    /// Sends `message` if a peer has room, otherwise hands it back.
    pub fn try_send(&mut self, message: impl Into<Message>) -> Result<(), Message> {
        self.try_send_message(message.into())
    }

    /// Sends `message`, blocking while no peer has room.
    pub fn send(&mut self, message: impl Into<Message>) {
        self.send_message(message.into());
    }

    /// Sends `message`, blocking for at most `timeout` while no peer has
    /// room. Hands it back if there is still no room.
    pub fn send_timeout(
        &mut self,
        message: impl Into<Message>,
        timeout: Duration,
    ) -> Result<(), Message> {
        self.send_message_timeout(message.into(), timeout)
    }

    /// Sends `message`, waiting while no peer has room.
    pub async fn send_async(&mut self, message: impl Into<Message>) {
        self.send_message_async(message.into()).await;
    }
}

impl Socket<Pair> {
    /// Sends `message` if there is a peer with room, otherwise hands it
    /// back.
    pub fn try_send(&mut self, message: impl Into<Message>) -> Result<(), Message> {
        self.try_send_message(message.into())
    }

    /// Sends `message`, blocking until there is a peer with room.
    pub fn send(&mut self, message: impl Into<Message>) {
        self.send_message(message.into());
    }

    /// Sends `message`, blocking for at most `timeout` while there is no
    /// peer with room. Hands it back if there is still no room.
    pub fn send_timeout(
        &mut self,
        message: impl Into<Message>,
        timeout: Duration,
    ) -> Result<(), Message> {
        self.send_message_timeout(message.into(), timeout)
    }

    /// Sends `message`, waiting until there is a peer with room.
    pub async fn send_async(&mut self, message: impl Into<Message>) {
        self.send_message_async(message.into()).await;
    }

    /// The next message, if one is waiting.
    pub fn try_recv(&mut self) -> Option<Message> {
        self.try_recv_next()
    }

    /// The next message, blocking until one arrives.
    pub fn recv(&mut self) -> Message {
        self.recv_next()
    }

    /// The next message, blocking for at most `timeout`; `None` if none
    /// arrives in time.
    pub fn recv_timeout(&mut self, timeout: Duration) -> Option<Message> {
        self.recv_next_timeout(timeout)
    }

    /// The next message, waiting until one arrives.
    pub async fn recv_async(&mut self) -> Message {
        self.recv_next_async().await
    }
}

/// A socket type that publishes to subscribers: PUB, XPUB and
/// [`ManualXPub`].
pub trait Publishes: SocketType<Routing = Publisher> {}

impl Publishes for Pub {}
impl Publishes for XPub {}
impl Publishes for ManualXPub {}

impl<T: Publishes> Socket<T> {
    /// Sends `message` to every subscriber that wants it and has room.
    /// Never blocks. Returns how many subscribers it was queued for: none if
    /// nobody has subscribed to it yet, as subscriptions take a moment to
    /// arrive after connecting.
    pub fn send(&mut self, message: impl Into<Message>) -> usize {
        self.peers.accept(&mut noop());
        self.routing.send(self.peers.list_mut(), message.into())
    }

    /// Sends `message` to every subscriber that wants it if all of them
    /// have room, otherwise hands it back, as libzmq's `ZMQ_XPUB_NODROP`.
    /// Returns how many subscribers it was queued for.
    pub fn try_send_all(&mut self, message: impl Into<Message>) -> Result<usize, Message> {
        let mut slot = Some(message.into());
        let poll = self.poll_send_all(&mut slot, &mut noop());
        ready(poll).ok_or_else(|| slot.take().expect("kept while pending"))
    }

    /// Sends `message` to every subscriber that wants it, blocking until
    /// all of them have room, as libzmq's `ZMQ_XPUB_NODROP`. Returns how
    /// many subscribers it was queued for.
    pub fn send_all(&mut self, message: impl Into<Message>) -> usize {
        let mut slot = Some(message.into());
        wait::block_on(|cx| self.poll_send_all(&mut slot, cx))
    }

    /// Sends `message` to every subscriber that wants it, blocking for at
    /// most `timeout` until all of them have room, as libzmq's
    /// `ZMQ_XPUB_NODROP`. Returns how many subscribers it was queued for,
    /// or hands it back if one still has no room.
    pub fn send_all_timeout(
        &mut self,
        message: impl Into<Message>,
        timeout: Duration,
    ) -> Result<usize, Message> {
        let mut slot = Some(message.into());
        wait::block_on_until(wait::deadline(timeout), |cx| {
            self.poll_send_all(&mut slot, cx)
        })
        .ok_or_else(|| slot.take().expect("kept while pending"))
    }

    /// Sends `message` to every subscriber that wants it, waiting until all
    /// of them have room, as libzmq's `ZMQ_XPUB_NODROP`. Returns how many
    /// subscribers it was queued for.
    pub async fn send_all_async(&mut self, message: impl Into<Message>) -> usize {
        let mut slot = Some(message.into());
        future::poll_fn(|cx| self.poll_send_all(&mut slot, cx)).await
    }

    pub(crate) fn poll_send_all(
        &mut self,
        slot: &mut Option<Message>,
        cx: &mut TaskContext<'_>,
    ) -> Poll<usize> {
        self.peers.accept(cx);
        self.routing.poll_send_all(self.peers.list_mut(), slot, cx)
    }

    /// Whether messages go to the subscribers that want none of their
    /// prefixes instead, as libzmq's `ZMQ_INVERT_MATCHING`. Its SUB peers
    /// should invert too, as they filter what arrives. Off by default.
    pub fn set_invert_matching(&mut self, on: bool) {
        self.routing.set_invert_matching(on);
    }
}

impl Socket<Sub> {
    /// Whether messages starting with none of the topics are received
    /// instead, as libzmq's `ZMQ_INVERT_MATCHING`. Its PUB peers must invert
    /// too, or they send only what is then dropped. Off by default.
    pub fn set_invert_matching(&mut self, on: bool) {
        self.routing.set_invert_matching(on);
    }

    /// Subscribes to messages whose first frame starts with `topic`. The
    /// empty topic matches everything. Subscribing to a topic again needs
    /// another [`unsubscribe`](Self::unsubscribe) to undo.
    pub fn subscribe(&mut self, topic: impl AsRef<[u8]>) {
        self.peers.accept(&mut noop());
        self.routing
            .subscribe(self.peers.list_mut(), topic.as_ref());
    }

    /// Removes one subscription to `topic`. Returns whether there was one.
    /// Messages already queued that no longer match are dropped.
    pub fn unsubscribe(&mut self, topic: impl AsRef<[u8]>) -> bool {
        self.peers.accept(&mut noop());
        self.routing
            .unsubscribe(self.peers.list_mut(), topic.as_ref())
    }

    /// The next message on a subscribed topic, if one is waiting.
    pub fn try_recv(&mut self) -> Option<Message> {
        self.try_recv_next()
    }

    /// The next message on a subscribed topic, blocking until one arrives.
    pub fn recv(&mut self) -> Message {
        self.recv_next()
    }

    /// The next message on a subscribed topic, blocking for at most
    /// `timeout`; `None` if none arrives in time.
    pub fn recv_timeout(&mut self, timeout: Duration) -> Option<Message> {
        self.recv_next_timeout(timeout)
    }

    /// The next message on a subscribed topic, waiting until one arrives.
    pub async fn recv_async(&mut self) -> Message {
        self.recv_next_async().await
    }
}

impl Socket<XPub> {
    /// Which subscriptions and cancels [`recv`](Self::recv) reports, as
    /// libzmq's `ZMQ_XPUB_VERBOSE` and `ZMQ_XPUB_VERBOSER`. By default only
    /// a topic's first subscription and last cancel.
    pub fn set_verbosity(&mut self, verbosity: Verbosity) {
        self.routing.set_verbosity(verbosity);
    }

    /// What each peer connected or accepted from now on is sent first, as
    /// libzmq's `ZMQ_XPUB_WELCOME_MSG`. A SUB receives it only if
    /// subscribed to it. None by default.
    pub fn set_welcome(&mut self, welcome: Option<Message>) {
        self.peers.set_greeting(welcome);
    }

    /// The next event, if one is waiting.
    pub fn try_recv(&mut self) -> Option<XPubEvent> {
        self.try_recv_next()
    }

    /// The next event, blocking until one arrives.
    pub fn recv(&mut self) -> XPubEvent {
        self.recv_next()
    }

    /// The next event, blocking for at most `timeout`; `None` if none
    /// arrives in time.
    pub fn recv_timeout(&mut self, timeout: Duration) -> Option<XPubEvent> {
        self.recv_next_timeout(timeout)
    }

    /// The next event, waiting until one arrives.
    pub async fn recv_async(&mut self) -> XPubEvent {
        self.recv_next_async().await
    }
}

impl Socket<ManualXPub> {
    /// What each peer connected or accepted from now on is sent first, as
    /// libzmq's `ZMQ_XPUB_WELCOME_MSG`. None by default.
    pub fn set_welcome(&mut self, welcome: Option<Message>) {
        self.peers.set_greeting(welcome);
    }

    /// The next request, if one is waiting.
    pub fn try_recv(&mut self) -> Option<SubscriptionRequest<'_>> {
        let (peer, event) = self.try_recv_next()?;
        Some(self.lend(peer, event))
    }

    /// The next request, blocking until one arrives.
    pub fn recv(&mut self) -> SubscriptionRequest<'_> {
        let (peer, event) = self.recv_next();
        self.lend(peer, event)
    }

    /// The next request, blocking for at most `timeout`; `None` if none
    /// arrives in time.
    pub fn recv_timeout(&mut self, timeout: Duration) -> Option<SubscriptionRequest<'_>> {
        let (peer, event) = self.recv_next_timeout(timeout)?;
        Some(self.lend(peer, event))
    }

    /// The next request, waiting until one arrives.
    pub async fn recv_async(&mut self) -> SubscriptionRequest<'_> {
        let (peer, event) = self.recv_next_async().await;
        self.lend(peer, event)
    }

    /// Lends the socket to what a peer sent, to act on it.
    fn lend(
        &mut self,
        peer: Option<zmq_core::PeerKey>,
        event: XPubEvent,
    ) -> SubscriptionRequest<'_> {
        SubscriptionRequest {
            socket: self,
            peer,
            event,
        }
    }
}

/// What a peer of a [`ManualXPub`] sent: a subscription or cancel to decide
/// on, or a message. Acting on it affects that peer alone; dropping it
/// leaves the peer's subscriptions as they were. Once the peer has gone,
/// acting on it does nothing.
#[must_use = "a subscription has no effect until accepted"]
pub struct SubscriptionRequest<'a> {
    socket: &'a mut Socket<ManualXPub>,
    peer: Option<zmq_core::PeerKey>,
    event: XPubEvent,
}

impl SubscriptionRequest<'_> {
    /// What the peer sent.
    pub fn event(&self) -> &XPubEvent {
        &self.event
    }

    /// Does what the peer asked: subscribes or unsubscribes it. Returns
    /// whether the peer is still there; a message asks for nothing, so
    /// accepting it does nothing.
    pub fn accept(&mut self) -> bool {
        match &self.event {
            XPubEvent::Subscribe(topic) => self.apply(Subscription::Subscribe(topic.clone())),
            XPubEvent::Cancel(topic) => self.apply(Subscription::Cancel(topic.clone())),
            XPubEvent::Message(_) => self.peer.is_some(),
        }
    }

    /// Subscribes the peer to `topic`. Returns whether the peer is still
    /// there.
    pub fn subscribe(&mut self, topic: impl Into<Frame>) -> bool {
        self.apply(Subscription::Subscribe(topic.into()))
    }

    /// Unsubscribes the peer from `topic`. Returns whether the peer is
    /// still there.
    pub fn unsubscribe(&mut self, topic: impl Into<Frame>) -> bool {
        self.apply(Subscription::Cancel(topic.into()))
    }

    /// Sends `message` to this peer alone, if it is subscribed to it and
    /// has room, as libzmq's `ZMQ_XPUB_MANUAL_LAST_VALUE` does after a
    /// subscription. Returns whether it was queued.
    pub fn send(&mut self, message: impl Into<Message>) -> bool {
        let Some(peer) = self.peer else {
            return false;
        };
        let socket = &mut *self.socket;
        socket
            .routing
            .send_to(socket.peers.list_mut(), peer, message.into())
    }

    fn apply(&mut self, subscription: Subscription) -> bool {
        self.peer
            .is_some_and(|peer| self.socket.routing.apply(peer, subscription))
    }
}

impl fmt::Debug for SubscriptionRequest<'_> {
    fn fmt(&self, f: &mut fmt::Formatter<'_>) -> fmt::Result {
        f.debug_struct("SubscriptionRequest")
            .field("event", &self.event)
            .finish_non_exhaustive()
    }
}

impl Socket<XSub> {
    /// Does nothing: an XSUB delivers every message, as libzmq's does, so
    /// there is nothing to invert. Invert the matching on its publishers.
    #[deprecated(note = "an XSUB delivers every message, so there is nothing to invert")]
    pub fn set_invert_matching(&mut self, on: bool) {
        let _ = on;
    }

    /// Asks its publishers for messages whose first frame starts with
    /// `topic`, as [`Socket<Sub>::subscribe`] does. Unlike SUB, it does not
    /// filter what arrives: as libzmq's XSUB, it delivers every message.
    pub fn subscribe(&mut self, topic: impl AsRef<[u8]>) {
        self.peers.accept(&mut noop());
        self.routing
            .subscribe(self.peers.list_mut(), topic.as_ref());
    }

    /// Removes one subscription to `topic`. Returns whether there was one.
    pub fn unsubscribe(&mut self, topic: impl AsRef<[u8]>) -> bool {
        self.peers.accept(&mut noop());
        self.routing
            .unsubscribe(self.peers.list_mut(), topic.as_ref())
    }

    /// Sends `message` to every publisher. Never blocks. As in libzmq, a
    /// single frame starting with `0x01` or `0x00` is a subscription and
    /// subscribes or unsubscribes to the rest.
    pub fn send(&mut self, message: impl Into<Message>) {
        self.peers.accept(&mut noop());
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

    /// The next message, if one is waiting.
    pub fn try_recv(&mut self) -> Option<Message> {
        self.try_recv_next()
    }

    /// The next message, blocking until one arrives.
    pub fn recv(&mut self) -> Message {
        self.recv_next()
    }

    /// The next message, blocking for at most `timeout`; `None` if none
    /// arrives in time.
    pub fn recv_timeout(&mut self, timeout: Duration) -> Option<Message> {
        self.recv_next_timeout(timeout)
    }

    /// The next message, waiting until one arrives.
    pub async fn recv_async(&mut self) -> Message {
        self.recv_next_async().await
    }
}

impl Socket<Pull> {
    /// The next message, if one is waiting.
    pub fn try_recv(&mut self) -> Option<Message> {
        self.try_recv_next()
    }

    /// The next message, blocking until one arrives.
    pub fn recv(&mut self) -> Message {
        self.recv_next()
    }

    /// The next message, blocking for at most `timeout`; `None` if none
    /// arrives in time.
    pub fn recv_timeout(&mut self, timeout: Duration) -> Option<Message> {
        self.recv_next_timeout(timeout)
    }

    /// The next message, waiting until one arrives.
    pub async fn recv_async(&mut self) -> Message {
        self.recv_next_async().await
    }
}

impl Socket<Dealer> {
    /// Sends `message` if a peer has room, otherwise hands it back.
    pub fn try_send(&mut self, message: impl Into<Message>) -> Result<(), Message> {
        self.try_send_message(message.into())
    }

    /// Sends `message`, blocking while no peer has room.
    pub fn send(&mut self, message: impl Into<Message>) {
        self.send_message(message.into());
    }

    /// Sends `message`, blocking for at most `timeout` while no peer has
    /// room. Hands it back if there is still no room.
    pub fn send_timeout(
        &mut self,
        message: impl Into<Message>,
        timeout: Duration,
    ) -> Result<(), Message> {
        self.send_message_timeout(message.into(), timeout)
    }

    /// Sends `message`, waiting while no peer has room.
    pub async fn send_async(&mut self, message: impl Into<Message>) {
        self.send_message_async(message.into()).await;
    }

    /// The next message, if one is waiting.
    pub fn try_recv(&mut self) -> Option<Message> {
        self.try_recv_next()
    }

    /// The next message, blocking until one arrives.
    pub fn recv(&mut self) -> Message {
        self.recv_next()
    }

    /// The next message, blocking for at most `timeout`; `None` if none
    /// arrives in time.
    pub fn recv_timeout(&mut self, timeout: Duration) -> Option<Message> {
        self.recv_next_timeout(timeout)
    }

    /// The next message, waiting until one arrives.
    pub async fn recv_async(&mut self) -> Message {
        self.recv_next_async().await
    }
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

    /// Sends `message` to the next peer with room and waits for its reply,
    /// blocking the thread for at most `timeout` in all. On timeout the
    /// request is abandoned and the socket stays usable, unlike libzmq's
    /// REQ after `ZMQ_RCVTIMEO`.
    ///
    /// Turns [correlation](Self::set_correlation) on for good, so that the
    /// reply to a timed-out request is never taken for a later one's. From
    /// then on a ROUTER application serving this socket sees the request
    /// id as an extra envelope frame; REP peers do not notice.
    pub fn request_timeout(
        &mut self,
        message: impl Into<Message>,
        timeout: Duration,
    ) -> Result<Message, RequestError> {
        self.set_correlation(true);
        let mut state = RequestState::Sending(Some(message.into()));
        match wait::block_on_until(wait::deadline(timeout), |cx| {
            self.poll_request(&mut state, cx)
        }) {
            Some(Ok(reply)) => Ok(reply),
            Some(Err(PeerGone)) => Err(RequestError::PeerGone),
            None => Err(RequestError::TimedOut),
        }
    }

    /// Sends `message` to the next peer with room and waits for its reply.
    /// If this future is dropped before the reply arrives, the request is
    /// abandoned; see [`set_correlation`](Self::set_correlation).
    pub async fn request_async(
        &mut self,
        message: impl Into<Message>,
    ) -> Result<Message, PeerGone> {
        let mut state = RequestState::Sending(Some(message.into()));
        future::poll_fn(|cx| self.poll_request(&mut state, cx)).await
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
                    let body = slot.take().expect("a send put back its message to retry");
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
    /// Always `Some` between polls: taken only to send, and put back if no
    /// peer has room.
    Sending(Option<Message>),
    Waiting(Outstanding),
}

impl Socket<Rep> {
    /// The next request, if one is waiting.
    pub fn try_recv(&mut self) -> Option<Request<'_>> {
        let received = self.try_recv_next()?;
        Some(self.lend(received))
    }

    /// The next request, blocking until one arrives.
    pub fn recv(&mut self) -> Request<'_> {
        let received = self.recv_next();
        self.lend(received)
    }

    /// The next request, blocking for at most `timeout`; `None` if none
    /// arrives in time.
    pub fn recv_timeout(&mut self, timeout: Duration) -> Option<Request<'_>> {
        let received = self.recv_next_timeout(timeout)?;
        Some(self.lend(received))
    }

    /// The next request, waiting until one arrives.
    pub async fn recv_async(&mut self) -> Request<'_> {
        let received = self.recv_next_async().await;
        self.lend(received)
    }

    /// Lends the socket to a request, to answer it.
    fn lend(&mut self, received: Received) -> Request<'_> {
        Request {
            socket: self,
            received,
        }
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

/// A send to one peer failed: no peer has that routing id, client id or
/// peer id, or it went away while the message waited (libzmq's
/// `EHOSTUNREACH`). What was being sent is handed back.
#[derive(Debug, Clone, PartialEq, Eq)]
pub struct UnknownPeer<T>(pub T);

impl<T> fmt::Display for UnknownPeer<T> {
    fn fmt(&self, f: &mut fmt::Formatter<'_>) -> fmt::Result {
        f.write_str("no peer has that id")
    }
}

impl<T: fmt::Debug> std::error::Error for UnknownPeer<T> {}

/// Why [`Socket::request_timeout`] got no reply.
#[derive(Debug, Clone, Copy, PartialEq, Eq)]
pub enum RequestError {
    /// The peer the request went to closed before replying.
    PeerGone,
    /// No reply arrived in time. The request is abandoned.
    TimedOut,
}

impl fmt::Display for RequestError {
    fn fmt(&self, f: &mut fmt::Formatter<'_>) -> fmt::Result {
        match self {
            Self::PeerGone => PeerGone.fmt(f),
            Self::TimedOut => f.write_str("no reply in time"),
        }
    }
}

impl std::error::Error for RequestError {}

impl Socket<Router> {
    /// Sends `message` to the peer with routing id `to` if it is known and
    /// has room, otherwise hands it back. Never blocks.
    pub fn try_send(
        &mut self,
        to: &RoutingId,
        message: impl Into<Message>,
    ) -> Result<(), RouterSendError> {
        self.peers.accept(&mut noop());
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
    ) -> Result<(), UnknownPeer<Message>> {
        let mut slot = Some(message.into());
        wait::block_on(|cx| self.poll_send(to, &mut slot, cx)).map_err(UnknownPeer)
    }

    /// Sends `message` to the peer with routing id `to`, blocking for at
    /// most `timeout` while its queue is full. Fails at once if no peer has
    /// that routing id, and with `Full` if there is still no room.
    pub fn send_timeout(
        &mut self,
        to: &RoutingId,
        message: impl Into<Message>,
        timeout: Duration,
    ) -> Result<(), RouterSendError> {
        let mut slot = Some(message.into());
        match wait::block_on_until(wait::deadline(timeout), |cx| {
            self.poll_send(to, &mut slot, cx)
        }) {
            Some(Ok(())) => Ok(()),
            Some(Err(message)) => Err(RouterSendError::UnknownPeer(message)),
            None => Err(RouterSendError::Full(
                slot.take().expect("kept while pending"),
            )),
        }
    }

    /// Sends `message` to the peer with routing id `to`, waiting while its
    /// queue is full. Fails if no peer has that routing id, or it goes away
    /// meanwhile.
    pub async fn send_async(
        &mut self,
        to: &RoutingId,
        message: impl Into<Message>,
    ) -> Result<(), UnknownPeer<Message>> {
        let mut slot = Some(message.into());
        future::poll_fn(|cx| self.poll_send(to, &mut slot, cx))
            .await
            .map_err(UnknownPeer)
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
        self.try_recv_next()
    }

    /// The next message and its sender's routing id, blocking until one
    /// arrives.
    pub fn recv(&mut self) -> (RoutingId, Message) {
        self.recv_next()
    }

    /// The next message and its sender's routing id, blocking for at most
    /// `timeout`; `None` if none arrives in time.
    pub fn recv_timeout(&mut self, timeout: Duration) -> Option<(RoutingId, Message)> {
        self.recv_next_timeout(timeout)
    }

    /// The next message and its sender's routing id, waiting until one
    /// arrives.
    pub async fn recv_async(&mut self) -> (RoutingId, Message) {
        self.recv_next_async().await
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

pub(crate) fn noop() -> TaskContext<'static> {
    zmq_core::noop_context()
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
