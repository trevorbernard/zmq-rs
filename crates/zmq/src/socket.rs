//! Typed sockets: each type has only the operations its pattern allows.

use std::convert::Infallible;
use std::fmt;
use std::future;
use std::marker::PhantomData;
use std::task::{Context as TaskContext, Poll};
use std::time::Duration;

use zmq_core::{
    ConnectionConfig, EndpointId, FairQueue, LoadBalancer, Outstanding, Pair as PairRouting,
    PeerGone, PeerList, Peers, Publisher, Received, Replier, Requester, RouteOutcome, RouterTable,
    SendError, Subscriber, Verbosity, XPubEvent,
};
use zmtp::{Frame, Message, Subscription};

use crate::endpoint::Target;
use crate::polling::{PollItem, PollReadable, PollWritable};
use crate::{Endpoint, Error, RoutingId, SocketOptions, endpoint, wait};

pub(crate) mod sealed {
    pub trait Sealed {}

    /// A socket that learns when its context is shut down.
    pub trait Terminates {
        /// Whether the context is shut down; if not, a shutdown wakes `cx`.
        fn poll_terminated(&self, cx: &mut std::task::Context<'_>) -> bool;
    }
}

impl<T: SocketType> sealed::Sealed for Socket<T> {}

impl<T: SocketType> sealed::Terminates for Socket<T> {
    fn poll_terminated(&self, cx: &mut TaskContext<'_>) -> bool {
        self.peers.poll_terminated(cx)
    }
}

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

impl<T: SocketType> Socket<T> {
    /// Fails once the context is shut down.
    pub(crate) fn alive(&self) -> Result<(), Terminated> {
        if self.peers.is_terminated() {
            Err(Terminated)
        } else {
            Ok(())
        }
    }

    /// Polls `poll` unless the context is shut down. While it is pending, a
    /// shutdown wakes `cx`, and the next poll fails.
    pub(crate) fn poll_alive<R>(
        &mut self,
        cx: &mut TaskContext<'_>,
        poll: impl FnOnce(&mut Self, &mut TaskContext<'_>) -> Poll<R>,
    ) -> Poll<Result<R, Terminated>> {
        self.alive()?;
        match poll(self, cx) {
            Poll::Ready(output) => Poll::Ready(Ok(output)),
            Poll::Pending if self.peers.poll_terminated(cx) => Poll::Ready(Err(Terminated)),
            Poll::Pending => Poll::Pending,
        }
    }
}

impl<T: Receives> Socket<T> {
    /// What a poll for readability received, or else the next.
    pub(crate) fn poll_recv(&mut self, cx: &mut TaskContext<'_>) -> Poll<T::Received> {
        if let Some(received) = self.pending.take() {
            return Poll::Ready(received);
        }
        T::poll_next(self, cx)
    }

    pub(crate) fn try_recv_next(&mut self) -> Result<Option<T::Received>, Terminated> {
        self.alive()?;
        Ok(ready(self.poll_recv(&mut noop())))
    }

    pub(crate) fn recv_next(&mut self) -> Result<T::Received, Terminated> {
        wait::block_on(|cx| self.poll_alive(cx, |socket, cx| socket.poll_recv(cx)))
    }

    pub(crate) fn recv_next_timeout(
        &mut self,
        timeout: Duration,
    ) -> Result<Option<T::Received>, Terminated> {
        wait::block_on_until(wait::deadline(timeout), |cx| {
            self.poll_alive(cx, |socket, cx| socket.poll_recv(cx))
        })
        .transpose()
    }

    pub(crate) async fn recv_next_async(&mut self) -> Result<T::Received, Terminated> {
        future::poll_fn(|cx| self.poll_alive(cx, |socket, cx| socket.poll_recv(cx))).await
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

    pub(crate) fn try_send_message(
        &mut self,
        message: Message,
    ) -> Result<(), TrySendError<Message>> {
        if self.peers.is_terminated() {
            return Err(TrySendError::Terminated(message));
        }
        let mut slot = Some(message);
        let _ = self.poll_send(&mut slot, &mut noop());
        slot.map_or(Ok(()), |message| Err(TrySendError::Full(message)))
    }

    pub(crate) fn send_message(&mut self, message: Message) -> Result<(), Terminated> {
        let mut slot = Some(message);
        wait::block_on(|cx| self.poll_alive(cx, |socket, cx| socket.poll_send(&mut slot, cx)))
    }

    pub(crate) fn send_message_timeout(
        &mut self,
        message: Message,
        timeout: Duration,
    ) -> Result<(), TrySendError<Message>> {
        let mut slot = Some(message);
        let sent = wait::block_on_until(wait::deadline(timeout), |cx| {
            self.poll_alive(cx, |socket, cx| socket.poll_send(&mut slot, cx))
        });
        unsent(sent, &mut slot)
    }

    pub(crate) async fn send_message_async(&mut self, message: Message) -> Result<(), Terminated> {
        let mut slot = Some(message);
        future::poll_fn(|cx| self.poll_alive(cx, |socket, cx| socket.poll_send(&mut slot, cx)))
            .await
    }
}

/// What a send that waited until a deadline came to: `None` if it was still
/// waiting. A send that did not go through left its message in `slot`.
pub(crate) fn unsent<R, M>(
    sent: Option<Result<R, Terminated>>,
    slot: &mut Option<M>,
) -> Result<R, TrySendError<M>> {
    let mut kept = || slot.take().expect("kept while pending");
    match sent {
        Some(Ok(sent)) => Ok(sent),
        Some(Err(Terminated)) => Err(TrySendError::Terminated(kept())),
        None => Err(TrySendError::Full(kept())),
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
    ) -> Poll<(zmq_core::PeerKey, XPubEvent)> {
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
        socket.peers.accept(cx);
        socket.routing.poll_ready(socket.peers.list_mut(), cx)
    }

    fn poll_send(
        socket: &mut Socket<Self>,
        slot: &mut Option<Message>,
        cx: &mut TaskContext<'_>,
    ) -> Poll<()> {
        socket.peers.accept(cx);
        poll_load_balanced(socket.peers.list_mut(), &mut socket.routing, slot, cx)
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
        socket.peers.accept(cx);
        poll_load_balanced(socket.peers.list_mut(), &mut socket.routing.send, slot, cx)
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

impl<T: Receives + WaitsForRoom> Socket<T> {
    /// This socket, to [`poll`](crate::poll()) until a receive or a send
    /// would not wait, as libzmq's `ZMQ_POLLIN | ZMQ_POLLOUT`. Once the
    /// poll reports it, try both: `try_recv` returns a message if one is
    /// waiting, and `try_send` succeeds if a peer has room.
    ///
    /// ```
    /// # use zmq::{Context, Dealer};
    /// # fn main() -> Result<(), Box<dyn std::error::Error>> {
    /// let context = Context::new()?;
    /// let mut a = context.socket::<Dealer>();
    /// a.bind("inproc://both")?;
    /// let mut b = context.socket::<Dealer>();
    /// b.connect("inproc://both")?;
    /// // Nothing to receive yet, but room to send.
    /// assert_eq!(zmq::poll([b.readable_or_writable()])?, [true]);
    /// assert!(b.try_recv()?.is_none());
    /// assert!(b.try_send("hello").is_ok());
    /// # Ok(())
    /// # }
    /// ```
    pub fn readable_or_writable(&mut self) -> PollItem<'_> {
        PollItem::either(self)
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
/// dropped: they go to peers whatever its send HWM.
#[derive(Debug)]
pub enum Sub {}

/// A PUB that also receives: each topic's first subscription and last
/// cancellation across its peers, and whatever XSUB peers send upstream,
/// as [`XPubEvent`]s (RFC 29). Used to build proxies and to see what
/// subscribers want.
#[derive(Debug)]
pub enum XPub {}

/// A SUB that also sends: [`send`](Socket::send) goes to every publisher
/// (RFC 29). Used with [`XPub`] to build proxies. Subscriptions are never
/// dropped; other messages are, for a publisher at the send HWM, as in
/// libzmq.
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
    type Received = (zmq_core::PeerKey, XPubEvent);

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

    /// PUSH never receives: what peers send is discarded.
    fn routing() -> LoadBalancer {
        LoadBalancer::send_only()
    }
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
    /// What it connected to, as written and as parsed, for `disconnect`.
    connected: Vec<(String, Target, EndpointId)>,
    /// What a poll for readability received, which the next receive
    /// returns.
    pub(crate) pending: Option<T::Received>,
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
    pub(crate) fn new(context: zmq_core::Context, options: SocketOptions) -> Self {
        let config = ConnectionConfig {
            identity: options.identity,
            send_hwm: options.send_hwm,
            recv_hwm: options.recv_hwm,
            // PEER's connect_peer names the peer at once.
            immediate: options.immediate && !is_peer(T::KIND),
            linger: options.linger,
            reconnect: options.reconnect,
            max_message_size: options.max_message_size,
            message_properties: options.message_properties,
            security: options.security,
            heartbeat: options.heartbeat,
            tcp_keepalive: options.tcp_keepalive,
            tcp: zmq_core::TcpTuning {
                type_of_service: options.type_of_service,
                bind_to_device: options.bind_to_device,
                busy_poll: None,
            },
            socks_proxy: options.socks_proxy.map(|proxy| proxy.0),
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

    /// When connections made from now on give up instead of reconnecting,
    /// as libzmq's `ZMQ_RECONNECT_STOP`, a draft option. Never by default:
    /// they retry as [`SocketOptions::reconnect`] says.
    ///
    /// A connection that gives up ends as one disconnected does. libzmq's
    /// `ZMQ_RECONNECT_STOP_AFTER_DISCONNECT` needs no flag here:
    /// [`disconnect`](Self::disconnect) always ends the retrying.
    #[cfg(feature = "draft")]
    pub fn set_reconnect_stop(&mut self, stop: crate::ReconnectStop) {
        self.peers.config_mut().reconnect_stop = stop;
    }

    /// How long a blocking receive on a TCP socket busy-polls its device
    /// before sleeping, as libzmq's `ZMQ_BUSY_POLL`, a draft option
    /// (Linux's `SO_BUSY_POLL`), for binds and connects made from now on.
    /// `None`, the default, leaves the system's. Ignored where the system
    /// does not support or permit it.
    #[cfg(feature = "draft")]
    pub fn set_busy_poll(&mut self, busy_poll: Option<Duration>) {
        self.peers.config_mut().tcp.busy_poll = busy_poll;
    }

    /// Adds a property announced to the peers of binds and connects made
    /// from now on, as libzmq's `ZMQ_METADATA`, a draft option. A peer reads
    /// it from the messages it receives, with [`Message::property`].
    ///
    /// The name must start with `X-` and the value must not be empty. A
    /// name already added keeps its first value, as in libzmq.
    #[cfg(feature = "draft")]
    pub fn add_metadata(
        &mut self,
        name: impl AsRef<[u8]>,
        value: impl AsRef<[u8]>,
    ) -> Result<(), crate::InvalidMetadata> {
        self.peers.config_mut().add_metadata(name, value)
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
    /// `tcp://eth0:5555` (an interface), `ipc:///tmp/feed`, `ipc://*`, `ipc://@name` (Linux, Android) or
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
        self.alive()?;
        let (id, bound) = match endpoint::bind_target(endpoint)? {
            Target::Tcp(addr) => self
                .peers
                .bind(addr)
                .map(|(id, addr)| (id, Endpoint::Tcp(addr))),
            Target::TcpHost { .. } => unreachable!("a bind resolves its host at once"),
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
    /// `tcp://eth0:*;localhost:5555` (from an interface or address),
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
        self.alive()?;
        let target = endpoint::connect_target(endpoint)?;
        let (id, key) = match &target {
            Target::Tcp(addr) => self.peers.connect(*addr),
            Target::TcpHost { host, port, source } => self.peers.connect_host(host, *port, *source),
            #[cfg(unix)]
            Target::Ipc(path) => self.peers.connect_ipc(path),
            #[cfg(not(unix))]
            Target::Ipc(_) => unreachable!("ipc is refused when parsing"),
            Target::IpcAnywhere => unreachable!("refused when parsing"),
            #[cfg(any(target_os = "linux", target_os = "android"))]
            Target::IpcAbstract(name) => self.peers.connect_ipc_abstract(name),
            Target::Inproc(name) => {
                let (id, key) = self.peers.connect_inproc(name);
                (id, Some(key))
            }
        };
        self.connected.push((endpoint.to_owned(), target, id));
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
        // Parsed and resolved only when not given as written, sparing a
        // lookup.
        let wanted = if as_written {
            None
        } else {
            Some(endpoint::connect_target(endpoint)?)
        };
        let wanted_addr = wanted.as_ref().and_then(Target::resolve);
        let mut found = Vec::new();
        self.connected.retain(|(written, connected, id)| {
            let keep = match &wanted {
                None => written != endpoint,
                Some(wanted) => !connected.names(wanted, wanted_addr),
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
    pub fn try_send(&mut self, message: impl Into<Message>) -> Result<(), TrySendError<Message>> {
        self.try_send_message(message.into())
    }

    /// Sends `message`, blocking while no peer has room.
    pub fn send(&mut self, message: impl Into<Message>) -> Result<(), Terminated> {
        self.send_message(message.into())
    }

    /// Sends `message`, blocking for at most `timeout` while no peer has
    /// room. Hands it back if there is still no room.
    pub fn send_timeout(
        &mut self,
        message: impl Into<Message>,
        timeout: Duration,
    ) -> Result<(), TrySendError<Message>> {
        self.send_message_timeout(message.into(), timeout)
    }

    /// Sends `message`, waiting while no peer has room.
    pub async fn send_async(&mut self, message: impl Into<Message>) -> Result<(), Terminated> {
        self.send_message_async(message.into()).await
    }
}

impl Socket<Pair> {
    /// Sends `message` if there is a peer with room, otherwise hands it
    /// back.
    pub fn try_send(&mut self, message: impl Into<Message>) -> Result<(), TrySendError<Message>> {
        self.try_send_message(message.into())
    }

    /// Sends `message`, blocking until there is a peer with room.
    pub fn send(&mut self, message: impl Into<Message>) -> Result<(), Terminated> {
        self.send_message(message.into())
    }

    /// Sends `message`, blocking for at most `timeout` while there is no
    /// peer with room. Hands it back if there is still no room.
    pub fn send_timeout(
        &mut self,
        message: impl Into<Message>,
        timeout: Duration,
    ) -> Result<(), TrySendError<Message>> {
        self.send_message_timeout(message.into(), timeout)
    }

    /// Sends `message`, waiting until there is a peer with room.
    pub async fn send_async(&mut self, message: impl Into<Message>) -> Result<(), Terminated> {
        self.send_message_async(message.into()).await
    }

    /// The next message, if one is waiting.
    pub fn try_recv(&mut self) -> Result<Option<Message>, Terminated> {
        self.try_recv_next()
    }

    /// The next message, blocking until one arrives.
    pub fn recv(&mut self) -> Result<Message, Terminated> {
        self.recv_next()
    }

    /// The next message, blocking for at most `timeout`; `None` if none
    /// arrives in time.
    pub fn recv_timeout(&mut self, timeout: Duration) -> Result<Option<Message>, Terminated> {
        self.recv_next_timeout(timeout)
    }

    /// The next message, waiting until one arrives.
    pub async fn recv_async(&mut self) -> Result<Message, Terminated> {
        self.recv_next_async().await
    }
}

/// A socket type that can greet each peer, as libzmq's `ZMQ_HELLO_MSG`
/// lets these do: DEALER, ROUTER, CLIENT, SERVER and PEER.
#[cfg(feature = "draft")]
pub trait SendsHello: SocketType {}

/// A socket type that can hear of a peer it accepted leaving, as libzmq's
/// `ZMQ_DISCONNECT_MSG` lets these do: ROUTER, SERVER and PEER.
#[cfg(feature = "draft")]
pub trait HearsDisconnects: SocketType {}

/// A socket type that can hear of a connection it made breaking, as
/// libzmq's `ZMQ_HICCUP_MSG` lets these do: DEALER, CLIENT and PEER.
#[cfg(feature = "draft")]
pub trait HearsHiccups: SocketType {}

#[cfg(feature = "draft")]
mod farewells {
    use super::{Dealer, HearsDisconnects, HearsHiccups, Router, SendsHello};
    use crate::draft::{Client, Peer, Server};

    impl SendsHello for Dealer {}
    impl SendsHello for Router {}
    impl SendsHello for Client {}
    impl SendsHello for Server {}
    impl SendsHello for Peer {}

    impl HearsDisconnects for Router {}
    impl HearsDisconnects for Server {}
    impl HearsDisconnects for Peer {}

    impl HearsHiccups for Dealer {}
    impl HearsHiccups for Client {}
    impl HearsHiccups for Peer {}
}

#[cfg(feature = "draft")]
impl<T: SendsHello> Socket<T> {
    /// What each peer of the binds and connects made from now on is sent
    /// first, on every connection and so again after each reconnection, as
    /// libzmq's `ZMQ_HELLO_MSG`, a draft option. One frame; empty, the
    /// default, sends nothing.
    pub fn set_hello_message(&mut self, hello: impl AsRef<[u8]>) {
        self.peers.config_mut().hello_message = Frame::copy_from_slice(hello.as_ref());
    }
}

#[cfg(feature = "draft")]
impl<T: HearsDisconnects> Socket<T> {
    /// What this socket receives from a peer when that peer's connection
    /// ends, after everything the peer sent, as libzmq's
    /// `ZMQ_DISCONNECT_MSG`, a draft option. One frame; empty, the default,
    /// delivers nothing.
    ///
    /// As in libzmq, only peers this socket accepted leave it, and only
    /// those whose handshake had completed: over `inproc`, those that
    /// connected to a name this socket bound. It applies to binds made
    /// from now on.
    pub fn set_disconnect_message(&mut self, disconnect: impl AsRef<[u8]>) {
        self.peers.config_mut().disconnect_message = Frame::copy_from_slice(disconnect.as_ref());
    }
}

#[cfg(feature = "draft")]
impl<T: HearsHiccups> Socket<T> {
    /// What this socket receives from a peer it connected to each time that
    /// connection breaks, before it reconnects, as libzmq's
    /// `ZMQ_HICCUP_MSG`, a draft option. One frame; empty, the default,
    /// delivers nothing.
    ///
    /// As in libzmq, only a connection whose handshake had completed leaves
    /// it, and `inproc` connections, which never break, never do. It
    /// applies to connects made from now on.
    pub fn set_hiccup_message(&mut self, hiccup: impl AsRef<[u8]>) {
        self.peers.config_mut().hiccup_message = Frame::copy_from_slice(hiccup.as_ref());
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
    pub fn send(&mut self, message: impl Into<Message>) -> Result<usize, Terminated> {
        self.alive()?;
        self.peers.accept(&mut noop());
        Ok(self.routing.send(self.peers.list_mut(), message.into()))
    }

    /// Sends `message` to every subscriber that wants it if all of them
    /// have room, otherwise hands it back, as libzmq's `ZMQ_XPUB_NODROP`.
    /// Returns how many subscribers it was queued for.
    pub fn try_send_all(
        &mut self,
        message: impl Into<Message>,
    ) -> Result<usize, TrySendError<Message>> {
        let mut slot = Some(message.into());
        let sent = match self.alive() {
            Ok(()) => ready(self.poll_send_all(&mut slot, &mut noop())).map(Ok),
            Err(terminated) => Some(Err(terminated)),
        };
        unsent(sent, &mut slot)
    }

    /// Sends `message` to every subscriber that wants it, blocking until
    /// all of them have room, as libzmq's `ZMQ_XPUB_NODROP`. Returns how
    /// many subscribers it was queued for.
    pub fn send_all(&mut self, message: impl Into<Message>) -> Result<usize, Terminated> {
        let mut slot = Some(message.into());
        wait::block_on(|cx| self.poll_alive(cx, |socket, cx| socket.poll_send_all(&mut slot, cx)))
    }

    /// Sends `message` to every subscriber that wants it, blocking for at
    /// most `timeout` until all of them have room, as libzmq's
    /// `ZMQ_XPUB_NODROP`. Returns how many subscribers it was queued for,
    /// or hands it back if one still has no room.
    pub fn send_all_timeout(
        &mut self,
        message: impl Into<Message>,
        timeout: Duration,
    ) -> Result<usize, TrySendError<Message>> {
        let mut slot = Some(message.into());
        let sent = wait::block_on_until(wait::deadline(timeout), |cx| {
            self.poll_alive(cx, |socket, cx| socket.poll_send_all(&mut slot, cx))
        });
        unsent(sent, &mut slot)
    }

    /// Sends `message` to every subscriber that wants it, waiting until all
    /// of them have room, as libzmq's `ZMQ_XPUB_NODROP`. Returns how many
    /// subscribers it was queued for.
    pub async fn send_all_async(
        &mut self,
        message: impl Into<Message>,
    ) -> Result<usize, Terminated> {
        let mut slot = Some(message.into());
        future::poll_fn(|cx| self.poll_alive(cx, |socket, cx| socket.poll_send_all(&mut slot, cx)))
            .await
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

    /// How many distinct topics its subscribers hold between them, as
    /// libzmq's `ZMQ_TOPICS_COUNT`, a draft option. Subscriptions take a
    /// moment to arrive; those that have are counted.
    #[cfg(feature = "draft")]
    pub fn topics_count(&mut self) -> usize {
        self.peers.accept(&mut noop());
        self.routing.sync(self.peers.list_mut());
        self.routing.topics_count()
    }
}

impl Socket<Sub> {
    /// How many distinct topics it is subscribed to, as libzmq's
    /// `ZMQ_TOPICS_COUNT`, a draft option. A topic subscribed twice counts
    /// once.
    #[cfg(feature = "draft")]
    #[must_use]
    pub fn topics_count(&self) -> usize {
        self.routing.topics_count()
    }

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
        let removed = self
            .routing
            .unsubscribe(self.peers.list_mut(), topic.as_ref());
        // One that a poll already took counts as queued.
        if self
            .pending
            .as_ref()
            .is_some_and(|message| !self.routing.wants(message))
        {
            self.pending = None;
        }
        removed
    }

    /// The next message on a subscribed topic, if one is waiting.
    pub fn try_recv(&mut self) -> Result<Option<Message>, Terminated> {
        self.try_recv_next()
    }

    /// The next message on a subscribed topic, blocking until one arrives.
    pub fn recv(&mut self) -> Result<Message, Terminated> {
        self.recv_next()
    }

    /// The next message on a subscribed topic, blocking for at most
    /// `timeout`; `None` if none arrives in time.
    pub fn recv_timeout(&mut self, timeout: Duration) -> Result<Option<Message>, Terminated> {
        self.recv_next_timeout(timeout)
    }

    /// The next message on a subscribed topic, waiting until one arrives.
    pub async fn recv_async(&mut self) -> Result<Message, Terminated> {
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
    pub fn try_recv(&mut self) -> Result<Option<XPubEvent>, Terminated> {
        self.try_recv_next()
    }

    /// The next event, blocking until one arrives.
    pub fn recv(&mut self) -> Result<XPubEvent, Terminated> {
        self.recv_next()
    }

    /// The next event, blocking for at most `timeout`; `None` if none
    /// arrives in time.
    pub fn recv_timeout(&mut self, timeout: Duration) -> Result<Option<XPubEvent>, Terminated> {
        self.recv_next_timeout(timeout)
    }

    /// The next event, waiting until one arrives.
    pub async fn recv_async(&mut self) -> Result<XPubEvent, Terminated> {
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
    pub fn try_recv(&mut self) -> Result<Option<SubscriptionRequest<'_>>, Terminated> {
        Ok(self
            .try_recv_next()?
            .map(|(peer, event)| self.lend(peer, event)))
    }

    /// The next request, blocking until one arrives.
    pub fn recv(&mut self) -> Result<SubscriptionRequest<'_>, Terminated> {
        let (peer, event) = self.recv_next()?;
        Ok(self.lend(peer, event))
    }

    /// The next request, blocking for at most `timeout`; `None` if none
    /// arrives in time.
    pub fn recv_timeout(
        &mut self,
        timeout: Duration,
    ) -> Result<Option<SubscriptionRequest<'_>>, Terminated> {
        Ok(self
            .recv_next_timeout(timeout)?
            .map(|(peer, event)| self.lend(peer, event)))
    }

    /// The next request, waiting until one arrives.
    pub async fn recv_async(&mut self) -> Result<SubscriptionRequest<'_>, Terminated> {
        let (peer, event) = self.recv_next_async().await?;
        Ok(self.lend(peer, event))
    }

    /// Lends the socket to what a peer sent, to act on it.
    fn lend(&mut self, peer: zmq_core::PeerKey, event: XPubEvent) -> SubscriptionRequest<'_> {
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
    peer: zmq_core::PeerKey,
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
            XPubEvent::Message(_) => self.socket.peers.list_mut().get(self.peer).is_some(),
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
        let socket = &mut *self.socket;
        socket
            .routing
            .send_to(socket.peers.list_mut(), self.peer, message.into())
    }

    fn apply(&mut self, subscription: Subscription) -> bool {
        self.socket.routing.apply(self.peer, subscription)
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

    /// Whether every unsubscription goes to the publishers, as libzmq's
    /// `ZMQ_XSUB_VERBOSE_UNSUBSCRIBE`, a draft option: even one of a topic
    /// subscribed more than once, or not at all. Off by default: only the
    /// unsubscription that ends a topic does.
    #[cfg(feature = "draft")]
    pub fn set_verbose_unsubscribe(&mut self, on: bool) {
        self.routing.set_verbose_unsubscribe(on);
    }

    /// How many distinct topics it is subscribed to, as libzmq's
    /// `ZMQ_TOPICS_COUNT`, a draft option. A topic subscribed twice counts
    /// once.
    #[cfg(feature = "draft")]
    #[must_use]
    pub fn topics_count(&self) -> usize {
        self.routing.topics_count()
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
    pub fn send(&mut self, message: impl Into<Message>) -> Result<(), Terminated> {
        self.alive()?;
        self.peers.accept(&mut noop());
        self.routing
            .send_upstream(self.peers.list_mut(), message.into());
        Ok(())
    }

    /// Passes on what an [`XPub`] heard, as a proxy does: subscriptions
    /// become ours, and messages are sent upstream.
    pub fn forward(&mut self, event: XPubEvent) -> Result<(), Terminated> {
        match event {
            XPubEvent::Subscribe(topic) => self.subscribe(topic),
            XPubEvent::Cancel(topic) => {
                self.unsubscribe(topic);
            }
            XPubEvent::Message(message) => return self.send(message),
        }
        Ok(())
    }

    /// The next message, if one is waiting.
    pub fn try_recv(&mut self) -> Result<Option<Message>, Terminated> {
        self.try_recv_next()
    }

    /// The next message, blocking until one arrives.
    pub fn recv(&mut self) -> Result<Message, Terminated> {
        self.recv_next()
    }

    /// The next message, blocking for at most `timeout`; `None` if none
    /// arrives in time.
    pub fn recv_timeout(&mut self, timeout: Duration) -> Result<Option<Message>, Terminated> {
        self.recv_next_timeout(timeout)
    }

    /// The next message, waiting until one arrives.
    pub async fn recv_async(&mut self) -> Result<Message, Terminated> {
        self.recv_next_async().await
    }
}

impl Socket<Pull> {
    /// The next message, if one is waiting.
    pub fn try_recv(&mut self) -> Result<Option<Message>, Terminated> {
        self.try_recv_next()
    }

    /// The next message, blocking until one arrives.
    pub fn recv(&mut self) -> Result<Message, Terminated> {
        self.recv_next()
    }

    /// The next message, blocking for at most `timeout`; `None` if none
    /// arrives in time.
    pub fn recv_timeout(&mut self, timeout: Duration) -> Result<Option<Message>, Terminated> {
        self.recv_next_timeout(timeout)
    }

    /// The next message, waiting until one arrives.
    pub async fn recv_async(&mut self) -> Result<Message, Terminated> {
        self.recv_next_async().await
    }
}

impl Socket<Dealer> {
    /// Sends `message` if a peer has room, otherwise hands it back.
    pub fn try_send(&mut self, message: impl Into<Message>) -> Result<(), TrySendError<Message>> {
        self.try_send_message(message.into())
    }

    /// Sends `message`, blocking while no peer has room.
    pub fn send(&mut self, message: impl Into<Message>) -> Result<(), Terminated> {
        self.send_message(message.into())
    }

    /// Sends `message`, blocking for at most `timeout` while no peer has
    /// room. Hands it back if there is still no room.
    pub fn send_timeout(
        &mut self,
        message: impl Into<Message>,
        timeout: Duration,
    ) -> Result<(), TrySendError<Message>> {
        self.send_message_timeout(message.into(), timeout)
    }

    /// Sends `message`, waiting while no peer has room.
    pub async fn send_async(&mut self, message: impl Into<Message>) -> Result<(), Terminated> {
        self.send_message_async(message.into()).await
    }

    /// The next message, if one is waiting.
    pub fn try_recv(&mut self) -> Result<Option<Message>, Terminated> {
        self.try_recv_next()
    }

    /// The next message, blocking until one arrives.
    pub fn recv(&mut self) -> Result<Message, Terminated> {
        self.recv_next()
    }

    /// The next message, blocking for at most `timeout`; `None` if none
    /// arrives in time.
    pub fn recv_timeout(&mut self, timeout: Duration) -> Result<Option<Message>, Terminated> {
        self.recv_next_timeout(timeout)
    }

    /// The next message, waiting until one arrives.
    pub async fn recv_async(&mut self) -> Result<Message, Terminated> {
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
    pub fn request(&mut self, message: impl Into<Message>) -> Result<Message, RequestError> {
        let mut state = RequestState::Sending(Some(message.into()));
        RequestError::from_poll(wait::block_on(|cx| {
            self.poll_alive(cx, |socket, cx| socket.poll_request(&mut state, cx))
        }))
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
            self.poll_alive(cx, |socket, cx| socket.poll_request(&mut state, cx))
        }) {
            Some(reply) => RequestError::from_poll(reply),
            None => Err(RequestError::TimedOut),
        }
    }

    /// Sends `message` to the next peer with room and waits for its reply.
    /// If this future is dropped before the reply arrives, the request is
    /// abandoned; see [`set_correlation`](Self::set_correlation).
    pub async fn request_async(
        &mut self,
        message: impl Into<Message>,
    ) -> Result<Message, RequestError> {
        let mut state = RequestState::Sending(Some(message.into()));
        RequestError::from_poll(
            future::poll_fn(|cx| {
                self.poll_alive(cx, |socket, cx| socket.poll_request(&mut state, cx))
            })
            .await,
        )
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
    pub fn try_recv(&mut self) -> Result<Option<Request<'_>>, Terminated> {
        Ok(self.try_recv_next()?.map(|received| self.lend(received)))
    }

    /// The next request, blocking until one arrives.
    pub fn recv(&mut self) -> Result<Request<'_>, Terminated> {
        let received = self.recv_next()?;
        Ok(self.lend(received))
    }

    /// The next request, blocking for at most `timeout`; `None` if none
    /// arrives in time.
    pub fn recv_timeout(&mut self, timeout: Duration) -> Result<Option<Request<'_>>, Terminated> {
        Ok(self
            .recv_next_timeout(timeout)?
            .map(|received| self.lend(received)))
    }

    /// The next request, waiting until one arrives.
    pub async fn recv_async(&mut self) -> Result<Request<'_>, Terminated> {
        let received = self.recv_next_async().await?;
        Ok(self.lend(received))
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

/// The socket's context was shut down with
/// [`Context::shutdown`](crate::Context::shutdown), as libzmq's `ETERM`:
/// every send and receive on its sockets fails from then on, and those
/// waiting are woken to fail. Drop the socket.
#[derive(Debug, Clone, Copy, PartialEq, Eq)]
pub struct Terminated;

impl fmt::Display for Terminated {
    fn fmt(&self, f: &mut fmt::Formatter<'_>) -> fmt::Result {
        f.write_str("the context was shut down")
    }
}

impl std::error::Error for Terminated {}

/// Why a send that does not wait, or waits only so long, did not go
/// through. What was being sent is handed back.
#[derive(Debug, Clone, PartialEq, Eq)]
#[non_exhaustive]
pub enum TrySendError<T> {
    /// No peer has room (libzmq's `EAGAIN`).
    Full(T),
    /// The context was shut down (libzmq's `ETERM`).
    Terminated(T),
}

impl<T> TrySendError<T> {
    /// What was being sent.
    pub fn into_inner(self) -> T {
        match self {
            Self::Full(unsent) | Self::Terminated(unsent) => unsent,
        }
    }

    #[cfg(feature = "draft")]
    pub(crate) fn map<U>(self, f: impl FnOnce(T) -> U) -> TrySendError<U> {
        match self {
            Self::Full(unsent) => TrySendError::Full(f(unsent)),
            Self::Terminated(unsent) => TrySendError::Terminated(f(unsent)),
        }
    }
}

impl<T> fmt::Display for TrySendError<T> {
    fn fmt(&self, f: &mut fmt::Formatter<'_>) -> fmt::Result {
        match self {
            Self::Full(_) => f.write_str("no peer has room"),
            Self::Terminated(_) => Terminated.fmt(f),
        }
    }
}

impl<T: fmt::Debug> std::error::Error for TrySendError<T> {}

/// What a send to one peer came to: `None` if it was still waiting at its
/// deadline. A send that did not go through left what it was sending in
/// `slot`, unless there is no such peer, which hands it back.
pub(crate) fn routed<M>(
    sent: Option<Result<Result<(), M>, Terminated>>,
    slot: &mut Option<M>,
) -> Result<(), SendError<M>> {
    let mut kept = || slot.take().expect("kept while pending");
    match sent {
        Some(Ok(Ok(()))) => Ok(()),
        Some(Ok(Err(unsent))) => Err(SendError::UnknownPeer(unsent)),
        Some(Err(Terminated)) => Err(SendError::Terminated(kept())),
        None => Err(SendError::Full(kept())),
    }
}

/// Why a request got no reply.
#[derive(Debug, Clone, Copy, PartialEq, Eq)]
#[non_exhaustive]
pub enum RequestError {
    /// The peer the request went to closed before replying.
    PeerGone,
    /// No reply arrived in time. The request is abandoned.
    TimedOut,
    /// The context was shut down (libzmq's `ETERM`).
    Terminated,
}

impl RequestError {
    fn from_poll(reply: Result<Result<Message, PeerGone>, Terminated>) -> Result<Message, Self> {
        match reply {
            Ok(Ok(reply)) => Ok(reply),
            Ok(Err(PeerGone)) => Err(Self::PeerGone),
            Err(Terminated) => Err(Self::Terminated),
        }
    }
}

impl fmt::Display for RequestError {
    fn fmt(&self, f: &mut fmt::Formatter<'_>) -> fmt::Result {
        match self {
            Self::PeerGone => PeerGone.fmt(f),
            Self::TimedOut => f.write_str("no reply in time"),
            Self::Terminated => Terminated.fmt(f),
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
    ) -> Result<(), SendError<Message>> {
        if self.peers.is_terminated() {
            return Err(SendError::Terminated(message.into()));
        }
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
    ) -> Result<(), SendError<Message>> {
        let mut slot = Some(message.into());
        let sent = wait::block_on(|cx| {
            self.poll_alive(cx, |socket, cx| socket.poll_send(to, &mut slot, cx))
        });
        routed(Some(sent), &mut slot)
    }

    /// Sends `message` to the peer with routing id `to`, blocking for at
    /// most `timeout` while its queue is full. Fails at once if no peer has
    /// that routing id, and with `Full` if there is still no room.
    pub fn send_timeout(
        &mut self,
        to: &RoutingId,
        message: impl Into<Message>,
        timeout: Duration,
    ) -> Result<(), SendError<Message>> {
        let mut slot = Some(message.into());
        let sent = wait::block_on_until(wait::deadline(timeout), |cx| {
            self.poll_alive(cx, |socket, cx| socket.poll_send(to, &mut slot, cx))
        });
        routed(sent, &mut slot)
    }

    /// Sends `message` to the peer with routing id `to`, waiting while its
    /// queue is full. Fails if no peer has that routing id, or it goes away
    /// meanwhile.
    pub async fn send_async(
        &mut self,
        to: &RoutingId,
        message: impl Into<Message>,
    ) -> Result<(), SendError<Message>> {
        let mut slot = Some(message.into());
        let sent = future::poll_fn(|cx| {
            self.poll_alive(cx, |socket, cx| socket.poll_send(to, &mut slot, cx))
        })
        .await;
        routed(Some(sent), &mut slot)
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

    /// Connects to `endpoint` as [`connect`](Self::connect) does, giving
    /// the peer there the routing id `peer`, as libzmq's
    /// `ZMQ_CONNECT_ROUTING_ID` set before a `zmq_connect`. Messages can be
    /// sent to `peer` at once, queuing until the connection is up, which
    /// lets a ROUTER talk first to a ROUTER it connects to. What the peer
    /// announces as its identity is not looked at.
    ///
    /// Fails with [`Error::RoutingIdInUse`] if a peer of this socket has
    /// that routing id already; libzmq does not allow it and aborts.
    ///
    /// ```
    /// use zmq::{Context, Router, RoutingId};
    ///
    /// let context = Context::new()?;
    /// let mut bound = context.socket::<Router>();
    /// bound.bind("inproc://routers")?;
    /// let mut connecting = context.socket::<Router>();
    /// let peer = RoutingId::new("bound")?;
    /// connecting.connect_with_routing_id("inproc://routers", peer.clone())?;
    /// connecting.send(&peer, "hello")?;
    /// let (_from, message) = bound.recv()?;
    /// assert_eq!(message, "hello".into());
    /// # Ok::<(), Box<dyn std::error::Error>>(())
    /// ```
    pub fn connect_with_routing_id(
        &mut self,
        endpoint: &str,
        peer: RoutingId,
    ) -> Result<(), Error> {
        self.alive()?;
        self.peers.accept(&mut noop());
        self.routing.table.sync(self.peers.list_mut());
        let connected = &self.connected;
        let taken = self.routing.table.is_taken(peer.as_bytes(), |endpoint| {
            connected.iter().any(|&(_, _, id)| id == endpoint)
        });
        if taken {
            return Err(Error::RoutingIdInUse);
        }
        self.connect_key(endpoint)?;
        let &(_, _, id) = self.connected.last().expect("just connected");
        self.routing
            .table
            .assign(id, zmtp::Frame::copy_from_slice(peer.as_bytes()));
        Ok(())
    }

    /// Whether a peer announcing a routing id already in use takes it over,
    /// disconnecting the peer that had it, as libzmq's
    /// `ZMQ_ROUTER_HANDOVER`. Off by default: the newcomer is ignored.
    pub fn set_handover(&mut self, on: bool) {
        self.routing.table.set_handover(on);
    }

    /// Which changes to connections bound or connected from now on this
    /// socket is told of, as libzmq's `ZMQ_ROUTER_NOTIFY`, a draft option:
    /// an empty message from the peer's routing id once its handshake has
    /// completed, when it disconnects, or both. None by default. Over TCP
    /// and IPC only: an `inproc` peer is never announced.
    ///
    /// A disconnection is told only of a peer whose handshake completed,
    /// after everything it sent.
    #[cfg(feature = "draft")]
    pub fn set_notify(&mut self, notify: crate::RouterNotify) {
        self.peers.config_mut().router_notify = notify;
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
    pub fn try_recv(&mut self) -> Result<Option<(RoutingId, Message)>, Terminated> {
        self.try_recv_next()
    }

    /// The next message and its sender's routing id, blocking until one
    /// arrives.
    pub fn recv(&mut self) -> Result<(RoutingId, Message), Terminated> {
        self.recv_next()
    }

    /// The next message and its sender's routing id, blocking for at most
    /// `timeout`; `None` if none arrives in time.
    pub fn recv_timeout(
        &mut self,
        timeout: Duration,
    ) -> Result<Option<(RoutingId, Message)>, Terminated> {
        self.recv_next_timeout(timeout)
    }

    /// The next message and its sender's routing id, waiting until one
    /// arrives.
    pub async fn recv_async(&mut self) -> Result<(RoutingId, Message), Terminated> {
        self.recv_next_async().await
    }
}

/// Sends the message in `slot` to the next peer with room, putting it back
/// and returning `Pending` if none has room. Shared by the load-balancing
/// sockets: PUSH, DEALER, CLIENT and SCATTER.
pub(crate) fn poll_load_balanced(
    peers: &mut PeerList,
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
