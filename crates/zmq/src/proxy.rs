//! The built-in proxy, as libzmq's `zmq_proxy_steerable`: forwards
//! messages between a frontend and a backend socket in both directions,
//! optionally copying them to a capture socket, and can be paused, resumed
//! and stopped through a [`ProxyControl`].

use std::future::poll_fn;
use std::sync::atomic::{AtomicU64, Ordering};
use std::sync::{Arc, Mutex, PoisonError};
use std::task::{Context as TaskContext, Poll, Waker};

use zmtp::{Frame, Message, Subscription};

use crate::socket::sealed;
use crate::{
    Dealer, Pair, Pub, Pull, Push, Router, RoutingId, Socket, Sub, XPub, XPubEvent, XSub, wait,
};

/// Messages a proxy moves in one direction before checking its control and
/// the other direction, so neither starves.
const BATCH: usize = 256;

/// A socket a [`Proxy`] can forward from and to, in libzmq's raw form: a
/// ROUTER's messages carry the peer's routing id as their first frame (and
/// it sends to the peer the first frame names), and an XPUB's subscription
/// events are `0x01`/`0x00`-prefixed messages, which an XSUB sends upstream
/// as subscriptions. REQ and REP cannot be proxied: their lockstep does not
/// fit a pass-through.
pub trait ProxyEnd: sealed::Sealed + Send {
    /// The next message this socket received, or `Pending`. Always
    /// `Pending` for sockets that cannot receive.
    #[doc(hidden)]
    fn poll_take(&mut self, cx: &mut TaskContext<'_>) -> Poll<Message>;

    /// Sends the message in `slot`, taking it once sent (or dropped, by
    /// patterns that drop). `Pending`, with `slot` kept, while there is no
    /// room. Sockets that cannot send drop it.
    #[doc(hidden)]
    fn poll_give(&mut self, slot: &mut Option<Message>, cx: &mut TaskContext<'_>) -> Poll<()>;
}

/// A socket a [`Proxy`] can copy every message to: PUB, DEALER, PUSH or
/// PAIR, as libzmq allows.
pub trait Capture: sealed::Sealed + Send {
    /// Sends a copy of `message` if there is room, or drops it.
    #[doc(hidden)]
    fn capture(&mut self, message: &Message);
}

/// Forwards messages between two sockets until told to stop.
///
/// ```no_run
/// use zmq::{Context, Dealer, Proxy, Router};
///
/// let context = Context::new()?;
/// let mut frontend = context.socket::<Router>();
/// frontend.bind("tcp://*:5559")?;
/// let mut backend = context.socket::<Dealer>();
/// backend.bind("tcp://*:5560")?;
/// let mut proxy = Proxy::new(&mut frontend, &mut backend);
/// let control = proxy.control();
/// # std::thread::spawn(move || control.terminate());
/// let statistics = proxy.run();
/// println!("forwarded {} message parts", statistics.frontend.received.messages);
/// # Ok::<(), Box<dyn std::error::Error>>(())
/// ```
///
/// Each direction holds at most one message in flight: while one side has
/// no room, the proxy stops reading from the other for that direction
/// only, so nothing is lost and the opposite direction keeps flowing.
pub struct Proxy<'a> {
    frontend: &'a mut dyn ProxyEnd,
    backend: &'a mut dyn ProxyEnd,
    capture: Option<&'a mut dyn Capture>,
    control: ProxyControl,
    to_backend: Option<Message>,
    to_frontend: Option<Message>,
}

/// Steers a running [`Proxy`] from any thread, as libzmq's control socket
/// does. Clones steer the same proxy.
#[derive(Clone)]
pub struct ProxyControl {
    shared: Arc<Shared>,
}

#[derive(Default)]
struct Shared {
    state: Mutex<State>,
    counters: [AtomicU64; 8],
}

#[derive(Default)]
struct State {
    mode: Mode,
    /// Wakes the proxy when its mode changes.
    waker: Option<Waker>,
}

#[derive(Debug, Clone, Copy, PartialEq, Eq, Default)]
enum Mode {
    #[default]
    Running,
    Paused,
    Terminated,
}

/// What a proxy has moved, as libzmq's STATISTICS reports it. Each part of
/// a multipart message counts as one message.
#[derive(Debug, Clone, Copy, PartialEq, Eq, Default)]
pub struct ProxyStatistics {
    pub frontend: SideStatistics,
    pub backend: SideStatistics,
}

/// One side of a proxy's traffic.
#[derive(Debug, Clone, Copy, PartialEq, Eq, Default)]
pub struct SideStatistics {
    /// Received by this side's socket.
    pub received: Count,
    /// Sent by this side's socket.
    pub sent: Count,
}

/// Message parts and their octets.
#[derive(Debug, Clone, Copy, PartialEq, Eq, Default)]
pub struct Count {
    pub messages: u64,
    pub bytes: u64,
}

/// Which counters a forwarded message updates.
#[derive(Clone, Copy)]
enum Direction {
    ToBackend,
    ToFrontend,
}

impl<'a> Proxy<'a> {
    /// A proxy between `frontend` and `backend`, which should already be
    /// bound or connected.
    pub fn new(frontend: &'a mut impl ProxyEnd, backend: &'a mut impl ProxyEnd) -> Self {
        Self {
            frontend,
            backend,
            capture: None,
            control: ProxyControl {
                shared: Arc::default(),
            },
            to_backend: None,
            to_frontend: None,
        }
    }

    /// Also sends a copy of every message, in both directions, to
    /// `capture`. A copy the capture socket has no room for is dropped,
    /// so a slow capture never holds up the proxy.
    pub fn capture(mut self, capture: &'a mut impl Capture) -> Self {
        self.capture = Some(capture);
        self
    }

    /// A handle to pause, resume or stop this proxy, and read its
    /// statistics.
    pub fn control(&self) -> ProxyControl {
        self.control.clone()
    }

    /// Forwards messages, blocking the thread, until
    /// [`ProxyControl::terminate`] is called. Returns the final statistics.
    pub fn run(&mut self) -> ProxyStatistics {
        wait::block_on(|cx| self.poll(cx))
    }

    /// Forwards messages until [`ProxyControl::terminate`] is called.
    pub async fn run_async(&mut self) -> ProxyStatistics {
        poll_fn(|cx| self.poll(cx)).await
    }

    fn poll(&mut self, cx: &mut TaskContext<'_>) -> Poll<ProxyStatistics> {
        match self.control.register(cx) {
            Mode::Terminated => return Poll::Ready(self.control.statistics()),
            // The control's waker brings us back on resume.
            Mode::Paused => return Poll::Pending,
            Mode::Running => {}
        }
        let shared = &*self.control.shared;
        let busy_backwards = pump(
            &mut *self.frontend,
            &mut *self.backend,
            &mut self.to_backend,
            &mut self.capture,
            shared,
            Direction::ToBackend,
            cx,
        );
        let busy_forwards = pump(
            &mut *self.backend,
            &mut *self.frontend,
            &mut self.to_frontend,
            &mut self.capture,
            shared,
            Direction::ToFrontend,
            cx,
        );
        if busy_backwards || busy_forwards {
            // A full batch: there may be more. Yield, so control commands
            // and other tasks get a turn, and come straight back.
            cx.waker().wake_by_ref();
        }
        Poll::Pending
    }
}

/// Moves up to a batch of messages from `from` to `to`. Returns whether it
/// stopped only because the batch was full. Every socket polled has `cx`
/// registered, so the proxy is woken when either can make progress.
fn pump(
    from: &mut dyn ProxyEnd,
    to: &mut dyn ProxyEnd,
    in_flight: &mut Option<Message>,
    capture: &mut Option<&mut dyn Capture>,
    shared: &Shared,
    direction: Direction,
    cx: &mut TaskContext<'_>,
) -> bool {
    for _ in 0..BATCH {
        if in_flight.is_none() {
            let Poll::Ready(message) = from.poll_take(cx) else {
                return false;
            };
            shared.count(direction, false, &message);
            if let Some(capture) = capture {
                capture.capture(&message);
            }
            *in_flight = Some(message);
        }
        let (parts, bytes) = size(in_flight.as_ref().expect("filled above"));
        if to.poll_give(in_flight, cx).is_pending() {
            return false;
        }
        shared.add(direction, true, parts, bytes);
    }
    true
}

fn size(message: &Message) -> (u64, u64) {
    let parts = message.len().get() as u64;
    let bytes = message.iter().map(|frame| frame.len() as u64).sum();
    (parts, bytes)
}

impl Shared {
    fn count(&self, direction: Direction, sent: bool, message: &Message) {
        let (parts, bytes) = size(message);
        self.add(direction, sent, parts, bytes);
    }

    /// Counters are in libzmq's STATISTICS order: frontend received and
    /// sent, then backend received and sent, each as messages then bytes.
    fn add(&self, direction: Direction, sent: bool, parts: u64, bytes: u64) {
        let side = match (direction, sent) {
            // Received by the frontend, or sent by the backend, and back.
            (Direction::ToBackend, false) => 0,
            (Direction::ToFrontend, true) => 1,
            (Direction::ToFrontend, false) => 2,
            (Direction::ToBackend, true) => 3,
        };
        self.counters[2 * side].fetch_add(parts, Ordering::Relaxed);
        self.counters[2 * side + 1].fetch_add(bytes, Ordering::Relaxed);
    }
}

impl ProxyControl {
    /// Stops forwarding. Messages wait in the sockets' queues until
    /// [`resume`](Self::resume).
    pub fn pause(&self) {
        self.set(Mode::Paused);
    }

    /// Starts forwarding again after [`pause`](Self::pause).
    pub fn resume(&self) {
        self.set(Mode::Running);
    }

    /// Makes [`Proxy::run`] return. A message already taken from one
    /// socket but not yet sent to the other is dropped.
    pub fn terminate(&self) {
        self.set(Mode::Terminated);
    }

    /// What the proxy has moved so far.
    pub fn statistics(&self) -> ProxyStatistics {
        let read = |i: usize| self.shared.counters[i].load(Ordering::Relaxed);
        let count = |i: usize| Count {
            messages: read(i),
            bytes: read(i + 1),
        };
        ProxyStatistics {
            frontend: SideStatistics {
                received: count(0),
                sent: count(2),
            },
            backend: SideStatistics {
                received: count(4),
                sent: count(6),
            },
        }
    }

    fn set(&self, mode: Mode) {
        let mut state = self.state();
        // Once terminated, a proxy stays terminated.
        if state.mode != Mode::Terminated {
            state.mode = mode;
        }
        if let Some(waker) = state.waker.take() {
            waker.wake();
        }
    }

    /// The current mode, registering `cx` to hear of changes.
    fn register(&self, cx: &mut TaskContext<'_>) -> Mode {
        let mut state = self.state();
        state.waker = Some(cx.waker().clone());
        state.mode
    }

    fn state(&self) -> std::sync::MutexGuard<'_, State> {
        self.shared
            .state
            .lock()
            .unwrap_or_else(PoisonError::into_inner)
    }
}

/// A subscription event in the message form an XSUB sends upstream.
fn raw_event(event: XPubEvent) -> Message {
    match event {
        XPubEvent::Subscribe(topic) => Subscription::Subscribe(topic).to_message(),
        XPubEvent::Cancel(topic) => Subscription::Cancel(topic).to_message(),
        XPubEvent::Message(message) => message,
    }
}

/// Splits off the routing id a ROUTER sends to, if the message has a body.
fn routed(message: Message) -> Option<(RoutingId, Message)> {
    let mut frames = message.into_iter();
    let id = frames.next()?;
    let body = Message::try_from(frames.collect::<Vec<Frame>>()).ok()?;
    (!id.is_empty()).then(|| (RoutingId::trusted(id), body))
}

/// Drops the message in `slot`, for sockets that cannot send.
fn drop_it(slot: &mut Option<Message>) -> Poll<()> {
    slot.take();
    Poll::Ready(())
}

impl ProxyEnd for Socket<Router> {
    fn poll_take(&mut self, cx: &mut TaskContext<'_>) -> Poll<Message> {
        self.poll_recv(cx).map(|(id, body)| {
            let mut frames = vec![id.into_frame()];
            frames.extend(body);
            Message::try_from(frames).expect("at least the routing id")
        })
    }

    fn poll_give(&mut self, slot: &mut Option<Message>, _: &mut TaskContext<'_>) -> Poll<()> {
        // ROUTER never blocks: an unroutable or unaddressed message is
        // dropped, as libzmq's ROUTER drops it.
        if let Some((id, body)) = slot.take().and_then(routed) {
            self.send(&id, body);
        }
        Poll::Ready(())
    }
}

impl ProxyEnd for Socket<Dealer> {
    fn poll_take(&mut self, cx: &mut TaskContext<'_>) -> Poll<Message> {
        self.poll_recv(cx)
    }

    fn poll_give(&mut self, slot: &mut Option<Message>, cx: &mut TaskContext<'_>) -> Poll<()> {
        self.poll_send(slot, cx)
    }
}

impl ProxyEnd for Socket<Pair> {
    fn poll_take(&mut self, cx: &mut TaskContext<'_>) -> Poll<Message> {
        self.poll_recv(cx)
    }

    fn poll_give(&mut self, slot: &mut Option<Message>, cx: &mut TaskContext<'_>) -> Poll<()> {
        self.poll_send(slot, cx)
    }
}

impl ProxyEnd for Socket<Pull> {
    fn poll_take(&mut self, cx: &mut TaskContext<'_>) -> Poll<Message> {
        self.poll_recv(cx)
    }

    fn poll_give(&mut self, slot: &mut Option<Message>, _: &mut TaskContext<'_>) -> Poll<()> {
        drop_it(slot)
    }
}

impl ProxyEnd for Socket<Push> {
    fn poll_take(&mut self, _: &mut TaskContext<'_>) -> Poll<Message> {
        Poll::Pending
    }

    fn poll_give(&mut self, slot: &mut Option<Message>, cx: &mut TaskContext<'_>) -> Poll<()> {
        self.poll_send(slot, cx)
    }
}

impl ProxyEnd for Socket<Pub> {
    fn poll_take(&mut self, _: &mut TaskContext<'_>) -> Poll<Message> {
        Poll::Pending
    }

    fn poll_give(&mut self, slot: &mut Option<Message>, _: &mut TaskContext<'_>) -> Poll<()> {
        if let Some(message) = slot.take() {
            self.send(message);
        }
        Poll::Ready(())
    }
}

impl ProxyEnd for Socket<Sub> {
    fn poll_take(&mut self, cx: &mut TaskContext<'_>) -> Poll<Message> {
        self.poll_recv(cx)
    }

    fn poll_give(&mut self, slot: &mut Option<Message>, _: &mut TaskContext<'_>) -> Poll<()> {
        drop_it(slot)
    }
}

impl ProxyEnd for Socket<XPub> {
    fn poll_take(&mut self, cx: &mut TaskContext<'_>) -> Poll<Message> {
        self.poll_recv(cx).map(raw_event)
    }

    fn poll_give(&mut self, slot: &mut Option<Message>, _: &mut TaskContext<'_>) -> Poll<()> {
        if let Some(message) = slot.take() {
            self.send(message);
        }
        Poll::Ready(())
    }
}

impl ProxyEnd for Socket<XSub> {
    fn poll_take(&mut self, cx: &mut TaskContext<'_>) -> Poll<Message> {
        self.poll_recv(cx)
    }

    fn poll_give(&mut self, slot: &mut Option<Message>, _: &mut TaskContext<'_>) -> Poll<()> {
        // Subscription-shaped messages subscribe; others go upstream.
        if let Some(message) = slot.take() {
            self.send(message);
        }
        Poll::Ready(())
    }
}

impl Capture for Socket<Pub> {
    fn capture(&mut self, message: &Message) {
        self.send(message.clone());
    }
}

impl Capture for Socket<Dealer> {
    fn capture(&mut self, message: &Message) {
        let _ = self.try_send(message.clone());
    }
}

impl Capture for Socket<Push> {
    fn capture(&mut self, message: &Message) {
        let _ = self.try_send(message.clone());
    }
}

impl Capture for Socket<Pair> {
    fn capture(&mut self, message: &Message) {
        let _ = self.try_send(message.clone());
    }
}

impl std::fmt::Debug for Proxy<'_> {
    fn fmt(&self, f: &mut std::fmt::Formatter<'_>) -> std::fmt::Result {
        f.debug_struct("Proxy")
            .field("capture", &self.capture.is_some())
            .field("statistics", &self.control.statistics())
            .finish_non_exhaustive()
    }
}

impl std::fmt::Debug for ProxyControl {
    fn fmt(&self, f: &mut std::fmt::Formatter<'_>) -> std::fmt::Result {
        f.debug_struct("ProxyControl")
            .field("mode", &self.state().mode)
            .finish_non_exhaustive()
    }
}
