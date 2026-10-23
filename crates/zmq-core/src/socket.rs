//! What every socket pattern shares: the peers a socket talks to, and the
//! routing strategies patterns apply to them (RFC 28-31).
//!
//! Waiting on many pipes at once works by polling each with the same waker:
//! any pipe that becomes ready wakes the task, which polls again.

use std::collections::{HashMap, HashSet};
use std::hash::{BuildHasher, RandomState};
use std::io;
use std::net::SocketAddr;
use std::task::{Context as TaskContext, Poll};

use zmq_pipe::{Reader, TryRecvError, TrySendError};
use zmtp::{Frame, Message};

use crate::transport::Address;
use crate::{Connection, ConnectionConfig, Context, Streaming};

/// A stable name for a peer within one socket. Keys only grow, so peers
/// stay sorted by key.
#[derive(Debug, Clone, Copy, PartialEq, Eq, Hash, PartialOrd, Ord)]
pub struct PeerKey(u64);

/// One peer of a socket.
#[derive(Debug)]
pub struct Peer {
    key: PeerKey,
    pub(crate) connection: Connection,
}

impl Peer {
    /// This peer's key.
    pub fn key(&self) -> PeerKey {
        self.key
    }

    /// The connection to this peer.
    pub fn connection(&self) -> &Connection {
        &self.connection
    }
}

/// A socket's peers, in the order they arrived.
#[derive(Debug, Default)]
pub struct PeerList {
    peers: Vec<Peer>,
    next_key: u64,
    /// Bumped whenever a peer is removed, so tables keyed by peer know to
    /// forget it.
    removals: u64,
}

impl PeerList {
    /// Adds a peer and returns its key.
    pub fn push(&mut self, connection: Connection) -> PeerKey {
        let key = PeerKey(self.next_key);
        self.next_key += 1;
        self.peers.push(Peer { key, connection });
        key
    }

    /// How many peers there are.
    pub fn len(&self) -> usize {
        self.peers.len()
    }

    /// Whether there are no peers.
    pub fn is_empty(&self) -> bool {
        self.peers.is_empty()
    }

    /// The peers, oldest first.
    pub fn iter(&self) -> impl Iterator<Item = &Peer> {
        self.peers.iter()
    }

    pub(crate) fn iter_mut(&mut self) -> impl Iterator<Item = &mut Peer> {
        self.peers.iter_mut()
    }

    /// The peer with `key`, if it is still here.
    pub fn get(&self, key: PeerKey) -> Option<&Peer> {
        let index = self.peers.binary_search_by_key(&key, Peer::key).ok()?;
        Some(&self.peers[index])
    }

    /// The peer with `key`, if it is still here.
    pub fn get_mut(&mut self, key: PeerKey) -> Option<&mut Peer> {
        let index = self.peers.binary_search_by_key(&key, Peer::key).ok()?;
        Some(&mut self.peers[index])
    }

    fn remove(&mut self, index: usize) {
        self.peers.remove(index);
        self.removals += 1;
    }

    pub(crate) fn retain_mut(&mut self, keep: impl FnMut(&mut Peer) -> bool) {
        let before = self.peers.len();
        self.peers.retain_mut(keep);
        if self.peers.len() != before {
            self.removals += 1;
        }
    }
}

/// The connections a socket talks to: those it connected, and those its
/// listeners accepted.
#[derive(Debug)]
pub struct Peers {
    context: Context,
    config: ConnectionConfig,
    list: PeerList,
    /// Accept queues of the socket's TCP and `inproc` listeners.
    listeners: Vec<Reader<Connection>>,
    /// What each new peer is sent first, if anything.
    greeting: Option<Message>,
}

impl Peers {
    /// No peers yet; new connections use `config`.
    pub fn new(context: Context, config: ConnectionConfig) -> Self {
        Self {
            context,
            config,
            list: PeerList::default(),
            listeners: Vec::new(),
            greeting: None,
        }
    }

    /// Sends each peer added from now on `greeting` before anything else,
    /// such as libzmq's `ZMQ_PROBE_ROUTER` empty message or
    /// `ZMQ_XPUB_WELCOME_MSG`.
    pub fn set_greeting(&mut self, greeting: Option<Message>) {
        self.greeting = greeting;
    }

    fn add(&mut self, connection: Connection) -> PeerKey {
        add(&mut self.list, self.greeting.as_ref(), connection)
    }

    /// The configuration new connections use.
    pub fn config_mut(&mut self) -> &mut ConnectionConfig {
        &mut self.config
    }

    /// Connects to `addr`. The peer is usable at once: messages queue until
    /// the connection is up, as in libzmq. With
    /// [`immediate`](ConnectionConfig::immediate) there is no peer until
    /// then, so no key.
    pub fn connect(&mut self, addr: SocketAddr) -> Option<PeerKey> {
        self.connect_to(Address::Tcp(addr))
    }

    fn connect_to(&mut self, addr: Address) -> Option<PeerKey> {
        if self.config.immediate {
            let connections = self.context.connect_immediate(addr, self.config.clone());
            self.listeners.push(connections);
            return None;
        }
        let connection = self.context.connect_to(addr, self.config.clone());
        Some(self.add(connection))
    }

    /// Binds `addr` and accepts peers on it. Returns the bound address.
    pub fn bind(&mut self, addr: SocketAddr) -> io::Result<SocketAddr> {
        let listener = self.context.bind(addr, self.config.clone())?;
        let local = listener.local_addr();
        self.listeners.push(listener.into_queue());
        Ok(local)
    }

    /// Binds a Unix domain socket at `path` and accepts peers on it.
    #[cfg(unix)]
    pub fn bind_ipc(&mut self, path: &std::path::Path) -> io::Result<()> {
        let listener = self.context.bind_ipc(path, self.config.clone())?;
        self.listeners.push(listener.into_queue());
        Ok(())
    }

    /// Binds a Unix domain socket at a fresh temporary path, returning it.
    #[cfg(unix)]
    pub fn bind_ipc_anywhere(&mut self) -> io::Result<std::path::PathBuf> {
        let listener = self.context.bind_ipc_anywhere(self.config.clone())?;
        let path = listener.path().to_owned();
        self.listeners.push(listener.into_queue());
        Ok(path)
    }

    /// Connects to the Unix domain socket at `path`, retrying until it is
    /// bound. No key with [`immediate`](ConnectionConfig::immediate), as
    /// for [`connect`](Self::connect).
    #[cfg(unix)]
    pub fn connect_ipc(&mut self, path: &std::path::Path) -> Option<PeerKey> {
        self.connect_to(Address::Ipc(path.to_owned()))
    }

    /// Binds `name` in Linux's abstract socket namespace and accepts peers
    /// on it.
    #[cfg(any(target_os = "linux", target_os = "android"))]
    pub fn bind_ipc_abstract(&mut self, name: &str) -> io::Result<()> {
        let listener = self.context.bind_ipc_abstract(name, self.config.clone())?;
        self.listeners.push(listener.into_queue());
        Ok(())
    }

    /// Connects to `name` in Linux's abstract socket namespace, retrying
    /// until it is bound. No key with
    /// [`immediate`](ConnectionConfig::immediate), as for
    /// [`connect`](Self::connect).
    #[cfg(any(target_os = "linux", target_os = "android"))]
    pub fn connect_ipc_abstract(&mut self, name: &str) -> Option<PeerKey> {
        self.connect_to(Address::IpcAbstract(name.to_owned()))
    }

    /// Binds the `inproc` `name` and accepts peers on it.
    pub fn bind_inproc(&mut self, name: &str) -> io::Result<()> {
        let listener = self.context.bind_inproc(name, self.config.clone())?;
        self.listeners.push(listener.into_queue());
        Ok(())
    }

    /// Connects to the `inproc` `name`, binding or not. `inproc` ignores
    /// [`immediate`](ConnectionConfig::immediate), as libzmq does.
    pub fn connect_inproc(&mut self, name: &str) -> PeerKey {
        let connection = self.context.connect_inproc(name, self.config.clone());
        self.add(connection)
    }

    /// A new monitor of this socket's events, from now on.
    pub fn monitor(&self) -> crate::Monitor {
        self.config.monitor.monitor()
    }

    /// How many peers the socket currently has.
    pub fn len(&self) -> usize {
        self.list.len()
    }

    /// Whether the socket has no peers.
    pub fn is_empty(&self) -> bool {
        self.list.is_empty()
    }

    /// Adds peers that listeners have accepted. Registers `cx` with every
    /// listener, so a new peer wakes a task waiting for one.
    pub fn accept(&mut self, cx: &mut TaskContext<'_>) {
        let (list, greeting) = (&mut self.list, self.greeting.as_ref());
        self.listeners.retain_mut(|listener| {
            loop {
                match listener.poll_recv(cx) {
                    Poll::Ready(Some(connection)) => {
                        add(list, greeting, connection);
                    }
                    // The context stopped: this listener will accept no more.
                    Poll::Ready(None) => break false,
                    Poll::Pending => break true,
                }
            }
        });
    }

    /// Drops peers whose sessions are gone, discarding anything they sent.
    /// For patterns that never receive, such as PUSH.
    pub fn prune_send_only(&mut self) {
        self.list.retain_mut(|peer| {
            loop {
                match peer.connection.rx.try_recv() {
                    Ok(_) => {}
                    Err(TryRecvError::Empty) => break true,
                    Err(TryRecvError::Closed) => break false,
                }
            }
        });
    }

    /// The peers, for a routing strategy.
    pub fn list_mut(&mut self) -> &mut PeerList {
        &mut self.list
    }
}

fn add(list: &mut PeerList, greeting: Option<&Message>, mut connection: Connection) -> PeerKey {
    if let Some(greeting) = greeting {
        // A new pipe has room, unless the peer is already gone.
        let _ = connection.tx.try_send(greeting.clone());
    }
    list.push(connection)
}

/// Round robin over the peers that have room, like libzmq's `lb_t`.
#[derive(Debug, Default)]
pub struct LoadBalancer {
    next: usize,
    /// A message too large for its peer's log, part way through it. Like
    /// libzmq's `_more`, nothing else is sent until it is done.
    streaming: Option<(PeerKey, Streaming)>,
}

impl LoadBalancer {
    /// Sends `message` to the next peer with room and returns that peer.
    /// Hands the message back, with `cx` registered on every peer, if none
    /// has room or there are no peers. Never streams: a message too large
    /// for a peer's log is not sent to that peer.
    pub fn poll_send(
        &mut self,
        peers: &mut PeerList,
        mut message: Message,
        cx: &mut TaskContext<'_>,
    ) -> Result<PeerKey, Message> {
        if self.poll_streaming(peers, cx).is_pending() {
            return Err(message);
        }
        let count = peers.peers.len();
        for offset in 0..count {
            let index = (self.next + offset) % count;
            let peer = &mut peers.peers[index];
            let tx = &mut peer.connection.tx;
            // Try first; only a full peer registers `cx`, then is tried once
            // more in case room appeared in between. Closed peers are skipped.
            match tx.try_send(message) {
                Ok(()) => {
                    self.next = (index + 1) % count;
                    return Ok(peer.key);
                }
                Err(TrySendError::Closed(back)) => {
                    message = back;
                    continue;
                }
                Err(TrySendError::Full(back)) => message = back,
            }
            if let Poll::Ready(Ok(())) = tx.poll_ready_for(cx, &message) {
                match tx.try_send(message) {
                    Ok(()) => {
                        self.next = (index + 1) % count;
                        return Ok(peer.key);
                    }
                    Err(TrySendError::Full(back) | TrySendError::Closed(back)) => message = back,
                }
            }
        }
        Err(message)
    }

    /// Like [`poll_send`](Self::poll_send), but a message too large for the
    /// next peer's log is streamed through it once that log has drained,
    /// staying on that peer until the message is written. Takes the message
    /// from `slot` once it is queued or streaming has started; `Ready` once
    /// it is all queued.
    pub fn poll_send_streaming(
        &mut self,
        peers: &mut PeerList,
        slot: &mut Option<Message>,
        cx: &mut TaskContext<'_>,
    ) -> Poll<()> {
        loop {
            if self.poll_streaming(peers, cx).is_pending() {
                return Poll::Pending;
            }
            let Some(message) = slot.take() else {
                return Poll::Ready(());
            };
            let message = match self.poll_send(peers, message, cx) {
                Ok(_) => return Poll::Ready(()),
                Err(message) => message,
            };
            let count = peers.peers.len();
            let chosen = (0..count)
                .map(|offset| (self.next + offset) % count)
                .find(|&index| {
                    let tx = &mut peers.peers[index].connection.tx;
                    tx.needs_streaming(&message)
                        && matches!(tx.poll_ready_for(cx, &message), Poll::Ready(Ok(())))
                });
            let Some(index) = chosen else {
                *slot = Some(message);
                return Poll::Pending;
            };
            self.next = (index + 1) % count;
            self.streaming = Some((peers.peers[index].key, Streaming::new(message)));
        }
    }

    /// Continues a message being streamed. `Ready` once there is none, or
    /// its peer is gone, which discards the rest.
    fn poll_streaming(&mut self, peers: &mut PeerList, cx: &mut TaskContext<'_>) -> Poll<()> {
        let Some((key, streaming)) = &mut self.streaming else {
            return Poll::Ready(());
        };
        if let Some(peer) = peers.get_mut(*key)
            && peer.connection.tx.poll_stream(streaming, cx).is_pending()
        {
            return Poll::Pending;
        }
        self.streaming = None;
        Poll::Ready(())
    }
}

/// Round robin over the peers that have a message waiting, like libzmq's
/// `fq_t`. Drops peers whose sessions are gone once drained.
#[derive(Debug, Default)]
pub struct FairQueue {
    next: usize,
}

impl FairQueue {
    /// The next message and the peer it came from, or `Pending` with `cx`
    /// registered on every peer.
    pub fn poll_recv(
        &mut self,
        peers: &mut PeerList,
        cx: &mut TaskContext<'_>,
    ) -> Poll<(PeerKey, Message)> {
        let mut tried = 0;
        while tried < peers.len() {
            let index = self.next % peers.len();
            let peer = &mut peers.peers[index];
            match peer.connection.rx.poll_recv(cx) {
                Poll::Ready(Some(message)) => {
                    self.next = index + 1;
                    return Poll::Ready((peer.key, message));
                }
                // Session gone and everything it delivered has been read.
                // The next peer slides into `index`.
                Poll::Ready(None) => {
                    peers.remove(index);
                    self.next = index;
                }
                Poll::Pending => {
                    self.next = index + 1;
                    tried += 1;
                }
            }
        }
        Poll::Pending
    }
}

/// ROUTER's map between peers and their routing ids, as in libzmq's
/// `router_t`.
///
/// A peer's routing id is the identity it announced in its handshake. A
/// peer that announced none gets a generated one: `0x00` and a 32-bit
/// counter starting at a random value. A peer announcing an identity
/// already in use is ignored: its messages are dropped and nothing can be
/// sent to it. With [`handover`](Self::set_handover) it takes the identity
/// over instead, and the peer that had it is disconnected.
#[derive(Debug)]
pub struct RouterTable {
    by_identity: HashMap<Box<[u8]>, PeerKey>,
    by_peer: HashMap<PeerKey, Route>,
    /// Peers whose handshake had not completed when last looked at.
    waiting: Vec<PeerKey>,
    /// Peers with keys below this have been seen.
    seen_below: u64,
    /// `PeerList::removals` when last synced.
    removals: u64,
    next_generated: u32,
    handover: bool,
}

/// What became of a peer's announced identity.
#[derive(Debug, Clone, PartialEq, Eq)]
enum Route {
    Routable(Frame),
    /// Duplicate identity: libzmq ignores the newcomer.
    Ignored,
}

/// What happened to a reply REP was asked to send.
#[derive(Debug, Clone, Copy, PartialEq, Eq)]
pub enum RouteOutcome {
    Sent,
    /// The requester is gone. The reply was dropped.
    NoSuchPeer,
    /// The requester's queue is full. The reply was dropped.
    PeerFull,
}

/// Why a ROUTER could not send without waiting. The message is handed back.
#[derive(Debug, Clone, PartialEq, Eq)]
pub enum RouterSendError {
    /// No peer has that routing id, or it has gone.
    UnknownRoutingId(Message),
    /// The peer's queue is full.
    Full(Message),
}

impl Default for RouterTable {
    fn default() -> Self {
        Self {
            by_identity: HashMap::new(),
            by_peer: HashMap::new(),
            waiting: Vec::new(),
            seen_below: 0,
            removals: 0,
            // libzmq starts the generated ids at a random value.
            next_generated: RandomState::new().hash_one(()) as u32,
            handover: false,
        }
    }
}

impl RouterTable {
    /// Whether a peer announcing an identity already in use takes it over,
    /// disconnecting the peer that had it (libzmq's `ZMQ_ROUTER_HANDOVER`).
    /// Otherwise the newcomer is ignored.
    pub fn set_handover(&mut self, on: bool) {
        self.handover = on;
    }

    /// Identifies peers whose handshake has completed and forgets peers that
    /// are gone.
    pub fn sync(&mut self, peers: &mut PeerList) {
        if peers.removals != self.removals {
            self.removals = peers.removals;
            let live: HashSet<PeerKey> = peers.iter().map(Peer::key).collect();
            self.by_peer.retain(|key, _| live.contains(key));
            self.by_identity.retain(|_, key| live.contains(key));
            self.waiting.retain(|key| live.contains(key));
        }
        let first_new = peers
            .peers
            .partition_point(|peer| peer.key.0 < self.seen_below);
        self.waiting
            .extend(peers.peers[first_new..].iter().map(Peer::key));
        self.seen_below = peers.next_key;
        for key in std::mem::take(&mut self.waiting) {
            let Some(peer) = peers.get(key) else {
                continue;
            };
            match peer.connection.peer() {
                Some(established) => self.identify(peers, key, established.peer_identity),
                None => self.waiting.push(key),
            }
        }
    }

    fn identify(&mut self, peers: &mut PeerList, key: PeerKey, announced: Frame) {
        let routing_id = if announced.is_empty() {
            let mut generated = [0u8; 5];
            generated[1..].copy_from_slice(&self.next_generated.to_be_bytes());
            self.next_generated = self.next_generated.wrapping_add(1);
            Frame::copy_from_slice(&generated)
        } else {
            announced
        };
        if let Some(&holder) = self.by_identity.get(routing_id.as_slice()) {
            if !self.handover {
                self.by_peer.insert(key, Route::Ignored);
                return;
            }
            self.by_peer.remove(&holder);
            if let Ok(index) = peers.peers.binary_search_by_key(&holder, Peer::key) {
                peers.remove(index);
            }
        }
        self.by_identity.insert(routing_id.as_slice().into(), key);
        self.by_peer.insert(key, Route::Routable(routing_id));
    }

    /// The routing id of `peer`, if it has been identified and not ignored.
    pub fn routing_id(&self, peer: PeerKey) -> Option<&Frame> {
        match self.by_peer.get(&peer) {
            Some(Route::Routable(routing_id)) => Some(routing_id),
            Some(Route::Ignored) | None => None,
        }
    }

    /// Sends `message` to the peer with `routing_id` if it is known and has
    /// room. Never blocks.
    pub fn try_send(
        &mut self,
        peers: &mut PeerList,
        routing_id: &[u8],
        message: Message,
    ) -> Result<(), RouterSendError> {
        let Some(peer) = self
            .by_identity
            .get(routing_id)
            .and_then(|&key| peers.get_mut(key))
        else {
            return Err(RouterSendError::UnknownRoutingId(message));
        };
        peer.connection
            .tx
            .try_send(message)
            .map_err(|err| match err {
                TrySendError::Full(message) => RouterSendError::Full(message),
                TrySendError::Closed(message) => RouterSendError::UnknownRoutingId(message),
            })
    }

    /// Sends the message in `slot` to the peer with `routing_id`, or
    /// returns `Pending` with `cx` registered while that peer is full. Fails
    /// with the message if no peer has that routing id or it has gone.
    pub fn poll_send(
        &mut self,
        peers: &mut PeerList,
        routing_id: &[u8],
        slot: &mut Option<Message>,
        cx: &mut TaskContext<'_>,
    ) -> Poll<Result<(), Message>> {
        let Some(message) = slot.take() else {
            return Poll::Ready(Ok(()));
        };
        let Some(peer) = self
            .by_identity
            .get(routing_id)
            .and_then(|&key| peers.get_mut(key))
        else {
            return Poll::Ready(Err(message));
        };
        let tx = &mut peer.connection.tx;
        match tx.poll_ready(cx) {
            Poll::Ready(Ok(())) => {}
            Poll::Ready(Err(_)) => return Poll::Ready(Err(message)),
            Poll::Pending => {
                *slot = Some(message);
                return Poll::Pending;
            }
        }
        match tx.try_send(message) {
            Ok(()) => Poll::Ready(Ok(())),
            Err(TrySendError::Closed(message)) => Poll::Ready(Err(message)),
            Err(TrySendError::Full(message)) => {
                *slot = Some(message);
                Poll::Pending
            }
        }
    }
}

/// SERVER's routing (RFC 41), as libzmq's `server_t`: every peer gets a
/// non-zero 32-bit id, from a counter that starts at a random value;
/// replies go to a peer by its id. Single-part messages only.
#[cfg(feature = "draft")]
#[derive(Debug)]
pub struct ServerTable {
    by_id: HashMap<std::num::NonZeroU32, PeerKey>,
    by_peer: HashMap<PeerKey, std::num::NonZeroU32>,
    /// Peers with keys below this have ids.
    seen_below: u64,
    /// `PeerList::removals` when last synced.
    removals: u64,
    next_id: u32,
    recv: FairQueue,
}

/// Why SERVER could not send to a client.
#[cfg(feature = "draft")]
#[derive(Debug, Clone, PartialEq, Eq)]
pub enum ServerSendError {
    /// No client has that id, or it has gone. The frame is handed back.
    NoSuchClient(Frame),
    /// The client's queue is full. The frame is handed back.
    Full(Frame),
}

#[cfg(feature = "draft")]
impl Default for ServerTable {
    fn default() -> Self {
        Self {
            by_id: HashMap::new(),
            by_peer: HashMap::new(),
            seen_below: 0,
            removals: 0,
            recv: FairQueue::default(),
            // libzmq starts its ids at a random value.
            next_id: RandomState::new().hash_one(()) as u32,
        }
    }
}

#[cfg(feature = "draft")]
impl ServerTable {
    /// Gives new peers their ids and forgets peers that are gone.
    pub fn sync(&mut self, peers: &PeerList) {
        if peers.removals != self.removals {
            self.removals = peers.removals;
            let live: HashSet<PeerKey> = peers.iter().map(Peer::key).collect();
            self.by_peer.retain(|key, _| live.contains(key));
            self.by_id.retain(|_, key| live.contains(key));
        }
        let first_new = peers
            .peers
            .partition_point(|peer| peer.key.0 < self.seen_below);
        for peer in &peers.peers[first_new..] {
            let id = loop {
                let candidate = self.next_id;
                self.next_id = self.next_id.wrapping_add(1);
                // Never zero, and never one still in use after wrapping.
                if let Some(id) = std::num::NonZeroU32::new(candidate)
                    && !self.by_id.contains_key(&id)
                {
                    break id;
                }
            };
            self.by_id.insert(id, peer.key);
            self.by_peer.insert(peer.key, id);
        }
        self.seen_below = peers.next_key;
    }

    /// The next single-part message and the id of the client that sent it,
    /// or `Pending` with `cx` registered on every peer. Multipart messages
    /// are dropped, as RFC 41 requires.
    pub fn poll_recv(
        &mut self,
        peers: &mut PeerList,
        cx: &mut TaskContext<'_>,
    ) -> Poll<(std::num::NonZeroU32, Frame)> {
        self.sync(peers);
        loop {
            let Poll::Ready((key, message)) = self.recv.poll_recv(peers, cx) else {
                return Poll::Pending;
            };
            let (frame, rest) = message.split_first();
            if !rest.is_empty() {
                continue;
            }
            if let Some(&id) = self.by_peer.get(&key) {
                return Poll::Ready((id, frame.clone()));
            }
        }
    }

    /// Sends `frame` to the client with `id`, without waiting. On `Full`,
    /// `cx` is registered to hear when it has room.
    pub fn try_send(
        &mut self,
        peers: &mut PeerList,
        id: std::num::NonZeroU32,
        frame: Frame,
        cx: &mut TaskContext<'_>,
    ) -> Result<(), ServerSendError> {
        self.sync(peers);
        let Some(peer) = self.by_id.get(&id).and_then(|&key| peers.get_mut(key)) else {
            return Err(ServerSendError::NoSuchClient(frame));
        };
        let tx = &mut peer.connection.tx;
        match tx.poll_ready(cx) {
            Poll::Ready(Ok(())) => match tx.try_send(Message::new(frame)) {
                Ok(()) => Ok(()),
                Err(TrySendError::Full(message)) => Err(ServerSendError::Full(only_frame(message))),
                Err(TrySendError::Closed(message)) => {
                    Err(ServerSendError::NoSuchClient(only_frame(message)))
                }
            },
            // The client's session has ended.
            Poll::Ready(Err(())) => Err(ServerSendError::NoSuchClient(frame)),
            Poll::Pending => Err(ServerSendError::Full(frame)),
        }
    }

    /// The id of the peer with `key`, once it is in the list.
    pub fn id_of(&mut self, peers: &PeerList, key: PeerKey) -> Option<std::num::NonZeroU32> {
        self.sync(peers);
        self.by_peer.get(&key).copied()
    }

    /// Closes the connection to the client with `id`. Returns whether there
    /// was one.
    pub fn disconnect(&mut self, peers: &mut PeerList, id: std::num::NonZeroU32) -> bool {
        self.sync(peers);
        let Some(key) = self.by_id.remove(&id) else {
            return false;
        };
        self.by_peer.remove(&key);
        peers.retain_mut(|peer| peer.key != key);
        true
    }
}

/// The frame of a message built from one.
#[cfg(feature = "draft")]
fn only_frame(message: Message) -> Frame {
    message.into_iter().next().expect("a message has a frame")
}

#[cfg(test)]
mod tests {
    use super::*;
    use crate::test_support::*;
    use std::sync::atomic::Ordering;

    #[test]
    fn load_balancer_round_robins() {
        let (a, mut sa) = connection(10);
        let (b, mut sb) = connection(10);
        let (c, mut sc) = connection(10);
        let mut peers = list(vec![a, b, c]);
        let (_, waker) = waker();
        let mut cx = TaskContext::from_waker(&waker);
        let mut lb = LoadBalancer::default();
        for text in ["1", "2", "3", "4", "5", "6"] {
            lb.poll_send(&mut peers, msg(text), &mut cx).unwrap();
        }
        assert_eq!(drain(&mut sa), [msg("1"), msg("4")]);
        assert_eq!(drain(&mut sb), [msg("2"), msg("5")]);
        assert_eq!(drain(&mut sc), [msg("3"), msg("6")]);
    }

    #[test]
    fn load_balancer_skips_full_peers() {
        let (a, mut sa) = connection(1);
        let (b, mut sb) = connection(10);
        let mut peers = list(vec![a, b]);
        let (_, waker) = waker();
        let mut cx = TaskContext::from_waker(&waker);
        let mut lb = LoadBalancer::default();
        for text in ["1", "2", "3", "4"] {
            lb.poll_send(&mut peers, msg(text), &mut cx).unwrap();
        }
        assert_eq!(drain(&mut sa), [msg("1")]);
        assert_eq!(drain(&mut sb), [msg("2"), msg("3"), msg("4")]);
    }

    #[test]
    fn load_balancer_waits_when_every_peer_is_full() {
        let (a, mut sa) = connection(1);
        let mut peers = list(vec![a]);
        let (count, waker) = waker();
        let mut cx = TaskContext::from_waker(&waker);
        let mut lb = LoadBalancer::default();
        lb.poll_send(&mut peers, msg("1"), &mut cx).unwrap();
        assert_eq!(lb.poll_send(&mut peers, msg("2"), &mut cx), Err(msg("2")));
        sa.from_socket.try_recv().unwrap();
        assert!(count.0.load(Ordering::SeqCst) > 0, "room wakes the sender");
        lb.poll_send(&mut peers, msg("2"), &mut cx).unwrap();
    }

    #[test]
    fn load_balancer_without_peers_hands_the_message_back() {
        let (_, waker) = waker();
        let mut cx = TaskContext::from_waker(&waker);
        let mut lb = LoadBalancer::default();
        let mut peers = PeerList::default();
        assert_eq!(lb.poll_send(&mut peers, msg("x"), &mut cx), Err(msg("x")));
    }

    #[test]
    fn fair_queue_alternates_between_peers() {
        let (a, mut sa) = connection(10);
        let (b, mut sb) = connection(10);
        for text in ["a1", "a2", "a3"] {
            sa.to_socket.try_send(msg(text)).unwrap();
        }
        for text in ["b1", "b2"] {
            sb.to_socket.try_send(msg(text)).unwrap();
        }
        let mut peers = list(vec![a, b]);
        let keys: Vec<PeerKey> = peers.iter().map(Peer::key).collect();
        let (_, waker) = waker();
        let mut cx = TaskContext::from_waker(&waker);
        let mut fq = FairQueue::default();
        let order: Vec<(PeerKey, Message)> = (0..5)
            .map(|_| match fq.poll_recv(&mut peers, &mut cx) {
                Poll::Ready(received) => received,
                Poll::Pending => panic!("messages are waiting"),
            })
            .collect();
        let (a, b) = (keys[0], keys[1]);
        assert_eq!(
            order,
            [
                (a, msg("a1")),
                (b, msg("b1")),
                (a, msg("a2")),
                (b, msg("b2")),
                (a, msg("a3"))
            ]
        );
        assert_eq!(fq.poll_recv(&mut peers, &mut cx), Poll::Pending);
    }

    #[test]
    fn fair_queue_is_woken_by_any_peer() {
        let (a, _sa) = connection(10);
        let (b, mut sb) = connection(10);
        let mut peers = list(vec![a, b]);
        let (count, waker) = waker();
        let mut cx = TaskContext::from_waker(&waker);
        let mut fq = FairQueue::default();
        assert_eq!(fq.poll_recv(&mut peers, &mut cx), Poll::Pending);
        sb.to_socket.try_send(msg("late")).unwrap();
        assert!(count.0.load(Ordering::SeqCst) > 0);
        assert!(
            matches!(fq.poll_recv(&mut peers, &mut cx), Poll::Ready((_, m)) if m == msg("late"))
        );
    }

    #[test]
    fn fair_queue_drains_then_drops_a_closed_peer() {
        let (a, mut sa) = connection(10);
        let (b, _sb) = connection(10);
        sa.to_socket.try_send(msg("last words")).unwrap();
        sa.to_socket.try_send(msg("bye")).unwrap();
        drop(sa);
        let mut peers = list(vec![a, b]);
        let (_, waker) = waker();
        let mut cx = TaskContext::from_waker(&waker);
        let mut fq = FairQueue::default();
        let mut next = || match fq.poll_recv(&mut peers, &mut cx) {
            Poll::Ready((_, message)) => Some(message),
            Poll::Pending => None,
        };
        assert_eq!(next(), Some(msg("last words")));
        assert_eq!(next(), Some(msg("bye")));
        assert_eq!(next(), None);
        assert_eq!(peers.len(), 1, "the closed peer is dropped once drained");
    }

    #[test]
    fn peers_are_found_by_key_after_removals() {
        let (a, _sa) = connection(10);
        let (b, sb) = connection(10);
        let (c, _sc) = connection(10);
        let mut peers = list(vec![a, b, c]);
        let keys: Vec<PeerKey> = peers.iter().map(Peer::key).collect();
        drop(sb);
        peers.retain_mut(|peer| !peer.connection.tx.is_closed());
        assert!(peers.get_mut(keys[1]).is_none());
        assert_eq!(peers.get_mut(keys[2]).map(|p| p.key()), Some(keys[2]));
    }

    // ---- RouterTable ----

    #[test]
    fn router_routes_by_announced_identity() {
        let (a, mut sa) = connection(10);
        let (b, mut sb) = connection(10);
        sa.establish("alice");
        sb.establish("bob");
        let mut peers = list(vec![a, b]);
        let mut table = RouterTable::default();
        table.sync(&mut peers);
        assert_eq!(table.try_send(&mut peers, b"bob", msg("hi bob")), Ok(()));
        assert_eq!(
            table.try_send(&mut peers, b"alice", msg("hi alice")),
            Ok(())
        );
        assert_eq!(drain(&mut sa), [msg("hi alice")]);
        assert_eq!(drain(&mut sb), [msg("hi bob")]);
    }

    #[test]
    fn router_hands_back_messages_it_cannot_route() {
        let (a, mut sa) = connection(1);
        sa.establish("alice");
        let mut peers = list(vec![a]);
        let mut table = RouterTable::default();
        table.sync(&mut peers);
        assert_eq!(
            table.try_send(&mut peers, b"nobody", msg("x")),
            Err(RouterSendError::UnknownRoutingId(msg("x")))
        );
        assert_eq!(table.try_send(&mut peers, b"alice", msg("1")), Ok(()));
        assert_eq!(
            table.try_send(&mut peers, b"alice", msg("2")),
            Err(RouterSendError::Full(msg("2")))
        );
        assert_eq!(drain(&mut sa), [msg("1")]);
    }

    #[test]
    fn router_waits_for_the_handshake_before_routing() {
        let (a, sa) = connection(10);
        let mut peers = list(vec![a]);
        let key = peers.iter().next().unwrap().key();
        let mut table = RouterTable::default();
        table.sync(&mut peers);
        assert_eq!(table.routing_id(key), None, "not identified yet");
        sa.establish("late");
        table.sync(&mut peers);
        assert_eq!(table.routing_id(key), Some(&Frame::from("late")));
    }

    #[test]
    fn router_generates_ids_for_anonymous_peers() {
        let (a, sa) = connection(10);
        let (b, sb) = connection(10);
        sa.establish("");
        sb.establish("");
        let mut peers = list(vec![a, b]);
        let keys: Vec<PeerKey> = peers.iter().map(Peer::key).collect();
        let mut table = RouterTable::default();
        table.sync(&mut peers);
        let first = table.routing_id(keys[0]).unwrap().clone();
        let second = table.routing_id(keys[1]).unwrap().clone();
        for id in [&first, &second] {
            assert_eq!(id.len(), 5);
            assert_eq!(id[0], 0, "generated ids start with a zero octet");
        }
        let counter = |id: &Frame| u32::from_be_bytes(id[1..].try_into().unwrap());
        assert_eq!(counter(&second), counter(&first).wrapping_add(1));
    }

    #[test]
    fn router_ignores_a_duplicate_identity() {
        let (a, mut sa) = connection(10);
        let (b, mut sb) = connection(10);
        sa.establish("same");
        sb.establish("same");
        let mut peers = list(vec![a, b]);
        let keys: Vec<PeerKey> = peers.iter().map(Peer::key).collect();
        let mut table = RouterTable::default();
        table.sync(&mut peers);
        assert_eq!(table.routing_id(keys[1]), None, "the newcomer is ignored");
        assert_eq!(table.try_send(&mut peers, b"same", msg("x")), Ok(()));
        assert_eq!(drain(&mut sa), [msg("x")]);
        assert!(drain(&mut sb).is_empty());
    }

    #[test]
    fn with_handover_a_duplicate_identity_replaces_the_holder() {
        let (a, sa) = connection(10);
        let (b, mut sb) = connection(10);
        sa.establish("same");
        sb.establish("same");
        let mut peers = list(vec![a, b]);
        let keys: Vec<PeerKey> = peers.iter().map(Peer::key).collect();
        let mut table = RouterTable::default();
        table.set_handover(true);
        table.sync(&mut peers);
        assert_eq!(table.routing_id(keys[0]), None);
        assert_eq!(table.routing_id(keys[1]), Some(&Frame::from("same")));
        assert!(peers.get(keys[0]).is_none(), "the holder is disconnected");
        assert_eq!(table.try_send(&mut peers, b"same", msg("x")), Ok(()));
        assert_eq!(drain(&mut sb), [msg("x")]);
    }

    #[test]
    fn router_forgets_peers_that_are_gone() {
        let (a, sa) = connection(10);
        sa.establish("gone");
        let mut peers = list(vec![a]);
        let mut table = RouterTable::default();
        table.sync(&mut peers);
        drop(sa);
        assert_eq!(
            table.try_send(&mut peers, b"gone", msg("w")),
            Err(RouterSendError::UnknownRoutingId(msg("w"))),
            "gone but not yet forgotten"
        );
        peers.retain_mut(|peer| !peer.connection.tx.is_closed());
        table.sync(&mut peers);
        assert_eq!(
            table.try_send(&mut peers, b"gone", msg("x")),
            Err(RouterSendError::UnknownRoutingId(msg("x")))
        );
        // A new peer may now take the identity.
        let (b, mut sb) = connection(10);
        sb.establish("gone");
        peers.push(b);
        table.sync(&mut peers);
        assert_eq!(table.try_send(&mut peers, b"gone", msg("y")), Ok(()));
        assert_eq!(drain(&mut sb), [msg("y")]);
    }
}
