# Architecture

zmq-rs follows libzmq's runtime design: sockets live in the user's threads,
and connections are served by a small pool of I/O threads that the sockets
talk to through lock-free pipes. The wire protocol (`zmtp`) and the pipes
(`zmq-pipe`) know nothing about threads or sockets; `zmq-core` puts them
together.

```
user thread                              I/O thread (one of N, each a mio event loop)
───────────                              ─────────────────────────────────────────────
Socket ──Writer<Message>──▶ pipe ──▶ Session ── Engine ── TcpStream
       ◀──Reader<Message>── pipe ◀──    │       Greeting → Handshake → Codec
                                        └─ owns the pipes
Context ─ IoCommand ─▶ mailbox + mio::Waker
```

## Crates

| Crate | Role | I/O |
|---|---|---|
| `zmtp` | Greeting, handshakes (NULL, PLAIN), commands, frame codecs: state machines over bytes | none |
| `zmq-pipe` | SPSC `Writer`/`Reader` with high-water marks and wakers | none |
| `zmq-core` | Context, I/O threads, sessions, engines, transports, routing | mio |
| `zmq` | Typed sockets for applications | via `zmq-core` |
| `tests/interop` | Tests against the reference libzmq | libzmq |

## Pieces in `zmq-core`

| Piece | Role | libzmq |
|---|---|---|
| `Context` | Shared handle to N I/O threads; dropping the last clone stops and joins them | `ctx_t` |
| `IoThread` | A `std` thread running `mio::Poll`, reached through a command channel plus a `mio::Waker` | `io_thread_t`, `mailbox_t` |
| `IoCommand` | The thread's command type (`Connect`, `Listen`, `Shutdown`, `Stop`) | `command_t` |
| `Listener` | A bound port; accepted connections arrive through a pipe of `Connection`s | `tcp_listener_t` |
| `IpcListener` | A bound `ipc://` path (Unix domain socket), served like a TCP listener; removes its socket file when it closes | `ipc_listener_t` |
| `Stream` | A connection's byte stream, TCP or Unix domain socket; engines and sessions are the same for both | `tcp_connecter_t`, `ipc_connecter_t` |
| `InprocListener` | A bound `inproc` name in the context's registry; paired connections arrive the same way | `ctx_t::endpoints`, `pend_connection` |
| `Session` | One per peer: owns both pipe ends and outlives individual connections, reconnecting on its `Reconnect` schedule and lingering after close | `session_base_t` |
| `Engine` | One TCP connection: greeting, handshake, then codecs, with partial-write buffering | `zmtp_engine_t` |
| `TokenWaker` | A pipe waker that queues its session or listener id once and wakes the event loop | `activate_read`, `activate_write` |

## Event flow

An I/O thread wakes for three reasons, all handled in one loop:

1. **Socket readiness** from mio, for a session's TCP stream.
2. **Pipe wakeups**: when a socket sends into an empty pipe, or reads from a
   full one, the pipe's waker queues the session id and wakes the loop.
3. **Commands** from other threads, such as a new connection or shutdown.

Each due session is driven until it can make no more progress: flush output,
take messages from the socket while less than 64 KiB is unsent, then read and
deliver input.

mio is edge-triggered, so reads and writes always continue until the OS says
`WouldBlock`. The exception is backpressure: when the receive pipe is full
the engine stops reading early and keeps the refused message. The pipe's
waker resumes it once the socket catches up, since the TCP stream will not
signal again for data it has already reported.

## Lifetimes

- A connection's pipes exist as soon as `connect` returns, so messages sent
  before the handshake are queued and delivered once it completes.
- A connecting session outlives its TCP connections. After a connection
  error (I/O error, peer closed, refused) it reconnects on its `Reconnect`
  schedule, keeping its pipes and any messages queued in them. As in libzmq
  (`session_base_t::engine_error`), a protocol error such as a failed
  handshake or an incompatible peer ends the session instead, closing its
  pipes. Accepted sessions never reconnect.
- A SUB or XSUB session remembers the topics it has carried, and the next
  connection sends them before anything else in the pipe. The new peer
  starts with no subscriptions, so this is how it learns them.
- `Reconnect::Fixed` waits the interval plus random jitter below it, and
  `Reconnect::Backoff` doubles from `initial` up to `max`, resetting once a
  handshake succeeds; both follow libzmq's `get_new_reconnect_ivl`.
- Dropping a connection's sender starts its linger: queued messages keep
  being delivered, across reconnects, until the queue is written or the
  `Linger` deadline passes. `Linger::For(Duration::ZERO)` discards them at
  once. Dropping only the receiver discards incoming messages but keeps the
  connection sending.
- `Context::shutdown(timeout)` closes listeners and every session, lets each
  deliver its queue within its linger and the timeout, then joins the I/O
  threads. Connections still delivering or still disconnected at the
  deadline are reported as abandoned.
- Dropping the last `Context` clone without `shutdown` closes everything at
  once. Unlike libzmq's `zmq_ctx_term`, nothing ever blocks on drop.
- `bind` binds immediately, so errors such as an address in use are reported
  to the caller, then hands the listener to an I/O thread.
- An accepted connection reaches the `Listener` only once its handshake has
  succeeded, as in libzmq (`session_base_t::engine_ready`); peers that fail
  never appear. Accepted sessions stay on the listener's I/O thread for now.
- Dropping the `Listener` closes the port: the accept queue's
  `Writer::poll_closed` wakes the I/O thread when its reader goes away.

## Sockets

A socket lives in the user's thread and owns a `Peers` set: the connections
it made with `connect`, plus those its listeners accepted. Routing
strategies work over that set, as in libzmq:

- `LoadBalancer` (`lb_t`) sends each message to the next peer with room,
  round robin, and waits while none has room.
- `FairQueue` (`fq_t`) takes the next message round robin from peers that
  have one waiting, reports which peer sent it, and drops a peer once its
  session is gone and everything it delivered has been read.
- `RouterTable` (`router_t`) maps routing ids to peers. A peer's routing id
  is the identity from its handshake; anonymous peers get `0x00` and a
  32-bit counter that starts at a random value; a peer reusing an identity
  already in use is ignored. ROUTER never blocks on send: a message for an
  unknown or full peer is dropped and the `RouteOutcome` says so.

- `Requester` (`req_t`) sends each request, behind an empty delimiter, to
  the next peer with room and accepts a reply only from that peer,
  discarding what other peers send. With correlation on, a 4-octet request
  id leads the envelope and replies with another id are dropped.
- `Replier` (`rep_t`) fair-queues requests, keeps every frame up to the
  delimiter as the envelope, and sends the reply back behind it.
- `Publisher` (`xpub_t`, `dist_t`) keeps each peer's topics, read from
  subscription messages as it sends, and queues a message for every peer
  subscribed to a prefix of its first frame that has room. It never
  blocks. A ZMTP 1.0 peer, which sends no subscriptions, is subscribed to
  everything. For XPUB it also counts the peers subscribed to each topic
  and queues `XPubEvent`s: a topic's first subscription and last cancel
  (including one caused by a peer leaving), and any other message a peer
  sent.
- `Subscriber` (`xsub_t`) counts its topics, sends them to each new peer
  and every change to all peers, and drops arriving messages that match no
  topic. `Topics` does the prefix matching for both. XSUB uses it too,
  with `send_upstream` for messages to every publisher.
- `Pair` (`pair_t`) keeps the oldest peer and closes every other. When the
  peer's session ends, what it delivered moves to a leftover queue that is
  received first, and the next peer to arrive takes its place.

In the `zmq` crate, REQ/REP lockstep is a borrow: `Socket<Req>::request`
takes `&mut self` until the reply arrives, and `Socket<Rep>::recv` returns a
`Request<'_>` that borrows the socket until `reply(self, ..)` consumes it.

Each peer has a stable `PeerKey`. Keys only grow and removals keep order,
so a peer is found by binary search. A connection learns its peer from the
handshake: the engine publishes the `Established` result before delivering
any message from that handshake, so ROUTER never sees a message it cannot
attribute.

A socket waits on many pipes at once by polling each with the same waker,
so blocking calls, `async` methods and `try_` methods share one code path.
In the `zmq` crate, `Socket<T>` only has the methods its type allows:
`Socket<Push>` has `send`, `Socket<Pull>` has `recv`, and calling the other
is a compile error.

## Security

`ConnectionConfig::security` (and `SocketOptions::security`) selects NULL,
a PLAIN client with its credentials, a PLAIN server with an
`Authenticator`, a CURVE client with its key pair and the server's public
key, or a CURVE server with its key pair and, optionally, an
`Authenticator`. The engine runs the matching `zmtp` state machine after
the greeting.

A PLAIN or CURVE server hands each client's `ZapRequest` to its authenticator with a
`ZapResponder`, then waits without reading further input. The authenticator
may answer at once or later from any thread; answering wakes the session.
A responder dropped unanswered counts as ZAP status 500, so a forgotten
answer rejects the client rather than hanging the handshake. A closure
`Fn(&ZapRequest) -> ZapReply` is an authenticator that answers at once.

A failed authentication is a protocol error: the session ends without
reconnecting, as in libzmq. The authenticated user id and metadata reach
the socket through `Connection::peer().authenticated`.

Once a CURVE handshake completes, the engine keeps its `CurveCodec`, and
every frame in or out goes through it: messages, and the PING, PONG,
SUBSCRIBE and CANCEL commands too. A frame that fails to open, or whose
nonce does not increase, is a protocol error and ends the session.

## Heartbeats and deadlines

An engine keeps deadlines rather than timers: the handshake deadline
(`handshake_timeout`, 30 s by default, starting once TCP connects), the next
PING, the PING timeout, and the TTL from the peer's last PING. Each drive
checks them, sends a PING if one is due, and reports the earliest; the
session sets an event-loop timer for it only when it is earlier than the
one already pending. As in libzmq, any frame from the peer clears the PING
timeout and TTL, and a PING carrying a TTL sets one. An expired deadline is
a connection error, so a connecting session reconnects. PINGs are sent only
to ZMTP 3.x peers.

## Proxy

`zmq::Proxy` is libzmq's `zmq_proxy_steerable`. It borrows a frontend and
a backend socket and forwards between them in both directions, in libzmq's
raw message form: a ROUTER's messages carry the routing id as their first
frame, and an XPUB's events become `0x01`/`0x00`-prefixed messages, which
an XSUB sends upstream as subscriptions. That raw form makes the classic
shapes work: ROUTER/DEALER is a shared queue, XSUB/XPUB a forwarder, and
PULL/PUSH a streamer.

- Which sockets can be ends (`ProxyEnd`: not REQ or REP, whose lockstep
  does not fit) and which can capture (`Capture`: PUB, DEALER, PUSH and
  PAIR, as libzmq requires) are traits, so a wrong choice does not compile.
- Each direction holds at most one message in flight. While the receiving
  side has no room, that direction stops reading and the other carries on;
  nothing is lost. libzmq blocks in `send`, holding up both directions.
- The proxy moves up to 256 messages per direction, then yields, so
  neither direction starves the other or the control.
- A capture copy the capture socket has no room for is dropped; a slow tap
  never holds up the proxy.
- `ProxyControl` replaces libzmq's control socket: a cloneable handle that
  pauses, resumes and terminates the proxy, waking it at once, and reads
  `ProxyStatistics` (libzmq's eight STATISTICS counters, counting each
  message part).
- `run` blocks the thread; `run_async` is the same as a future, for any
  executor.

## `inproc`

An `inproc` connection is a pair of pipes and nothing else: no I/O thread,
no engine, no encoding, so a message's frames move between sockets
without being copied. Each context keeps a registry of bound names and of
connections waiting for a name.

- Binding a name that a live listener holds fails with `AddrInUse`.
  Dropping the listener (its socket) frees the name.
- Connecting to a bound name pairs at once. Connecting to an unbound name
  creates the pipes anyway, so messages queue, and parks the far end until
  the name is bound. A parked connection whose socket is dropped is
  forgotten.
- Pairing fills both `PeerSlot`s with what a handshake would have
  established (ZMTP 3.1, the other side's socket type and identity). An
  incompatible pair is never paired: the far end is dropped, which closes
  the connection.
- As in libzmq, each direction's HWM is the sender's send HWM plus the
  receiver's receive HWM, unlimited if either is. A connection made
  before the bind only knows its own side, so it uses its own HWMs.
- `inproc` connections never reconnect, and security and heartbeats do
  not apply.

## `ipc`

An `ipc://` connection is a Unix domain socket and otherwise exactly a TCP
connection: the same sessions, engines, reconnection, security and
heartbeats run over a `Stream` that is either kind. Unix only.

- Binding a path that holds a socket file nobody listens on replaces it,
  as a crashed process leaves it. A live socket or any other kind of file
  is `AddrInUse`. libzmq unlinks whatever is there.
- `ipc://*` binds `socket` in a new temporary directory and reports the
  path.
- When the listener closes, its socket file is removed, along with the
  directory a wildcard made, but only if the file is still the one it
  created (same device and inode). libzmq removes only wildcard files.
- ZAP sees an empty address for IPC peers, as in libzmq.
- Abstract socket names (`ipc://@name`, Linux) are not supported yet; see
  the to-do list below.

### To do on Linux: abstract socket names

Linux-only work that cannot be built or tested on macOS. Reference:
libzmq's `src/ipc_address.cpp` (`resolve` and `to_string`).

Behaviour to match:

- `ipc://@name` names a socket in the abstract namespace: `sun_path`
  starts with a NUL byte, then `name`. There is no file: the name vanishes
  when the last socket bound to it closes.
- `ipc://@` alone is invalid (libzmq: `EINVAL`), as is a name too long for
  `sun_path` (108 octets including the leading NUL; libzmq: `ENAMETOOLONG`).
- A second bind to a live name fails with `EADDRINUSE` from the kernel, so
  there is no stale-file probe and no cleanup.
- `ipc://@*` is not a wildcard in libzmq; treat `*` as a literal name.
- Elsewhere (macOS, BSDs), `ipc://@name` stays an ordinary relative path
  starting with `@`, as in libzmq.

Steps:

- [ ] **Parsing** (`crates/zmq/src/endpoint.rs`, `local_target`): on Linux
  and Android, `ipc://@name` becomes a new `Target::IpcAbstract(Vec<u8>)`.
  An empty name is `InvalidEndpoint`. Add `Endpoint::IpcAbstract` (or a
  field on `Endpoint::Ipc`) that displays as `ipc://@name`.
- [ ] **Address** (`crates/zmq-core/src/transport.rs`): add
  `Address::IpcAbstract(Vec<u8>)` under
  `#[cfg(any(target_os = "linux", target_os = "android"))]`. Connect with
  `mio::net::UnixStream::connect_addr` and bind with
  `mio::net::UnixListener::bind_addr`, both taking a
  `std::os::unix::net::SocketAddr` built by
  `std::os::linux::net::SocketAddrExt::from_abstract_name`. Both mio
  functions exist in 1.2.4.
- [ ] **Context and Peers** (`context.rs`, `socket.rs`): add
  `Context::bind_ipc_abstract(name, config)` and
  `connect_ipc_abstract(name, config)` beside `bind_ipc`/`connect_ipc`. The
  listener needs no `IpcCleanup` (pass `None`). `IpcListener::path` has no
  meaning here: give the listener an enum address, or add a separate
  listener type.
- [ ] **`Socket::bind`/`connect`** (`crates/zmq/src/socket.rs`): dispatch
  the new target and return the new endpoint.
- [ ] **Tests** (`crates/zmq/tests/ipc.rs`, `#[cfg(target_os = "linux")]`):
  - push/pull over `ipc://@name`, and the displayed endpoint round-trips;
  - a second bind is `AddrInUse`, and the name is free again once the
    first socket is dropped;
  - `ipc://@` is `InvalidEndpoint`, and an over-long name fails;
  - a connect before the bind retries until it works;
  - no file appears anywhere.
- [ ] **Interop** (`tests/interop/tests/ipc.rs`): libzmq binds
  `ipc://@name` and we connect, and the reverse.
- [ ] **Docs**: replace the bullet above with the behaviour, and add the
  Linux-only note to `docs/rfc-compliance.md` if anything differs from
  libzmq.

Also check while on Linux:

- [ ] Run the stale-file mutation: drop the `is_socket()` check in
  `transport::ipc::bind_ipc`. On Linux, `connect()` to a regular file
  fails with `ECONNREFUSED`, so `an_ordinary_file_is_never_replaced`
  should fail. On macOS this mutant survives because `connect()` fails
  differently there.
- [ ] Run the whole suite 20 times and `nix flake check` on Linux. The ipc
  tests use `std::env::temp_dir()`, so check that their paths fit within
  Linux's 108-octet `sun_path`.

## Status

| Slice | Contents | State |
|---|---|---|
| 1 | Context, I/O threads, TCP connect, NULL handshake, sessions, engines | done |
| 2 | Bind/listen, accepting peers | done |
| 3 | Reconnection with backoff (sessions outlive connections), linger, shutdown | done |
| 4a | `Peers`, load balancing and fair queuing; PUSH/PULL as typed sockets in `zmq` | done |
| 4b | DEALER and ROUTER: identity routing and envelopes | done |
| 4c | REQ and REP | done |
| 5a | PLAIN on connections, with an `Authenticator` trait | done |
| 5b | Heartbeats (ZMTP 3.1 PING/PONG) and the handshake deadline | done |
| 5c | `inproc` | done |
| 6a | PUB and SUB; subscriptions on the wire and replayed after reconnecting | done |
| 6b | XPUB and XSUB | done |
| 6c | PAIR | done |
| 7 | `ipc://` | done |
| 8a | CURVE in `zmtp`: Z85, keys, handshake, MESSAGE boxing | done |
| 8b | CURVE on connections and sockets | done |
| 9 | Built-in proxy, with capture and steering | done |
