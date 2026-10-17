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

## Status

| Slice | Contents | State |
|---|---|---|
| 1 | Context, I/O threads, TCP connect, NULL handshake, sessions, engines | done |
| 2 | Bind/listen, accepting peers | done |
| 3 | Reconnection with backoff (sessions outlive connections), linger, shutdown | done |
| 4 | Load balancing and fair queuing; PUSH/PULL and DEALER/ROUTER; typed sockets in `zmq` | next |
| 5 | PLAIN with an `Authenticator` trait, `inproc`, heartbeats | |
