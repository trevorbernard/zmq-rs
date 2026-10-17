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
| `IoCommand` | The thread's command type (`Connect`, `Stop`, ...) | `command_t` |
| `Session` | One per peer: owns both pipe ends; will own reconnection | `session_base_t` |
| `Engine` | One TCP connection: greeting, handshake, then codecs, with partial-write buffering | `zmtp_engine_t` |
| `SessionWaker` | A pipe waker that queues its session id once and wakes the event loop | `activate_read`, `activate_write` |

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
- When a connection ends (peer closed, error, incompatible peer), the session
  drops its pipe ends: the socket sees the receive pipe close and sends fail.
- Dropping a socket's pipe ends closes the TCP stream.
- Dropping the context stops every I/O thread, closing all connections.

## Status

| Slice | Contents | State |
|---|---|---|
| 1 | Context, I/O threads, TCP connect, NULL handshake, sessions, engines | done |
| 2 | Bind/listen, accepting peers | next |
| 3 | Reconnection with backoff (sessions outlive connections), linger, shutdown | |
| 4 | Load balancing and fair queuing; PUSH/PULL and DEALER/ROUTER; typed sockets in `zmq` | |
| 5 | PLAIN with an `Authenticator` trait, `inproc`, heartbeats | |
