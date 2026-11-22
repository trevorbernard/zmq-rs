# zmq-rs

[![CI](https://github.com/trevorbernard/zmq-rs/actions/workflows/ci.yml/badge.svg?branch=main)](https://github.com/trevorbernard/zmq-rs/actions/workflows/ci.yml)
[![Fuzz](https://github.com/trevorbernard/zmq-rs/actions/workflows/fuzz.yml/badge.svg?branch=main)](https://github.com/trevorbernard/zmq-rs/actions/workflows/fuzz.yml)

An idiomatic, type-safe Rust port of [libzmq](https://github.com/zeromq/libzmq).

- Wire-compatible with libzmq: ZMTP 3.1/3.0 with downgrade to 2.0/1.0, NULL/PLAIN/CURVE.
- Own I/O threads (no async runtime required); blocking and executor-agnostic async APIs.
- Invalid states are compile errors: each socket type exposes only the operations
  its pattern allows.
- Every libzmq socket type but STREAM (not planned) and DGRAM (not yet), the draft ones behind the `draft` feature, over
  tcp, ipc and inproc, with libzmq's proxies, monitor events, `zmq_poll`
  and message properties (`zmq_msg_gets`).

See [docs/libzmq-compatibility.md](docs/libzmq-compatibility.md) for what libzmq has that zmq-rs lacks and where the two differ,
and [docs/rfc-compliance.md](docs/rfc-compliance.md) for how each ZeroMQ RFC is met.
[docs/architecture.md](docs/architecture.md) compares the runtime with libzmq's.
[docs/benchmarks.md](docs/benchmarks.md) compares throughput and latency with libzmq's.
Requires Rust 1.99.

Contributions follow C4, as ZeroMQ's do: see [CONTRIBUTING.md](CONTRIBUTING.md).
Licensed under the [MPL 2.0](LICENSE).

## Quick start

Every example below is compiled and run as a doctest of the `zmq` crate.

A socket's type is a type parameter, so a `Socket<Push>` has `send` but no
`recv`. Blocking calls such as `recv` have non-blocking `try_` and async
`_async` variants; the async ones work on any executor.

### Pipeline: PUSH and PULL

PUSH round-robins messages to its peers and never drops; PULL fair-queues
from its peers.

```rust
use zmq::{Context, Pull, Push};

fn main() -> Result<(), Box<dyn std::error::Error>> {
    let context = Context::new()?;
    let mut sink = context.socket::<Pull>();
    let endpoint = sink.bind("tcp://127.0.0.1:*")?;

    let mut source = context.socket::<Push>();
    source.connect(&endpoint)?;
    source.send("task 1")?;
    source.send(["header", "body"])?;

    assert_eq!(sink.recv()?, "task 1");
    let message = sink.recv()?;
    assert_eq!(message.len().get(), 2);
    assert_eq!(message.last(), "body");
    Ok(())
}
```

### Request-reply: REQ and REP

`request` sends and waits for the reply in one call, so REQ can't fall out of
lockstep. REP's `recv` lends out a `Request` that must be answered before the
next one can be received.

```rust
use std::thread;

use zmq::{Context, Rep, Req};

fn main() -> Result<(), Box<dyn std::error::Error>> {
    let context = Context::new()?;
    let mut rep = context.socket::<Rep>();
    let endpoint = rep.bind("tcp://127.0.0.1:*")?;
    let server = thread::spawn(move || {
        let request = rep.recv().unwrap();
        let name = String::from_utf8_lossy(request.body().last()).into_owned();
        request.reply(format!("hello, {name}")).unwrap();
    });

    let mut req = context.socket::<Req>();
    req.connect(&endpoint)?;
    assert_eq!(req.request("world")?, "hello, world");
    server.join().unwrap();
    Ok(())
}
```

### Publish-subscribe: PUB and SUB

`send` never blocks and returns how many subscribers the message was queued
for. Over TCP, subscriptions travel after the connection is up, so a
publisher drops what it sends before then; over `inproc` they arrive with the
connection.

```rust
use zmq::{Context, Pub, Sub};

fn main() -> Result<(), Box<dyn std::error::Error>> {
    let context = Context::new()?;
    let mut publisher = context.socket::<Pub>();
    publisher.bind("inproc://weather")?;

    let mut subscriber = context.socket::<Sub>();
    subscriber.subscribe("london");
    subscriber.connect("inproc://weather")?;

    assert_eq!(publisher.send("paris 18C")?, 0);
    assert_eq!(publisher.send("london 12C")?, 1);
    assert_eq!(subscriber.recv()?, "london 12C");
    Ok(())
}
```

### Routing: ROUTER and DEALER

ROUTER receives each message with its sender's routing id, and `send` names
the peer. Sending to an unknown routing id fails and hands the message back.

```rust
use zmq::{Context, Dealer, Identity, Router, RoutingId, SocketOptions};

fn main() -> Result<(), Box<dyn std::error::Error>> {
    let context = Context::new()?;
    let mut broker = context.socket::<Router>();
    let endpoint = broker.bind("tcp://127.0.0.1:*")?;

    let options = SocketOptions::new().identity(Identity::new("worker-1")?);
    let mut worker = context.socket_with::<Dealer>(options);
    worker.connect(&endpoint)?;
    worker.send("ready")?;

    let (from, message) = broker.recv()?;
    assert_eq!(from.as_bytes(), b"worker-1");
    assert_eq!(message, "ready");
    broker.send(&from, "job 42")?;
    assert_eq!(worker.recv()?, "job 42");

    let stranger = RoutingId::new("nobody")?;
    assert!(broker.send(&stranger, "lost").is_err());
    Ok(())
}
```

### Waiting on several sockets: `poll`

`zmq::poll` waits until any of its sockets is ready and says which, as
`zmq_poll`. Each socket is borrowed only for the call, and one reported
readable returns a message from its next `try_recv`. Only socket types that
receive have `readable`, and only those whose sends can wait have
`writable`.

```rust
use std::time::Duration;

use zmq::{Context, Pull, Push};

fn main() -> Result<(), Box<dyn std::error::Error>> {
    let context = Context::new()?;
    let mut jobs = context.socket::<Pull>();
    jobs.bind("inproc://jobs")?;
    let mut control = context.socket::<Pull>();
    control.bind("inproc://control")?;

    let mut boss = context.socket::<Push>();
    boss.connect("inproc://jobs")?;
    boss.send("job 1")?;

    let ready = zmq::poll_timeout([jobs.readable(), control.readable()], Duration::from_secs(1))?;
    let Some([job, stop]) = ready else {
        return Ok(()); // nothing within a second
    };
    assert!(job && !stop);
    assert_eq!(jobs.try_recv()?, Some("job 1".into()));
    Ok(())
}
```

### Security: CURVE with an authenticator

Clients must know the server's public key. The server asks an authenticator
(libzmq's ZAP handler) about each client; here, only one client key is let in.

```rust
use zmq::{
    Context, Keypair, Metadata, Pull, Push, Security, SocketOptions, ZapCredentials, ZapReply,
    ZapRequest,
};

fn main() -> Result<(), Box<dyn std::error::Error>> {
    let server_keys = Keypair::generate();
    let server_public = server_keys.public();
    let client_keys = Keypair::generate();
    let trusted = client_keys.public();

    let authenticator = move |request: &ZapRequest| match request.credentials {
        ZapCredentials::Curve(key) if key == trusted => ZapReply::Ok {
            user_id: "alice".into(),
            metadata: Metadata::default(),
        },
        _ => ZapReply::AuthenticationFailure,
    };

    let context = Context::new()?;
    let security = Security::curve_server_authenticated(server_keys, "global", authenticator);
    let mut server = context.socket_with::<Pull>(SocketOptions::new().security(security));
    let endpoint = server.bind("tcp://127.0.0.1:*")?;

    let security = Security::curve_client(client_keys, server_public);
    let mut client = context.socket_with::<Push>(SocketOptions::new().security(security));
    client.connect(&endpoint)?;
    client.send("encrypted")?;
    assert_eq!(server.recv()?, "encrypted");
    Ok(())
}
```

## Performance

On the two machines in [docs/benchmarks.md](docs/benchmarks.md), measured
as libzmq's own `perf/` tools measure, throughput is ahead of libzmq's or
level everywhere but ipc at 64 KiB on an M1 (0.80), and latency is lower
everywhere but tcp at 256 B on a Xeon (0.90). Small messages gain most:
[why](docs/benchmarks.md#why-we-are-ahead-at-8-b-over-tcp), profiled.

The library can't choose its own optimization settings: the application's
profile builds it. Fat LTO with one codegen unit is not a clear win; measure
your own workload before adopting it. In
[docs/benchmarks.md](docs/benchmarks.md)'s throughput cases it ran 256 B
messages 2–5% faster, but 8 B messages 15–22% slower, with 4 KiB level:

```toml
[profile.release]
lto = "fat"
codegen-units = 1
```

The allocator matters more. Every message larger than 23 octets is
allocated by the sender and freed on another thread, which glibc's malloc
handles poorly. In the benchmarks at 256 B over inproc, glibc's `malloc`
and `free` took about half the cycles, and under mimalloc or jemalloc
throughput ran up to three times as fast, for libzmq as well.

## Differences from libzmq

The ones an application meets first. The full list, with which are to
stay and which are to go, is in
[docs/libzmq-compatibility.md](docs/libzmq-compatibility.md).

- ROUTER `send` fails for an unknown routing id, as with libzmq's
  `ROUTER_MANDATORY`. Ignoring `try_send`'s error drops the message, which is
  libzmq's default.
- REQ's `request` fails with `RequestError::PeerGone` if the peer's
  connection closes before it replies; libzmq waits forever.
- `Context::shutdown` is `zmq_ctx_shutdown` and `zmq_ctx_term` in one call,
  with a deadline: sends and receives on the context's sockets fail with
  `Terminated` (libzmq's `ETERM`) at once, waking threads blocked in them,
  and it returns without waiting for the sockets to be dropped.
- Timeouts are per call: `recv_timeout` and `send_timeout` beside every
  blocking `recv` and `send`, rather than the `RCVTIMEO` and `SNDTIMEO`
  socket options. A send that times out hands the message back. REQ's
  `request_timeout` leaves the socket usable after a timeout, turning on
  request correlation to drop the late reply.
- XPUB receives typed `XPubEvent`s rather than `0x01`/`0x00`-prefixed messages.
- `ManualXPub` (`XPUB_MANUAL`) hands out a `SubscriptionRequest` per
  subscription, tied to the peer that sent it.
- PUB `send` returns how many subscribers the message was queued for.
- With `CONFLATE`, subscriptions are never conflated; libzmq can lose all but
  the last.

Not planned unless someone asks: STREAM sockets, WebSockets, TIPC, VMCI,
GSSAPI, the TCP and IPC accept filters, PGM and NORM, and Windows. Not
yet: UDP, a socket's file descriptor for another event loop, polling
plain file descriptors, and libzmq's ZAP handler socket, among others.
The full list is in
[docs/libzmq-compatibility.md](docs/libzmq-compatibility.md).

## Testing

- **Against libzmq 4.3.5:** every pattern over tcp, and the NULL, PLAIN
  and CURVE handshakes in both roles; over ipc, PUSH and PULL
  (`tests/interop`). What is not yet tested against it is listed under
  [Evidence](docs/libzmq-compatibility.md#evidence).
- **Against libzmq 2.2 and 3.2,** which speak ZMTP 1.0 and 2.0, as
  separate processes the Nix dev shell builds (`just test-legacy`).
- **libzmq's own regression tests,** 80 of its 139 ported, the rest listed
  with why (`crates/zmq/tests/libzmq`).
- **Property tests against models:** subscription matching, XPUB events,
  ROUTER routing, pipes, the codec and handshakes.
- **Fuzzing** of every handshake and the framing, and of the sockets
  through a real connection, seeded with libzmq's corpora and its security
  advisories' reproducers, in CI and weekly ([fuzz/README.md](fuzz/README.md)).
- **Concurrency checks:** `zmq-pipe`'s queue under loom and Miri, and its
  tests and the shutdown stress tests under ThreadSanitizer, in CI.
- **Mutation testing** with cargo-mutants, per crate (`just mutants zmtp`).
- **The ZeroMQ Guide's examples,** 76 of them, in [examples](examples).

## Development

```sh
nix develop        # or `direnv allow`
just               # lists the workflows
just ci            # rustfmt, clippy, tests, loom, as CI runs them
just test-legacy   # against libzmq 2.2 and 3.2
just loom          # zmq-pipe's queue, every interleaving; also miri, tsan
just mutants zmtp  # changes to a crate's code that no test notices
just perf          # throughput and latency next to libzmq
just fuzz-all      # every fuzz target, a minute each
nix flake check    # tests, clippy, rustfmt, as Nix builds them
```

## Layout

| Crate | Role |
|---|---|
| `zmtp` | Wire protocol as a sans-IO state machine |
| `zmq-pipe` | Lock-free SPSC pipes with high-water marks |
| `zmq-core` | I/O threads, transports, sessions, socket patterns |
| `zmq` | Public, type-safe API |
| `tests/interop` | Interoperability tests and benchmarks against the reference libzmq |
| `examples` | The ZeroMQ Guide's examples (`cargo run -p zguide --bin hwserver`) |
| `fuzz` | cargo-fuzz targets for `zmtp`, outside the workspace (nightly) |
