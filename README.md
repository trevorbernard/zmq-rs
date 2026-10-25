# zmq-rs

An idiomatic, type-safe Rust port of [libzmq](https://github.com/zeromq/libzmq).

- Wire-compatible with libzmq: ZMTP 3.1/3.0 with downgrade to 2.0/1.0, NULL/PLAIN/CURVE.
- Own I/O threads (no async runtime required); blocking and executor-agnostic async APIs.
- Invalid states are compile errors: each socket type exposes only the operations
  its pattern allows.

See [docs/rfc-compliance.md](docs/rfc-compliance.md) for how each ZeroMQ RFC is met.
[docs/benchmarks.md](docs/benchmarks.md) compares throughput and latency with libzmq's.

## Quick start

Every example below is compiled and run as a doctest of the `zmq` crate.

A socket's type is a type parameter, so a `Socket<Push>` has `send` but no
`recv`. Blocking calls such as `recv` have non-blocking `try_` and async
`_async` variants; the async ones work on any executor.

### Pipeline: PUSH and PULL

PUSH round-robins messages to its peers and never drops; PULL fair-queues
from its peers.

```rust
use zmq::{Context, Message, Pull, Push};

fn main() -> Result<(), Box<dyn std::error::Error>> {
    let context = Context::new()?;
    let mut sink = context.socket::<Pull>();
    let endpoint = sink.bind("tcp://127.0.0.1:*")?.to_string();

    let mut source = context.socket::<Push>();
    source.connect(&endpoint)?;
    source.send("task 1");
    let mut multipart = Message::new("header");
    multipart.push("body");
    source.send(multipart);

    assert_eq!(sink.recv(), "task 1".into());
    let message = sink.recv();
    assert_eq!(message.len().get(), 2);
    assert_eq!(&**message.last(), b"body");
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
    let endpoint = rep.bind("tcp://127.0.0.1:*")?.to_string();
    let server = thread::spawn(move || {
        let request = rep.recv();
        let name = String::from_utf8_lossy(request.body().last()).into_owned();
        request.reply(format!("hello, {name}"));
    });

    let mut req = context.socket::<Req>();
    req.connect(&endpoint)?;
    assert_eq!(req.request("world")?, "hello, world".into());
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

    assert_eq!(publisher.send("paris 18C"), 0);
    assert_eq!(publisher.send("london 12C"), 1);
    assert_eq!(subscriber.recv(), "london 12C".into());
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
    let endpoint = broker.bind("tcp://127.0.0.1:*")?.to_string();

    let mut worker = context.socket_with::<Dealer>(SocketOptions {
        identity: Identity::new("worker-1")?,
        ..SocketOptions::default()
    });
    worker.connect(&endpoint)?;
    worker.send("ready");

    let (from, message) = broker.recv();
    assert_eq!(from.as_bytes(), b"worker-1");
    assert_eq!(message, "ready".into());
    broker.send(&from, "job 42")?;
    assert_eq!(worker.recv(), "job 42".into());

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
    boss.send("job 1");

    let ready = zmq::poll_timeout([jobs.readable(), control.readable()], Duration::from_secs(1));
    let Some([job, stop]) = ready else {
        return Ok(()); // nothing within a second
    };
    assert!(job && !stop);
    assert_eq!(jobs.try_recv(), Some("job 1".into()));
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
    let mut server = context.socket_with::<Pull>(SocketOptions {
        security: Security::curve_server_authenticated(server_keys, "global", authenticator),
        ..SocketOptions::default()
    });
    let endpoint = server.bind("tcp://127.0.0.1:*")?.to_string();

    let mut client = context.socket_with::<Push>(SocketOptions {
        security: Security::curve_client(client_keys, server_public),
        ..SocketOptions::default()
    });
    client.connect(&endpoint)?;
    client.send("encrypted");
    assert_eq!(server.recv(), "encrypted".into());
    Ok(())
}
```

## Performance

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

Deliberate, beyond the API being typed. The full list, with reasons, is in
[docs/rfc-compliance.md](docs/rfc-compliance.md).

- ROUTER `send` fails for an unknown routing id, as with libzmq's
  `ROUTER_MANDATORY`. Ignoring `try_send`'s error drops the message, which is
  libzmq's default.
- REQ's `request` returns `PeerGone` if the peer's connection closes before
  it replies; libzmq waits forever.
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
- The PEER socket ignores `IMMEDIATE`; libzmq refuses `connect_peer` with it.

Not yet: STREAM sockets, and polling plain file descriptors beside sockets,
which `zmq_poll` also does.

## Development

```sh
nix develop        # or `direnv allow`
cargo test --workspace
nix flake check    # tests, clippy, rustfmt
cargo bench -p zmq-pipe   # pipe throughput, one process per measurement
cargo bench -p zmq-interop --bench perf   # throughput and latency next to libzmq
```

## Layout

| Crate | Role |
|---|---|
| `zmtp` | Wire protocol as a sans-IO state machine |
| `zmq-pipe` | Lock-free SPSC pipes with high-water marks |
| `zmq-core` | I/O threads, transports, sessions, socket patterns |
| `zmq` | Public, type-safe API |
| `tests/interop` | Interoperability tests and benchmarks against the reference libzmq |
