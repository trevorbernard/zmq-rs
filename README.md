# zmq-rs

An idiomatic, type-safe Rust port of [libzmq](https://github.com/zeromq/libzmq).

- Wire-compatible with libzmq: ZMTP 3.1/3.0 with downgrade to 2.0/1.0, NULL/PLAIN/CURVE.
- Own I/O threads (no async runtime required); blocking and executor-agnostic async APIs.
- Invalid states are compile errors: each socket type exposes only the operations
  its pattern allows.

See [docs/rfc-compliance.md](docs/rfc-compliance.md) for how each ZeroMQ RFC is met.

## Development

```sh
nix develop        # or `direnv allow`
cargo test --workspace
nix flake check    # tests, clippy, rustfmt
cargo bench -p zmq-pipe   # pipe throughput, one process per measurement
```

## Layout

| Crate | Role |
|---|---|
| `zmtp` | Wire protocol as a sans-IO state machine |
| `zmq-pipe` | Lock-free SPSC pipes with high-water marks |
| `zmq-core` | I/O threads, transports, sessions, socket patterns |
| `zmq` | Public, type-safe API |
| `tests/interop` | Interoperability tests against the reference libzmq |
