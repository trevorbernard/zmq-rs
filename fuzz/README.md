# Fuzzing

libFuzzer targets for the `zmtp` crate and, through `engine`, the
sockets above it, run with
[cargo-fuzz](https://github.com/rust-fuzz/cargo-fuzz) on nightly:

```sh
just fuzz-seed --fetch     # or without --fetch to skip the downloads
just fuzz null_handshake   # one target, until stopped
just fuzz-all 300          # every target at once, five minutes each
just fuzz-coverage         # line coverage over the corpora
```

| Target | Input |
|---|---|
| `null_handshake` | Raw traffic from a peer to a NULL PUB server, as libzmq's `test_bind_null_fuzzer` |
| `plain_server` | Raw traffic from a client to a PLAIN ROUTER server whose ZAP accepts everyone |
| `plain_client` | Raw traffic from a server to a PLAIN DEALER client |
| `curve_server` | Raw traffic from a client to a CURVE ROUTER server, as libzmq's `test_bind_curve_fuzzer` |
| `curve_client` | Raw traffic from a server to a CURVE DEALER client, as libzmq's `test_connect_curve_fuzzer` |
| `curve_session` | A real CURVE handshake, then messages, commands, tampering and replays; checks that the handshake succeeds exactly when it should, and that what arrives is what was sent |
| `frame_decoder` | First octet picks ZMTP 1.0/2.0/3.0/3.1 framing, the rest is traffic |
| `command_parse` | A command body; checks that parse, encode, parse is stable |
| `roundtrip` | Messages, commands, subscriptions and memberships; checks they decode as encoded |
| `z85_decode` | Z85 text; checks that accepted text re-encodes to itself |
| `engine` | First octet picks a socket type, binding or connecting, a mechanism and heartbeats; the rest is raw traffic from a peer to a real socket over ipc, through the engine, I/O thread and routing. Fails on a panic on any thread, or if the engine never notices the peer close |

Wire targets take the same input shape as libzmq's harnesses, and the
CURVE ones its fixed keys, so libzmq's seed corpora and `zmtp.dict` apply
as they are; `engine` takes them behind its first octet, which `seed.sh
--fetch` adds. Raw traffic cannot get past CURVE's authenticators without
the client's transient secret, which is what `curve_session` is for.

`seed.sh` writes hand-built traffic and the reproducers from libzmq's
OSS-Fuzz security advisories to `corpus/`. `--fetch` adds libzmq's seed
corpora and the public ClusterFuzz corpus for `z85_decode`. ClusterFuzz's
ZMTP corpora are not public.

CI runs every target for 30 seconds from the hand-built seeds on each
push, and for an hour each weekly (`.github/workflows/fuzz.yml`), from a
corpus cached between weekly runs. Run that one on demand from the
Actions tab, with a duration. A failed run uploads its crashes.
