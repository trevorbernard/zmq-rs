# Fuzzing

libFuzzer targets for the `zmtp` crate, run with
[cargo-fuzz](https://github.com/rust-fuzz/cargo-fuzz) on nightly:

```sh
nix develop .#fuzz
cd fuzz
./seed.sh --fetch          # or ./seed.sh to skip the downloads
cargo fuzz run null_handshake -- -dict=zmtp.dict
```

| Target | Input |
|---|---|
| `null_handshake` | Raw traffic from a peer to a NULL PUB server, as libzmq's `test_bind_null_fuzzer` |
| `plain_server` | Raw traffic from a client to a PLAIN ROUTER server whose ZAP accepts everyone |
| `frame_decoder` | First octet picks ZMTP 1.0/2.0/3.0/3.1 framing, the rest is traffic |
| `command_parse` | A command body; checks that parse, encode, parse is stable |
| `z85_decode` | Z85 text; checks that accepted text re-encodes to itself |

Wire targets take the same input shape as libzmq's harnesses, so libzmq's
seed corpora and `zmtp.dict` apply as they are. `seed.sh` writes
hand-built traffic and the reproducers from libzmq's OSS-Fuzz security
advisories to `corpus/`. `--fetch` adds libzmq's seed corpora and the
public ClusterFuzz corpus for `z85_decode`. ClusterFuzz's ZMTP corpora
are not public.
