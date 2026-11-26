# libzmq's regression tests

libzmq v4.3.5's `tests/test_*.cpp`, ported to zmq-rs's API, one module
each. They test zmq-rs's behavior against libzmq's own expectations; how
it interoperates with libzmq is tested in `tests/interop`.

```sh
cargo nextest run -p zmq --all-features --test libzmq
cargo nextest run -p zmq --all-features --test libzmq spec_req   # one module
```

Where a test asserts something other than libzmq's expectation, its
module's header says so, and the difference is a Divergence or an
Exception in
[`docs/libzmq-compatibility.md`](../../../../docs/libzmq-compatibility.md),
which also says which of the features below are to be added.
A test marked `#[ignore = "zmq-rs bug: ..."]` found a bug not yet fixed.

## Not ported

Of 139 tests, 80 are ported and these 59 are left out. Some of the
ported ones lack cases, for the same reasons: `poller`, `monitor`,
`security_zap` and `radio_dish`.

**Gaps**, features zmq-rs does not have yet:

| Feature | Tests |
|---|---|
| UDP | `dgram` |
| Polling file descriptors | `zmq_poll_fd`, `zmq_ppoll_fd`, `zmq_ppoll_signals` |
| `USE_FD`, `SRCFD` | `use_fd`, `srcfd` |
| A proxy with one socket as both ends, or a REP end | `proxy_single_socket` |
| A limit on a context's sockets | `many_sockets`, `system` |
| Context options: `BLOCKY`, `IPV6`, thread options | `ctx_options` |
| `SNDBUF`, `RCVBUF`, `PRIORITY` | `setsockopt` |
| Draft sockets used from several threads | `thread_safe` |
| `zmq_has`, `zmq_version` | `capabilities`, `ancillaries` |

**Portable, and not ported yet:** `issue_566` and `sodium`. `fork` and
`ctx_destroy` are covered by tests of our own, `crates/zmq/tests/fork.rs`
and `terminated.rs`, not by ports.

**Exclusions**, not planned unless someone asks for them:

| Feature | Tests |
|---|---|
| STREAM sockets | `stream`, `stream_disconnect`, `stream_empty`, `stream_exceeds_buffer`, `stream_timeout`, `bind_stream_fuzzer`, `connect_stream_fuzzer` |
| WebSockets | `ws_transport`, `wss_transport`, `bind_ws_fuzzer`, `connect_ws_fuzzer` |
| TIPC | `address_tipc`, `connect_delay_tipc`, `pair_tipc`, `reqrep_tipc`, `reqrep_device_tipc`, `router_mandatory_tipc`, `shutdown_stress_tipc`, `sub_forward_tipc`, `term_endpoint_tipc` |
| VMCI | `pair_vmci`, `reqrep_vmci` |
| GSSAPI | `security_gssapi` |
| TCP and IPC accept filters, which libzmq deprecated for ZAP | `tcp_accept_filter`, `filter_ipc` |
| Timers | `timers` |

**libzmq's C API rather than behavior:** `msg_ffn`, `msg_flags`,
`msg_init`, `getsockopt_memset`, `socket_null`, `iov`, `atomics`.

**Fuzzers,** covered by `fuzz/`: `bind_fuzzer`, `bind_null_fuzzer`,
`bind_curve_fuzzer`, `connect_fuzzer`, `connect_null_fuzzer` and
`connect_curve_fuzzer` by `engine` and the `zmtp` wire targets, and
`z85_decode_fuzzer` by `z85_decode`. `socket_options_fuzzer` feeds bytes
to `zmq_setsockopt`, which typed `SocketOptions` have no counterpart to.
