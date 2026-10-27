# libzmq's regression tests

libzmq v4.3.5's `tests/test_*.cpp`, ported to zmq-rs's API, one module
each. They test zmq-rs's behavior against libzmq's own expectations; how
it interoperates with libzmq is tested in `tests/interop`.

```sh
cargo nextest run -p zmq --all-features --test libzmq
cargo nextest run -p zmq --all-features --test libzmq spec_req   # one module
```

Where zmq-rs differs from libzmq on purpose, a test asserts zmq-rs's
behavior and cites [`docs/rfc-compliance.md`](../../../../docs/rfc-compliance.md).
A test marked `#[ignore = "zmq-rs bug: ..."]` found a bug not yet fixed.

## Not ported

Of 139 tests, these are left out.

**Features zmq-rs does not have** (a to-do list as much as an exclusion):

| Feature | Tests |
|---|---|
| STREAM sockets | `stream`, `stream_disconnect`, `stream_empty`, `stream_exceeds_buffer`, `stream_timeout`, `bind_stream_fuzzer`, `connect_stream_fuzzer` |
| WebSockets | `ws_transport`, `wss_transport`, `bind_ws_fuzzer`, `connect_ws_fuzzer` |
| TIPC, VMCI, UDP | `address_tipc`, `connect_delay_tipc`, `pair_tipc`, `reqrep_tipc`, `reqrep_device_tipc`, `router_mandatory_tipc`, `shutdown_stress_tipc`, `sub_forward_tipc`, `term_endpoint_tipc`, `pair_vmci`, `reqrep_vmci`, `dgram` |
| GSSAPI, SOCKS | `security_gssapi`, `socks` |
| Polling file descriptors | `zmq_poll_fd`, `zmq_ppoll_fd`, `zmq_ppoll_signals` |
| Timers | `timers` |
| `ROUTER_NOTIFY` | `router_notify` |
| `CONNECT_ROUTING_ID` | `connect_rid` |
| PEER's hello, disconnect and hiccup messages | `hello_msg`, `disconnect_msg`, `hiccup_msg` |
| `TOPICS_COUNT` | `pubsub_topics_count` |
| `XSUB_VERBOSE_UNSUBSCRIBE` | `xsub_verbose` |
| `RECONNECT_STOP` | `reconnect_options` |
| TCP and IPC accept filters | `tcp_accept_filter`, `filter_ipc` |
| `USE_FD`, `SRCFD` | `use_fd`, `srcfd` |
| `BUSY_POLL`, `TOS`, `BINDTODEVICE` | `busy_poll`, `diffserv`, `pair_tcp_cap_net_admin` |
| Binding a source address (`tcp://src;dst`) | `bind_src_address` |
| Application metadata (`ZMQ_METADATA`) | `app_meta` |

**libzmq's C API rather than behavior:** `msg_ffn`, `msg_flags`,
`msg_init`, `ctx_options`, `ctx_destroy`, `setsockopt`,
`getsockopt_memset`, `sockopt_hwm`, `socket_null`, `iov`, `atomics`,
`capabilities`, `ancillaries`, `system`, `sodium`, `fork`,
`thread_safe`, `many_sockets`, `issue_566`.

**Fuzzers,** covered by `fuzz/`: `bind_fuzzer`, `bind_null_fuzzer`,
`bind_curve_fuzzer`, `connect_fuzzer`, `connect_null_fuzzer` and
`connect_curve_fuzzer` by `engine` and the `zmtp` wire targets, and
`z85_decode_fuzzer` by `z85_decode`. `socket_options_fuzzer` feeds bytes
to `zmq_setsockopt`, which typed `SocketOptions` have no counterpart to.

**Stress tests,** not yet covered: `shutdown_stress`.
