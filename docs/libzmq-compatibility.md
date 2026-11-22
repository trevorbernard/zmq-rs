# libzmq compatibility

What libzmq v4.3.5 (the Reference) offers, and what zmq-rs does about each
piece. This is the only list of Gaps, Divergences, Exceptions and
Exclusions: other documents explain a behaviour and link here for its
status. The terms are defined in [`GLOSSARY.md`](../GLOSSARY.md), and the
rule that decides between a Divergence and an Exception in
[ADR 0001](adr/0001-libzmq-behaviour-wins.md).

| Status | Meaning |
|---|---|
| Same | zmq-rs has it and no difference is known |
| Gap | libzmq has it, zmq-rs does not, and it is to be added |
| Divergence | both have it and it behaves differently; to be closed |
| Exception | a Divergence kept on purpose |
| Exclusion | a Gap not planned unless someone asks |
| N/A | meaningless in Rust, or not observable |

**How far to trust this.** It was compiled on 2026-11-22 by reading both
sources, zmq-rs at `8736dba` and libzmq at `v4.3.5`. Nothing in it was
established by running a test. A row marked Same is therefore Unverified
unless [Evidence](#evidence) says otherwise, and each Divergence is to be
confirmed by a failing test before it is closed.

## Exclusions

Not planned unless someone asks for them.

| Exclusion | Covers |
|---|---|
| STREAM sockets | `ZMQ_STREAM`, `ZMQ_STREAM_NOTIFY`, and `ZMQ_ROUTER_RAW`, the same raw TCP mode on a ROUTER |
| WebSockets | `ws://`, `wss://`, the `ZMQ_WSS_*` options |
| TIPC | `tipc://` |
| VMCI | `vmci://`, the `ZMQ_VMCI_*` options |
| GSSAPI | the mechanism and the `ZMQ_GSSAPI_*` options |
| Accept filters | `ZMQ_TCP_ACCEPT_FILTER`, `ZMQ_IPC_FILTER_PID`, `_UID`, `_GID`, which libzmq deprecated for ZAP |
| PGM and NORM | `pgm://`, `epgm://`, `norm://`, `ZMQ_RATE`, `ZMQ_RECOVERY_IVL`, `ZMQ_MULTICAST_MAXTPDU`, the `ZMQ_NORM_*` options; optional in libzmq's own build |
| Windows | the platform, with `ipc` on it and `ZMQ_LOOPBACK_FASTPATH` |
| VSOCK | `vsock://`, added to libzmq after the Reference |

## Divergences to close

Each breaks an application that works on libzmq, or changes what a peer
sees, with no safety gain.

| # | libzmq | zmq-rs today |
|---|---|---|
| D12 | `ZMQ_HEARTBEAT_TTL` is truncated to deciseconds, and `EINVAL` above the maximum | Rounded up, and saturating |
| D13 | `zmq_connect_peer` is refused under `ZMQ_IMMEDIATE` | `IMMEDIATE` is ignored |
| D14 | `zmq_unbind` of an `inproc` name frees the name only | Also closes the connections accepted on it |
| D15 | A ZMTP 2.0 peer's socket-type octet is not checked | Checked, and a mismatch refused |
| D16 | An XSUB's send and an XPUB's receive read a subscription from any frame of a message, unless `ZMQ_ONLY_FIRST_SUBSCRIBE` | Read one only from a single-frame message; no option |
| D17 | A disconnect stops reconnection only with `ZMQ_RECONNECT_STOP_AFTER_DISCONNECT` | Always |
| D18 | `ENODEV`, `ENAMETOOLONG`, `ENOCOMPATPROTO` and `EPROTONOSUPPORT` are distinct | `ENODEV` is `InvalidEndpoint`, `ENAMETOOLONG` an `Io` error, and the last two are both `UnsupportedTransport` |
| D19 | `zmq_socket` fails with `ETERM` once the context is shut down | Succeeds; the socket's operations then fail |
| D20 | A REQ drops the connection on a reply with no delimiter | Drops the reply and keeps waiting |
| D21 | A PAIR drops what a departed peer had delivered, with its pipe | Still delivers it, ahead of the next peer's |

## Exceptions

Divergences that stand.

**Safety: libzmq's behaviour is a crash, a memory bug or a loss of data.**

| libzmq | zmq-rs |
|---|---|
| An `ipc` bind unlinks whatever is at the path, a live socket or a regular file included | Removes only a stale socket file; anything else is `AddrInUse` |
| Subscriptions and joins are dropped at the HWM, and conflated under `ZMQ_CONFLATE` | Never dropped, never conflated |
| Aborts on a `ZMQ_CONNECT_ROUTING_ID` already in use | `Error::RoutingIdInUse` |
| Aborts on a peer's group name over 255 octets | Disconnects the peer |
| Aborts if `ZMQ_TOS` or `ZMQ_BUSY_POLL` cannot be set | Ignores the failure |
| A PING shorter than its TTL is read out of bounds | Refused |
| A heartbeat TTL of 65.6 s or more overflows | Held without overflow |
| CURVE's vouch is checked against the client's short-term key only | Also against the server's long-term key |
| PLAIN's ERROR is sent as `^RROR` (libzmq#4943) | Sent as the RFC has it; both forms accepted. The one Exception that is neither a hazard nor an API shape: copying the bug would fail every other implementation |

**API shape: the same Capability, typed.**

| libzmq | zmq-rs |
|---|---|
| `ZMQ_RCVTIMEO`, `ZMQ_SNDTIMEO` | `recv_timeout`, `send_timeout` per call |
| `ZMQ_SNDMORE`, `ZMQ_RCVMORE`, `zmq_msg_more` | A `Message` is the whole multipart message |
| `ZMQ_ROUTER_MANDATORY` off drops silently | A send to an unknown routing id always reports it; ignoring the error is libzmq's default |
| `ZMQ_XPUB_NODROP` | `send_all` and its variants, per call |
| XPUB receives `01`/`00`-prefixed messages | `XPubEvent` |
| `ZMQ_XPUB_MANUAL`, `ZMQ_XPUB_MANUAL_LAST_VALUE` | `ManualXPub`, `SubscriptionRequest` |
| `zmq_ctx_term` blocks until every socket is closed; `zmq_ctx_shutdown` only unblocks | The context ends when its last handle and socket are dropped; `Context::shutdown(timeout)` does both with a deadline |
| `zmq_proxy_steerable` takes a control socket | `ProxyControl` handle, with the same four commands |
| `zmq_socket_monitor` gives an `inproc` endpoint | `Monitor` handle |
| An option is accepted and ignored on a socket type that has no use for it | No such setter |
| Draft sockets refuse `ZMQ_SNDMORE` at run time | They send and receive a `Frame`, so multipart does not compile |
| `EFSM`, `ENOTSUP`, `ENOTSOCK` | Do not compile |
| `EINTR` from a blocking call | A signal does not interrupt one; `recv_timeout`, or `Context::shutdown` from another thread |

**Behaviour.**

| libzmq | zmq-rs | Why it stands |
|---|---|---|
| A REQ waits forever if its peer closes before replying | `RequestError::PeerGone` | The typed `request` has no state to be stuck in |
| A REQ is stuck (`EFSM`) after a receive times out, unless `ZMQ_REQ_RELAXED` | Takes a new request; `request_timeout` turns on correlation for good | As above; a ROUTER serving it then sees the request id frame |
| `zmq_unbind` closes the listener in the background | Returns once it is closed | A stronger guarantee |
| A source address or SOCKS proxy that cannot be resolved is accepted and never connects | `InvalidEndpoint` at once | Never worked in libzmq |
| Closes on a socket-type mismatch | Sends ERROR, then closes | RFC 37; Unverified that libzmq treats the two alike |
| Application metadata names are any octets | RFC 37's characters, compared without case | Never valid on the wire |
| A ZMTP 1.0 subscriber is reported only when it leaves, as a cancel | Reported as `Subscribe("")` on arrival too | libzmq's cancel has no subscription to match |
| `ZMQ_TOPICS_COUNT` on a publisher is stale until the socket is next used | Current | Not observable as a difference |
| The least loaded I/O thread takes a new connection | The next in turn | Not observable |
| Greeting revision 2 is read as ZMTP 3.1 | As ZMTP 2.0 | No peer sends it |
| `as-server` is always sent as 0 | The RFC's value | libzmq never reads it |
| Message metadata is always attached | `MessageProperties::Off` omits it | Off by default |

## Socket types

| libzmq | zmq-rs | Status |
|---|---|---|
| PAIR, PUB, SUB, REQ, REP, DEALER, ROUTER, PULL, PUSH, XPUB, XSUB | `Socket<T>` for each | Same |
| STREAM | | Exclusion |
| SERVER, CLIENT, RADIO, DISH, GATHER, SCATTER, PEER, CHANNEL (draft) | `Socket<T>`, with the `draft` feature | Same |
| DGRAM (draft) | | Gap |
| Draft sockets are used from several threads at once | Each needs `&mut`, so a `Mutex` | Gap |

## Transports and endpoints

| libzmq | zmq-rs | Status |
|---|---|---|
| `tcp://`: address, `*`, interface name, host name, wildcard port, `source;host:port` | The same | Same |
| `tcp://` with `ZMQ_IPV6`: `*` binds `::` for both families, names resolve to IPv6 first | No option: `*` is `0.0.0.0`, names prefer IPv4 | Gap |
| An IPv6 literal is refused without `ZMQ_IPV6` | Accepted | Same (accepts more) |
| `ipc://`: path, `ipc://*`, abstract `@name` on Linux | The same | Same |
| `inproc://`, connect before bind, between any two socket types | The same | Same; Verified for mismatched types (`inproc_connect::multiple_threads`, ported) |
| `udp://` for RADIO, DISH and DGRAM: unicast, multicast, broadcast | | Gap |
| `pgm://`, `epgm://`, `norm://`, `ws://`, `wss://`, `tipc://`, `vmci://` | | Exclusion |
| `ZMQ_USE_FD`: a listener or connection from a file descriptor the application supplies | | Gap |

Platforms: Linux is what CI runs. macOS is built on and benchmarked but
has no CI: a Gap. Windows is an Exclusion. The others libzmq supports
have not been judged.

## Socket options

Draft options are marked (d). Options that only an Exclusion uses are in
[Exclusions](#exclusions).

| libzmq | zmq-rs | Status |
|---|---|---|
| `ZMQ_AFFINITY` | | Gap |
| `ZMQ_ROUTING_ID` (`ZMQ_IDENTITY`) | `SocketOptions::identity`, `set_identity` | Same; Verified for a leading zero octet (`tests/interop/tests/router.rs`). An empty one, libzmq's `EINVAL`, is accepted as none |
| `ZMQ_SUBSCRIBE`, `ZMQ_UNSUBSCRIBE` | `subscribe`, `unsubscribe` | Same |
| `ZMQ_SNDBUF`, `ZMQ_RCVBUF` | | Gap |
| `ZMQ_RCVMORE` | | Exception (API shape) |
| `ZMQ_FD` | | Gap |
| `ZMQ_EVENTS` | `readable`, `writable` in `zmq::poll`; no way to read the flags | Gap |
| `ZMQ_TYPE` | The type parameter | N/A |
| `ZMQ_LINGER` | `SocketOptions::linger`, `set_linger` | Same |
| `ZMQ_RECONNECT_IVL` | `Reconnect::Fixed`, `Reconnect::Never` | Same; Verified for 0 (`tests/interop/tests/reconnect.rs`) |
| `ZMQ_RECONNECT_IVL_MAX` | `Reconnect::Backoff` | Same; Verified (`tests/interop/tests/reconnect.rs`) |
| `ZMQ_BACKLOG` | `SocketOptions::backlog`, `set_backlog` | Same; Verified (`tests/interop/tests/backlog.rs`) |
| `ZMQ_MAXMSGSIZE` | `max_message_size` | Same; Verified (`tests/interop/tests/max_message_size.rs`) |
| No limit on a message's frames together, so one of endless `MORE` frames is held without bound (GHSA-h9gj-c9m7-qxqm) | `max_multipart_size`, off by default | zmq-rs only |
| `ZMQ_SNDHWM`, `ZMQ_RCVHWM` | `send_hwm`, `recv_hwm` and their setters | Same |
| `ZMQ_MULTICAST_HOPS`, `ZMQ_MULTICAST_LOOP` (d) | | Gap, with UDP |
| `ZMQ_RCVTIMEO`, `ZMQ_SNDTIMEO` | Per call | Exception (API shape) |
| `ZMQ_IPV6`, `ZMQ_IPV4ONLY` | | Gap |
| `ZMQ_LAST_ENDPOINT` | The `Endpoint` that `bind` returns; nothing after a connect | Same; Gap after a connect |
| `ZMQ_ROUTER_MANDATORY` | Always on | Exception (API shape) |
| `ZMQ_TCP_KEEPALIVE`, `_CNT`, `_IDLE`, `_INTVL` | `TcpKeepalive` | Same; Gap: forcing probes off |
| `ZMQ_IMMEDIATE` | `SocketOptions::immediate`, `set_immediate` | Same; Divergence D13 |
| `ZMQ_XPUB_VERBOSE`, `ZMQ_XPUB_VERBOSER` | `Verbosity` | Same |
| `ZMQ_MECHANISM` | | Gap |
| `ZMQ_PLAIN_SERVER`, `_USERNAME`, `_PASSWORD` | `Security::plain_server`, `Security::PlainClient` | Same |
| `ZMQ_CURVE_SERVER`, `_PUBLICKEY`, `_SECRETKEY`, `_SERVERKEY` | `Security::curve_server`, `curve_client`, `Keypair` | Same |
| `ZMQ_ZAP_DOMAIN`, `ZMQ_ZAP_ENFORCE_DOMAIN` (d) | A domain comes only with an `Authenticator` | Gap, with the ZAP handler socket |
| `ZMQ_PROBE_ROUTER` | `set_probe` on DEALER and ROUTER | Same; Gap on REQ |
| `ZMQ_REQ_CORRELATE` | `set_correlation` | Same |
| `ZMQ_REQ_RELAXED` | Always | Exception |
| `ZMQ_CONFLATE` | `set_conflate` | Same; Exception: subscriptions |
| `ZMQ_ROUTER_HANDOVER` | `set_handover` | Same |
| `ZMQ_TOS` | `type_of_service` | Same |
| `ZMQ_CONNECT_ROUTING_ID` (`ZMQ_CONNECT_RID`) | `connect_with_routing_id` | Same |
| `ZMQ_HANDSHAKE_IVL` | `handshake_timeout` | Same |
| `ZMQ_SOCKS_PROXY`, `ZMQ_SOCKS_USERNAME` (d), `ZMQ_SOCKS_PASSWORD` (d) | `SocksProxy` | Same |
| `ZMQ_XPUB_NODROP` | `send_all` | Exception (API shape); Gap on RADIO |
| `ZMQ_XPUB_MANUAL`, `ZMQ_XPUB_MANUAL_LAST_VALUE` (d) | `ManualXPub` | Exception (API shape) |
| `ZMQ_XPUB_WELCOME_MSG` | `set_welcome` on XPUB | Same; Gap on PUB |
| `ZMQ_INVERT_MATCHING` | `set_invert_matching` | Same |
| `ZMQ_HEARTBEAT_IVL`, `ZMQ_HEARTBEAT_TIMEOUT` | `Heartbeat` | Same; Verified for 0 (`tests/interop/tests/heartbeat.rs`) |
| `ZMQ_HEARTBEAT_TTL` | `Heartbeat::ttl` | Divergence D12 |
| `ZMQ_CONNECT_TIMEOUT` | | Gap |
| `ZMQ_TCP_MAXRT` | | Gap |
| `ZMQ_THREAD_SAFE` | | N/A |
| `ZMQ_USE_FD` | | Gap |
| `ZMQ_BINDTODEVICE` | `bind_to_device` | Same |
| `ZMQ_METADATA` (d) | `add_metadata` | Same; Verified for the order announced (`tests/interop/tests/metadata.rs`) |
| `ZMQ_ROUTER_NOTIFY` (d) | `set_notify` | Same |
| `ZMQ_IN_BATCH_SIZE` (d), `ZMQ_OUT_BATCH_SIZE` (d) | | Gap |
| `ZMQ_ONLY_FIRST_SUBSCRIBE` (d) | | Divergence D16 |
| `ZMQ_RECONNECT_STOP` (d) | `set_reconnect_stop` | Same; Divergence D17 |
| `ZMQ_HELLO_MSG` (d), `ZMQ_DISCONNECT_MSG` (d), `ZMQ_HICCUP_MSG` (d) | `set_hello_message`, `set_disconnect_message`, `set_hiccup_message` | Same |
| `ZMQ_PRIORITY` (d) | | Gap |
| `ZMQ_BUSY_POLL` (d) | `set_busy_poll` | Same |
| `ZMQ_XSUB_VERBOSE_UNSUBSCRIBE` (d) | `set_verbose_unsubscribe` | Same |
| `ZMQ_TOPICS_COUNT` (d) | `topics_count` | Same |
| Setting an option back to its default (`-1`, `0`, an empty string) | Not possible for the maximum message size, heartbeat, keepalive, device and SOCKS proxy setters | Gap |
| `zmq_getsockopt` of a value the application set | | N/A |

## Context options

| libzmq | zmq-rs | Status |
|---|---|---|
| `ZMQ_IO_THREADS` | `Context::with_io_threads` | Same; Verified for 0 (`tests/interop/tests/context.rs`) |
| `ZMQ_MAX_SOCKETS`, `ZMQ_SOCKET_LIMIT` | No limit | Gap |
| `ZMQ_THREAD_PRIORITY`, `ZMQ_THREAD_SCHED_POLICY` | | Gap |
| `ZMQ_THREAD_AFFINITY_CPU_ADD`, `_REMOVE` | | Gap |
| `ZMQ_THREAD_NAME_PREFIX`, `zmq_ctx_set_ext`, `zmq_ctx_get_ext` (d) | Threads are named `zmq-io-N` | Gap |
| `ZMQ_IPV6` | | Gap |
| `ZMQ_BLOCKY` | Behaves as on | Gap |
| `ZMQ_MAX_MSGSZ`, `ZMQ_MSG_T_SIZE`, `ZMQ_ZERO_COPY_RECV` (d) | | N/A |

## Functions

| libzmq | zmq-rs | Status |
|---|---|---|
| `zmq_ctx_new`, `zmq_init` | `Context::new`, `with_io_threads` | Same |
| `zmq_ctx_term`, `zmq_ctx_destroy`, `zmq_term`, `zmq_ctx_shutdown` | Drop, `Context::shutdown` | Exception (API shape) |
| `zmq_socket` | `Context::socket`, `socket_with` | Same; Divergence D19 |
| `zmq_close` | Drop | Same |
| `zmq_bind`, `zmq_connect`, `zmq_unbind`, `zmq_disconnect` | `bind`, `connect`, `unbind`, `disconnect` | Same; Divergence D14 |
| `zmq_send`, `zmq_recv`, `zmq_msg_send`, `zmq_msg_recv`, `zmq_send_const` | `send`, `recv`, their `try_`, `_timeout` and `_async` forms | Same |
| `zmq_sendiov`, `zmq_recviov` | | N/A |
| `zmq_msg_init*`, `_close`, `_move`, `_copy`, `_data`, `_size` | `Frame`, `Message` | Same |
| `zmq_msg_gets` | `Message::property` | Same; Gap for a draft socket's `Frame` and an XPUB's subscription events |
| `zmq_msg_get (ZMQ_SRCFD)` | | Gap |
| `zmq_msg_routing_id`, `zmq_msg_group` and their setters (d) | `ClientId`, `PeerId`, `Group` | Same |
| `zmq_poll` over sockets | `zmq::poll`, `poll_timeout`, over a set fixed at compile time | Same; Gap: REQ and `Monitor` cannot be polled |
| `zmq_poll` over file descriptors, `zmq_ppoll` | | Gap |
| `zmq_poller_*` (d), with `zmq_poller_fd` and `_add_fd` | | Gap |
| `zmq_timers_*` (d) | | Gap |
| `zmq_proxy`, `zmq_device` | `Proxy` | Same; Gap: a REP end, or one socket as both ends |
| `zmq_proxy_steerable` | `ProxyControl` | Exception (API shape) |
| `zmq_socket_monitor`, `zmq_socket_monitor_versioned` (d) | `Socket::monitor` | Exception (API shape); see [Monitor events](#monitor-events) |
| `zmq_socket_monitor_pipes_stats` (d) | | Gap |
| `zmq_socket_get_peer_state` (d) | | Gap |
| `zmq_z85_encode`, `zmq_z85_decode` | Keys only; the general codec is not exported by `zmq` | Gap |
| `zmq_curve_keypair`, `zmq_curve_public` | `Keypair::generate`, `Keypair::from_secret` | Same; Gap: a secret key as Z85 |
| `zmq_join`, `zmq_leave`, `zmq_connect_peer` (d) | `join`, `leave`, `connect_peer` | Same |
| `zmq_version`, `zmq_has` | | Gap |
| `zmq_errno`, `zmq_strerror`, `zmq_atomic_counter_*`, `zmq_stopwatch_*`, `zmq_sleep`, `zmq_threadstart`, `zmq_threadclose` | | N/A |
| The ZAP handler at `inproc://zeromq.zap.01`, consulted by every socket of the context | `Authenticator`, given to one socket | Gap |

## Monitor events

| libzmq | zmq-rs | Status |
|---|---|---|
| `CONNECTED`, `LISTENING`, `ACCEPTED`, `DISCONNECTED` | `Connected`, `Listening`, `Accepted`, `Disconnected` | Same |
| `CONNECT_RETRIED` | `ConnectRetried` | Same |
| `BIND_FAILED`, `ACCEPT_FAILED` | `BindFailed`, `AcceptFailed` | Same |
| `CLOSED` | `Closed`, for a listener and for a failed connection attempt | Same; Verified for an attempt (`tests/interop/tests/reconnect.rs`) |
| `CONNECT_DELAYED`, `CLOSE_FAILED` | | Gap |
| `MONITOR_STOPPED` | `MonitorStopped`, as `recv`'s error | Same |
| `HANDSHAKE_SUCCEEDED`, `HANDSHAKE_FAILED_NO_DETAIL`, `_PROTOCOL`, `_AUTH` | `HandshakeSucceeded`, `HandshakeFailed` | Same |
| The `ZMQ_PROTOCOL_ERROR_*` codes | Four kinds: greeting, handshake, decode, CURVE | Gap |
| The mask of events asked for | Every event | Gap |
| An event's local and remote address (d) | One endpoint | Gap |
| `PIPES_STATS` (d) | | Gap |
| An event's file descriptor or errno | An `io::ErrorKind` for the two failures | N/A |

## Evidence

A row is Verified when a test shows it: against libzmq itself for what a
peer sees, in both bind directions; by libzmq's own test, ported with its
assertion unchanged, for what only the application sees. A row that is
Verified says so and names its test; no other row has been judged against
that bar. A Divergence is taken off its list once it is closed. What is
known to fall short of the bar:

- **One bind direction only**, against libzmq: every pair but PUSH/PULL,
  PAIR, PEER, DEALER/DEALER and ROUTER/DEALER.
- **No test against libzmq** for:
  - IPv6
  - `ipc` beyond PUSH/PULL over NULL
  - PLAIN succeeding through the typed sockets
  - CURVE with ZAP when libzmq is the server
  - a ZMTP 3.0 peer (libzmq 4.0 or 4.1)
  - `XPUB_VERBOSE`, `XPUB_VERBOSER`, `XPUB_NODROP`, `INVERT_MATCHING`,
    `TOPICS_COUNT`, `XSUB_VERBOSE_UNSUBSCRIBE`
  - `ROUTER_NOTIFY`, `CONNECT_ROUTING_ID`
  - `unbind` and `disconnect`
  - reconnect intervals and `RECONNECT_STOP`
  - `DISCONNECT_MSG`, `HICCUP_MSG`
  - the HWM of a typed socket
  - the SOCKS proxy
  - an option set on the libzmq side with zmq-rs as its peer: `CONFLATE`,
    `IMMEDIATE`, `ROUTER_HANDOVER`, `REQ_CORRELATE`, `REQ_RELAXED`,
    `HELLO_MSG`
- **Ported tests that assert something other than libzmq's expectation.**
  Each is a Divergence or an Exception above, or has yet to be recorded
  as one: about thirty, listed in the headers of the modules in
  `crates/zmq/tests/libzmq/`.
- **libzmq tests ported in part**: `poller` (6 of 49 cases), `monitor`
  (4 of 10), `security_zap` (12 of 30), `radio_dish` (6 of 16),
  `inproc_connect` (lacks `shutdown_during_pend`).
- **Not settled by reading the sources**:
  - which `ipc` socket files libzmq removes when a listener closes
  - whether libzmq's proxy stalls both directions when one end is full
  - the length limit of a ZAP domain
  - whether `Security::null_authenticated` with an empty domain asks its
    authenticator, where libzmq would not
  - link-local addresses with a zone (`fe80::1%eth0`)
  - what becomes of a connection that is never retried: `Reconnect::Never`
    closes its pipes, where libzmq's connecter stays, idle, after a first
    attempt fails
