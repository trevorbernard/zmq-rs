# RFC compliance

This port targets wire compatibility with libzmq (the reference implementation)
and conformance with the ZeroMQ RFCs at <https://rfc.zeromq.org/>. Where libzmq
and an RFC disagree, **interop with libzmq wins on what we accept**, and **the
RFC wins on what we send**, unless noted.

Each section lists the normative requirements, the libzmq behaviour we must
match, and the resulting design decision. libzmq paths refer to `src/` in the
libzmq repository.

## Scope

| RFC | Name | Status | In scope |
|---|---|---|---|
| 13 | ZMTP 1.0 | deprecated | yes, downgrade only |
| 15 | ZMTP 2.0 | deprecated | yes, downgrade only |
| 23 | ZMTP 3.0 | stable | yes |
| 37 | ZMTP 3.1 | draft | yes (what we send) |
| 24 | ZMTP-PLAIN | stable | yes |
| 25 / 26 | ZMTP-CURVE / CurveZMQ | stable | yes |
| 27 | ZAP | stable | yes (in-process trait, see below) |
| 32 | Z85 | stable | yes |
| 38 | ZMTP-GSSAPI | draft | later |
| 28 | REQREP | stable | yes |
| 29 | PUBSUB | stable | yes |
| 30 | PIPELINE | stable | yes |
| 31 | EXPAIR | stable | yes |
| 41 | CLIENTSERVER | draft | yes |
| 48 | RADIODISH | draft | yes |
| 49 | SCATTERGATHER | draft | yes |
| 51 | P2P | draft | yes |
| 52 | CHANNEL | draft | yes |
| 45 | ZWS 2.0 | draft | later |
| 39 | ZWS 1.0 | stable | no (libzmq never implemented it) |

Protocols built on top of ZeroMQ (MDP, ZRE, FILEMQ, ...) are out of scope.

## Wire protocol: ZMTP (RFC 13, 15, 23, 37)

### Greeting and version negotiation

- The greeting is 64 octets: `FF | 8 padding | 7F | major | minor | mechanism[20] | as-server | 31 zero filler`.
  - Padding MUST NOT be interpreted.
  - The mechanism is ASCII `A-Z0-9-_`, NUL-padded. An exact mismatch MUST close the connection.
- A peer MUST accept any version at or above its own, and MAY downgrade for a lower peer.
- If we read the peer's full greeting, we MUST send our full greeting.
- **Detection** (RFC 37, matching libzmq `zmtp_engine.cpp`):
  1. Send the pseudo-signature `FF | u64 BE (identity_len + 1) | 7F`. This is also a valid ZMTP 1.0 frame header.
  2. Read 1 octet. If it is not `FF`, the peer speaks **V1** and that octet is its identity length.
  3. Otherwise read through octet 9. If `[9] & 1 == 0`, the peer is **V1** with a long length.
  4. Otherwise send major `03` and read `[10]`. A value below 3 means **V2**: send our socket-type byte, for a 12-octet greeting. A value of 3 or more means **V3**: send the rest of the 64 octets.
  5. Within V3, minor `0` is 3.0, and minor `>= 1` (or an unknown future minor) is treated as 3.1.
- libzmq deviations:
  - libzmq maps revision `2` to 3.1; we follow the RFC and treat it as V2.
  - libzmq never validates the V2 peer's socket-type byte. We validate it, mapping libzmq's enum values 0–10.
  - libzmq always sends `as-server = 0` and never reads it. We send the RFC value and never require it from the peer.

**Design:** an incremental, sans-IO greeting parser yields

```rust
enum NegotiatedVersion { V1 { peer_identity_len: u64 }, V2 { peer_socket_type: u8 }, V3_0, V3_1 }
```

which selects a `Codec` (`V1 | V2 | V3_0 | V3_1`). The codec owns both frame
encoding and the wire form of subscriptions.

### Downgrade security

- RFC 37: if the local mechanism is not NULL and a V1 or V2 peer is detected, the connection MUST be closed, because the older versions have no security handshake. Implementations MUST guard against downgrade attacks.
- libzmq additionally refuses old peers when a ZAP domain is set on a NULL socket.

**Design:** the handshake state machine has no transition from a legacy
version to a non-NULL mechanism, so a downgraded connection can only ever run
NULL. Old peers are also refused when NULL has ZAP configured. Downgrade is
on by default, as in libzmq, behind a `legacy_peers` option that can switch it
off.

### Framing

- **V3:** `flags | size(1 or 8, BE) | body`. Flag bits: 0 = MORE, 1 = LONG, 2 = COMMAND; bits 7–3 MUST be 0. Commands SHALL NOT set MORE. A long size is at most 2^63-1.
- **V2:** the same layout with no COMMAND bit.
- **V1:** `length | flags | body`, where the length covers the flags octet too.
  - Lengths 1..254 take 1 octet; longer lengths are `FF` followed by a u64.
  - A length of 0 is invalid.
- Messages SHALL be delivered atomically.
- libzmq deviation: its V2/V3 decoder ignores reserved bits and accepts MORE on commands.

**Design:**
- Decode leniently by default, as libzmq does, with an optional strict mode. Encode strictly.
- Always reject a size of 2^63 or more, enforce `max_msg_size` before allocating, and reject a V1 length of 0.
- `zmtp::Frame` and `zmtp::Command` are distinct types.

### Commands and metadata

- A command is `u8 name_len (1..255) | name | data`.
- A property is `u8 name_len (1..255) | name [A-Za-z0-9-_.+] | u32 BE value_len (< 2^31) | value`.
  - Names are case-insensitive; the `X-` prefix is reserved for applications.
- Socket-Type SHOULD be sent and the pairing SHOULD be validated. On a mismatch the peer SHOULD send ERROR and disconnect.
- libzmq deviations:
  - It compares property names case-sensitively.
  - It accepts a missing Socket-Type.
  - It closes on a type mismatch without sending ERROR.
- ERROR is `u8 len + reason (0..255)`. It is fatal: do not reconnect with the same credentials.

**Design:**
- `PropertyName` is a validated newtype with case-insensitive `Eq` and `Hash`. User metadata must start with `X-`.
- A missing Socket-Type is accepted, for libzmq interop.
- On a mismatch we send ERROR and then close.
- `Command` is a typed enum: `Ready`, `Error`, `Ping`, `Pong`, `Subscribe`, `Cancel`, `Join`, `Leave`, `Unknown`. Unknown commands are ignored after the handshake.

### Valid socket pairs (RFC 23/37, plus libzmq)

| Socket type | Valid peers |
|---|---|
| REQ | REP, ROUTER |
| REP | REQ, DEALER |
| DEALER | REP, DEALER, ROUTER |
| ROUTER | REQ, DEALER, ROUTER |
| PUB, XPUB | SUB, XSUB |
| SUB, XSUB | PUB, XPUB |
| PUSH | PULL |
| PULL | PUSH |
| PAIR | PAIR |
| CLIENT | SERVER |
| SERVER | CLIENT |
| RADIO | DISH |
| DISH | RADIO |
| SCATTER | GATHER |
| GATHER | SCATTER |
| PEER | PEER |
| CHANNEL | CHANNEL |

**Design:** a `const fn` table on socket types, checked when READY arrives
and when an `inproc` connection is paired. Incompatible `inproc` sockets
are never paired, so they exchange nothing. libzmq checks only in the
handshake, which `inproc` skips, so it pairs any two socket types (a
DEALER can feed a PULL). We apply RFC 23's rule on every transport.

### NULL handshake

- RFC 37: the client sends READY and waits, and the server replies with READY.
- RFC 23 and libzmq: both sides send READY immediately.

**Design:** send READY eagerly, as RFC 23 and libzmq do, so we interoperate
with both. Messages flow only after READY has been both sent and received.

### Identity

- The Identity property is 0..255 octets. It is meaningful only from REQ, DEALER or ROUTER towards a ROUTER.
- RFC 37: the first octet SHALL NOT be `00`, which is reserved.
- libzmq accepts a leading `00` from peers. For an empty identity, ROUTER auto-assigns `00 + u32`.

**Design:**
- `RoutingId` is non-empty and at most 255 octets. A local routing id may not start with `00`.
- Peer identities with a leading `00` are accepted.
- Auto-assigned ids are a distinct internal variant.

### Heartbeats (RFC 37)

- PING is `u16 BE TTL (deciseconds) | context (0..16 octets)`, and PONG echoes the context.
- Any inbound traffic counts as a sign of life.
- Do not copy these libzmq bugs:
  - `uint16_t ttl *= 100` overflows.
  - A short PING causes an out-of-bounds read.
  - Malformed commands are silently ignored.

**Design:** reject a PING body shorter than 2 octets. Hold the TTL as a
`Duration`. Any decoded frame resets the liveness deadlines. `Heartbeat`
sets the interval, timeout and TTL we announce; an expired deadline is a
connection error, so a connecting session reconnects.

### Subscriptions on the wire

| Version | Wire form |
|---|---|
| V1 | none (filtering is on the subscriber side). A PUB facing a V1 peer injects a subscribe-all, as libzmq does. |
| V2, V3.0 | data frame prefixed `01` (subscribe) or `00` (unsubscribe) |
| V3.1 | `SUBSCRIBE` and `CANCEL` commands |

**Design:** `zmtp::Subscription` is `Subscribe(Frame)` or `Cancel(Frame)`,
with both wire forms. Inside the process, subscriptions travel in the
message form, as in libzmq: a SUB or XSUB socket sends `01`/`00`-prefixed
messages, and its engine turns them into commands for ZMTP 3.1 peers. A PUB
or XPUB engine turns incoming commands back into that form. Only those
socket types convert, so another socket's message that happens to start
with `01` is left alone. `inproc` needs no conversion at all.

## Security (RFC 24, 25, 26, 27, 32)

### PLAIN (RFC 24)

- Exchange: `HELLO(u8 len username, u8 len password)` → `WELCOME` → `INITIATE(metadata)` → `READY(metadata)`.
- Username and password are each 0..255 octets.
- PLAIN SHOULD NOT be used without transport security.
- libzmq always sends a ZAP request for PLAIN, and the handshake fails if there is no handler.

- **libzmq bug:** `src/plain_common.hpp` declares the ERROR prefix as the C literal `"\x05ERROR"`. A `\x` escape consumes every following hex digit, so `\x05E` is the single octet `0x5E` and libzmq's PLAIN server sends ERROR as `^RROR reason-length reason`. Its PLAIN client likewise expects `^RROR` and treats an RFC-correct ERROR as an unexpected command. This is still present on libzmq `master`; reported as [zeromq/libzmq#4943](https://github.com/zeromq/libzmq/issues/4943).

**Design:**
- A `Credential` newtype checks the 255-octet limit when it is constructed; the password is zeroized and never printed.
- The server returns a `ZapRequest` and waits for a `ZapReply`, so authentication can be synchronous or asynchronous.
- Our PLAIN client also accepts libzmq's `^RROR` form. No valid command starts with `0x5E`, so this cannot misread anything else.
- We send the RFC form. A libzmq client then closes with "unexpected command" rather than "authentication failed"; the connection fails either way.

### CURVE (RFC 25, 26)

| Command | Size (octets) | Notes |
|---|---|---|
| HELLO | exactly 200 | version 1.0, 72 zero octets of padding (not checked on receipt) |
| WELCOME | 168 | |
| INITIATE | >= 257 | |
| READY | >= 30 | |
| MESSAGE | >= 33 | |

Short-nonce prefixes are `CurveZMQHELLO---`, `CurveZMQINITIATE`,
`CurveZMQREADY---`, `CurveZMQMESSAGEC` and `CurveZMQMESSAGES`. Long-nonce
prefixes are `WELCOME-`, `COOKIE--` and `VOUCH---`.

- Short nonces SHALL strictly increase per direction.
  - libzmq checks `nonce > last`, so gaps are allowed, and starts at 1.
  - Sending fails at `u64::MAX` rather than wrapping.
- Any malformed command SHALL disconnect. The server never sends ERROR for a bad MESSAGE, and the client never sends ERROR at all.
- libzmq deviations we must match:
  - MESSAGE goes on the wire as a single-part **data frame**, not the command frame RFC 25 describes: libzmq's mechanism replaces the message, flags included, before framing. A COMMAND-flagged MESSAGE sent to libzmq is taken for a command and its contents are never delivered. We send data frames and accept both forms.
  - The MESSAGE flags octet carries MORE (`0x01`) **and COMMAND (`0x02`)**, although the RFC requires bits 7–1 to be zero.
  - Post-handshake commands are boxed as MESSAGE with COMMAND set.
  - On receipt, mask the flags with `0x03`.
  - The server keeps per-connection state instead of being stateless; this is indistinguishable on the wire.
- **Crypto:** RustCrypto `crypto_box` (`SalsaBox`, curve25519xsalsa20poly1305) and `xsalsa20poly1305` for cookies.
  - NaCl puts the tag before the ciphertext; use the detached APIs and lay out `tag || ct` explicitly.
  - **Verify against HELLO/WELCOME captures from libzmq before trusting the layout.**

**Design:**
- `Keypair` can only be generated or derived from its secret key, so a mismatched pair cannot be constructed. `SecretKey` is zeroized, not `Clone`, and its `Debug` prints nothing of it.
- Transient keys exist only inside the mechanism.

**Done in `zmtp` (sans-IO):**
- `CurveClient`, `CurveServer` and `PendingCurveAuthentication` mirror PLAIN's state machines. ZAP sees `ZapCredentials::Curve(PublicKey)`, the client's long-term key, after the vouch has proven the client holds its secret.
- `CurveCodec` boxes and unboxes frames after the handshake: the client sends from nonce 3 and the server from nonce 2, each refuses a nonce not above the peer's last, and sending stops at `u64::MAX`.
- Checks beyond libzmq: the vouch must name our long-term key as well as C' (RFC 26; libzmq checks only C').
- Verified on the wire against libzmq 4.3.5 in both roles, with multipart messages and 100 KB frames, using the key pairs from libzmq's `zmq_curve(7)`; public keys derived from those secrets match the published ones.

**Done on connections and sockets:**
- `Security::curve_client(keypair, server_key)`, `Security::curve_server(keypair)` and `Security::curve_server_authenticated(keypair, domain, authenticator)`.
- Without an authenticator a CURVE server accepts any client that knows its public key, as libzmq does with no ZAP handler, and the connection records no authentication. With one, the authenticator sees `ZapCredentials::Curve(client_key)`.
- After the handshake every frame is boxed, including PING, PONG, SUBSCRIBE and CANCEL, as libzmq boxes them.
- Security applies to TCP and IPC. Over `inproc` no mechanism runs, as in libzmq.

### ZAP (RFC 27)

- Request frames: `"" | "1.0" | request_id | domain | address | identity | mechanism | credentials...`.
- Reply frames: `"" | "1.0" | request_id | status | text | user_id | metadata`.
- Status codes: 200 OK, 300 temporary failure, 400 authentication failure, 500 internal error.
- User-Id and metadata are meaningful only with 200.
- The handler binds `inproc://zeromq.zap.01`. That endpoint is internal to the process and never appears on the wire.
- libzmq behaviour to match:
  - ERROR reason strings are `"400"` or `"500"`. A 300 closes the connection silently.
  - NULL uses ZAP only when a domain is set.
  - CURVE without a handler accepts the connection (encryption only, the "Stonehouse" pattern) unless `ZAP_ENFORCE_DOMAIN` is set.

**Design:**
- `Authenticator` is a Rust trait: `authenticate(&self, ZapRequest, ZapResponder)`. The responder may answer at once or later from any thread, so authentication never has to block an I/O thread; a responder dropped unanswered counts as status 500. A closure `Fn(&ZapRequest) -> ZapReply` implements it for the synchronous case. `ZapReply::Ok { user_id, metadata }` is the only variant that carries those fields. An optional adapter can bind the RFC's inproc endpoint for existing handlers.
- NULL with ZAP is `Security::null_authenticated(domain, authenticator)`: before sending READY, the socket asks the authenticator about the peer (`ZapCredentials::Null`, its address), as libzmq does for a NULL socket with a ZAP domain. Success sends READY and records the user id; 400 and 500 send ERROR, and 300 closes silently. ZMTP 1.0 and 2.0 peers are refused at the greeting, as libzmq refuses them whenever ZAP is in use. Verified against libzmq both ways, its ZAP handler included.
- CURVE with ZAP is `Security::curve_server_authenticated`; see CURVE above.
- **A single function maps `ZapStatus` to READY, a silent close, or ERROR, and every mechanism goes through it.** libzmq commit ba63f037 fixed a GSSAPI bug where READY was sent despite a rejection; this design rules that bug out.
- The sans-IO handshake has a `WaitingForZap` state.
- CURVE server authentication is `Security::curve_server` (none) or `Security::curve_server_authenticated` (an authenticator).

### Z85 (RFC 32)

- Binary length must be a multiple of 4 and string length a multiple of 5. Each group of 4 octets is a big-endian u32.
- Test vector: `86 4F D2 6F B5 59 F7 5B` ↔ `HelloWorld`.

**Design:** `PublicKey::from_z85` accepts exactly 40 characters. The generic
encoder takes input whose length is checked, and never pads.

## Socket patterns

Rules shared by every pattern:

- One queue per peer, created on connect (whether or not the connection is up yet) and destroyed along with its messages on disconnect.
- HWM is configurable.
- Messages are never modified.
- HWM is checked per **message**, not per frame.
- A `Message` must contain at least one frame, and an empty frame is valid. ROUTER and REP traffic relies on empty delimiter frames.

### REQREP (RFC 28)

- **REQ**
  - Sends an empty delimiter frame, round-robins requests, and keeps strict lockstep.
  - Accepts replies only from the peer it last sent to; others are discarded.
  - With no peers it blocks or errors, and never drops.
  - libzmq also drops the connection when a reply has no delimiter.
- **REP**
  - Strips the whole envelope (every identity frame plus the delimiter), stores it, and restores it on reply.
  - Never blocks on send.
  - Requests without a delimiter are dropped.
- **DEALER**: round-robin, fair-queue, never drops. libzmq also allows DEALER↔DEALER.
- **ROUTER**
  - Prefixes received messages with the identity frame.
  - In libzmq, by default, an unknown or full peer means a silent drop and never a block. `mandatory` returns an error instead.
  - A duplicate identity is ignored, unless `handover` is set.
  - Here, sending to an unknown routing id always fails, handing the message back (`ZMQ_ROUTER_MANDATORY`). `try_send` also fails when the peer is full; `send` waits for room. Ignoring `try_send`'s error is libzmq's default.
  - `set_handover` and `set_probe` (also on DEALER) are libzmq's `ROUTER_HANDOVER` and `PROBE_ROUTER`. A probe goes to every new peer, connected or accepted; on reconnection it is not re-sent, as in libzmq, whose pipe also survives.
- libzmq extensions: `REQ_RELAXED`, `REQ_CORRELATE` (a 4-octet request id before the delimiter), `PROBE_ROUTER`, `CONNECT_ROUTING_ID`.

**Design changes from the initial sketch:**
- `Request<'_>` (REP) owns the full envelope as a stack of frames. It is opaque, with an optional read accessor, so multi-hop proxies work.
- ROUTER's `Message` keeps the delimiter visible, and the user handles it. ROUTER also exposes a raw frame path for proxies.
- REQ: `correlate` is a socket option, not a mode that switches on implicitly when a future is dropped.
  - A dropped `request_async` future leaves the socket in relaxed mode: the next request is allowed, and the late reply is filtered out by pipe and, when `correlate` is on, by request id.
- Dropping a REP `Request` without replying is allowed by the RFC, but the REQ peer then stalls. This is documented on `Request`.
- Two deliberate differences from libzmq's REQ:
  - A reply without a delimiter is dropped and the wait continues; libzmq drops the connection.
  - If the asked peer's connection closes before replying, `request` returns `PeerGone`; libzmq waits forever.

### PUBSUB (RFC 29)

- **PUB and XPUB** never block; a full queue means a silent drop. Matching is a binary prefix match on the first frame.
- **XPUB**
  - Delivers subscription requests to the application. It MAY de-duplicate them, and it SHALL generate unsubscribes when a subscriber disconnects.
  - libzmq also delivers non-subscription messages sent by XSUB peers.
- **XSUB**
  - Forwards to all publishers.
  - SHOULD unsubscribe on close, and resends its subscriptions on reconnect.
- libzmq extensions: `XPUB_NODROP` (errors instead of dropping, opt-in), `VERBOSE`/`VERBOSER`, `MANUAL`, `WELCOME_MSG`, `INVERT_MATCHING`, `ONLY_FIRST_SUBSCRIBE`, `CONFLATE`.

**Design changes:**
- `Socket<XPub>` receives `XPubEvent::{Subscribe(Frame), Cancel(Frame), Message(Message)}` rather than libzmq's `01`/`00`-prefixed messages.
- `Socket<XSub>` has `subscribe`, `unsubscribe`, `send` and `recv`, and `forward(XPubEvent)` for proxies, so an XPUB's events pass straight upstream.
- `Pub::send` never returns an HWM error unless `no_drop` is set.

**Done (XPUB and XSUB):**
- XPUB reports a topic's first subscription and last cancel across all peers, as libzmq does without `XPUB_VERBOSE`. A peer that disconnects cancels its topics. Anything else a peer sends is delivered as `XPubEvent::Message`.
- Two differences from libzmq:
  - A cancel for a topic the peer never subscribed to is ignored. libzmq notifies anyway (`rm_result == not_found`; its code marks this with a TODO).
  - A ZMTP 1.0 subscriber's implicit subscription is reported as `Subscribe("")`. libzmq reports nothing on arrival, yet still sends the cancel when the peer leaves.
- XSUB `send`: a single frame starting with `01` or `00` subscribes or unsubscribes, as libzmq does, so the topics stay counted. Multi-frame messages are always user messages. libzmq would also read a subscription from their first frame.
- XSUB, like SUB, queues without limit towards its peers, so neither subscriptions nor upstream messages are dropped at the HWM. libzmq's `dist_t` drops at the HWM.
- Not yet: `XPUB_VERBOSE`/`VERBOSER`, `MANUAL`, `WELCOME_MSG`, `INVERT_MATCHING`, `NODROP`, `ONLY_FIRST_SUBSCRIBE`.

**Done (PUB and SUB):**
- `Socket<Pub>::send` returns how many subscribers the message was queued for, so a full queue or a topic nobody wants is visible without being an error.
- A SUB counts its subscriptions: every `subscribe` goes upstream, as in libzmq, while `unsubscribe` sends CANCEL only for the last one. A publisher keeps one subscription per topic and peer, as libzmq's XPUB does.
- A SUB filters what arrives too. That covers publishers that send everything, and messages queued before an `unsubscribe`.
- A SUB's subscriptions go to a peer as soon as `connect` returns. Its queues towards peers are unbounded, so a subscription is never dropped at the HWM.
- A session replays the topics it has carried to each new connection after a reconnect, which is the effect of libzmq's hiccup. Cancelled topics are not replayed.

**`CONFLATE`** (PUSH, PULL, PUB, SUB, DEALER): each queue holds only the latest message and the HWM no longer applies. libzmq conflates both directions, which can lose all but the last of a SUB's subscriptions; here the subscription direction never conflates.

### PIPELINE (RFC 30) and EXPAIR (RFC 31)

- **PUSH** round-robins to available peers. With none available it blocks or errors, and never drops.
- **PULL** fair-queues.
- **PAIR** has at most one peer; libzmq terminates any second pipe. With no peer it blocks or errors.

**Done (PAIR):**
- The oldest peer is the one. Any other is turned away by closing its connection, as libzmq terminates the pipe. Messages a turned-away peer already sent are lost with it, as in libzmq; a libzmq PAIR keeps reconnecting and gets in once the slot is free.
- The slot frees only when the peer's session ends for good. A connecting session that is reconnecting keeps it, as libzmq's pipe does.
- Difference from libzmq: messages the departed peer had already delivered are still received, before anything from the next peer. libzmq drops them with the pipe.
- `send` and `recv` wait while there is no peer or no room; `try_send` hands the message back.

### Thread-safe types (RFC 41, 48, 49, 51, 52)

These are libzmq's draft socket types, behind the `draft` Cargo feature
(off by default, like libzmq's `--enable-drafts`), since their RFCs are
drafts and their API may change. `zmtp` knows their names regardless.

A libzmq built without drafts still creates these sockets, but aborts on
their first handshake (`mechanism.cpp`: the socket-type name table only
lists them in a drafts build). The flake therefore uses
`zeromq.override { enableDrafts = true; }`; the draft interop tests check
`zmq_has("draft")` and skip themselves on a libzmq without them, as in
GitHub CI, which installs Ubuntu's.

- Single-part only. Received multipart messages MUST be discarded, and the peer MAY be disconnected.
- **SERVER and PEER**
  - The routing id is a local, non-zero u32 assigned by us, never by the peer. libzmq starts it at a random value and increments.
  - Unknown id → `HostUnreachable`. Full queue → block, or `WouldBlock` for a non-blocking send.
  - `connect_peer` returns the id synchronously.
- **CLIENT and SCATTER** round-robin and never drop. **GATHER** fair-queues.

**Done (CLIENT and SERVER):**
- `Socket<Client>` and `Socket<Server>` send and receive `Frame`s, not `Message`s, so sending a multipart message does not compile. Received multipart messages are dropped.
- SERVER gives each peer a `ClientId`: non-zero, from a counter that starts at a random value, as libzmq does. `send(id, frame)` waits while that client is full and fails with `NoSuchClient` for an unknown or departed one; `try_send` reports `Full` or `NoSuchClient` and hands the frame back. `disconnect(id)` closes a client's connection.
- Verified against libzmq's CLIENT and SERVER both ways, a libzmq SERVER replying included, through its draft routing-id API looked up at run time.

**Done (RADIO and DISH):**
- `Socket<Radio>::send(&group, body)` reaches every DISH peer that joined the group, matched exactly, and never blocks: a full dish misses the message, as in libzmq. It returns how many dishes it was queued for.
- `Socket<Dish>` joins and leaves groups (`false` for a second join, or leaving one not joined, as libzmq refuses them), and receives `(Group, Frame)`. It filters what arrives, so messages queued before a leave are dropped. Its queues towards peers are unbounded, so a join is never dropped.
- On the wire, messages are `group (MORE) | body` and membership changes are JOIN and LEAVE commands, whatever the ZMTP 3.x version, as libzmq's DISH sends them. Inside the process, DISH writes `zmtp::Membership` in a `0x01`/`0x00` message form, as SUB does subscriptions; engines convert, and a session replays its groups after reconnecting.
- The interop tests drive libzmq's RADIO and DISH through its draft API (`zmq_join`, `zmq_msg_set_group`, `zmq_msg_group`), looked up at run time: a libzmq without drafts exports them only under C++-mangled names.
- Not supported: UDP, RADIO/DISH's other transport.

**Done (SCATTER and GATHER):**
- `Socket<Scatter>` load-balances single frames to its GATHER peers and waits while none has room, as PUSH does; `Socket<Gather>` fair-queues them, as PULL does, and drops multipart messages. Sending a multipart message does not compile.
- Verified against libzmq's SCATTER and GATHER in both directions.

**Done (PEER and CHANNEL):**
- `Socket<Peer>` routes as SERVER does, by the `PeerId` it gives every peer, connected or accepted. `connect_peer(endpoint)` returns the new peer's id at once, as libzmq's `zmq_connect_peer` does, so messages to it queue before the connection is up. `send`, `try_send` (`PeerSendError`), `recv` and `disconnect` mirror SERVER's.
- `Socket<Channel>` is PAIR in single frames: the first peer is the one, and multipart messages received are dropped.
- Not supported: libzmq's `ZMQ_HELLO_MSG`, `ZMQ_DISCONNECT_MSG` and `ZMQ_HICCUP_MSG` options for PEER.
- Verified against libzmq's PEER both ways and its CHANNEL.
- **CHANNEL** has a single peer.
- **RADIO**
  - Silently drops when full, and never blocks unless `no_drop` is set.
  - Never delivers JOIN or LEAVE to the application.
- **DISH**
  - Filters by exact match. Joining the same group twice or leaving an unjoined group is an error.
  - Replays its joins on reconnect.
- Group: 0..255 octets with no NUL octets.
  - libzmq **aborts the process** on a remote group over 255 octets; we disconnect the peer instead.
- **RADIO/DISH wire format:**
  - Over TCP, IPC or WS: two frames, `group (MORE) | body`.
  - JOIN and LEAVE are commands: `\x04JOIN` or `\x05LEAVE`, followed by the group.
  - Over UDP: a datagram `u8 group_len | group | body`, at most 8192 octets. RADIO only connects and DISH only binds.

**Design:**
- `RoutingId` for SERVER and PEER is `NonZeroU32` (a separate type from the ROUTER identity).
- `Group::new` is fallible.
- A UDP RADIO send over 8192 octets returns `TooLarge` instead of being silently dropped.
- A sealed `CompatiblePeer` trait encodes the pairing table.

## ZWS (RFC 45), later

- libzmq implements only 2.0.
- Subprotocols: `ZWS2.0/NULL`, `ZWS2.0`, `ZWS2.0/PLAIN`, and `ZWS2.0/CURVE` (a libzmq extension).
- Each WS binary message carries one frame, prefixed by a flag octet (`01` = more, `02` = command).
- Text frames and fragmented messages are rejected.
- Plain `ZWS2.0` carries no READY, so the socket-type check is skipped.
