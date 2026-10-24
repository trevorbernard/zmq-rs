# Benchmarks

Throughput and latency next to libzmq's, measured as libzmq's `perf/`
tools measure them, to find where we are slower.

```sh
cargo bench -p zmq-interop --bench perf                 # everything, about 8 minutes
cargo bench -p zmq-interop --bench perf -- thr tcp 64k  # a subset
cargo bench -p zmq-interop --bench perf -- --quick      # smoke test
```

A filter is a kind (`thr`, `lat`), transport (`tcp`, `ipc`, `inproc`), size
(`8`, `256`, `4k`, `64k`) or pair (`ours`, `libzmq`, `ours-libzmq`,
`libzmq-ours`). A case runs if it matches some filter in every dimension
filtered on.

Other patterns, such as fan-in, fan-out and CURVE, have their own bench
([below](#other-patterns)), as do publishing and fair queueing alone.

## What is measured

| | Throughput | Latency |
|---|---|---|
| libzmq tool | `local_thr` / `remote_thr` | `local_lat` / `remote_lat` |
| Sockets | PUSH connects, PULL binds | REQ connects, REP binds and echoes |
| Timed | At PULL, from the first message to the last | At REQ, after one untimed round trip |
| Reported | Messages per second, and megabits per second | Mean one-way µs (`elapsed / 2N`), and p50/p99 of each round trip halved |
| Count | 1,000,000 messages; 100,000 at 64 KiB | 20,000 round trips; 5,000 at 64 KiB |

- **Pairs** are written sender→receiver, or requester→replier.
  `ours→libzmq` and `libzmq→ours` show which side a gap is on. inproc
  can't join two libraries, so it has the same-library pairs only.
- **Our sockets** use the blocking API.
- **libzmq's sockets** are driven through `zmq_interop::libzmq` exactly as
  its tools drive them: one `zmq_msg_t` reused for every receive, nothing
  copied out.
- **One difference from libzmq's tools:** each throughput sender writes
  its payload, of either library. `remote_thr` leaves the payload
  unwritten, but safe Rust can't. Zeroing 64 KiB costs about as much as
  sending it over inproc, so writing on one side only skewed the result.
  At inproc 64 KiB the ratio read 0.72 before both sides wrote and 1.54
  after.
- **Isolation:**
  - Every measurement runs in a fresh child process.
  - Over tcp and ipc, each side has its own context and thread, standing
    in for libzmq's two processes.
  - One warm-up run per case, then 5 rounds with cases interleaved.
  - Cells give the median `[min–max]`.
- **Ratio:** `ours/libzmq` compares the same-library medians and reads
  above 1.00 when we are faster.
- **Failures:** each receiver checks every message's size and the count. A
  child still running after 120 s is killed and the run fails.

## Results: Apple M1 Pro

Apple M1 Pro (10 cores, 16 GB), macOS 26.4.1, libzmq 4.3.5 from nixpkgs,
commit `b2abe9f` plus the benchmark. 2026-10-17. This predates the pipe,
fair-queue and I/O changes measured on Linux below.

Our throughput ratio is above 1.00 everywhere except ipc at 64 KiB
(0.79), and inproc at 4 KiB, which is level within noise. Our latency is
lower everywhere.

### Throughput, msg/s (Mb/s)

**tcp**

| Size | ours→ours | libzmq→libzmq | ours→libzmq | libzmq→ours | ours/libzmq |
|---|---|---|---|---|---|
| 8 B | 8.84M [6.08M–10.16M] (566) | 5.54M [5.21M–5.73M] (354) | 5.68M | 9.54M | 1.60 |
| 256 B | 5.94M [4.42M–6.67M] (12168) | 3.41M [3.30M–3.58M] (6976) | 4.66M | 3.93M | 1.74 |
| 4 KiB | 873.1k [853.7k–881.8k] (28610) | 364.8k [327.3k–385.9k] (11954) | 962.1k | 372.7k | 2.39 |
| 64 KiB | 72.3k [61.0k–81.1k] (37928) | 55.0k [50.4k–58.4k] (28810) | 73.7k | 55.7k | 1.32 |

**ipc**

| Size | ours→ours | libzmq→libzmq | ours→libzmq | libzmq→ours | ours/libzmq |
|---|---|---|---|---|---|
| 8 B | 7.15M [6.83M–8.21M] (457) | 5.64M [3.94M–5.80M] (361) | 4.92M | 9.47M | 1.27 |
| 256 B | 5.41M [4.44M–6.00M] (11077) | 2.45M [2.07M–2.46M] (5023) | 3.54M | 2.30M | 2.21 |
| 4 KiB | 266.9k [257.9k–273.4k] (8744) | 246.6k [235.4k–259.3k] (8081) | 253.2k | 225.4k | 1.08 |
| 64 KiB | 14.7k [13.5k–16.1k] (7721) | 18.6k [17.9k–19.5k] (9770) | 14.4k | 16.7k | 0.79 |

**inproc**

| Size | ours→ours | libzmq→libzmq | ours/libzmq |
|---|---|---|---|
| 8 B | 12.08M [12.00M–12.60M] (773) | 6.88M [6.04M–7.27M] (441) | 1.75 |
| 256 B | 10.51M [9.33M–12.18M] (21515) | 6.25M [6.15M–6.49M] (12795) | 1.68 |
| 4 KiB | 4.55M [3.94M–4.97M] (149039) | 4.80M [4.73M–5.04M] (157235) | 0.95 |
| 64 KiB | 1.36M [1.19M–1.47M] (712642) | 880.3k [738.2k–905.2k] (461514) | 1.54 |

### Latency, one-way µs (p50/p99)

**tcp**

| Size | ours→ours | libzmq→libzmq | ours→libzmq | libzmq→ours | ours/libzmq |
|---|---|---|---|---|---|
| 8 B | 20.8 [20.1–21.9] (20.0/44.5) | 33.3 [33.1–35.4] (31.9/58.7) | 29.2 | 28.4 | 1.60 |
| 256 B | 22.0 [20.7–24.0] (21.1/42.9) | 34.3 [33.3–34.8] (32.0/67.2) | 29.2 | 29.3 | 1.56 |
| 4 KiB | 21.6 [20.9–22.9] (21.3/38.8) | 34.4 [34.0–35.5] (32.2/72.1) | 29.5 | 29.5 | 1.60 |
| 64 KiB | 32.9 [32.0–34.9] (31.6/58.0) | 58.6 [56.6–62.6] (56.0/102.3) | 46.3 | 47.7 | 1.78 |

**ipc**

| Size | ours→ours | libzmq→libzmq | ours→libzmq | libzmq→ours | ours/libzmq |
|---|---|---|---|---|---|
| 8 B | 11.2 [10.9–11.3] (10.8/23.8) | 24.0 [23.5–25.0] (23.8/44.0) | 19.3 | 19.2 | 2.16 |
| 256 B | 11.6 [11.4–13.0] (11.5/18.0) | 23.8 [23.5–24.3] (23.6/43.5) | 19.4 | 19.5 | 2.04 |
| 4 KiB | 12.4 [11.7–13.8] (11.8/26.9) | 24.5 [23.9–26.4] (23.8/53.0) | 20.1 | 19.6 | 1.98 |
| 64 KiB | 72.3 [70.7–75.1] (70.2/93.7) | 80.0 [76.9–87.7] (81.9/105.8) | 75.6 | 73.1 | 1.11 |

**inproc**

| Size | ours→ours | libzmq→libzmq | ours/libzmq |
|---|---|---|---|
| 8 B | 1.6 [1.5–1.7] (1.5/3.1) | 14.5 [14.2–15.1] (14.5/35.7) | 9.23 |
| 256 B | 1.6 [1.6–1.9] (1.5/3.4) | 14.1 [14.0–15.3] (14.4/31.1) | 8.71 |
| 4 KiB | 1.6 [1.6–1.7] (1.5/3.4) | 14.1 [13.9–14.7] (15.0/24.3) | 8.74 |
| 64 KiB | 1.7 [1.6–1.7] (1.5/3.9) | 14.2 [13.9–14.9] (15.0/25.6) | 8.51 |

### Reading them

- **ipc at 64 KiB is our one throughput gap, and it is on the sending
  side.** Our sender reaches 14.4k msg/s into libzmq, where libzmq's own
  sender reaches 16.7k into ours. ipc throughput at 4 KiB and up is far
  below tcp for both libraries on this machine, so Unix-socket buffering
  is probably part of it. Not yet investigated.
- **inproc latency, about 1.6 µs against 14 µs.** Our blocking calls park
  the thread and are woken directly by the pipe. libzmq wakes a blocked
  socket through its mailbox's signaler, a file descriptor it polls.
- **Mixed pairs land between the two same-library pairs** for latency, so
  both of our ends contribute to the difference.
- **Run-to-run spread is wide on macOS,** up to ±20% at the small sizes
  (see min–max). Compare medians, and rerun a subset before acting on a
  difference under about 10%.

## Results: Linux, Intel Xeon

Intel Xeon E5-1650 v4 (6 cores, 12 threads, 64 GB), Linux 6.18, the
`schedutil` governor, libzmq 4.3.5 from nixpkgs, commit `d13552c`.
2026-10-24.

Our throughput is ahead or level everywhere except 64 KiB over tcp
(0.93) and inproc (0.90, a case whose runs spread widely for both
libraries). Our latency is lower everywhere.

### Throughput, msg/s (Mb/s)

**tcp**

| Size | ours→ours | libzmq→libzmq | ours→libzmq | libzmq→ours | ours/libzmq |
|---|---|---|---|---|---|
| 8 B | 11.09M [10.47M–12.02M] (710) | 6.30M [5.98M–6.41M] (403) | 6.43M | 6.77M | 1.76 |
| 256 B | 2.18M [2.14M–2.22M] (4471) | 1.59M [1.53M–1.62M] (3253) | 2.22M | 1.56M | 1.37 |
| 4 KiB | 487.3k [474.2k–522.9k] (15967) | 248.5k [227.7k–273.2k] (8141) | 514.3k | 233.4k | 1.96 |
| 64 KiB | 33.4k [32.6k–34.2k] (17504) | 35.9k [35.0k–38.1k] (18837) | 34.3k | 36.2k | 0.93 |

**ipc**

| Size | ours→ours | libzmq→libzmq | ours→libzmq | libzmq→ours | ours/libzmq |
|---|---|---|---|---|---|
| 8 B | 10.20M [10.02M–10.27M] (653) | 6.29M [6.01M–6.37M] (403) | 6.78M | 6.71M | 1.62 |
| 256 B | 2.15M [2.05M–2.18M] (4401) | 1.99M [1.94M–2.04M] (4081) | 2.08M | 2.01M | 1.08 |
| 4 KiB | 593.4k [576.0k–604.4k] (19444) | 395.3k [389.6k–398.1k] (12952) | 526.9k | 432.6k | 1.50 |
| 64 KiB | 52.4k [50.3k–53.2k] (27464) | 48.5k [47.5k–48.7k] (25419) | 52.1k | 54.3k | 1.08 |

**inproc**

| Size | ours→ours | libzmq→libzmq | ours/libzmq |
|---|---|---|---|
| 8 B | 9.98M [9.02M–10.61M] (638) | 6.22M [6.04M–6.36M] (398) | 1.60 |
| 256 B | 1.89M [1.85M–1.92M] (3880) | 1.95M [1.93M–2.03M] (3996) | 0.97 |
| 4 KiB | 1.33M [1.31M–1.41M] (43727) | 1.29M [1.27M–1.35M] (42217) | 1.04 |
| 64 KiB | 145.5k [133.0k–166.4k] (76310) | 162.6k [118.0k–182.7k] (85227) | 0.90 |

### Latency, one-way µs (p50/p99)

**tcp**

| Size | ours→ours | libzmq→libzmq | ours→libzmq | libzmq→ours | ours/libzmq |
|---|---|---|---|---|---|
| 8 B | 25.4 [19.5–40.4] (23.0/40.8) | 53.1 [25.4–71.1] (44.1/90.0) | 50.5 | 48.4 | 2.09 |
| 256 B | 40.6 [23.7–45.1] (37.9/60.5) | 60.8 [44.3–67.3] (69.9/88.0) | 46.0 | 47.4 | 1.50 |
| 4 KiB | 48.1 [35.1–55.2] (48.7/67.7) | 60.5 [39.1–73.3] (71.5/96.8) | 57.1 | 64.3 | 1.26 |
| 64 KiB | 108.4 [99.6–117.4] (107.7/177.7) | 146.2 [122.6–152.9] (154.4/191.0) | 133.0 | 129.1 | 1.35 |

**ipc**

| Size | ours→ours | libzmq→libzmq | ours→libzmq | libzmq→ours | ours/libzmq |
|---|---|---|---|---|---|
| 8 B | 25.7 [18.9–29.7] (25.9/39.0) | 44.2 [25.4–48.7] (51.0/61.4) | 28.8 | 21.1 | 1.72 |
| 256 B | 15.8 [13.2–20.7] (15.7/22.4) | 32.0 [23.5–37.7] (24.1/63.8) | 37.4 | 29.5 | 2.02 |
| 4 KiB | 21.6 [17.5–31.1] (21.7/31.4) | 50.6 [41.1–58.7] (57.2/71.2) | 25.5 | 21.9 | 2.34 |
| 64 KiB | 40.8 [36.1–66.9] (38.5/57.4) | 48.2 [44.4–68.5] (45.3/90.4) | 48.3 | 40.0 | 1.18 |

**inproc**

| Size | ours→ours | libzmq→libzmq | ours/libzmq |
|---|---|---|---|
| 8 B | 4.6 [4.4–4.7] (4.4/6.6) | 8.7 [7.6–10.2] (9.0/13.7) | 1.89 |
| 256 B | 4.7 [3.8–4.8] (4.3/6.9) | 10.6 [8.6–11.1] (11.0/14.8) | 2.24 |
| 4 KiB | 5.5 [4.2–6.1] (5.0/7.7) | 9.2 [7.0–10.4] (8.9/14.7) | 1.67 |
| 64 KiB | 4.8 [3.9–4.8] (4.6/7.4) | 8.5 [6.6–9.5] (7.9/12.9) | 1.78 |

### Reading them

- **Pipes used to be std's `mpsc`,** a multi-producer list doing two
  read-modify-writes per send and per receive, with a 1992 B block
  allocated every 31 items and freed on the other thread. The chunked
  queue that replaced it (`9f0e714`) took `zmq-pipe`'s throughput
  benchmark from 50 to 27 ns a message (65 to 25 ns at a high-water mark
  of 1000), and 256 B over ipc from 0.85 to 1.06, over inproc from 0.86
  to 1.05.
- **Its readiness lives in each slot, not in one shared counter.**
  libzmq's ypipe publishes through one counter; ported as is, that was
  slower than `mpsc` (75 ns a message), because with the reader keeping
  up the counter moves a second cache line between cores per message.
- **Blocking calls borrow the thread's waker** rather than cloning it,
  saving two atomic operations a call (`51f314a`): ipc at 8 B went from
  6.26M to 6.76M msg/s. Before, they allocated a waker per call, freed on
  the peer's thread, which glibc handles badly (`0fd7eff`).
- **Writers wake their reader only when it is waiting** (`17b2266`), as
  libzmq's ypipe does; a reader about to wait marks the slot it waits on.
- **256 B is bound by the allocator for both libraries.** About 55% of
  either's cycles at inproc 256 B go to glibc's malloc and free: each
  allocates once per message on the sender, freed on the receiver.
  Under mimalloc or jemalloc (`LD_PRELOAD`) both run up to three times as
  fast; libzmq's 8 B case barely moves, as it stores small messages
  inline.
- **Release builds with fat LTO** and one codegen unit run 7–19% faster
  at 8 B and 256 B (measured at `51f314a`); the bench uses the default
  profile.
- **tcp at 64 KiB is behind on copies.** Our I/O threads spend 41% of
  their time in memmove and 10% in memset, against libzmq's 15%: the
  encoder copies every payload into its output buffer, where libzmq
  writes large ones from the message, and the decoder zeroes its read
  buffer.
- **Noise:** inproc 64 KiB throughput spreads widely for both libraries
  in every run, and so does latency at small sizes. The machine ran the
  `schedutil` governor; rerun a subset before acting on a difference
  under about 10%.

## Other patterns

`perf` covers one PUSH to one PULL and REQ to REP. The scenarios bench
compares throughput over tcp in what it leaves out:

```sh
cargo bench -p zmq-interop --bench scenarios          # everything, a few minutes
cargo bench -p zmq-interop --bench scenarios -- fanin  # cases whose name contains a filter
```

| Case | What |
|---|---|
| `multipart/N` | PUSH to PULL, N frames of 64 octets |
| `fanout/N` | One PUB to N SUBs, each subscribed to everything; 64 octets, no high-water marks |
| `fanin/N` | N PUSH sockets, one thread each, in one context, into one PULL; 256 octets |
| `router/N` | N DEALERs, one thread each, in one context, into one ROUTER; 256 octets |
| `curve/S` | PUSH to PULL under CURVE, S octets |

Each case runs in a fresh process, once to warm up and then five
rounds; the table gives medians.

### Results (`d13552c`)

On the Xeon above:

| Case | ours msg/s | libzmq msg/s | ours/libzmq |
|---|---|---|---|
| multipart/2 | 1.76M | 2.00M | 0.88 |
| multipart/8 | 559.5k | 535.2k | 1.05 |
| fanout/1 | 2.95M | 3.81M | 0.77 |
| fanout/4 | 1.82M | 1.51M | 1.21 |
| fanout/16 | 637.0k | 354.0k | 1.80 |
| fanin/16 | 4.29M | 2.57M | 1.67 |
| fanin/64 | 3.67M | 2.42M | 1.52 |
| router/1 | 2.20M | 1.55M | 1.42 |
| router/16 | 4.33M | 2.55M | 1.70 |
| curve/256 | 269.5k | 662.3k | 0.41 |
| curve/64k | 4.5k | 11.6k | 0.39 |

- **Many connections on one I/O thread** were 0.61 (`fanin/16`) and 0.65
  (`router/16`) until an engine did one round per drive (`d13552c`). It
  used to drain one connection's pipe before the next, freeing that
  socket thread's messages while it allocated more under the same glibc
  arena lock: 60% of the sending I/O thread went to `free`, 83% of that
  to the lock. libzmq's engines return to the poller after each batch.
- **CURVE is behind on the cipher.** RustCrypto's `salsa20` has no SIMD
  code: at 64 KiB it takes 61% of our cycles and runs about 3.7 times
  slower than libsodium's. At 256 B, Poly1305's AVX2 helpers are not
  inlined without `-C target-cpu=native`, which takes `curve/256` to
  0.70. Each frame also costs three allocations and about four copies
  on sending.
- **`fanout/1` was 0.77 while the publisher cloned the message for every
  subscriber,** its only one included: cloning a `Vec`-backed frame
  allocates a shared header, freed on the I/O thread. The last
  subscriber now takes the message itself, which gives 4.29M msg/s
  against libzmq's 3.73M (1.15) with 4 and 16 subscribers unchanged.
  A message for several subscribers still pays that one allocation.
- **`multipart/2` was 0.88 while every message of more than one frame
  allocated its frame vector,** on both ends. libzmq has no such list:
  each frame is its own `zmq_msg_t` in the pipe, flushed with the last.
  A message now holds its second frame inline, which gives 2.86M msg/s
  against libzmq's 1.92M (1.49); a third frame still moves them to a
  vector. `Message` grew from 56 to 72 octets, and with it each pipe
  slot; 8 B over ipc may be 3–5% slower for it, within run-to-run
  spread, and tcp and inproc are level.

## Publisher fan-out

`Publisher::send` alone, with criterion, for routing changes:

```sh
cargo bench -p zmq-core --bench fanout                      # everything, about 2 minutes
cargo bench -p zmq-core --bench fanout -- long_topics       # one group
cargo bench -p zmq-core --bench fanout -- --save-baseline a # compare later with --baseline a
```

Peers are inproc SUB connections, each with one subscription; their
queues are drained outside the timed sends.

| Group | Peers | Topics | Message matches |
|---|---|---|---|
| `one_match_among_many/N` | N | distinct, 10 octets | one peer |
| `long_topics/L` | 100 | L octets, differing in the last two | one peer |
| `all_match/N` | N | empty | every peer |

### Results: shared topic index (`e091061`)

Median per send on the Intel Xeon E5-1650 v4 (`schedutil` governor),
against matching each peer's own topics (`0efa9cc`):

| Case | Per-peer | Shared index | Change |
|---|---|---|---|
| `one_match_among_many/1` | 446 ns | 433 ns | −3% |
| `one_match_among_many/10` | 3.93 µs | 1.25 µs | −68% |
| `one_match_among_many/100` | 39.0 µs | 8.94 µs | −77% |
| `one_match_among_many/1000` | 426 µs | 106 µs | −75% |
| `long_topics/16` | 55.8 µs | 9.19 µs | −84% |
| `long_topics/256` | 721 µs | 15.0 µs | −98% |
| `long_topics/4096` | 11.4 ms | 108 µs | −99% |
| `all_match/1` | 164 ns | 206 ns | +26% |
| `all_match/10` | 1.48 µs | 1.54 µs | +4% |
| `all_match/100` | 17.3 µs | 15.9 µs | −8% |

- **The index wins once peers or topic lengths grow:** each peer costs
  one set lookup rather than a hash per octet of the message.
- **It loses about 40 ns with one peer subscribed to everything,**
  the cost of collecting the matched set.

### Results: looking up only topic lengths (`prefixes_of`)

Looking up only prefix lengths some topic has, and not rehashing the
buckets' already-keyed hashes, against the shared index above:

| Case | Shared index | Length lookups | Change |
|---|---|---|---|
| `one_match_among_many/1` | 447 ns | 219 ns | −51% |
| `one_match_among_many/10` | 1.20 µs | 980 ns | −18% |
| `one_match_among_many/100` | 9.02 µs | 8.87 µs | −2% |
| `one_match_among_many/1000` | 107 µs | 109 µs | +2% |
| `long_topics/16` | 9.37 µs | 8.78 µs | −6% |
| `long_topics/256` | 14.9 µs | 9.15 µs | −38% |
| `long_topics/4096` | 111 µs | 13.6 µs | −88% |
| `all_match/1` | 206 ns | 207 ns | 0% |
| `all_match/10` | 1.57 µs | 1.54 µs | −2% |
| `all_match/100` | 16.1 µs | 16.0 µs | −1% |

- **Matching costs about 1.2 ns per message octet,** from about 24: the
  per-octet hash step is all that is left.
- **About 107 ns per peer at 1000 peers is spent outside matching.**

### Results: cheaper sync and peer lookups (`19ab4c2`)

`sync` reading with `try_recv` instead of registering a no-op waker on
every peer, and a multiplicative hasher for maps keyed by peer, against
the length lookups above:

| Case | Length lookups | Cheaper sync | Change |
|---|---|---|---|
| `one_match_among_many/1` | 221 ns | 152 ns | −31% |
| `one_match_among_many/10` | 1.00 µs | 339 ns | −66% |
| `one_match_among_many/100` | 8.85 µs | 2.17 µs | −75% |
| `one_match_among_many/1000` | 107 µs | 30.1 µs | −72% |
| `long_topics/16` | 8.92 µs | 2.20 µs | −75% |
| `long_topics/256` | 9.17 µs | 2.51 µs | −73% |
| `long_topics/4096` | 13.7 µs | 6.87 µs | −50% |
| `all_match/1` | 205 ns | 134 ns | −35% |
| `all_match/10` | 1.53 µs | 913 ns | −40% |
| `all_match/100` | 16.2 µs | 9.36 µs | −42% |

- **Each peer cost about 107 ns at 1000 peers, matching aside.** Timed
  apart: about 44 ns polling its empty pipe, 16 ns hashing its key for
  `subscribers`, 14 ns for the rest of `sync`, and 35 ns in the send
  loop, mostly hashing its key twice more.
- **About 30 ns per peer is left.** Visiting only matched peers, and
  reading only peers with input waiting, would remove most of it.

## Fair queueing

`FairQueue::poll_recv` alone, with criterion, for PULL, DEALER, ROUTER
and the other fair-queued sockets:

```sh
cargo bench -p zmq-core --bench fairqueue
```

Peers are inproc PUSH connections; senders queue each batch outside the
timed receives.

| Group | Peers | Sending |
|---|---|---|
| `one_sender_among_many/N` | N | one |
| `all_send/N` | N | every one |

### Results: only peers that may have input (`f7ac168`)

Median per receive on the Xeon above, against polling every peer in turn:

| Case | Every peer | Ready peers | Change |
|---|---|---|---|
| `one_sender_among_many/1` | 30.0 ns | 32.2 ns | +7% |
| `one_sender_among_many/10` | 223 ns | 32.7 ns | −85% |
| `one_sender_among_many/100` | 2.08 µs | 32.1 ns | −98% |
| `one_sender_among_many/1000` | 36.4 µs | 32.3 ns | −99.9% |
| `all_send/1` | 30.2 ns | 32.1 ns | +7% |
| `all_send/10` | 30.0 ns | 32.1 ns | +7% |
| `all_send/100` | 31.8 ns | 36.0 ns | +14% |

- **Each idle peer cost 20–36 ns a receive:** a pipe check, a waker
  registration and, later, a wakeup. A peer found empty now leaves the
  turn until its pipe wakes it, as `fq_t` keeps only active pipes.
- **2–4 ns more when every peer sends,** for the turn's bookkeeping.
