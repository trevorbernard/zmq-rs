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
  - Children run with glibc's heap trimming off
    (`GLIBC_TUNABLES=glibc.malloc.trim_threshold=1073741824`, unless
    `GLIBC_TUNABLES` is already set). Otherwise glibc returns the top of
    the heap to the kernel as the receiver frees, the sender faults it back
    in, and how often depends on thread timing. At inproc 64 KiB, either
    library ran anywhere from 110k to 330k msg/s, following its page
    faults; trimming off, both run at about 330k.
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
`schedutil` governor, libzmq 4.3.5 from nixpkgs, commit `f3e7a19`.
2026-10-25.

Our throughput is ahead or level everywhere. Our latency is lower
everywhere except ipc at 64 KiB (0.90), whose runs overlap: ours has
the same median as in earlier runs, 51 µs, and libzmq's spans 45–75.

### Throughput, msg/s (Mb/s)

**tcp**

| Size | ours→ours | libzmq→libzmq | ours→libzmq | libzmq→ours | ours/libzmq |
|---|---|---|---|---|---|
| 8 B | 10.29M [9.46M–11.06M] (659) | 6.44M [6.05M–6.72M] (412) | 6.77M | 6.47M | 1.60 |
| 256 B | 2.06M [2.00M–2.15M] (4221) | 1.61M [1.56M–1.62M] (3297) | 2.14M | 1.57M | 1.28 |
| 4 KiB | 498.8k [471.4k–527.4k] (16345) | 282.4k [257.8k–351.7k] (9254) | 500.3k | 243.7k | 1.77 |
| 64 KiB | 36.4k [34.3k–37.0k] (19076) | 35.9k [35.0k–36.4k] (18830) | 36.5k | 36.8k | 1.01 |

**ipc**

| Size | ours→ours | libzmq→libzmq | ours→libzmq | libzmq→ours | ours/libzmq |
|---|---|---|---|---|---|
| 8 B | 9.26M [8.77M–10.09M] (593) | 5.90M [5.50M–6.13M] (378) | 6.75M | 6.05M | 1.57 |
| 256 B | 2.08M [2.06M–2.10M] (4267) | 1.99M [1.93M–2.00M] (4074) | 2.09M | 1.93M | 1.05 |
| 4 KiB | 603.6k [586.5k–607.6k] (19779) | 445.5k [441.2k–449.1k] (14597) | 543.6k | 478.8k | 1.36 |
| 64 KiB | 60.1k [58.5k–60.5k] (31491) | 48.1k [47.3k–49.7k] (25213) | 59.2k | 44.4k | 1.25 |

**inproc**

| Size | ours→ours | libzmq→libzmq | ours/libzmq |
|---|---|---|---|
| 8 B | 11.45M [10.09M–11.89M] (733) | 5.91M [5.81M–6.24M] (378) | 1.94 |
| 256 B | 1.95M [1.88M–1.99M] (3997) | 1.92M [1.84M–1.94M] (3928) | 1.02 |
| 4 KiB | 1.34M [1.31M–1.48M] (43839) | 1.34M [1.30M–1.41M] (43878) | 1.00 |
| 64 KiB | 337.4k [332.6k–346.7k] (176918) | 346.5k [335.7k–351.7k] (181644) | 0.97 |

### Latency, one-way µs (p50/p99)

**tcp**

| Size | ours→ours | libzmq→libzmq | ours→libzmq | libzmq→ours | ours/libzmq |
|---|---|---|---|---|---|
| 8 B | 28.3 [24.2–34.3] (22.9/56.2) | 47.3 [34.0–63.5] (45.6/85.3) | 51.5 | 34.7 | 1.67 |
| 256 B | 39.9 [24.1–49.1] (36.6/64.7) | 59.6 [55.3–75.0] (71.6/85.9) | 54.2 | 58.1 | 1.50 |
| 4 KiB | 49.4 [33.9–54.6] (52.5/67.5) | 66.6 [65.3–77.1] (73.3/94.0) | 56.5 | 61.5 | 1.35 |
| 64 KiB | 97.5 [80.3–104.5] (99.2/133.0) | 126.6 [84.6–146.0] (128.4/193.3) | 111.5 | 121.7 | 1.30 |

**ipc**

| Size | ours→ours | libzmq→libzmq | ours→libzmq | libzmq→ours | ours/libzmq |
|---|---|---|---|---|---|
| 8 B | 27.2 [15.4–28.2] (26.4/37.6) | 38.4 [29.1–47.2] (41.7/61.1) | 23.4 | 25.0 | 1.41 |
| 256 B | 20.6 [14.2–27.9] (20.2/32.9) | 40.4 [36.4–47.1] (43.2/65.1) | 26.1 | 26.4 | 1.96 |
| 4 KiB | 30.7 [18.4–33.9] (31.0/40.5) | 48.9 [42.4–56.6] (57.6/72.0) | 26.9 | 28.4 | 1.59 |
| 64 KiB | 50.9 [32.3–62.8] (50.9/64.6) | 46.0 [44.7–75.1] (45.8/60.5) | 51.8 | 44.8 | 0.90 |

**inproc**

| Size | ours→ours | libzmq→libzmq | ours/libzmq |
|---|---|---|---|
| 8 B | 4.6 [4.0–5.6] (4.5/6.2) | 8.8 [6.9–11.2] (8.3/13.7) | 1.93 |
| 256 B | 4.7 [4.0–5.6] (4.5/6.5) | 10.3 [7.1–11.5] (10.3/12.8) | 2.20 |
| 4 KiB | 4.6 [4.3–5.3] (4.5/6.0) | 9.5 [7.0–11.3] (9.4/12.4) | 2.05 |
| 64 KiB | 4.6 [3.6–4.7] (4.4/6.0) | 7.5 [6.9–9.7] (7.3/11.5) | 1.64 |

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
- **tcp at 64 KiB was 0.93 on copies.** Our I/O threads spent 41% of
  their time in memmove, against libzmq's 15%: the encoder copied every
  payload into its output buffer. Frames of 8 KiB and up are now written
  from their own buffers with vectored writes, as libzmq's encoder sends
  what will not fit its batch, and memmove is down to 4%: 36.1k msg/s
  against libzmq's 36.0k over tcp (1.00), 59.2k against 47.8k over ipc
  (1.24). The decoder also zeroed its read buffer before each read,
  about 31% of the receiving I/O thread, since `Read::read` needs
  initialized memory and `Read::read_buf` is unstable. Bodies of 16 KiB
  and up are now read with `read_to_end`, which reads into a vector's
  spare capacity through the standard streams' own `read_buf`; that
  memset is gone, but throughput is unchanged, as this loopback case is
  bound elsewhere: by the kernel and by memory, not by either library.
  The same pipeline with no ZeroMQ at all, a producer thread writing a
  fresh 64 KiB buffer per message to a writer thread, over a socket to a
  reader thread reading each into a fresh vector for a consumer thread,
  runs at 35k msg/s over tcp and 57k over a Unix socket. Reading each
  body in one `read` rather than `read_to_end`'s growing reads, three
  reads a message rather than six, changed nothing.
- **Noise:** latency at small sizes spreads widely for both libraries.
  inproc 64 KiB throughput did too, until the bench turned glibc's heap
  trimming off. The machine ran the `schedutil` governor; rerun a
  subset before acting on a difference under about 10%.

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

### Results (`f3e7a19`)

On the Xeon above:

| Case | ours msg/s | libzmq msg/s | ours/libzmq |
|---|---|---|---|
| multipart/2 | 2.80M | 2.01M | 1.39 |
| multipart/8 | 563.5k | 539.5k | 1.04 |
| fanout/1 | 4.14M | 3.63M | 1.14 |
| fanout/4 | 1.79M | 1.36M | 1.31 |
| fanout/16 | 563.7k | 353.9k | 1.59 |
| fanin/16 | 3.97M | 2.54M | 1.56 |
| fanin/64 | 3.36M | 2.53M | 1.33 |
| router/1 | 2.21M | 1.56M | 1.42 |
| router/16 | 3.97M | 2.56M | 1.55 |
| curve/256 | 685.0k | 656.1k | 1.04 |
| curve/64k | 13.2k | 11.6k | 1.13 |

The bullets below give each case's numbers when its fix landed.

- **Many connections on one I/O thread** were 0.61 (`fanin/16`) and 0.65
  (`router/16`) until an engine did one round per drive (`d13552c`). It
  used to drain one connection's pipe before the next, freeing that
  socket thread's messages while it allocated more under the same glibc
  arena lock: 60% of the sending I/O thread went to `free`, 83% of that
  to the lock. libzmq's engines return to the poller after each batch.
- **CURVE was 0.41 at 256 B and 0.39 at 64 KiB on the cipher.**
  RustCrypto's `salsa20` has no SIMD code: at 64 KiB it took 61% of our
  cycles, about 3.7 times libsodium's, and Poly1305's AVX2 helpers are
  not inlined without `-C target-cpu=native`. Messages now go through
  `dryoc`'s libsodium-compatible box, whose Salsa20 runs on AVX2 or
  AVX-512 when the CPU has them, sealed and opened in place: one copy
  each way where sending took three allocations and four copies. With
  that, 675.3k msg/s against libzmq's 646.5k at 256 B (1.04) and 13.8k
  against 11.6k at 64 KiB (1.18). The handshake uses dryoc too.
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

## Allocations

Heap allocations per message, or per round trip for latency, counted
in the benches' children with an `LD_PRELOAD` shim over glibc's `malloc`
family. Each case ran at two counts and the difference was divided by
the extra messages, so setup cancels out. Every count includes the
sender's payload, and both libraries free it on another thread.

| Case | Before | After | libzmq |
|---|---|---|---|
| thr inproc, any size | 1 | 1 | 1 |
| thr tcp 8 B | 1 | 0 | 0 |
| thr tcp 256 B | 1.07 | 1.07 | 1.04 |
| thr tcp 4 KiB | 2.01 | 2.02 | 1.76 |
| thr tcp 64 KiB | 3 | 2 | 2 |
| lat inproc, any size | 3 | 0 | 0 |
| lat tcp 256 B | 7.06 (16.9 KB) | 0.21 (658 B) | 0 (16 B) |
| lat tcp 64 KiB | 9.06 | 4.06 | 2 |
| multipart/2 | 3.04 | 2.03 | 2.03 |
| curve/64k | 5 | 3 | 4 |

- **A receive in request-reply allocated 8 KiB.** The decoder reserved
  a full 8 KiB whenever less was spare, and with the message just
  decoded still alive its buffer was shared, so `reserve` allocated
  another and copied the zeroed tail into it, twice a round trip.
  libzmq takes its buffer at the next read event, by which time the
  message is gone. Reads now go into what is left until under 1 KiB is.
- **REQ and REP collected frames into vectors** to split a request
  from its envelope and a reply from its delimiter: three a round trip.
  A delimiter-only envelope, a request straight from a REQ, now
  allocates nothing.
- **Vectored writes allocated their slice list,** 48 octets for each
  message of 8 KiB or more; it is now on the stack. CURVE cloned the
  boxed frame and sliced the opened one, each allocating a `Bytes`
  header; both are now taken and advanced instead.
- **The 8 B row was the bench's,** which built each payload as a vector
  that the frame then copied inline. It now copies into the frame, as
  libzmq's `zmq_msg_init_size` stores a small message inline.
- **What remains is `Bytes` keeping its count apart from the data.**
  A buffer shared between frames, at 4 KiB, or a payload cloned for
  several subscribers, as in `fanout/4` (2.09 against 1.04), allocates
  a header where libzmq's content carries its own. Over tcp at 64 KiB,
  the two above libzmq's are the bench's REP cloning the body.
- **Throughput and latency did not move** beyond run-to-run spread: a
  malloc costs tens of nanoseconds against microseconds a message.

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
