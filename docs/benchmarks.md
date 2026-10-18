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
commit `b2abe9f` plus the benchmark. 2026-10-17.

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
`schedutil` governor, libzmq 4.3.5 from nixpkgs, commit `17b2266`.
2026-10-18.

Our latency is lower everywhere except ipc 64 KiB (0.84), where both
libraries' runs spread too widely to compare. Our throughput is level
(0.94–0.95) or ahead except at 256 B over ipc and inproc (0.86).

### Throughput, msg/s (Mb/s)

**tcp**

| Size | ours→ours | libzmq→libzmq | ours→libzmq | libzmq→ours | ours/libzmq |
|---|---|---|---|---|---|
| 8 B | 6.65M [6.39M–6.79M] (426) | 6.18M [5.89M–6.23M] (395) | 6.24M | 6.89M | 1.08 |
| 256 B | 1.75M [1.71M–1.80M] (3586) | 1.54M [1.51M–1.66M] (3149) | 1.78M | 1.56M | 1.14 |
| 4 KiB | 530.4k [497.5k–562.1k] (17381) | 243.9k [234.9k–249.3k] (7994) | 499.9k | 235.1k | 2.17 |
| 64 KiB | 34.8k [33.4k–36.0k] (18268) | 36.9k [35.5k–37.3k] (19356) | 34.9k | 36.3k | 0.94 |

**ipc**

| Size | ours→ours | libzmq→libzmq | ours→libzmq | libzmq→ours | ours/libzmq |
|---|---|---|---|---|---|
| 8 B | 6.31M [6.01M–6.53M] (404) | 6.64M [5.73M–6.93M] (425) | 6.22M | 4.97M | 0.95 |
| 256 B | 1.72M [1.69M–1.74M] (3530) | 2.01M [1.98M–2.04M] (4122) | 1.75M | 1.98M | 0.86 |
| 4 KiB | 610.5k [607.8k–622.5k] (20006) | 400.9k [396.4k–404.4k] (13137) | 529.9k | 422.8k | 1.52 |
| 64 KiB | 52.8k [52.3k–54.4k] (27671) | 47.5k [47.5k–48.6k] (24893) | 52.9k | 51.9k | 1.11 |

**inproc**

| Size | ours→ours | libzmq→libzmq | ours/libzmq |
|---|---|---|---|
| 8 B | 7.41M [6.89M–8.34M] (474) | 6.23M [6.14M–6.47M] (398) | 1.19 |
| 256 B | 1.66M [1.63M–1.69M] (3403) | 1.93M [1.90M–2.03M] (3961) | 0.86 |
| 4 KiB | 1.38M [1.36M–1.41M] (45185) | 1.28M [1.27M–1.39M] (41857) | 1.08 |
| 64 KiB | 166.5k [143.4k–177.1k] (87320) | 163.7k [139.0k–261.2k] (85829) | 1.02 |

### Latency, one-way µs (p50/p99)

**tcp**

| Size | ours→ours | libzmq→libzmq | ours→libzmq | libzmq→ours | ours/libzmq |
|---|---|---|---|---|---|
| 8 B | 34.3 [23.7–39.6] (27.4/62.4) | 67.5 [46.2–69.1] (71.9/92.3) | 60.3 | 60.7 | 1.97 |
| 256 B | 54.5 [31.4–54.9] (55.1/72.2) | 66.7 [64.1–73.4] (71.4/92.3) | 55.6 | 60.9 | 1.22 |
| 4 KiB | 53.3 [45.8–61.5] (56.4/77.4) | 75.0 [54.8–76.7] (75.6/100.4) | 66.6 | 65.4 | 1.41 |
| 64 KiB | 99.8 [82.3–111.6] (110.5/169.4) | 146.0 [133.5–153.5] (155.0/215.0) | 125.6 | 136.1 | 1.46 |

**ipc**

| Size | ours→ours | libzmq→libzmq | ours→libzmq | libzmq→ours | ours/libzmq |
|---|---|---|---|---|---|
| 8 B | 32.7 [22.9–35.4] (31.7/51.0) | 47.2 [22.6–50.3] (51.8/66.7) | 34.9 | 37.7 | 1.45 |
| 256 B | 22.1 [19.1–34.4] (22.5/28.7) | 47.6 [39.0–48.3] (52.2/69.9) | 37.4 | 37.8 | 2.15 |
| 4 KiB | 36.5 [24.8–41.7] (36.0/54.4) | 54.7 [40.7–56.5] (56.7/74.0) | 43.5 | 40.4 | 1.50 |
| 64 KiB | 66.8 [43.8–79.1] (68.7/85.9) | 56.3 [42.5–64.7] (54.5/78.7) | 44.1 | 51.7 | 0.84 |

**inproc**

| Size | ours→ours | libzmq→libzmq | ours/libzmq |
|---|---|---|---|
| 8 B | 4.5 [4.3–5.5] (4.3/6.4) | 9.3 [7.5–10.6] (8.9/11.9) | 2.06 |
| 256 B | 4.7 [4.2–6.4] (4.5/6.6) | 9.6 [6.6–12.3] (9.7/12.0) | 2.03 |
| 4 KiB | 4.9 [3.8–6.3] (4.6/7.5) | 10.5 [7.3–11.8] (9.8/18.3) | 2.13 |
| 64 KiB | 4.7 [3.7–5.1] (4.6/6.5) | 7.3 [6.8–10.2] (7.0/14.3) | 1.54 |

### Reading them

- **Blocking calls used to allocate a waker each,** freed on the peer's
  thread by its wake: a cross-thread free per message, which glibc's
  allocator handles badly and macOS's does not. Reusing one waker per
  thread (`0fd7eff`) raised 8 B throughput from 0.58 to 0.92 over inproc,
  0.80 to 0.96 over ipc and 0.95 to 1.07 over tcp, and ended a 2× spread
  between runs at inproc 8 B.
- **Writers used to wake their reader on every send,** an atomic
  read-modify-write on a line the reader also uses, even while the reader
  was busy. Like libzmq's ypipe, a writer now wakes only a reader that
  flagged it is about to wait (`17b2266`). inproc 8 B went from 0.92 to
  1.19. In the pipe benchmark (`zmq-pipe`'s `throughput`), a message
  takes 49 ns instead of 137 ns with no high-water mark and 47 ns
  instead of 217 ns at 1000, but 635 ns instead of 514 ns at 16, where
  the writer is nearly always waiting and the reader nearly always
  empty.
- **256 B over ipc and inproc is our remaining gap, and only under
  glibc's allocator.** Preloading mimalloc gives 1.01 over inproc and
  1.11 over ipc; jemalloc 0.93 and 1.56 (measured at `0fd7eff`). Both
  libraries allocate once per message, on the sender, freed on the
  receiver; switching `calloc` for `malloc` and raising
  `glibc.malloc.tcache_max` to 4096 changed nothing.
- **Per-call housekeeping is not the bottleneck.** Every send and receive
  polls the socket's listeners for new peers, and PUSH checks every peer
  for closure, where libzmq checks its mailbox only about once a
  millisecond. Skipping both gained about 2% at 256 B and nothing at 8 B.
  Skipping only the receiver's made inproc 8 B 40% *slower*: the receiver
  found the pipe empty more often, and the cost moved to the pipe's
  cache lines passing between the two cores on each message.
- **Small-message ipc throughput is behind on the receiving side:** our
  sender keeps up with libzmq's receiver (ours→libzmq matches
  ours→ours), while our receiver holds libzmq's sender back
  (libzmq→ours 4.97M against 6.64M for libzmq→libzmq; 5.25M against
  6.38M at `0fd7eff`). At `b824630` it was the sending side.
- **The allocator matters to both libraries from 256 B up.** Under
  mimalloc or jemalloc (`LD_PRELOAD`) both run up to three times as
  fast over inproc; the 8 B case barely moves for libzmq, which stores
  small messages inline.
- **tcp at 256 B is 1.5M–1.8M msg/s for both libraries,** against
  3.4M–5.9M on the M1, so this machine bounds that case.
- **Noise:** inproc 64 KiB throughput spread widely for libzmq
  (139.0k–261.2k), as it has in every run, and ipc 64 KiB latency for
  both (ours 43.8–79.1 µs, libzmq 42.5–64.7 µs). The machine was idle
  before the run but ran the `schedutil` governor; rerun with the
  `performance` governor before acting on a difference there.

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
