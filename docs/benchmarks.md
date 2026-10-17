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
`schedutil` governor, libzmq 4.3.5 from nixpkgs, commit `b824630`.
2026-10-17.

Our latency is lower everywhere, but our throughput is not: it trails
libzmq for small messages over ipc and inproc, and at 64 KiB over inproc.

### Throughput, msg/s (Mb/s)

**tcp**

| Size | ours→ours | libzmq→libzmq | ours→libzmq | libzmq→ours | ours/libzmq |
|---|---|---|---|---|---|
| 8 B | 5.51M [5.32M–5.58M] (353) | 5.84M [5.42M–5.96M] (373) | 5.56M | 6.14M | 0.95 |
| 256 B | 1.56M [1.52M–1.61M] (3197) | 1.59M [1.58M–1.61M] (3255) | 1.55M | 1.62M | 0.98 |
| 4 KiB | 576.9k [544.0k–580.4k] (18903) | 255.9k [240.9k–258.1k] (8384) | 507.8k | 234.8k | 2.25 |
| 64 KiB | 34.7k [33.6k–35.8k] (18169) | 36.3k [35.6k–36.7k] (19028) | 34.8k | 36.0k | 0.95 |

**ipc**

| Size | ours→ours | libzmq→libzmq | ours→libzmq | libzmq→ours | ours/libzmq |
|---|---|---|---|---|---|
| 8 B | 5.14M [4.81M–5.20M] (329) | 6.39M [6.09M–6.58M] (409) | 5.19M | 5.92M | 0.80 |
| 256 B | 1.48M [1.46M–1.53M] (3032) | 2.02M [2.00M–2.09M] (4144) | 1.51M | 2.03M | 0.73 |
| 4 KiB | 606.5k [590.8k–610.1k] (19873) | 404.2k [396.4k–408.8k] (13246) | 563.1k | 428.3k | 1.50 |
| 64 KiB | 54.0k [52.7k–54.3k] (28312) | 47.8k [46.7k–49.1k] (25073) | 52.8k | 52.6k | 1.13 |

**inproc**

| Size | ours→ours | libzmq→libzmq | ours/libzmq |
|---|---|---|---|
| 8 B | 3.31M [2.35M–4.74M] (212) | 6.26M [6.15M–6.51M] (400) | 0.53 |
| 256 B | 1.54M [1.45M–1.59M] (3158) | 1.94M [1.91M–1.96M] (3982) | 0.79 |
| 4 KiB | 1.32M [1.30M–1.40M] (43164) | 1.35M [1.30M–1.39M] (44348) | 0.97 |
| 64 KiB | 211.0k [200.2k–226.1k] (110605) | 332.9k [113.4k–344.1k] (174511) | 0.63 |

### Latency, one-way µs (p50/p99)

**tcp**

| Size | ours→ours | libzmq→libzmq | ours→libzmq | libzmq→ours | ours/libzmq |
|---|---|---|---|---|---|
| 8 B | 37.1 [20.9–48.7] (36.1/65.7) | 68.7 [60.6–69.1] (70.8/89.2) | 56.5 | 59.4 | 1.85 |
| 256 B | 50.2 [22.2–56.3] (52.8/70.1) | 66.6 [60.3–71.5] (69.1/89.5) | 55.8 | 58.9 | 1.33 |
| 4 KiB | 55.0 [44.8–56.0] (58.3/75.0) | 74.2 [62.9–75.9] (78.4/98.1) | 62.9 | 67.9 | 1.35 |
| 64 KiB | 112.9 [104.8–113.9] (110.2/180.8) | 120.2 [84.1–153.9] (144.1/161.6) | 127.7 | 125.7 | 1.06 |

**ipc**

| Size | ours→ours | libzmq→libzmq | ours→libzmq | libzmq→ours | ours/libzmq |
|---|---|---|---|---|---|
| 8 B | 21.2 [20.0–27.8] (19.4/35.8) | 43.8 [38.0–48.1] (51.5/65.3) | 32.9 | 37.3 | 2.07 |
| 256 B | 26.5 [20.7–28.2] (22.6/46.1) | 44.1 [37.9–50.4] (50.2/67.7) | 38.0 | 43.7 | 1.67 |
| 4 KiB | 37.4 [32.5–40.2] (38.2/53.7) | 53.4 [31.2–57.9] (55.7/72.0) | 43.2 | 43.1 | 1.43 |
| 64 KiB | 55.3 [29.8–74.6] (51.6/85.7) | 55.4 [52.3–73.7] (51.3/85.9) | 58.0 | 57.1 | 1.00 |

**inproc**

| Size | ours→ours | libzmq→libzmq | ours/libzmq |
|---|---|---|---|
| 8 B | 5.5 [3.2–6.7] (5.7/7.7) | 9.7 [6.2–12.6] (9.5/12.9) | 1.76 |
| 256 B | 4.8 [4.1–6.3] (4.6/6.7) | 11.2 [6.5–13.5] (10.3/15.9) | 2.34 |
| 4 KiB | 5.2 [4.7–5.6] (5.0/7.4) | 10.7 [7.0–12.3] (9.9/15.7) | 2.05 |
| 64 KiB | 4.9 [4.3–5.1] (4.8/6.8) | 9.8 [8.2–10.2] (9.6/14.3) | 2.01 |

### Reading them

- **inproc throughput is the largest gap, and no transport is
  involved,** so it is in our sockets or pipes: 0.53 at 8 B, 0.79 at
  256 B, 0.63 at 64 KiB. On the M1 the same cases read 1.75, 1.68 and
  1.54. Suspect how parking and waking threads behaves on Linux; the
  `zmq-pipe` benchmark here costs 407 ns a message at HWM 16 against
  166 ns at HWM 1000. Not yet investigated.
- **Small-message ipc throughput is behind on the receiving side:**
  libzmq→ours matches libzmq→libzmq, ours→libzmq does not.
- **The M1's ipc gap at 64 KiB is gone here** (1.13).
- **tcp at 256 B is about 1.5M msg/s for both libraries,** against
  3.4M–5.9M on the M1, so this machine bounds that case, not either
  library.
- **Noise:** spread is tighter than on macOS except two cells: our inproc
  throughput at 8 B (2.35M–4.74M) and libzmq's at 64 KiB
  (113.4k–344.1k). The machine was not otherwise idle (load average
  about 1.5) and ran the `schedutil` governor; rerun those with the
  `performance` governor before acting on them.
