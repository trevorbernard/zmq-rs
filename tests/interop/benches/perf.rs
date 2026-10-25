//! Throughput and latency next to libzmq's, as libzmq's `perf/` tools
//! measure them: PUSH to PULL and REQ to REP, over tcp, ipc and inproc.
//!
//! Each case runs with our sockets and libzmq's on both ends, and, over
//! tcp and ipc, with one library on each end, which shows whether a gap is
//! on the sending or the receiving side. libzmq is driven through
//! `zmq_interop::libzmq` at the cost of its own tools: messages are reused
//! and never copied out. One difference: each throughput sender, of either
//! library, allocates and writes every payload, where libzmq's `remote_thr`
//! leaves it unwritten. Zeroing a 64 KiB payload costs about as much as
//! sending it over inproc, so writing on one side only would skew the
//! comparison.
//!
//! As in `zmq-pipe`'s benchmark, every measurement runs in a fresh child
//! process, cases are interleaved across rounds, and the report gives the
//! median with min and max.
//!
//! `cargo bench -p zmq-interop --bench perf [--quick] [filter...]`. A
//! filter is a kind (`thr`, `lat`), transport (`tcp`, `ipc`, `inproc`),
//! size (`8`, `256`, `4k`, `64k`) or pair (`ours`, `libzmq`,
//! `ours-libzmq`, `libzmq-ours`); a case runs if it matches some filter of
//! every dimension filtered on. `--quick` runs every case once, briefly, as
//! a smoke test.

use std::env;
use std::hint;
use std::io::Read;
use std::path::PathBuf;
use std::process::{self, Child, Command, Stdio};
use std::thread;
use std::time::{Duration, Instant};

use zmq::{Context, Endpoint, Frame, Message, Pull, Push, Rep, Req};
use zmq_interop::libzmq::{self, Kind, Msg};

const ROUNDS: usize = 5;
const SIZES: [(&str, usize); 4] = [("8", 8), ("256", 256), ("4k", 4096), ("64k", 65536)];

#[derive(Debug, Clone, Copy, PartialEq, Eq)]
enum Bench {
    Throughput,
    Latency,
}

#[derive(Debug, Clone, Copy, PartialEq, Eq)]
enum Transport {
    Tcp,
    Ipc,
    Inproc,
}

/// Which library is on each end: sender then receiver for throughput,
/// requester then replier for latency.
#[derive(Debug, Clone, Copy, PartialEq, Eq)]
enum Pair {
    Ours,
    Libzmq,
    OursToLibzmq,
    LibzmqToOurs,
}

const BENCHES: [(&str, Bench); 2] = [("thr", Bench::Throughput), ("lat", Bench::Latency)];
const TRANSPORTS: [(&str, Transport); 3] = [
    ("tcp", Transport::Tcp),
    ("ipc", Transport::Ipc),
    ("inproc", Transport::Inproc),
];
const PAIRS: [(&str, Pair); 4] = [
    ("ours", Pair::Ours),
    ("libzmq", Pair::Libzmq),
    ("ours-libzmq", Pair::OursToLibzmq),
    ("libzmq-ours", Pair::LibzmqToOurs),
];

impl Pair {
    fn ours_first(self) -> bool {
        matches!(self, Self::Ours | Self::OursToLibzmq)
    }

    fn ours_second(self) -> bool {
        matches!(self, Self::Ours | Self::LibzmqToOurs)
    }

    fn header(self) -> &'static str {
        match self {
            Self::Ours => "ours→ours",
            Self::Libzmq => "libzmq→libzmq",
            Self::OursToLibzmq => "ours→libzmq",
            Self::LibzmqToOurs => "libzmq→ours",
        }
    }
}

#[derive(Debug, Clone, Copy, PartialEq, Eq)]
struct Case {
    bench: Bench,
    transport: Transport,
    size: usize,
    pair: Pair,
}

impl Case {
    fn all() -> impl Iterator<Item = Self> {
        BENCHES.iter().flat_map(|&(_, bench)| {
            TRANSPORTS.iter().flat_map(move |&(_, transport)| {
                SIZES.iter().flat_map(move |&(_, size)| {
                    PAIRS
                        .iter()
                        // inproc can't join two libraries.
                        .filter(move |&&(_, pair)| {
                            transport != Transport::Inproc
                                || matches!(pair, Pair::Ours | Pair::Libzmq)
                        })
                        .map(move |&(_, pair)| Self {
                            bench,
                            transport,
                            size,
                            pair,
                        })
                })
            })
        })
    }

    /// The messages or round trips to time: libzmq's tools leave this to
    /// the caller, so it is sized to take about a second.
    fn count(self, quick: bool) -> usize {
        match (quick, self.bench, self.size > 4096) {
            (true, _, _) => 1_000,
            (false, Bench::Throughput, false) => 1_000_000,
            (false, Bench::Throughput, true) => 100_000,
            (false, Bench::Latency, false) => 20_000,
            (false, Bench::Latency, true) => 5_000,
        }
    }

    fn args(self, count: usize) -> Vec<String> {
        vec![
            name(&BENCHES, self.bench).to_owned(),
            name(&TRANSPORTS, self.transport).to_owned(),
            self.size.to_string(),
            name(&PAIRS, self.pair).to_owned(),
            count.to_string(),
        ]
    }

    fn parse(args: &[String]) -> (Self, usize) {
        let [bench, transport, size, pair, count] = args else {
            panic!("child arguments: <kind> <transport> <size> <pair> <count>");
        };
        let case = Self {
            bench: value(&BENCHES, bench),
            transport: value(&TRANSPORTS, transport),
            size: size.parse().expect("size"),
            pair: value(&PAIRS, pair),
        };
        (case, count.parse().expect("count"))
    }

    /// Whether every dimension that some filter names matches one of them.
    fn selected(self, filters: &[String]) -> bool {
        let size = SIZES
            .iter()
            .find(|&&(_, s)| s == self.size)
            .map(|&(n, _)| n);
        let dimensions: [(&[&str], &str); 4] = [
            (&names(&BENCHES), name(&BENCHES, self.bench)),
            (&names(&TRANSPORTS), name(&TRANSPORTS, self.transport)),
            (&SIZES.map(|(n, _)| n), size.unwrap_or_default()),
            (&names(&PAIRS), name(&PAIRS, self.pair)),
        ];
        dimensions.iter().all(|(all, mine)| {
            let named: Vec<&String> = filters
                .iter()
                .filter(|f| all.contains(&f.as_str()))
                .collect();
            named.is_empty() || named.iter().any(|f| f.as_str() == *mine)
        })
    }
}

fn name<T: PartialEq + Copy, const N: usize>(table: &[(&'static str, T); N], v: T) -> &'static str {
    table
        .iter()
        .find(|(_, t)| *t == v)
        .map(|(n, _)| *n)
        .expect("listed")
}

fn value<T: Copy, const N: usize>(table: &[(&'static str, T); N], n: &str) -> T {
    table
        .iter()
        .find(|(name, _)| *name == n)
        .map(|(_, v)| *v)
        .unwrap_or_else(|| panic!("unknown {n}"))
}

fn names<T, const N: usize>(table: &[(&'static str, T); N]) -> [&'static str; N] {
    std::array::from_fn(|i| table[i].0)
}

fn main() {
    let args: Vec<String> = env::args().skip(1).collect();
    if args.first().is_some_and(|a| a == "--child") {
        let (case, count) = Case::parse(&args[1..]);
        let sample = match case.bench {
            Bench::Throughput => Sample::Throughput(throughput(case, count)),
            Bench::Latency => Sample::Latency(latency(case, count)),
        };
        println!("{}", sample.encode());
        return;
    }
    let quick = args.iter().any(|a| a == "--quick");
    // `cargo bench` passes `--bench`; anything else not a flag filters.
    let filters: Vec<String> = args.into_iter().filter(|a| !a.starts_with("--")).collect();
    let known: Vec<&str> = names(&BENCHES)
        .into_iter()
        .chain(names(&TRANSPORTS))
        .chain(SIZES.map(|(n, _)| n))
        .chain(names(&PAIRS))
        .collect();
    if let Some(unknown) = filters.iter().find(|f| !known.contains(&f.as_str())) {
        eprintln!(
            "unknown filter {unknown}; expected one of {}",
            known.join(", ")
        );
        process::exit(2);
    }
    let cases: Vec<Case> = Case::all().filter(|c| c.selected(&filters)).collect();
    let (rounds, warm_up, limit) = if quick {
        (1, false, Duration::from_secs(10))
    } else {
        (ROUNDS, true, Duration::from_secs(120))
    };
    let exe = env::current_exe().expect("benchmark executable");
    let run = |case: Case| -> Sample {
        let count = case.count(quick);
        match spawn(&exe, &case.args(count), limit) {
            Ok(line) => Sample::decode(case.bench, &line),
            Err(failure) => {
                eprintln!("{case:?} failed: {failure}");
                process::exit(1);
            }
        }
    };
    if warm_up {
        for &case in &cases {
            run(case);
        }
    }
    let mut samples: Vec<Vec<Sample>> = vec![Vec::with_capacity(rounds); cases.len()];
    for round in 0..rounds {
        eprintln!("round {} of {rounds}", round + 1);
        for (case, samples) in cases.iter().zip(&mut samples) {
            samples.push(run(*case));
        }
    }
    report(&cases, samples, rounds);
}

/// Runs a child for one measurement and returns the line it printed.
fn spawn(exe: &PathBuf, args: &[String], limit: Duration) -> Result<String, String> {
    let mut command = Command::new(exe);
    // glibc returns the top of its heap to the kernel whenever the
    // receiver's frees leave enough of it unused, and the sender faults it
    // back in. How often depends on thread timing: at inproc 64 KiB, either
    // library ran anywhere from 110k to 330k msg/s, following its page
    // faults. Never trimming measures the libraries rather than that.
    if env::var_os("GLIBC_TUNABLES").is_none() {
        command.env("GLIBC_TUNABLES", "glibc.malloc.trim_threshold=1073741824");
    }
    let mut child = command
        .arg("--child")
        .args(args)
        .stdout(Stdio::piped())
        .stderr(Stdio::piped())
        .spawn()
        .map_err(|err| format!("spawn: {err}"))?;
    let started = Instant::now();
    let status = loop {
        if let Some(status) = child.try_wait().map_err(|err| err.to_string())? {
            break status;
        }
        if started.elapsed() > limit {
            let _ = child.kill();
            let _ = child.wait();
            return Err(format!("killed after {limit:?}\n{}", output(&mut child).1));
        }
        thread::sleep(Duration::from_millis(10));
    };
    let (stdout, stderr) = output(&mut child);
    if status.success() {
        Ok(stdout.trim().to_owned())
    } else {
        Err(format!("{status}\n{stderr}"))
    }
}

fn output(child: &mut Child) -> (String, String) {
    let mut stdout = String::new();
    let mut stderr = String::new();
    if let Some(mut pipe) = child.stdout.take() {
        let _ = pipe.read_to_string(&mut stdout);
    }
    if let Some(mut pipe) = child.stderr.take() {
        let _ = pipe.read_to_string(&mut stderr);
    }
    (stdout, stderr)
}

#[derive(Debug, Clone, Copy)]
enum Sample {
    /// Messages per second.
    Throughput(f64),
    Latency(Latency),
}

/// One-way latencies in microseconds: the mean as libzmq's `remote_lat`
/// reports it, and the median and 99th percentile of each round trip
/// halved.
#[derive(Debug, Clone, Copy)]
struct Latency {
    mean: f64,
    p50: f64,
    p99: f64,
}

impl Sample {
    fn encode(self) -> String {
        match self {
            Self::Throughput(rate) => format!("{rate}"),
            Self::Latency(Latency { mean, p50, p99 }) => format!("{mean} {p50} {p99}"),
        }
    }

    fn decode(bench: Bench, line: &str) -> Self {
        let numbers: Vec<f64> = line
            .split_whitespace()
            .map(|n| n.parse().expect("child prints numbers"))
            .collect();
        match (bench, numbers.as_slice()) {
            (Bench::Throughput, &[rate]) => Self::Throughput(rate),
            (Bench::Latency, &[mean, p50, p99]) => Self::Latency(Latency { mean, p50, p99 }),
            _ => panic!("unexpected child output {line:?}"),
        }
    }

    /// The figure runs are ranked by.
    fn key(self) -> f64 {
        match self {
            Self::Throughput(rate) => rate,
            Self::Latency(latency) => latency.mean,
        }
    }
}

fn report(cases: &[Case], samples: Vec<Vec<Sample>>, rounds: usize) {
    let mut results: Vec<(Case, Vec<Sample>)> = cases.iter().copied().zip(samples).collect();
    for (_, samples) in &mut results {
        samples.sort_by(|a, b| a.key().total_cmp(&b.key()));
    }
    let find = |case: Case| results.iter().find(|(c, _)| *c == case).map(|(_, s)| s);
    println!("{rounds} process(es) per case; median [min–max]");
    for &(_, bench) in &BENCHES {
        for &(transport_name, transport) in &TRANSPORTS {
            let pairs: Vec<Pair> = PAIRS
                .iter()
                .map(|&(_, p)| p)
                .filter(|&pair| {
                    SIZES.iter().any(|&(_, size)| {
                        find(Case {
                            bench,
                            transport,
                            size,
                            pair,
                        })
                        .is_some()
                    })
                })
                .collect();
            if pairs.is_empty() {
                continue;
            }
            let title = match bench {
                Bench::Throughput => "throughput, msg/s (Mb/s)",
                Bench::Latency => "latency, one-way µs (p50/p99)",
            };
            println!("\n{transport_name} {title}");
            print!("{:<6}", "size");
            for pair in &pairs {
                print!("{:<36}", pair.header());
            }
            println!("ours/libzmq");
            for &(size_name, size) in &SIZES {
                let cell = |pair| {
                    find(Case {
                        bench,
                        transport,
                        size,
                        pair,
                    })
                };
                if pairs.iter().all(|&pair| cell(pair).is_none()) {
                    continue;
                }
                print!("{size_name:<6}");
                for &pair in &pairs {
                    print!(
                        "{:<36}",
                        cell(pair).map_or_else(String::new, |s| format_cell(s, size))
                    );
                }
                let ratio = match (cell(Pair::Ours), cell(Pair::Libzmq)) {
                    (Some(ours), Some(theirs)) => {
                        let (ours, theirs) = (median(ours).key(), median(theirs).key());
                        let ratio = match bench {
                            Bench::Throughput => ours / theirs,
                            Bench::Latency => theirs / ours,
                        };
                        format!("{ratio:.2}")
                    }
                    _ => "–".to_owned(),
                };
                println!("{ratio}");
            }
        }
    }
}

fn median(sorted: &[Sample]) -> Sample {
    sorted[sorted.len() / 2]
}

fn format_cell(sorted: &[Sample], size: usize) -> String {
    let (min, max) = (sorted[0].key(), sorted[sorted.len() - 1].key());
    match median(sorted) {
        Sample::Throughput(rate) => {
            let megabits = rate * size as f64 * 8.0 / 1e6;
            format!(
                "{} [{}–{}] ({megabits:.0})",
                rate_text(rate),
                rate_text(min),
                rate_text(max)
            )
        }
        Sample::Latency(Latency { mean, p50, p99 }) => {
            format!("{mean:.1} [{min:.1}–{max:.1}] ({p50:.1}/{p99:.1})")
        }
    }
}

fn rate_text(rate: f64) -> String {
    if rate >= 1e6 {
        format!("{:.2}M", rate / 1e6)
    } else {
        format!("{:.1}k", rate / 1e3)
    }
}

/// Where the receiving or replying side binds.
fn endpoint(transport: Transport) -> Endpoint {
    match transport {
        Transport::Tcp => unreachable!("tcp binds to a port the system picks"),
        Transport::Ipc => {
            Endpoint::Ipc(env::temp_dir().join(format!("zmq-perf-{}.sock", process::id())))
        }
        Transport::Inproc => Endpoint::Inproc("perf".to_owned()),
    }
}

/// One end of a benchmark, of either library.
#[allow(
    clippy::large_enum_variant,
    reason = "made once per run; a box would add an indirection to the timed loop"
)]
enum End<T: zmq::SocketType> {
    Ours(zmq::Socket<T>),
    Libzmq(libzmq::Socket),
}

/// The two contexts a case runs in: one per side over tcp and ipc, as
/// libzmq's tools run in two processes, and one shared over inproc.
struct Contexts {
    ours: (Context, Context),
    libzmq: (libzmq::Context, libzmq::Context),
}

impl Contexts {
    fn new(transport: Transport) -> Self {
        let ours = Context::new().expect("context");
        let libzmq = libzmq::Context::new();
        if transport == Transport::Inproc {
            Self {
                ours: (ours.clone(), ours),
                libzmq: (libzmq.clone(), libzmq),
            }
        } else {
            Self {
                ours: (ours, Context::new().expect("context")),
                libzmq: (libzmq, libzmq::Context::new()),
            }
        }
    }
}

/// Makes the binding end of type `T` (libzmq `kind`) and returns where the
/// other end connects.
fn bind<T: zmq::SocketType>(
    ours: bool,
    context: &Context,
    libzmq_context: &libzmq::Context,
    kind: Kind,
    transport: Transport,
) -> (End<T>, Endpoint) {
    if ours {
        let mut socket = context.socket::<T>();
        let target = match transport {
            Transport::Tcp => "tcp://127.0.0.1:*".to_owned(),
            _ => endpoint(transport).to_string(),
        };
        let bound = socket.bind(&target).expect("bind");
        (End::Ours(socket), bound)
    } else {
        let socket = libzmq::Socket::in_context(libzmq_context, kind);
        let bound = match transport {
            Transport::Tcp => Endpoint::Tcp(socket.bind_loopback()),
            _ => {
                let endpoint = endpoint(transport);
                socket.bind(&endpoint.to_string());
                endpoint
            }
        };
        (End::Libzmq(socket), bound)
    }
}

fn connect<T: zmq::SocketType>(
    ours: bool,
    context: &Context,
    libzmq_context: &libzmq::Context,
    kind: Kind,
    endpoint: &Endpoint,
) -> End<T> {
    if ours {
        let mut socket = context.socket::<T>();
        socket.connect(&endpoint.to_string()).expect("connect");
        End::Ours(socket)
    } else {
        let socket = libzmq::Socket::in_context(libzmq_context, kind);
        socket.connect(endpoint.clone());
        End::Libzmq(socket)
    }
}

/// Messages per second through PUSH to PULL, timed at the receiver from
/// the first message to the last, as libzmq's `local_thr`.
fn throughput(case: Case, count: usize) -> f64 {
    let Case {
        transport,
        size,
        pair,
        ..
    } = case;
    let contexts = Contexts::new(transport);
    let (receiver, endpoint) = bind::<Pull>(
        pair.ours_second(),
        &contexts.ours.1,
        &contexts.libzmq.1,
        Kind::Pull,
        transport,
    );
    let sender = connect::<Push>(
        pair.ours_first(),
        &contexts.ours.0,
        &contexts.libzmq.0,
        Kind::Push,
        &endpoint,
    );
    // The sender is handed back rather than dropped: libzmq's linger is
    // off in the wrapper, so dropping it early would discard what it queued.
    let sending = thread::spawn(move || match sender {
        End::Ours(mut push) => {
            // Built as libzmq's sender builds its own: a small payload
            // copied inline, where a vector would allocate and be freed,
            // and a larger one allocated and zeroed in place. A copy costs
            // more than zeroing on Apple Silicon, which zeroes cache lines
            // without reading them, and `black_box` keeps the zeroing from
            // becoming a calloc, which may hand back zeroed pages unwritten.
            let small = vec![0; size.min(Frame::INLINE_CAPACITY)];
            for _ in 0..count {
                let frame = if size <= Frame::INLINE_CAPACITY {
                    Frame::copy_from_slice(&small)
                } else {
                    let mut payload = Vec::with_capacity(size);
                    payload.resize(size, hint::black_box(0));
                    Frame::from(payload)
                };
                push.send(Message::from(frame));
            }
            End::Ours(push)
        }
        End::Libzmq(push) => {
            for _ in 0..count {
                push.send_sized(size).expect("libzmq send");
            }
            End::Libzmq(push)
        }
    });
    let elapsed = match receiver {
        End::Ours(mut pull) => {
            let mut receive = || {
                let message = pull.recv();
                assert_eq!(message.len().get(), 1, "single frame");
                assert_eq!(message.last().len(), size, "message size");
            };
            receive();
            let started = Instant::now();
            for _ in 1..count {
                receive();
            }
            started.elapsed()
        }
        End::Libzmq(pull) => {
            let mut msg = Msg::new();
            let mut receive = || {
                assert_eq!(
                    pull.recv_msg(&mut msg).expect("libzmq receive"),
                    size,
                    "message size"
                );
            };
            receive();
            let started = Instant::now();
            for _ in 1..count {
                receive();
            }
            started.elapsed()
        }
    };
    drop(sending.join().expect("sender"));
    remove_ipc(transport);
    (count - 1) as f64 / elapsed.as_secs_f64()
}

/// One-way latency through REQ and REP, timed at the requester, as
/// libzmq's `remote_lat`.
fn latency(case: Case, count: usize) -> Latency {
    let Case {
        transport,
        size,
        pair,
        ..
    } = case;
    let contexts = Contexts::new(transport);
    let (replier, endpoint) = bind::<Rep>(
        pair.ours_second(),
        &contexts.ours.1,
        &contexts.libzmq.1,
        Kind::Rep,
        transport,
    );
    let requester = connect::<Req>(
        pair.ours_first(),
        &contexts.ours.0,
        &contexts.libzmq.0,
        Kind::Req,
        &endpoint,
    );
    let rounds = count + 1;
    let replying = thread::spawn(move || match replier {
        End::Ours(mut rep) => {
            for _ in 0..rounds {
                let request = rep.recv();
                assert_eq!(request.body().last().len(), size, "request size");
                let body = request.body().clone();
                assert_eq!(request.reply(body), zmq::RouteOutcome::Sent, "reply sent");
            }
            End::Ours(rep)
        }
        End::Libzmq(rep) => {
            let mut msg = Msg::new();
            for _ in 0..rounds {
                assert_eq!(
                    rep.recv_msg(&mut msg).expect("libzmq receive"),
                    size,
                    "request size"
                );
                rep.send_msg(&mut msg).expect("libzmq reply");
            }
            End::Libzmq(rep)
        }
    });
    let mut round_trips = Vec::with_capacity(count);
    let elapsed = match requester {
        End::Ours(mut req) => {
            let mut message = Message::from(vec![0; size]);
            let mut round_trip = |message: Message| {
                let reply = req.request(message).expect("reply");
                assert_eq!(reply.last().len(), size, "reply size");
                reply
            };
            message = round_trip(message);
            let started = Instant::now();
            for _ in 0..count {
                let sent = Instant::now();
                message = round_trip(message);
                round_trips.push(sent.elapsed());
            }
            started.elapsed()
        }
        End::Libzmq(req) => {
            let mut msg = Msg::with_size(size).expect("libzmq message");
            let round_trip = |msg: &mut Msg| {
                req.send_msg(msg).expect("libzmq request");
                assert_eq!(req.recv_msg(msg).expect("libzmq reply"), size, "reply size");
            };
            round_trip(&mut msg);
            let started = Instant::now();
            for _ in 0..count {
                let sent = Instant::now();
                round_trip(&mut msg);
                round_trips.push(sent.elapsed());
            }
            started.elapsed()
        }
    };
    drop(replying.join().expect("replier"));
    remove_ipc(transport);
    round_trips.sort_unstable();
    let one_way = |d: Duration| d.as_secs_f64() * 1e6 / 2.0;
    Latency {
        mean: elapsed.as_secs_f64() * 1e6 / (2 * count) as f64,
        p50: one_way(round_trips[round_trips.len() / 2]),
        p99: one_way(round_trips[round_trips.len() * 99 / 100]),
    }
}

/// libzmq leaves its ipc socket file behind; ours removes its own.
fn remove_ipc(transport: Transport) {
    if transport == Transport::Ipc
        && let Endpoint::Ipc(path) = endpoint(transport)
    {
        let _ = std::fs::remove_file(path);
    }
}
