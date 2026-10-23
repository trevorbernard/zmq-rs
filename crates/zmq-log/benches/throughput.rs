//! Log against pipe between two threads: records of 8 and 256 octets, the
//! writer copying each in, the reader taking everything committed at once.
//!
//! Each measurement runs in a fresh child process and modes are interleaved
//! across rounds, as in `zmq-pipe`'s bench.
//!
//! `cargo bench -p zmq-log [filter]`, where a mode is `log-8`, `pipe-8`,
//! `log-256` or `pipe-256`.

use std::env;
use std::num::NonZeroUsize;
use std::process::Command;
use std::sync::Arc;
use std::task::{Context, Poll, Wake, Waker};
use std::thread;
use std::time::Instant;

use zmq_log::log;
use zmq_pipe::{Hwm, pipe};

const MESSAGES: u64 = 5_000_000;
const ROUNDS: usize = 7;
const HWM: usize = 1000;
const MODES: [&str; 4] = ["log-8", "pipe-8", "log-256", "pipe-256"];

fn main() {
    let args: Vec<String> = env::args().skip(1).collect();
    if let [flag, mode] = args.as_slice()
        && flag == "--child"
    {
        println!("{}", run(mode));
        return;
    }
    let filter = args.iter().find(|arg| !arg.starts_with("--"));
    let exe = env::current_exe().expect("benchmark executable");
    let mut results: Vec<(&str, Vec<f64>)> = MODES
        .iter()
        .filter(|name| filter.is_none_or(|f| name.contains(f.as_str())))
        .map(|&name| (name, Vec::with_capacity(ROUNDS)))
        .collect();
    for _ in 0..ROUNDS {
        for (name, samples) in &mut results {
            let output = Command::new(&exe)
                .args(["--child", name])
                .output()
                .expect("run child");
            assert!(output.status.success(), "{name} child failed");
            let ns: f64 = String::from_utf8_lossy(&output.stdout)
                .trim()
                .parse()
                .expect("child prints ns per message");
            samples.push(ns);
        }
    }
    println!("{ROUNDS} processes x {MESSAGES} messages, ns per message:");
    for (name, mut samples) in results {
        samples.sort_by(f64::total_cmp);
        println!(
            "  {name:<9} min {:>6.1}  median {:>6.1}  max {:>6.1}",
            samples[0],
            samples[samples.len() / 2],
            samples[samples.len() - 1]
        );
    }
}

fn run(mode: &str) -> f64 {
    let (kind, size) = mode.split_once('-').expect("mode is kind-size");
    let size: usize = size.parse().expect("size");
    let start = Instant::now();
    match kind {
        "log" => run_log(size),
        "pipe" => run_pipe(size),
        _ => panic!("unknown mode {mode}"),
    }
    start.elapsed().as_nanos() as f64 / MESSAGES as f64
}

fn run_log(size: usize) {
    let (mut writer, mut reader) = log(
        NonZeroUsize::new(256 * 1024).unwrap(),
        NonZeroUsize::new(HWM),
    );
    let consumer = thread::spawn(move || {
        let waker = thread_waker();
        let mut cx = Context::from_waker(&waker);
        let mut octets = 0usize;
        loop {
            match reader.poll_committed(&mut cx) {
                Poll::Ready(true) => {
                    let [a, b] = reader.committed();
                    let len = a.len() + b.len();
                    octets += len;
                    reader.release(len, (len / size) as u64);
                }
                Poll::Ready(false) => return octets,
                Poll::Pending => thread::park(),
            }
        }
    });
    let waker = thread_waker();
    let mut cx = Context::from_waker(&waker);
    let record = vec![7u8; size];
    for _ in 0..MESSAGES {
        while writer.poll_writable(&mut cx, size) == Poll::Pending {
            thread::park();
        }
        writer.put(&record);
        writer.end_message();
        writer.commit();
    }
    drop(writer);
    assert_eq!(consumer.join().unwrap(), size * MESSAGES as usize);
}

fn run_pipe(size: usize) {
    let (mut writer, mut reader) = pipe::<Vec<u8>>(Hwm::Messages(NonZeroUsize::new(HWM).unwrap()));
    let consumer = thread::spawn(move || {
        let mut octets = 0usize;
        while let Some(record) = reader.recv_blocking() {
            octets += record.len();
        }
        octets
    });
    for _ in 0..MESSAGES {
        writer.send_blocking(vec![7u8; size]).unwrap();
    }
    drop(writer);
    assert_eq!(consumer.join().unwrap(), size * MESSAGES as usize);
}

fn thread_waker() -> Waker {
    struct Unpark(thread::Thread);
    impl Wake for Unpark {
        fn wake(self: Arc<Self>) {
            self.0.unpark();
        }
    }
    Waker::from(Arc::new(Unpark(thread::current())))
}
