//! Pipe throughput between two threads.
//!
//! Results on one machine vary by tens of percent between runs, depending on
//! heap state and which cores the threads land on. So every measurement runs
//! in a fresh child process, modes are interleaved across rounds, and the
//! report gives min, median and max.
//!
//! `cargo bench -p zmq-pipe [mode]`, where mode is one of `unlimited`,
//! `hwm-1000` or `hwm-16`.

use std::env;
use std::num::NonZeroUsize;
use std::process::Command;
use std::thread;
use std::time::Instant;

use zmq_pipe::{Hwm, pipe};

const MESSAGES: u64 = 5_000_000;
const ROUNDS: usize = 7;
const MODES: [(&str, usize); 3] = [("unlimited", 0), ("hwm-1000", 1000), ("hwm-16", 16)];

fn main() {
    let args: Vec<String> = env::args().skip(1).collect();
    if let [flag, mode] = args.as_slice()
        && flag == "--child"
    {
        println!("{}", run(hwm(mode)));
        return;
    }
    // `cargo bench` passes `--bench`; anything else filters by mode name.
    let filter = args.iter().find(|arg| !arg.starts_with("--"));
    let exe = env::current_exe().expect("benchmark executable");
    let mut results: Vec<(&str, Vec<f64>)> = MODES
        .iter()
        .filter(|(name, _)| filter.is_none_or(|f| name.contains(f.as_str())))
        .map(|&(name, _)| (name, Vec::with_capacity(ROUNDS)))
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
            "  {name:<10} min {:>6.1}  median {:>6.1}  max {:>6.1}",
            samples[0],
            samples[samples.len() / 2],
            samples[samples.len() - 1]
        );
    }
}

fn hwm(mode: &str) -> Hwm {
    let &(_, limit) = MODES
        .iter()
        .find(|(name, _)| *name == mode)
        .unwrap_or_else(|| panic!("unknown mode {mode}"));
    NonZeroUsize::new(limit).map_or(Hwm::Unlimited, Hwm::Messages)
}

/// Nanoseconds per message for one writer thread and one reader thread.
fn run(hwm: Hwm) -> f64 {
    let (mut writer, mut reader) = pipe::<u64>(hwm);
    let start = Instant::now();
    let consumer = thread::spawn(move || {
        let mut sum = 0u64;
        while let Some(value) = reader.recv_blocking() {
            sum = sum.wrapping_add(value);
        }
        sum
    });
    for i in 0..MESSAGES {
        writer.send_blocking(i).expect("reader alive");
    }
    drop(writer);
    let sum = consumer.join().expect("reader thread");
    assert_eq!(sum, MESSAGES * (MESSAGES - 1) / 2, "every message arrived");
    start.elapsed().as_secs_f64() * 1e9 / MESSAGES as f64
}
