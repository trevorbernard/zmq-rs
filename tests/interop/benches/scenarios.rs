//! Throughput in the cases `perf` leaves out, ours against libzmq's, over
//! tcp: multipart messages, PUB fan-out, many PUSH peers into one PULL,
//! DEALER into ROUTER, and CURVE.
//!
//! `cargo bench -p zmq-interop --bench scenarios [filter...]`. A case runs
//! if its name contains some filter.

use std::env;
use std::process::{Command, Stdio};
use std::sync::Arc;
use std::sync::atomic::{AtomicUsize, Ordering};
use std::thread;
use std::time::{Duration, Instant};

use zmq::{
    Context, Dealer, Frame, Hwm, Keypair, Message, Pub, Pull, Push, Router, Security,
    SocketOptions, Sub,
};
use zmq_interop::libzmq::{self, Kind, Msg};

const ROUNDS: usize = 5;

#[derive(Debug, Clone, Copy)]
enum Scenario {
    /// PUSH to PULL, `frames` frames of 64 octets each.
    Multipart { frames: usize },
    /// One PUB to `subs` SUBs, each subscribed to everything.
    Fanout { subs: usize },
    /// `pushers` PUSH sockets, one thread each, into one PULL.
    Fanin { pushers: usize },
    /// `dealers` DEALERs into one ROUTER.
    Router { dealers: usize },
    /// PUSH to PULL under CURVE.
    Curve { size: usize },
}

const CASES: [(&str, Scenario); 11] = [
    ("multipart/2", Scenario::Multipart { frames: 2 }),
    ("multipart/8", Scenario::Multipart { frames: 8 }),
    ("fanout/1", Scenario::Fanout { subs: 1 }),
    ("fanout/4", Scenario::Fanout { subs: 4 }),
    ("fanout/16", Scenario::Fanout { subs: 16 }),
    ("fanin/16", Scenario::Fanin { pushers: 16 }),
    ("fanin/64", Scenario::Fanin { pushers: 64 }),
    ("router/1", Scenario::Router { dealers: 1 }),
    ("router/16", Scenario::Router { dealers: 16 }),
    ("curve/256", Scenario::Curve { size: 256 }),
    ("curve/64k", Scenario::Curve { size: 65536 }),
];

fn main() {
    let args: Vec<String> = env::args().skip(1).collect();
    if args.first().is_some_and(|a| a == "--child") {
        let scenario = CASES
            .iter()
            .find(|(name, _)| *name == args[1])
            .expect("case")
            .1;
        let ours = args[2] == "ours";
        println!("{}", run(scenario, ours));
        return;
    }
    let filters: Vec<&String> = args.iter().filter(|a| !a.starts_with("--")).collect();
    let cases: Vec<&str> = CASES
        .iter()
        .map(|(name, _)| *name)
        .filter(|name| filters.is_empty() || filters.iter().any(|f| name.contains(f.as_str())))
        .collect();
    let exe = env::current_exe().expect("exe");
    let mut samples = vec![(Vec::new(), Vec::new()); cases.len()];
    for round in 0..=ROUNDS {
        for (i, name) in cases.iter().enumerate() {
            for (lib, out) in [("ours", 0), ("libzmq", 1)] {
                let output = Command::new(&exe)
                    .args(["--child", name, lib])
                    .stderr(Stdio::inherit())
                    .output()
                    .expect("child");
                assert!(output.status.success(), "{name} {lib} failed");
                let rate: f64 = String::from_utf8_lossy(&output.stdout)
                    .trim()
                    .parse()
                    .expect("rate");
                // Round 0 warms up.
                if round > 0 {
                    let pair = &mut samples[i];
                    if out == 0 { &mut pair.0 } else { &mut pair.1 }.push(rate);
                }
            }
        }
        eprintln!("round {round} done");
    }
    println!("| Case | ours msg/s | libzmq msg/s | ours/libzmq |");
    println!("|---|---|---|---|");
    for (name, (ours, theirs)) in cases.iter().zip(samples) {
        let (o, t) = (median(ours), median(theirs));
        println!(
            "| {name} | {} | {} | {:.2} |",
            rate_text(o),
            rate_text(t),
            o / t
        );
    }
}

fn median(mut v: Vec<f64>) -> f64 {
    v.sort_by(f64::total_cmp);
    v[v.len() / 2]
}

fn rate_text(rate: f64) -> String {
    if rate >= 1e6 {
        format!("{:.2}M", rate / 1e6)
    } else {
        format!("{:.1}k", rate / 1e3)
    }
}

fn unlimited() -> SocketOptions {
    SocketOptions {
        send_hwm: Hwm::Unlimited,
        recv_hwm: Hwm::Unlimited,
        ..SocketOptions::default()
    }
}

fn run(scenario: Scenario, ours: bool) -> f64 {
    match scenario {
        Scenario::Multipart { frames } => multipart(ours, frames, 500_000),
        Scenario::Fanout { subs } => fanout(ours, subs, 200_000),
        Scenario::Fanin { pushers } => fanin(ours, pushers, 1_000_000),
        Scenario::Router { dealers } => router(ours, dealers, 1_000_000),
        Scenario::Curve { size } => curve(ours, size, if size > 4096 { 50_000 } else { 500_000 }),
    }
}

fn ours_message(frames: usize, size: usize) -> Message {
    let frames: Vec<Frame> = (0..frames).map(|_| Frame::from(vec![0; size])).collect();
    Message::try_from(frames).expect("frames")
}

fn multipart(ours: bool, frames: usize, count: usize) -> f64 {
    if ours {
        let (send_cx, recv_cx) = (Context::new().unwrap(), Context::new().unwrap());
        let mut pull = recv_cx.socket::<Pull>();
        let bound = pull.bind("tcp://127.0.0.1:*").unwrap();
        let mut push = send_cx.socket::<Push>();
        push.connect(&bound.to_string()).unwrap();
        let sender = thread::spawn(move || {
            for _ in 0..count {
                push.send(ours_message(frames, 64));
            }
            push
        });
        let mut receive = || assert_eq!(pull.recv().len().get(), frames);
        receive();
        let started = Instant::now();
        for _ in 1..count {
            receive();
        }
        let elapsed = started.elapsed();
        drop(sender.join());
        (count - 1) as f64 / elapsed.as_secs_f64()
    } else {
        let pull = libzmq::Socket::new(Kind::Pull);
        let addr = pull.bind_loopback();
        let push = libzmq::Socket::new(Kind::Push);
        push.connect(addr);
        let sender = thread::spawn(move || {
            for _ in 0..count {
                for i in 0..frames {
                    push.send_sized_part(64, i + 1 < frames).unwrap();
                }
            }
            push
        });
        let mut msg = Msg::new();
        let mut receive = || {
            for i in 0..frames {
                pull.recv_msg(&mut msg).unwrap();
                assert_eq!(msg.more(), i + 1 < frames);
            }
        };
        receive();
        let started = Instant::now();
        for _ in 1..count {
            receive();
        }
        let elapsed = started.elapsed();
        drop(sender.join());
        (count - 1) as f64 / elapsed.as_secs_f64()
    }
}

/// Messages per second published, timed from the first real message to the
/// last subscriber receiving the last. Small warm-up messages go out until
/// every subscriber has seen one, so all are connected and subscribed.
fn fanout(ours: bool, subs: usize, count: usize) -> f64 {
    const SIZE: usize = 64;
    let warmed = Arc::new(AtomicUsize::new(0));
    let finished = Arc::new(AtomicUsize::new(0));
    let mut threads = Vec::new();
    let elapsed;
    if ours {
        let pub_cx = Context::new().unwrap();
        let mut publisher = pub_cx.socket_with::<Pub>(unlimited());
        let bound = publisher.bind("tcp://127.0.0.1:*").unwrap().to_string();
        for _ in 0..subs {
            let (bound, warmed, finished) = (bound.clone(), warmed.clone(), finished.clone());
            threads.push(thread::spawn(move || {
                let cx = Context::new().unwrap();
                let mut sub = cx.socket_with::<Sub>(unlimited());
                sub.subscribe("");
                sub.connect(&bound).unwrap();
                let mut seen_warm = false;
                let mut got = 0;
                while got < count {
                    let message = sub.recv();
                    if message.last().len() == SIZE {
                        got += 1;
                    } else if !seen_warm {
                        seen_warm = true;
                        warmed.fetch_add(1, Ordering::SeqCst);
                    }
                }
                finished.fetch_add(1, Ordering::SeqCst);
            }));
        }
        while warmed.load(Ordering::SeqCst) < subs {
            publisher.send(vec![0u8; 1]);
            thread::sleep(Duration::from_millis(1));
        }
        let started = Instant::now();
        for _ in 0..count {
            publisher.send(vec![0u8; SIZE]);
        }
        for t in threads {
            t.join().unwrap();
        }
        elapsed = started.elapsed();
    } else {
        let publisher = libzmq::Socket::new(Kind::Pub);
        publisher.set_hwm(0, 0);
        let addr = publisher.bind_loopback();
        for _ in 0..subs {
            let (warmed, finished) = (warmed.clone(), finished.clone());
            threads.push(thread::spawn(move || {
                let sub = libzmq::Socket::new(Kind::Sub);
                sub.set_hwm(0, 0);
                sub.set_receive_timeout(Duration::from_secs(60));
                sub.subscribe(b"");
                sub.connect(addr);
                let mut msg = Msg::new();
                let mut seen_warm = false;
                let mut got = 0;
                while got < count {
                    if sub.recv_msg(&mut msg).unwrap() == SIZE {
                        got += 1;
                    } else if !seen_warm {
                        seen_warm = true;
                        warmed.fetch_add(1, Ordering::SeqCst);
                    }
                }
                finished.fetch_add(1, Ordering::SeqCst);
            }));
        }
        while warmed.load(Ordering::SeqCst) < subs {
            publisher.send_sized(1).unwrap();
            thread::sleep(Duration::from_millis(1));
        }
        let started = Instant::now();
        for _ in 0..count {
            publisher.send_sized(SIZE).unwrap();
        }
        for t in threads {
            t.join().unwrap();
        }
        elapsed = started.elapsed();
    }
    count as f64 / elapsed.as_secs_f64()
}

/// Total messages per second into one PULL from `pushers` PUSH sockets of
/// 256 octets each, timed at the PULL from the first to the last.
fn fanin(ours: bool, pushers: usize, count: usize) -> f64 {
    const SIZE: usize = 256;
    let each = count / pushers;
    let total = each * pushers;
    let elapsed;
    if ours {
        let mut threads = Vec::new();
        let cx = Context::new().unwrap();
        let mut pull = cx.socket::<Pull>();
        let bound = pull.bind("tcp://127.0.0.1:*").unwrap().to_string();
        let send_cx = Context::new().unwrap();
        for _ in 0..pushers {
            let mut push = send_cx.socket::<Push>();
            push.connect(&bound).unwrap();
            threads.push(thread::spawn(move || {
                for _ in 0..each {
                    push.send(vec![0u8; SIZE]);
                }
                push
            }));
        }
        assert_eq!(pull.recv().last().len(), SIZE);
        let started = Instant::now();
        for _ in 1..total {
            assert_eq!(pull.recv().last().len(), SIZE);
        }
        elapsed = started.elapsed();
        for t in threads {
            drop(t.join());
        }
    } else {
        let mut threads = Vec::new();
        let pull = libzmq::Socket::new(Kind::Pull);
        let addr = pull.bind_loopback();
        let send_cx = libzmq::Context::new();
        for _ in 0..pushers {
            let push = libzmq::Socket::in_context(&send_cx, Kind::Push);
            push.connect(addr);
            threads.push(thread::spawn(move || {
                for _ in 0..each {
                    push.send_sized(SIZE).unwrap();
                }
                push
            }));
        }
        let mut msg = Msg::new();
        assert_eq!(pull.recv_msg(&mut msg).unwrap(), SIZE);
        let started = Instant::now();
        for _ in 1..total {
            assert_eq!(pull.recv_msg(&mut msg).unwrap(), SIZE);
        }
        elapsed = started.elapsed();
        for t in threads {
            drop(t.join());
        }
    }
    (total - 1) as f64 / elapsed.as_secs_f64()
}

/// DEALERs, one thread each, into one ROUTER, 256 octets per message.
/// libzmq's ROUTER delivers the routing id as a frame of its own.
fn router(ours: bool, dealers: usize, count: usize) -> f64 {
    const SIZE: usize = 256;
    let each = count / dealers;
    let total = each * dealers;
    let elapsed;
    if ours {
        let mut threads = Vec::new();
        let cx = Context::new().unwrap();
        let mut router = cx.socket::<Router>();
        let bound = router.bind("tcp://127.0.0.1:*").unwrap().to_string();
        let send_cx = Context::new().unwrap();
        for _ in 0..dealers {
            let mut dealer = send_cx.socket::<Dealer>();
            dealer.connect(&bound).unwrap();
            threads.push(thread::spawn(move || {
                for _ in 0..each {
                    dealer.send(vec![0u8; SIZE]);
                }
                dealer
            }));
        }
        assert_eq!(router.recv().1.last().len(), SIZE);
        let started = Instant::now();
        for _ in 1..total {
            assert_eq!(router.recv().1.last().len(), SIZE);
        }
        elapsed = started.elapsed();
        for t in threads {
            drop(t.join());
        }
    } else {
        let mut threads = Vec::new();
        let router = libzmq::Socket::new(Kind::Router);
        let addr = router.bind_loopback();
        let send_cx = libzmq::Context::new();
        for _ in 0..dealers {
            let dealer = libzmq::Socket::in_context(&send_cx, Kind::Dealer);
            dealer.connect(addr);
            threads.push(thread::spawn(move || {
                for _ in 0..each {
                    dealer.send_sized(SIZE).unwrap();
                }
                dealer
            }));
        }
        let mut msg = Msg::new();
        let mut receive = || {
            router.recv_msg(&mut msg).unwrap();
            assert!(msg.more());
            assert_eq!(router.recv_msg(&mut msg).unwrap(), SIZE);
        };
        receive();
        let started = Instant::now();
        for _ in 1..total {
            receive();
        }
        elapsed = started.elapsed();
        for t in threads {
            drop(t.join());
        }
    }
    (total - 1) as f64 / elapsed.as_secs_f64()
}

fn curve(ours: bool, size: usize, count: usize) -> f64 {
    let server = Keypair::generate();
    let client = Keypair::generate();
    if ours {
        let (send_cx, recv_cx) = (Context::new().unwrap(), Context::new().unwrap());
        let server_key = server.public();
        let mut pull = recv_cx.socket_with::<Pull>(SocketOptions {
            security: Security::curve_server(server),
            ..SocketOptions::default()
        });
        let bound = pull.bind("tcp://127.0.0.1:*").unwrap();
        let mut push = send_cx.socket_with::<Push>(SocketOptions {
            security: Security::curve_client(client, server_key),
            ..SocketOptions::default()
        });
        push.connect(&bound.to_string()).unwrap();
        let sender = thread::spawn(move || {
            for _ in 0..count {
                push.send(vec![0u8; size]);
            }
            push
        });
        assert_eq!(pull.recv().last().len(), size);
        let started = Instant::now();
        for _ in 1..count {
            assert_eq!(pull.recv().last().len(), size);
        }
        let elapsed = started.elapsed();
        drop(sender.join());
        (count - 1) as f64 / elapsed.as_secs_f64()
    } else {
        let pull = libzmq::Socket::new(Kind::Pull);
        pull.set_curve_server(server.secret().expose());
        let addr = pull.bind_loopback();
        let push = libzmq::Socket::new(Kind::Push);
        push.set_curve_client(
            client.public().as_bytes(),
            client.secret().expose(),
            server.public().as_bytes(),
        );
        push.connect(addr);
        let sender = thread::spawn(move || {
            for _ in 0..count {
                push.send_sized(size).unwrap();
            }
            push
        });
        let mut msg = Msg::new();
        assert_eq!(pull.recv_msg(&mut msg).unwrap(), size);
        let started = Instant::now();
        for _ in 1..count {
            assert_eq!(pull.recv_msg(&mut msg).unwrap(), size);
        }
        let elapsed = started.elapsed();
        drop(sender.join());
        (count - 1) as f64 / elapsed.as_secs_f64()
    }
}
