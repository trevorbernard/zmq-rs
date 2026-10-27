//! Our sockets against real old libzmqs: 2.2, which speaks ZMTP 1.0, and
//! 3.2, which speaks ZMTP 2.0, so we downgrade to each protocol with the
//! implementation that defined it.
//!
//! Each runs as a process, `tests/interop/legacy/peer.c`, as two libzmqs
//! cannot share one. The Nix dev shell sets `ZMQ_LEGACY_PEER_2` and
//! `ZMQ_LEGACY_PEER_3` to them; without those, the tests skip.

use std::env;
use std::io::{BufRead, BufReader, Write};
use std::net::TcpListener;
use std::process::{Child, ChildStdin, Command, Stdio};
use std::sync::atomic::{AtomicBool, Ordering};
use std::sync::{Arc, mpsc};
use std::thread;
use std::time::{Duration, Instant};

use zmq::{
    Context, Dealer, Identity, Message, Pub, Pull, Push, Rep, Req, Router, RoutingId, Security,
    SocketOptions, Sub, XPub, XPubEvent, ZapReply, ZapRequest,
};

const FIVE: Duration = Duration::from_secs(5);

/// An old libzmq, by the release the peer was built against.
#[derive(Debug, Clone, Copy)]
enum Libzmq {
    /// libzmq 2.2, speaking ZMTP 1.0.
    V2,
    /// libzmq 3.2, speaking ZMTP 2.0.
    V3,
}

impl Libzmq {
    fn variable(self) -> &'static str {
        match self {
            Self::V2 => "ZMQ_LEGACY_PEER_2",
            Self::V3 => "ZMQ_LEGACY_PEER_3",
        }
    }
}

/// A socket of an old libzmq in its own process, driven a line at a time.
struct Peer {
    child: Child,
    stdin: ChildStdin,
    lines: mpsc::Receiver<String>,
}

impl Peer {
    /// A `kind` socket of `libzmq` that binds or connects `endpoint`,
    /// with `options` such as `--identity <hex>`. `None`, saying so, if
    /// that peer was not built.
    fn start(
        libzmq: Libzmq,
        kind: &str,
        how: &str,
        endpoint: &str,
        options: &[&str],
    ) -> Option<Self> {
        let Some(path) = env::var_os(libzmq.variable()) else {
            eprintln!(
                "skipped: {} is not set; the Nix dev shell sets it",
                libzmq.variable()
            );
            return None;
        };
        let mut child = Command::new(path)
            .args([kind, how, endpoint])
            .args(options)
            .stdin(Stdio::piped())
            .stdout(Stdio::piped())
            .spawn()
            .expect("the legacy peer starts");
        let stdin = child.stdin.take().expect("piped");
        let stdout = child.stdout.take().expect("piped");
        let (send, lines) = mpsc::channel();
        thread::spawn(move || {
            for line in BufReader::new(stdout).lines() {
                let Ok(line) = line else { break };
                if send.send(line).is_err() {
                    break;
                }
            }
        });
        let mut peer = Self {
            child,
            stdin,
            lines,
        };
        assert_eq!(peer.line(), "ready");
        Some(peer)
    }

    fn line(&mut self) -> String {
        let line = self
            .lines
            .recv_timeout(Duration::from_secs(10))
            .expect("the legacy peer answers");
        assert!(!line.starts_with("error"), "legacy peer: {line}");
        line
    }

    fn command(&mut self, command: &str) -> String {
        writeln!(self.stdin, "{command}").expect("the legacy peer reads");
        self.line()
    }

    fn send(&mut self, frames: &[&[u8]]) {
        let hex: Vec<String> = frames.iter().map(|frame| hex(frame)).collect();
        assert_eq!(self.command(&format!("send {}", hex.join(" "))), "sent");
    }

    /// The next message, or `None` after the peer's five-second timeout.
    fn recv(&mut self) -> Option<Message> {
        let line = self.command("recv");
        if line == "timeout" {
            return None;
        }
        let frames = line.strip_prefix("msg ").expect("a message");
        let frames: Vec<zmq::Frame> = frames.split(' ').map(unhex).map(zmq::Frame::from).collect();
        Some(Message::try_from(frames).expect("at least one frame"))
    }
}

impl Drop for Peer {
    fn drop(&mut self) {
        let _ = writeln!(self.stdin, "quit");
        let started = Instant::now();
        while started.elapsed() < Duration::from_secs(2) {
            if let Ok(Some(_)) = self.child.try_wait() {
                return;
            }
            thread::sleep(Duration::from_millis(10));
        }
        let _ = self.child.kill();
        let _ = self.child.wait();
    }
}

fn hex(frame: &[u8]) -> String {
    if frame.is_empty() {
        return "-".to_owned();
    }
    frame.iter().map(|octet| format!("{octet:02x}")).collect()
}

fn unhex(hex: &str) -> Vec<u8> {
    if hex == "-" {
        return Vec::new();
    }
    (0..hex.len())
        .step_by(2)
        .map(|at| u8::from_str_radix(&hex[at..at + 2], 16).expect("hex"))
        .collect()
}

/// A TCP endpoint nothing listens on: libzmq 2.x cannot bind port 0.
fn free_endpoint() -> String {
    let listener = TcpListener::bind("127.0.0.1:0").unwrap();
    format!("tcp://{}", listener.local_addr().unwrap())
}

fn named(identity: &'static str) -> SocketOptions {
    SocketOptions {
        identity: Identity::new(identity).unwrap(),
        ..SocketOptions::default()
    }
}

fn message(frames: &[&'static str]) -> Message {
    let frames: Vec<zmq::Frame> = frames
        .iter()
        .map(|&frame| zmq::Frame::from(frame))
        .collect();
    Message::try_from(frames).unwrap()
}

/// Runs each case against both old libzmqs.
macro_rules! against_both {
    ($($case:ident),* $(,)?) => {
        mod libzmq2 {
            $(#[test] fn $case() { super::$case(super::Libzmq::V2) })*
        }
        mod libzmq3 {
            $(#[test] fn $case() { super::$case(super::Libzmq::V3) })*
        }
    };
}

against_both!(
    we_pull_from_an_old_push,
    an_old_pull_takes_from_our_push,
    our_req_asks_an_old_rep,
    an_old_req_asks_our_rep,
    our_router_answers_an_old_dealer_by_its_identity,
    an_old_router_answers_our_dealer_by_its_identity,
    an_old_sub_gets_only_its_topic_from_our_pub,
    our_sub_gets_only_its_topic_from_an_old_pub,
    an_old_peer_is_refused_where_we_require_security,
);

/// A ZMTP 1.0 SUB never sends its subscriptions, so it is subscribed to
/// everything as it connects, and an XPUB waiting for subscriptions hears
/// that at once. Only libzmq 2.2 speaks ZMTP 1.0.
#[test]
fn our_xpub_hears_an_old_sub_subscribe_to_everything() {
    let endpoint = free_endpoint();
    let Some(_sub) = Peer::start(
        Libzmq::V2,
        "SUB",
        "bind",
        &endpoint,
        &["--subscribe", &hex(b"news")],
    ) else {
        return;
    };
    let context = Context::new().unwrap();
    let mut xpub = context.socket::<XPub>();
    xpub.connect(&endpoint).unwrap();
    let (heard, event) = mpsc::channel();
    thread::spawn(move || heard.send(xpub.recv()));
    assert_eq!(
        event.recv_timeout(FIVE).unwrap(),
        XPubEvent::Subscribe(zmq::Frame::new())
    );
}

/// Messages, multipart ones included, reach us, over a connection that
/// announced no properties: there is no READY before ZMTP 3.0.
fn we_pull_from_an_old_push(libzmq: Libzmq) {
    let context = Context::new().unwrap();
    let mut pull = context.socket_with::<Pull>(SocketOptions {
        message_properties: zmq::MessageProperties::Attach,
        ..SocketOptions::default()
    });
    let endpoint = pull.bind("tcp://127.0.0.1:*").unwrap().to_string();
    let Some(mut push) = Peer::start(libzmq, "PUSH", "connect", &endpoint, &[]) else {
        return;
    };
    push.send(&[b"one"]);
    push.send(&[b"two", b"", b"parts"]);
    let first = pull.recv_timeout(FIVE).expect("the first message");
    assert_eq!(first, message(&["one"]));
    assert_eq!(
        first.property("Socket-Type"),
        None,
        "no READY before ZMTP 3.0"
    );
    assert_eq!(
        first.property("Peer-Address"),
        Some(b"127.0.0.1".as_slice())
    );
    assert_eq!(
        pull.recv_timeout(FIVE),
        Some(message(&["two", "", "parts"]))
    );
}

fn an_old_pull_takes_from_our_push(libzmq: Libzmq) {
    let endpoint = free_endpoint();
    let Some(mut pull) = Peer::start(libzmq, "PULL", "bind", &endpoint, &[]) else {
        return;
    };
    let context = Context::new().unwrap();
    let mut push = context.socket::<Push>();
    push.connect(&endpoint).unwrap();
    push.send("one");
    push.send(message(&["two", "", "parts"]));
    assert_eq!(pull.recv(), Some(message(&["one"])));
    assert_eq!(pull.recv(), Some(message(&["two", "", "parts"])));
}

fn our_req_asks_an_old_rep(libzmq: Libzmq) {
    let endpoint = free_endpoint();
    let Some(mut rep) = Peer::start(libzmq, "REP", "bind", &endpoint, &[]) else {
        return;
    };
    let context = Context::new().unwrap();
    let mut req = context.socket::<Req>();
    req.connect(&endpoint).unwrap();
    let asking = thread::spawn(move || req.request("question"));
    assert_eq!(rep.recv(), Some(message(&["question"])));
    rep.send(&[b"answer"]);
    let reply = asking.join().unwrap().expect("a reply");
    assert_eq!(reply, message(&["answer"]));
}

fn an_old_req_asks_our_rep(libzmq: Libzmq) {
    let context = Context::new().unwrap();
    let mut rep = context.socket::<Rep>();
    let endpoint = rep.bind("tcp://127.0.0.1:*").unwrap().to_string();
    let Some(mut req) = Peer::start(libzmq, "REQ", "connect", &endpoint, &[]) else {
        return;
    };
    req.send(&[b"question"]);
    let request = rep.recv_timeout(FIVE).expect("the request");
    assert_eq!(request.body(), &message(&["question"]));
    request.reply("answer");
    assert_eq!(req.recv(), Some(message(&["answer"])));
}

/// ZMTP 1.0 and 2.0 carry the identity in the greeting's first frame.
fn our_router_answers_an_old_dealer_by_its_identity(libzmq: Libzmq) {
    let context = Context::new().unwrap();
    let mut router = context.socket::<Router>();
    let endpoint = router.bind("tcp://127.0.0.1:*").unwrap().to_string();
    let Some(mut dealer) = Peer::start(
        libzmq,
        "DEALER",
        "connect",
        &endpoint,
        &["--identity", &hex(b"old")],
    ) else {
        return;
    };
    dealer.send(&[b"hello"]);
    let (from, request) = router.recv_timeout(FIVE).expect("the dealer's message");
    assert_eq!(from.as_bytes(), b"old");
    assert_eq!(request, message(&["hello"]));
    router
        .send(&RoutingId::new("old").unwrap(), "welcome")
        .unwrap();
    assert_eq!(dealer.recv(), Some(message(&["welcome"])));
}

fn an_old_router_answers_our_dealer_by_its_identity(libzmq: Libzmq) {
    let endpoint = free_endpoint();
    let Some(mut router) = Peer::start(libzmq, "ROUTER", "bind", &endpoint, &[]) else {
        return;
    };
    let context = Context::new().unwrap();
    let mut dealer = context.socket_with::<Dealer>(named("ours"));
    dealer.connect(&endpoint).unwrap();
    dealer.send("hello");
    assert_eq!(router.recv(), Some(message(&["ours", "hello"])));
    router.send(&[b"ours", b"welcome"]);
    assert_eq!(dealer.recv_timeout(FIVE), Some(message(&["welcome"])));
}

/// Over ZMTP 1.0 the subscriber filters for itself, as it never tells us
/// what it wants; over 2.0 it sends its subscriptions and we filter.
/// Either way it gets only its topic.
fn an_old_sub_gets_only_its_topic_from_our_pub(libzmq: Libzmq) {
    let context = Context::new().unwrap();
    let mut publisher = context.socket::<Pub>();
    let endpoint = publisher.bind("tcp://127.0.0.1:*").unwrap().to_string();
    let Some(mut sub) = Peer::start(
        libzmq,
        "SUB",
        "connect",
        &endpoint,
        &["--subscribe", &hex(b"news")],
    ) else {
        return;
    };
    let done = Arc::new(AtomicBool::new(false));
    let publishing = thread::spawn({
        let done = Arc::clone(&done);
        move || {
            let started = Instant::now();
            while !done.load(Ordering::Relaxed) && started.elapsed() < FIVE {
                publisher.send("sports: skipped");
                publisher.send("news: wanted");
                thread::sleep(Duration::from_millis(5));
            }
        }
    });
    for _ in 0..5 {
        assert_eq!(sub.recv(), Some(message(&["news: wanted"])));
    }
    done.store(true, Ordering::Relaxed);
    drop(sub);
    publishing.join().unwrap();
}

fn our_sub_gets_only_its_topic_from_an_old_pub(libzmq: Libzmq) {
    let endpoint = free_endpoint();
    let Some(mut publisher) = Peer::start(libzmq, "PUB", "bind", &endpoint, &[]) else {
        return;
    };
    let context = Context::new().unwrap();
    let mut sub = context.socket::<Sub>();
    sub.subscribe("news");
    sub.connect(&endpoint).unwrap();
    let mut received = Vec::new();
    let started = Instant::now();
    while received.len() < 5 {
        assert!(started.elapsed() < FIVE, "got {received:?}");
        publisher.send(&[b"sports: skipped"]);
        publisher.send(&[b"news: wanted"]);
        if let Some(message) = sub.recv_timeout(Duration::from_millis(10)) {
            received.push(message);
        }
    }
    assert!(received.iter().all(|m| *m == message(&["news: wanted"])));
}

/// RFC 37: a peer speaking ZMTP 1.0 or 2.0 has no security handshake, so
/// where a mechanism other than NULL is required its connection MUST be
/// closed. Nothing it sends arrives.
fn an_old_peer_is_refused_where_we_require_security(libzmq: Libzmq) {
    let context = Context::new().unwrap();
    let mut pull = context.socket_with::<Pull>(SocketOptions {
        security: Security::plain_server("", |_: &ZapRequest| ZapReply::Ok {
            user_id: zmq::Frame::from("anyone"),
            metadata: zmq::Metadata::new(),
        }),
        ..SocketOptions::default()
    });
    let endpoint = pull.bind("tcp://127.0.0.1:*").unwrap().to_string();
    let Some(mut push) = Peer::start(libzmq, "PUSH", "connect", &endpoint, &[]) else {
        return;
    };
    push.send(&[b"unauthenticated"]);
    assert_eq!(pull.recv_timeout(Duration::from_millis(500)), None);
}
