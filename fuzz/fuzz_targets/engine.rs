#![no_main]

use std::fs;
use std::io::{ErrorKind, Read, Write};
use std::net::Shutdown;
use std::os::unix::net::{UnixListener, UnixStream};
use std::path::{Path, PathBuf};
use std::sync::LazyLock;
use std::sync::atomic::{AtomicU64, Ordering};
use std::thread;
use std::time::{Duration, Instant};

use libfuzzer_sys::fuzz_target;
use zmq::{
    Context, Credential, Dealer, EventKind, Frame, Heartbeat, Linger, Metadata, Monitor, Pair,
    PlainCredentials, Pub, Pull, Push, Rep, Router, Security, Socket, SocketOptions, SocketType,
    Sub, XPub, XSub, ZapReply, ZapRequest,
};
use zmtp_fuzz::curve;

/// How long the engine may take to notice that the peer closed.
const LIMIT: Duration = Duration::from_secs(5);

static CONTEXT: LazyLock<Context> = LazyLock::new(|| {
    // libFuzzer's main runs rather than Rust's, which would ignore SIGPIPE:
    // a write to a closed connection would kill the process, not fail.
    // SAFETY: SIG_IGN is a valid disposition for SIGPIPE.
    unsafe { libc::signal(libc::SIGPIPE, libc::SIG_IGN) };
    Context::new().expect("a context")
});

static DIR: LazyLock<PathBuf> = LazyLock::new(|| {
    let dir = std::env::temp_dir().join(format!("zmtp-fuzz-{}", std::process::id()));
    fs::create_dir_all(&dir).expect("a directory for sockets");
    dir
});

/// A path no earlier run used, so a listener the I/O thread has yet to
/// close cannot collide with this run's.
fn fresh_path() -> PathBuf {
    static NEXT: AtomicU64 = AtomicU64::new(0);
    DIR.join(NEXT.fetch_add(1, Ordering::Relaxed).to_string())
}

/// Which end of the connection the socket is.
#[derive(Debug, Clone, Copy)]
enum Direction {
    /// It binds and accepts the raw peer, as libzmq's `test_bind_*_fuzzer`.
    Bind,
    /// It connects to the raw peer, as libzmq's `test_connect_*_fuzzer`.
    Connect,
}

/// What the input's first octet picks: the socket type (low four bits,
/// modulo the ten types), the direction (bit 4), the mechanism (bits 5
/// and 6) and heartbeats (bit 7).
#[derive(Debug, Clone, Copy)]
struct Setup {
    kind: u8,
    direction: Direction,
    mechanism: u8,
    heartbeat: Option<Heartbeat>,
}

impl From<u8> for Setup {
    fn from(octet: u8) -> Self {
        Self {
            kind: (octet & 0x0f) % 10,
            direction: if octet & 0x10 == 0 {
                Direction::Bind
            } else {
                Direction::Connect
            },
            mechanism: (octet >> 5) & 0x03,
            // Fast enough to ping, and to time out, within a run.
            heartbeat: (octet & 0x80 != 0).then(|| Heartbeat {
                interval: Duration::from_millis(1),
                timeout: Duration::from_millis(20),
                ttl: Duration::from_millis(100),
            }),
        }
    }
}

fn accept_everyone(_: &ZapRequest) -> ZapReply {
    ZapReply::Ok {
        user_id: Frame::from("fuzz"),
        metadata: Metadata::new(),
    }
}

/// NULL, PLAIN or CURVE as the role demands, with libzmq's fuzzer keys so
/// its CURVE corpora apply; and NULL behind ZAP for a server.
fn security(setup: Setup) -> Security {
    match (setup.direction, setup.mechanism) {
        (_, 0) => Security::Null,
        (Direction::Bind, 1) => Security::plain_server("", accept_everyone),
        (Direction::Bind, 2) => Security::null_authenticated("", accept_everyone),
        (Direction::Bind, _) => Security::curve_server(curve::server_keys()),
        (Direction::Connect, 1) => Security::PlainClient(PlainCredentials {
            username: Credential::new("user").expect("short"),
            password: Credential::new("pass").expect("short"),
        }),
        (Direction::Connect, 2) => Security::Null,
        (Direction::Connect, _) => {
            Security::curve_client(curve::client_keys(), curve::server_public())
        }
    }
}

/// What a run does with its socket between reads: receive whatever arrived
/// and answer where the pattern allows, so the engine's inbound pipe never
/// stays full and its outbound path has work.
trait Step {
    fn step(&mut self);
}

impl Step for Socket<Pub> {
    fn step(&mut self) {
        self.send("topic body");
    }
}

impl Step for Socket<Sub> {
    fn step(&mut self) {
        while self.try_recv().is_some() {}
    }
}

impl Step for Socket<XPub> {
    fn step(&mut self) {
        while self.try_recv().is_some() {}
        self.send("topic body");
    }
}

impl Step for Socket<XSub> {
    fn step(&mut self) {
        while self.try_recv().is_some() {}
    }
}

impl Step for Socket<Pull> {
    fn step(&mut self) {
        while self.try_recv().is_some() {}
    }
}

impl Step for Socket<Push> {
    fn step(&mut self) {
        let _ = self.try_send("work");
    }
}

impl Step for Socket<Pair> {
    fn step(&mut self) {
        while let Some(message) = self.try_recv() {
            let _ = self.try_send(message);
        }
    }
}

impl Step for Socket<Dealer> {
    fn step(&mut self) {
        while let Some(message) = self.try_recv() {
            let _ = self.try_send(message);
        }
    }
}

impl Step for Socket<Router> {
    fn step(&mut self) {
        while let Some((from, message)) = self.try_recv() {
            let _ = self.try_send(&from, message);
        }
    }
}

impl Step for Socket<Rep> {
    fn step(&mut self) {
        while let Some(request) = self.try_recv() {
            request.reply("reply");
        }
    }
}

/// A socket of type `T` set up as `setup` says, attached to `endpoint`.
fn open<T: SocketType>(
    setup: Setup,
    endpoint: &str,
    prepare: impl FnOnce(&mut Socket<T>),
) -> (Socket<T>, Monitor) {
    let options = SocketOptions {
        // The context outlives every run: nothing may linger.
        linger: Linger::For(Duration::ZERO),
        max_message_size: Some(zmtp_fuzz::MAX_MESSAGE_SIZE),
        security: security(setup),
        heartbeat: setup.heartbeat,
        ..SocketOptions::default()
    };
    let mut socket = CONTEXT.socket_with::<T>(options);
    let monitor = socket.monitor();
    prepare(&mut socket);
    match setup.direction {
        Direction::Bind => socket.bind(endpoint).map(drop),
        Direction::Connect => socket.connect(endpoint),
    }
    .expect("attached");
    (socket, monitor)
}

fn boxed<T: SocketType>(
    setup: Setup,
    endpoint: &str,
    prepare: impl FnOnce(&mut Socket<T>),
) -> (Box<dyn Step>, Monitor)
where
    Socket<T>: Step + 'static,
{
    let (socket, monitor) = open::<T>(setup, endpoint, prepare);
    (Box::new(socket), monitor)
}

fn socket(setup: Setup, endpoint: &str) -> (Box<dyn Step>, Monitor) {
    match setup.kind {
        0 => boxed::<Pub>(setup, endpoint, |_| {}),
        1 => boxed::<Sub>(setup, endpoint, |s| s.subscribe("")),
        2 => boxed::<XPub>(setup, endpoint, |s| s.set_welcome(Some("welcome".into()))),
        3 => boxed::<XSub>(setup, endpoint, |s| s.subscribe("")),
        4 => boxed::<Pull>(setup, endpoint, |_| {}),
        5 => boxed::<Push>(setup, endpoint, |_| {}),
        6 => boxed::<Pair>(setup, endpoint, |_| {}),
        7 => boxed::<Dealer>(setup, endpoint, |_| {}),
        8 => boxed::<Router>(setup, endpoint, |_| {}),
        _ => boxed::<Rep>(setup, endpoint, |_| {}),
    }
}

/// The raw peer's end, once the socket has reached it or it the socket.
fn raw_peer(path: &Path, listener: Option<&UnixListener>) -> UnixStream {
    match listener {
        Some(listener) => {
            listener.set_nonblocking(true).expect("nonblocking");
            let deadline = Instant::now() + LIMIT;
            loop {
                match listener.accept() {
                    Ok((stream, _)) => return stream,
                    Err(err) if err.kind() == ErrorKind::WouldBlock => {
                        assert!(Instant::now() < deadline, "the socket never connected");
                        thread::sleep(Duration::from_micros(100));
                    }
                    Err(err) => panic!("accept: {err}"),
                }
            }
        }
        None => UnixStream::connect(path).expect("the socket is listening"),
    }
}

/// Sends `traffic` and closes, reading and discarding what comes back and
/// stepping the socket meanwhile, until the engine reports the connection
/// gone.
fn exchange(mut stream: UnixStream, traffic: &[u8], socket: &mut dyn Step, monitor: &mut Monitor) {
    stream.set_nonblocking(true).expect("nonblocking");
    let deadline = Instant::now() + LIMIT;
    let mut rest = traffic;
    let mut sink = [0; 4096];
    let mut shut = false;
    loop {
        if !rest.is_empty() {
            match stream.write(rest) {
                Ok(written) => rest = &rest[written..],
                Err(err) if err.kind() == ErrorKind::WouldBlock => {}
                // The engine closed: the rest has nowhere to go.
                Err(_) => rest = &[],
            }
        }
        if rest.is_empty() && !shut {
            let _ = stream.shutdown(Shutdown::Write);
            shut = true;
        }
        while matches!(stream.read(&mut sink), Ok(read) if read > 0) {}
        socket.step();
        while let Ok(event) = monitor.try_recv() {
            if event.kind == EventKind::Disconnected {
                return;
            }
        }
        assert!(
            Instant::now() < deadline,
            "the engine never noticed the connection end"
        );
        thread::sleep(Duration::from_micros(50));
    }
}

// Raw traffic from a peer to a real socket over ipc, through the engine,
// the I/O thread and the socket's routing: the first octet picks the setup,
// the rest is what the peer sends before closing. Fails on a panic on any
// thread (libfuzzer-sys aborts on one), or if the engine never notices the
// peer has gone.
fuzz_target!(|data: &[u8]| {
    let Some((&octet, traffic)) = data.split_first() else {
        return;
    };
    let setup = Setup::from(octet);
    let path = fresh_path();
    let endpoint = format!("ipc://{}", path.display());
    let listener = match setup.direction {
        Direction::Bind => None,
        Direction::Connect => Some(UnixListener::bind(&path).expect("a listener")),
    };
    let (mut socket, mut monitor) = socket(setup, &endpoint);
    let stream = raw_peer(&path, listener.as_ref());
    exchange(stream, traffic, socket.as_mut(), &mut monitor);
    drop(socket);
    drop(listener);
    let _ = fs::remove_file(&path);
});
