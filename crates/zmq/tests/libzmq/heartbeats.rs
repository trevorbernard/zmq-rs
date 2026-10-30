//! libzmq's `tests/test_heartbeats.cpp`: ZMTP 3.1 heartbeats
//! (`ZMQ_HEARTBEAT_IVL`, `_TIMEOUT` and `_TTL`), watched through the
//! server's monitor, with a hand-written ZMTP peer where libzmq uses one.

use std::io::{Read, Write};
use std::net::{TcpListener, TcpStream};
use std::time::{Duration, Instant};

use zmq::{
    Context, Dealer, EventKind, Heartbeat, Keypair, Monitor, Pair, Pub, PublicKey, Pull, Push, Rep,
    Req, Router, SecretKey, Security, Socket, SocketOptions, SocketType, Sub,
};
use zmq_test_ports::{free_endpoint, free_port};

use crate::support::{FIVE, SETTLE, settle};

const ZMTP_GREETING_NULL: [u8; 64] = {
    let mut greeting = [0; 64];
    greeting[0] = 0xff;
    greeting[9] = 0x7f;
    greeting[10] = 3;
    greeting[12] = b'N';
    greeting[13] = b'U';
    greeting[14] = b'L';
    greeting[15] = b'L';
    greeting
};

const ZMTP_READY_DEALER: &[u8] =
    b"\x04\x29\x05READY\x0bSocket-Type\x00\x00\x00\x06DEALER\x08Identity\x00\x00\x00\x00";

// libzmq's keys for these tests.
const SERVER_SECRET: &str = "JTKVSB%%)wK0E.X)V>+}o?pNmC{O&4W4b!Ni{Lh6";
const SERVER_PUBLIC: &str = "rq:rM>}U?@Lns47E1%kR.o@n%FcmmsL/@{H8]yf7";
const CLIENT_SECRET: &str = "D:)Q[IlAW!ahhC2ac:9*A}h:p?([4%wOTJ%JR%cs";

fn keypair(secret: &str) -> Keypair {
    Keypair::from_secret(SecretKey::from_z85(secret).unwrap())
}

/// The next ACCEPTED, CONNECTED or DISCONNECTED event, the only ones
/// libzmq's test monitors, if one comes within `timeout`.
fn next_event(monitor: &mut Monitor, timeout: Duration) -> Option<EventKind> {
    let deadline = Instant::now() + timeout;
    loop {
        let event = monitor
            .recv_timeout(deadline.saturating_duration_since(Instant::now()))
            .ok()?;
        if matches!(
            event.kind,
            EventKind::Accepted | EventKind::Connected | EventKind::Disconnected
        ) {
            return Some(event.kind);
        }
    }
}

/// A bound server of type `T` and its monitor, heartbeating every 50 ms if
/// asked to, as libzmq's `prep_server_socket`.
fn server<T: SocketType>(
    context: &Context,
    heartbeats: bool,
    curve: bool,
) -> (Socket<T>, Monitor, String) {
    let security = if curve {
        Security::curve_server(keypair(SERVER_SECRET))
    } else {
        Security::Null
    };
    let mut server = context.socket_with::<T>(SocketOptions {
        heartbeat: heartbeats.then(|| Heartbeat::every(Duration::from_millis(50))),
        security,
        ..SocketOptions::default()
    });
    let monitor = server.monitor();
    let endpoint = server.bind(&free_endpoint()).unwrap().to_string();
    (server, monitor, endpoint)
}

fn read_frame(stream: &mut TcpStream) -> Vec<u8> {
    let mut header = [0; 2];
    stream.read_exact(&mut header).unwrap();
    assert_eq!(header[0] & 0x02, 0, "a short frame");
    let mut body = vec![0; usize::from(header[1])];
    stream.read_exact(&mut body).unwrap();
    body
}

/// Speaks a ZMTP 3.0 DEALER's NULL handshake over `stream`, then, with
/// `mock_ping`, checks that PINGs are answered with their context, a long
/// one cut to 16 octets.
fn mock_handshake(stream: &mut TcpStream, mock_ping: bool) {
    stream.write_all(&ZMTP_GREETING_NULL).unwrap();
    let mut greeting = [0; 64];
    stream.read_exact(&mut greeting).unwrap();
    stream.write_all(ZMTP_READY_DEALER).unwrap();
    let ready = read_frame(stream);
    assert!(ready.starts_with(b"\x05READY"));

    if mock_ping {
        let ping = [4, 10, 4, b'P', b'I', b'N', b'G', 0, 0, b'L', b'O', b'L'];
        stream.write_all(&ping).unwrap();
        let mut long_ping = [b'z'; 67];
        long_ping[..12].copy_from_slice(&ping);
        long_ping[1] = 65;
        stream.write_all(&long_ping).unwrap();

        let mut pong = [0; 10];
        stream.read_exact(&mut pong).unwrap();
        assert_eq!(&pong, b"\x04\x08\x04PONGLOL");
        let mut long_pong = [0; 23];
        stream.read_exact(&mut long_pong).unwrap();
        assert_eq!(&long_pong, b"\x04\x15\x04PONGLOLzzzzzzzzzzzzz");
    }
}

/// A broken connection, or a stuck one whose peer never answers PINGs: an
/// accepted event, then a disconnect.
fn heartbeat_timeout(mock_ping: bool) {
    let context = Context::new().unwrap();
    let (_server, mut monitor, endpoint) = server::<Router>(&context, !mock_ping, false);
    let mut stream = TcpStream::connect(endpoint.trim_start_matches("tcp://")).unwrap();
    mock_handshake(&mut stream, mock_ping);

    assert_eq!(next_event(&mut monitor, FIVE), Some(EventKind::Accepted));
    if !mock_ping {
        assert_eq!(
            next_event(&mut monitor, FIVE),
            Some(EventKind::Disconnected)
        );
    }
}

#[test]
fn heartbeat_timeout_router() {
    heartbeat_timeout(false);
}

#[test]
fn heartbeat_timeout_router_mock_ping() {
    heartbeat_timeout(true);
}

/// A server that does not heartbeat itself drops a client that announces a
/// 100 ms TTL but pings only every 250 ms.
fn heartbeat_ttl<C: SocketType, S: SocketType>() {
    let context = Context::new().unwrap();
    let (_server, mut monitor, endpoint) = server::<S>(&context, false, false);
    let mut client = context.socket_with::<C>(SocketOptions {
        heartbeat: Some(Heartbeat {
            interval: Duration::from_millis(250),
            timeout: Duration::from_millis(250),
            ttl: Duration::from_millis(100),
        }),
        ..SocketOptions::default()
    });
    client.connect(&endpoint).unwrap();

    assert_eq!(next_event(&mut monitor, FIVE), Some(EventKind::Accepted));
    settle();
    assert_eq!(
        next_event(&mut monitor, FIVE),
        Some(EventKind::Disconnected)
    );
}

/// Normal operation: PINGs and PONGs flow, and the connection stays up.
fn heartbeat_notimeout<C: SocketType, S: SocketType>(curve: bool) {
    let context = Context::new().unwrap();
    let (_server, mut monitor, endpoint) = server::<S>(&context, true, curve);
    let mut client = context.socket_with::<C>(SocketOptions {
        security: if curve {
            Security::curve_client(
                keypair(CLIENT_SECRET),
                PublicKey::from_z85(SERVER_PUBLIC).unwrap(),
            )
        } else {
            Security::Null
        },
        ..SocketOptions::default()
    });
    client.connect(&endpoint).unwrap();
    settle();

    assert_eq!(next_event(&mut monitor, FIVE), Some(EventKind::Accepted));
    assert_eq!(
        next_event(&mut monitor, 3 * SETTLE),
        None,
        "still connected"
    );
}

macro_rules! heartbeat_tests {
    ($($client:ident $server:ident: $ttl:ident $notimeout:ident $with_curve:ident;)*) => {
        $(
            #[test]
            fn $ttl() {
                heartbeat_ttl::<$client, $server>();
            }

            #[test]
            fn $notimeout() {
                heartbeat_notimeout::<$client, $server>(false);
            }

            #[test]
            fn $with_curve() {
                heartbeat_notimeout::<$client, $server>(true);
            }
        )*
    };
}

heartbeat_tests! {
    Dealer Router: heartbeat_ttl_dealer_router heartbeat_notimeout_dealer_router
        heartbeat_notimeout_dealer_router_with_curve;
    Req Rep: heartbeat_ttl_req_rep heartbeat_notimeout_req_rep heartbeat_notimeout_req_rep_with_curve;
    Pull Push: heartbeat_ttl_pull_push heartbeat_notimeout_pull_push
        heartbeat_notimeout_pull_push_with_curve;
    Sub Pub: heartbeat_ttl_sub_pub heartbeat_notimeout_sub_pub heartbeat_notimeout_sub_pub_with_curve;
    Pair Pair: heartbeat_ttl_pair_pair heartbeat_notimeout_pair_pair
        heartbeat_notimeout_pair_pair_with_curve;
}

#[cfg(feature = "draft")]
mod draft {
    use zmq::{Client, Gather, Scatter, Server};

    use super::{heartbeat_notimeout, heartbeat_ttl};

    heartbeat_tests! {
        Gather Scatter: heartbeat_ttl_gather_scatter heartbeat_notimeout_gather_scatter
            heartbeat_notimeout_gather_scatter_with_curve;
        Client Server: heartbeat_ttl_client_server heartbeat_notimeout_client_server
            heartbeat_notimeout_client_server_with_curve;
    }
}

// libzmq's `test_setsockopt_heartbeat_*` set `ZMQ_HEARTBEAT_TTL` and read
// it back, rounded down to whole deciseconds, refusing more than 0xffff of
// them. zmq-rs has no getsockopt, so these read the TTL a client announces
// in its PINGs instead.

/// The TTL, in deciseconds, of the first PING a PAIR client heartbeating
/// with `ttl` sends to a hand-written ZMTP 3.1 server.
fn announced_ttl(ttl: Duration) -> u16 {
    let listener = TcpListener::bind(free_port()).unwrap();
    let endpoint = format!("tcp://{}", listener.local_addr().unwrap());
    let context = Context::new().unwrap();
    let mut client = context.socket_with::<Pair>(SocketOptions {
        heartbeat: Some(Heartbeat {
            ttl,
            ..Heartbeat::every(Duration::from_millis(10))
        }),
        ..SocketOptions::default()
    });
    client.connect(&endpoint).unwrap();

    let (mut stream, _) = listener.accept().unwrap();
    stream.set_read_timeout(Some(FIVE)).unwrap();
    let mut greeting = ZMTP_GREETING_NULL;
    greeting[11] = 1;
    stream.write_all(&greeting).unwrap();
    stream.read_exact(&mut [0; 64]).unwrap();
    stream
        .write_all(b"\x04\x1a\x05READY\x0bSocket-Type\x00\x00\x00\x04PAIR")
        .unwrap();
    loop {
        let frame = read_frame(&mut stream);
        if let Some(ping) = frame.strip_prefix(b"\x04PING") {
            return u16::from_be_bytes([ping[0], ping[1]]);
        }
    }
}

const DECISECOND: Duration = Duration::from_millis(100);

#[test]
fn setsockopt_heartbeat_ttl_max() {
    let max = DECISECOND * 0x10000 - Duration::from_millis(1);
    assert_eq!(announced_ttl(max), 0xffff);
}

/// libzmq refuses a TTL above the maximum with `EINVAL`; zmq-rs announces
/// the maximum (`Ttl::from_duration` saturates).
#[test]
fn setsockopt_heartbeat_ttl_more_than_max_fails() {
    assert_eq!(announced_ttl(DECISECOND * 0x10000), 0xffff);
}

/// libzmq rounds 99 ms down to no TTL at all; zmq-rs rounds up to whole
/// deciseconds (`Ttl::from_duration`), so a TTL asked for is never lost.
#[test]
fn setsockopt_heartbeat_ttl_near_zero() {
    assert_eq!(announced_ttl(DECISECOND - Duration::from_millis(1)), 1);
}
