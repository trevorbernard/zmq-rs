//! libzmq's `tests/test_reconnect_ivl.cpp`: with reconnection off
//! (`ZMQ_RECONNECT_IVL` -1, zmq-rs's `Reconnect::Never`), a connection that
//! breaks stays broken, even once the endpoint is bound again, until the
//! peer connects anew.

use std::net::TcpListener;
use std::time::Duration;

use zmq::{Context, Message, Pair, Reconnect, Socket, SocketOptions};

use crate::support::{FIVE, within};

/// How long `expect_bounce_fail` waits, as libzmq's 250 ms timeouts.
const BRIEFLY: Duration = Duration::from_millis(250);

fn content() -> Message {
    let mut message = Message::new("12345678ABCDEFGH12345678abcdefgh");
    message.push("12345678ABCDEFGH12345678abcdefgh");
    message
}

/// A message from `client` to `server` and back, as libzmq's `bounce`.
fn bounce(server: &mut Socket<Pair>, client: &mut Socket<Pair>) {
    client.send_timeout(content(), FIVE).expect("sent");
    let received = server.recv_timeout(FIVE).expect("the request");
    assert_eq!(received, content());
    server.send_timeout(received, FIVE).expect("sent back");
    assert_eq!(client.recv_timeout(FIVE), Some(content()));
}

/// As libzmq's `expect_bounce_fail`: whether or not the sends succeed,
/// neither side receives anything.
fn expect_bounce_fail(server: &mut Socket<Pair>, client: &mut Socket<Pair>) {
    let _ = client.send_timeout(content(), BRIEFLY);
    assert_eq!(server.recv_timeout(BRIEFLY), None);
    let _ = server.send_timeout(content(), BRIEFLY);
    assert_eq!(client.recv_timeout(BRIEFLY), None);
}

fn reconnect_ivl_against_pair_socket(context: &Context, server: &mut Socket<Pair>, endpoint: &str) {
    let mut client = context.socket_with::<Pair>(SocketOptions {
        reconnect: Reconnect::Never,
        ..SocketOptions::default()
    });
    client.connect(endpoint).unwrap();
    bounce(server, &mut client);

    server.unbind(endpoint).unwrap();
    expect_bounce_fail(server, &mut client);

    server.bind(endpoint).unwrap();
    expect_bounce_fail(server, &mut client);

    client.connect(endpoint).unwrap();
    bounce(server, &mut client);
}

fn reconnect_ivl(bind: &str) {
    within(FIVE * 2, {
        let bind = bind.to_owned();
        move || {
            let context = Context::new().unwrap();
            let mut server = context.socket::<Pair>();
            let endpoint = server.bind(&bind).unwrap().to_string();
            reconnect_ivl_against_pair_socket(&context, &mut server, &endpoint);
        }
    });
}

// A path of its own, as libzmq's make_random_ipc_endpoint: `ipc://*`'s
// directory goes when unbound, so its path could not be bound again.
#[cfg(unix)]
#[test]
fn reconnect_ivl_ipc() {
    let path =
        std::env::temp_dir().join(format!("zmq-rs-reconnect-ivl-{}.ipc", std::process::id()));
    reconnect_ivl(&format!("ipc://{}", path.display()));
}

#[test]
fn reconnect_ivl_tcp_ipv4() {
    reconnect_ivl("tcp://127.0.0.1:*");
}

#[test]
fn reconnect_ivl_tcp_ipv6() {
    if TcpListener::bind("[::1]:0").is_err() {
        eprintln!("ipv6 is not available");
        return;
    }
    reconnect_ivl("tcp://[::1]:*");
}
