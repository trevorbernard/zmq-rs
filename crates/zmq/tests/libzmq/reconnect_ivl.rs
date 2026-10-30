//! libzmq's `tests/test_reconnect_ivl.cpp`: with reconnection off
//! (`ZMQ_RECONNECT_IVL` -1, zmq-rs's `Reconnect::Never`), a connection that
//! breaks stays broken, even once the endpoint is bound again, until the
//! peer connects anew.

use std::net::TcpListener;
use std::time::Duration;

use zmq::{Context, Pair, Reconnect, Socket, SocketOptions};
use zmq_test_ports::{free_endpoint, free_port};

use crate::support::{FIVE, bounce_content, bounce_pair, within};

/// How long `expect_bounce_fail` waits, as libzmq's 250 ms timeouts.
const BRIEFLY: Duration = Duration::from_millis(250);

/// As libzmq's `expect_bounce_fail`: whether or not the sends succeed,
/// neither side receives anything.
fn expect_bounce_fail(server: &mut Socket<Pair>, client: &mut Socket<Pair>) {
    let _ = client.send_timeout(bounce_content(), BRIEFLY);
    assert_eq!(server.recv_timeout(BRIEFLY), None);
    let _ = server.send_timeout(bounce_content(), BRIEFLY);
    assert_eq!(client.recv_timeout(BRIEFLY), None);
}

fn reconnect_ivl_against_pair_socket(context: &Context, server: &mut Socket<Pair>, endpoint: &str) {
    let mut client = context.socket_with::<Pair>(SocketOptions {
        reconnect: Reconnect::Never,
        ..SocketOptions::default()
    });
    client.connect(endpoint).unwrap();
    bounce_pair(server, &mut client);

    server.unbind(endpoint).unwrap();
    expect_bounce_fail(server, &mut client);

    server.bind(endpoint).unwrap();
    expect_bounce_fail(server, &mut client);

    client.connect(endpoint).unwrap();
    bounce_pair(server, &mut client);
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
    reconnect_ivl(&free_endpoint());
}

#[test]
fn reconnect_ivl_tcp_ipv6() {
    if TcpListener::bind("[::1]:0").is_err() {
        eprintln!("ipv6 is not available");
        return;
    }
    reconnect_ivl(&format!("tcp://[::1]:{}", free_port().port()));
}
