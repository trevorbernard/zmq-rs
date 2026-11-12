//! `ZMQ_SOCKS_PROXY` beyond libzmq's own test: a proxy that refuses, and
//! what does not go through one.

use std::io::{Read, Write};
use std::net::TcpListener;
use std::sync::mpsc;
use std::thread;
use std::time::Duration;

use zmq::{Context, Message, Pull, Push, Reconnect, SocketOptions, SocksProxy};
use zmq_test_ports::free_endpoint;

const FIVE: Duration = Duration::from_secs(5);

fn proxied(address: &str) -> SocketOptions {
    SocketOptions {
        socks_proxy: Some(SocksProxy::new(address).unwrap()),
        reconnect: Reconnect::Fixed {
            interval: Duration::from_millis(10),
        },
        ..SocketOptions::default()
    }
}

/// A proxy that answers every CONNECT with `reply`, reporting each client.
fn refusing_proxy(reply: u8) -> (String, mpsc::Receiver<()>) {
    let listener = TcpListener::bind("127.0.0.1:0").unwrap();
    let address = listener.local_addr().unwrap().to_string();
    let (seen, clients) = mpsc::channel();
    thread::spawn(move || {
        for client in listener.incoming() {
            let mut client = client.unwrap();
            let mut greeting = [0; 3];
            client.read_exact(&mut greeting).unwrap();
            assert_eq!(greeting, [5, 1, 0]);
            client.write_all(&[5, 0]).unwrap();
            let mut request = [0; 10];
            client.read_exact(&mut request).unwrap();
            client
                .write_all(&[5, reply, 0, 1, 0, 0, 0, 0, 0, 0])
                .unwrap();
            if seen.send(()).is_err() {
                return;
            }
        }
    });
    (address, clients)
}

/// libzmq closes the connection and tries again later, whatever the proxy
/// refused with.
#[test]
fn a_connection_the_proxy_refuses_is_tried_again() {
    let (address, clients) = refusing_proxy(5);
    let context = Context::new().unwrap();
    let mut push = context.socket::<Push>();
    let endpoint = push.bind(&free_endpoint()).unwrap().to_string();
    let mut pull = context.socket_with::<Pull>(proxied(&address));
    pull.connect(&endpoint).unwrap();

    for _ in 0..3 {
        clients.recv_timeout(FIVE).expect("another attempt");
    }
    assert_eq!(pull.try_recv().unwrap(), None);
}

#[cfg(unix)]
#[test]
fn ipc_and_inproc_connections_do_not_go_through_the_proxy() {
    let (address, clients) = refusing_proxy(5);
    let context = Context::new().unwrap();
    for endpoint in ["ipc://*", "inproc://past-the-proxy"] {
        let mut push = context.socket::<Push>();
        let bound = push.bind(endpoint).unwrap().to_string();
        let mut pull = context.socket_with::<Pull>(proxied(&address));
        pull.connect(&bound).unwrap();
        push.send_timeout("direct", FIVE).unwrap();
        let message = pull.recv_timeout(FIVE).unwrap().expect("the message");
        assert_eq!(message, Message::from("direct"));
    }
    assert!(clients.try_recv().is_err(), "the proxy saw no one");
}

/// A bind listens where it is told: the proxy is for connections out.
#[test]
fn binds_do_not_go_through_the_proxy() {
    let (address, clients) = refusing_proxy(5);
    let context = Context::new().unwrap();
    let mut pull = context.socket_with::<Pull>(proxied(&address));
    let endpoint = pull.bind(&free_endpoint()).unwrap().to_string();
    let mut push = context.socket::<Push>();
    push.connect(&endpoint).unwrap();
    push.send_timeout("direct", FIVE).unwrap();
    let message = pull.recv_timeout(FIVE).unwrap().expect("the message");
    assert_eq!(message, Message::from("direct"));
    assert!(clients.try_recv().is_err(), "the proxy saw no one");
}

#[test]
fn a_proxy_address_needs_a_host_and_a_port() {
    for address in ["", "proxy", ":1080", "proxy:", "proxy:0", "*:1080"] {
        assert!(SocksProxy::new(address).is_err(), "{address}");
    }
    for address in [
        "proxy:1080",
        "127.0.0.1:1080",
        "[::1]:1080",
        "127.0.0.1:0;proxy:1080",
    ] {
        assert!(SocksProxy::new(address).is_ok(), "{address}");
    }
}
