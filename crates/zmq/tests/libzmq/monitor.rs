//! libzmq's `tests/test_monitor.cpp`: the events a connecting and a
//! listening socket report, from listening to closing.
//!
//! zmq-rs's monitor is a handle from `Socket::monitor`, not an inproc
//! endpoint read by another socket, and it ends, as libzmq's
//! `ZMQ_EVENT_MONITOR_STOPPED` does, when the channel closes. So these are
//! not ported: `test_monitor_invalid_protocol_fails` and
//! `test_monitor_versioned_invalid_socket_type`, which check the endpoint
//! and the reading socket's type; the `_tipc` case, as zmq-rs has no TIPC;
//! and the `test_monitor_versioned_stats_*` cases, as it has no
//! `ZMQ_EVENT_PIPES_STATS`. An event carries one endpoint where libzmq's
//! version 2 events carry a local and a remote address: the connecting
//! socket's events name what it connected to, and the listening socket's
//! name where it is bound.

use std::net::TcpListener;
use std::time::Duration;

use zmq::{Context, Dealer, Event, EventKind, Linger, Monitor, RecvTimeoutError, SocketOptions};
use zmq_test_ports::{free_endpoint, free_port};

use crate::support::{FIVE, bounce_dealer, within};

/// The next event, or `None` once the monitor has stopped.
fn next_event(monitor: &mut Monitor) -> Option<Event> {
    match monitor.recv_timeout(FIVE) {
        Ok(event) => Some(event),
        Err(RecvTimeoutError::Closed) => None,
        Err(RecvTimeoutError::Timeout) => panic!("no event within {FIVE:?}"),
    }
}

fn expect_event(monitor: &mut Monitor, kind: EventKind, endpoint: &str) {
    let event = next_event(monitor).expect("the monitor is still running");
    assert_eq!(event.kind, kind);
    assert_eq!(event.endpoint, endpoint);
}

/// As libzmq's `test_monitor_versioned_basic`: a client and a server,
/// each monitored, bounce a message and close.
fn monitor_basic(bind: &str) {
    let bind = bind.to_owned();
    within(FIVE * 2, move || {
        let context = Context::new().unwrap();
        // libzmq closes them with zero linger.
        let options = SocketOptions {
            linger: Linger::For(Duration::ZERO),
            ..SocketOptions::default()
        };
        let mut client = context.socket_with::<Dealer>(options.clone());
        let mut server = context.socket_with::<Dealer>(options);
        let mut client_monitor = client.monitor();
        let mut server_monitor = server.monitor();

        let endpoint = server.bind(&bind).unwrap().to_string();
        client.connect(&endpoint).unwrap();
        bounce_dealer(&mut server, &mut client);
        drop(client);
        drop(server);

        expect_event(&mut client_monitor, EventKind::Connected, &endpoint);
        expect_event(
            &mut client_monitor,
            EventKind::HandshakeSucceeded,
            &endpoint,
        );
        // The server may close first; the client then sees the connection
        // break before its own close ends its retries.
        if let Some(event) = next_event(&mut client_monitor) {
            assert_eq!(event.kind, EventKind::Disconnected);
            assert_eq!(event.endpoint, endpoint);
            while let Some(event) = next_event(&mut client_monitor) {
                assert!(
                    matches!(event.kind, EventKind::ConnectRetried { .. }),
                    "unexpected {event:?}"
                );
            }
        }

        expect_event(&mut server_monitor, EventKind::Listening, &endpoint);
        expect_event(&mut server_monitor, EventKind::Accepted, &endpoint);
        expect_event(
            &mut server_monitor,
            EventKind::HandshakeSucceeded,
            &endpoint,
        );
        // Then the listener closing and the connection ending, in either
        // order, as the server may see the client close first.
        while let Some(event) = next_event(&mut server_monitor) {
            assert!(
                matches!(event.kind, EventKind::Closed | EventKind::Disconnected),
                "unexpected {event:?}"
            );
            assert_eq!(event.endpoint, endpoint);
        }
    });
}

#[test]
fn monitor_basic_tcp() {
    monitor_basic(&free_endpoint());
}

#[test]
fn monitor_versioned_basic_tcp_ipv4() {
    monitor_basic(&free_endpoint());
}

#[test]
fn monitor_versioned_basic_tcp_ipv6() {
    if TcpListener::bind("[::1]:0").is_err() {
        eprintln!("ipv6 is not available");
        return;
    }
    monitor_basic(&format!("tcp://[::1]:{}", free_port().port()));
}

#[cfg(unix)]
#[test]
fn monitor_versioned_basic_ipc() {
    monitor_basic("ipc://*");
}
