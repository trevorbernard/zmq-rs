//! libzmq's `tests/test_reconnect_options.cpp`: what a connecting socket's
//! monitor hears when its peer goes away, and how `ZMQ_RECONNECT_STOP`
//! (zmq-rs's `set_reconnect_stop`, with the `draft` feature) ends the
//! retrying.
//!
//! zmq-rs's monitor reports neither `ZMQ_EVENT_CONNECT_DELAYED` nor a
//! `ZMQ_EVENT_CLOSED` for a connection attempt, so where libzmq's test
//! ends on a `CLOSED`, these check that no retry follows.

use std::time::Duration;

use zmq::{
    Context, EventKind, Monitor, Pub, Reconnect, RecvTimeoutError, Socket, SocketOptions, Sub,
};
use zmq_test_ports::free_endpoint;

use crate::support::{FIVE, SETTLE, zero_linger};

fn expect(monitor: &mut Monitor, kind: EventKind) {
    let event = monitor.recv_timeout(FIVE).expect("an event");
    assert_eq!(event.kind, kind);
}

fn expect_retry(monitor: &mut Monitor) {
    let event = monitor.recv_timeout(FIVE).expect("an event");
    assert!(
        matches!(event.kind, EventKind::ConnectRetried { .. }),
        "{event:?}"
    );
}

fn expect_nothing(monitor: &mut Monitor, wait: Duration) {
    assert_eq!(
        monitor.recv_timeout(wait).map(|event| event.kind),
        Err(RecvTimeoutError::Timeout)
    );
}

fn publisher(context: &Context, endpoint: &str) -> Socket<Pub> {
    let mut publisher = context.socket_with::<Pub>(zero_linger());
    publisher.bind(endpoint).unwrap();
    publisher
}

fn subscriber(context: &Context, interval: Duration) -> Socket<Sub> {
    context.socket_with::<Sub>(SocketOptions {
        reconnect: Reconnect::Fixed { interval },
        ..zero_linger()
    })
}

/// Connects to a publisher at `endpoint` that then goes away, as each of
/// libzmq's cases starts.
fn lose_the_publisher(context: &Context, sub: &mut Socket<Sub>, monitor: &mut Monitor) -> String {
    let endpoint = free_endpoint();
    let publisher = publisher(context, &endpoint);
    sub.connect(&endpoint).unwrap();
    expect(monitor, EventKind::Connected);
    expect(monitor, EventKind::HandshakeSucceeded);
    drop(publisher);
    expect(monitor, EventKind::Disconnected);
    expect_retry(monitor);
    endpoint
}

#[test]
fn reconnect_default() {
    let context = Context::new().unwrap();
    // So long that only one reconnection is scheduled.
    let mut sub = subscriber(&context, Duration::from_secs(60));
    let mut monitor = sub.monitor();
    lose_the_publisher(&context, &mut sub, &mut monitor);
    expect_nothing(&mut monitor, SETTLE);
}

#[test]
fn reconnect_success() {
    let context = Context::new().unwrap();
    let mut sub = subscriber(&context, Duration::from_secs(1));
    let mut monitor = sub.monitor();
    let endpoint = lose_the_publisher(&context, &mut sub, &mut monitor);
    expect_nothing(&mut monitor, SETTLE);

    let _publisher = publisher(&context, &endpoint);
    expect(&mut monitor, EventKind::Connected);
    expect(&mut monitor, EventKind::HandshakeSucceeded);
    expect_nothing(&mut monitor, SETTLE);
}

#[cfg(feature = "draft")]
mod stop {
    use std::net::TcpListener;

    use zmq::{HandshakeFailure, ReconnectStop};
    use zmq_test_ports::free_port;

    use super::*;

    /// Several of the default 100 to 200 ms retry intervals.
    const RETRIES: Duration = Duration::from_secs(1);

    #[test]
    fn reconnect_stop_on_refused() {
        let context = Context::new().unwrap();
        let mut sub = context.socket_with::<Sub>(zero_linger());
        sub.set_reconnect_stop(ReconnectStop {
            connection_refused: true,
            ..ReconnectStop::default()
        });
        let mut monitor = sub.monitor();
        lose_the_publisher(&context, &mut sub, &mut monitor);
        // The one retry is refused, and is the last.
        expect_nothing(&mut monitor, RETRIES);
    }

    /// Without the option, the same refusals are retried for good.
    #[test]
    fn a_refusal_is_retried_by_default() {
        let context = Context::new().unwrap();
        let mut sub = context.socket_with::<Sub>(zero_linger());
        let mut monitor = sub.monitor();
        lose_the_publisher(&context, &mut sub, &mut monitor);
        expect_retry(&mut monitor);
        expect_retry(&mut monitor);
    }

    #[test]
    fn reconnect_stop_on_handshake_failed() {
        // libzmq's is a STREAM socket: it accepts and says nothing.
        let dummy = TcpListener::bind(free_port()).unwrap();
        let endpoint = format!("tcp://{}", dummy.local_addr().unwrap());

        let context = Context::new().unwrap();
        let mut sub = context.socket_with::<Sub>(SocketOptions {
            handshake_timeout: Some(Duration::from_secs(1)),
            ..zero_linger()
        });
        sub.set_reconnect_stop(ReconnectStop {
            handshake_failed: true,
            ..ReconnectStop::default()
        });
        let mut monitor = sub.monitor();
        sub.connect(&endpoint).unwrap();

        expect(&mut monitor, EventKind::Connected);
        expect(
            &mut monitor,
            EventKind::HandshakeFailed(HandshakeFailure::NoDetail),
        );
        expect(&mut monitor, EventKind::Disconnected);
        expect_nothing(&mut monitor, RETRIES);
    }

    /// Without the option, a handshake that times out is tried again.
    #[test]
    fn a_handshake_timeout_is_retried_by_default() {
        let dummy = TcpListener::bind(free_port()).unwrap();
        let endpoint = format!("tcp://{}", dummy.local_addr().unwrap());

        let context = Context::new().unwrap();
        let mut sub = context.socket_with::<Sub>(SocketOptions {
            handshake_timeout: Some(Duration::from_secs(1)),
            ..zero_linger()
        });
        let mut monitor = sub.monitor();
        sub.connect(&endpoint).unwrap();

        expect(&mut monitor, EventKind::Connected);
        expect(
            &mut monitor,
            EventKind::HandshakeFailed(HandshakeFailure::NoDetail),
        );
        expect(&mut monitor, EventKind::Disconnected);
        expect_retry(&mut monitor);
    }

    /// libzmq's needs `ZMQ_RECONNECT_STOP_AFTER_DISCONNECT` for this;
    /// zmq-rs never retries an endpoint the socket has disconnected.
    #[cfg(unix)]
    #[test]
    fn reconnect_stop_after_disconnect() {
        let context = Context::new().unwrap();
        // An endpoint that cannot be connected to: nothing is bound to it
        // any more.
        let endpoint = context.socket::<Pub>().bind("ipc://*").unwrap().to_string();

        let mut sub = context.socket_with::<Sub>(zero_linger());
        let mut monitor = sub.monitor();
        sub.connect(&endpoint).unwrap();
        expect_retry(&mut monitor);

        sub.disconnect(&endpoint).unwrap();
        // One retry may have been on its way.
        if let Ok(event) = monitor.recv_timeout(SETTLE) {
            assert!(
                matches!(event.kind, EventKind::ConnectRetried { .. }),
                "{event:?}"
            );
        }
        expect_nothing(&mut monitor, RETRIES);
    }
}
