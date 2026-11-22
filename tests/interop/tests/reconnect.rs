//! When a connection is tried again, compared with libzmq through both
//! libraries' monitors. Nothing listens at the endpoints connected to.

use std::time::Duration;

use zmq::{Context, EventKind, Monitor, Push, Reconnect, SocketOptions};
use zmq_interop::libzmq::{self, Kind, Socket};
use zmq_test_ports::{free_endpoint, free_port};

const MS: fn(u64) -> Duration = Duration::from_millis;

/// The intervals of our first `count` retries.
fn our_intervals(monitor: &mut Monitor, count: usize) -> Vec<Duration> {
    let mut intervals = Vec::new();
    while intervals.len() < count {
        let event = monitor
            .recv_timeout(Duration::from_secs(5))
            .unwrap()
            .unwrap_or_else(|| panic!("a retry after {intervals:?}"));
        if let EventKind::ConnectRetried { interval } = event.kind {
            intervals.push(interval);
        }
    }
    intervals
}

fn our_push(reconnect: Reconnect) -> (zmq::Socket<Push>, Monitor) {
    let context = Context::new().unwrap();
    let mut push = context.socket_with::<Push>(SocketOptions::new().reconnect(reconnect));
    let monitor = push.monitor();
    push.connect(free_endpoint()).unwrap();
    (push, monitor)
}

fn libzmq_push(interval: i32, max: i32) -> (Socket, libzmq::Monitor) {
    let push = Socket::new(Kind::Push);
    push.set_reconnect(interval, max);
    let monitor = push.monitor();
    push.connect(free_port());
    (push, monitor)
}

#[test]
fn libzmq_backs_off_without_jitter() {
    let (_push, monitor) = libzmq_push(10, 40);
    assert_eq!(
        monitor.values_of(libzmq::EVENT_CONNECT_RETRIED, 5),
        [10, 20, 40, 40, 40]
    );
}

#[test]
fn we_back_off_without_jitter() {
    let (_push, mut monitor) = our_push(Reconnect::Backoff {
        initial: MS(10),
        max: MS(40),
    });
    assert_eq!(
        our_intervals(&mut monitor, 5),
        [MS(10), MS(20), MS(40), MS(40), MS(40)]
    );
}
