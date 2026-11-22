//! When a connection is tried again, compared with libzmq through both
//! libraries' monitors. Nothing listens at the endpoints connected to.

use std::time::{Duration, Instant};

use zmq::{Context, EventKind, Monitor, Push, Reconnect, SocketOptions};
use zmq_interop::libzmq::{self, Kind, Socket};
use zmq_test_ports::free_endpoint;

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

/// A `tcp` endpoint and an `ipc` one, with nothing listening at either.
fn dead_endpoints() -> [String; 2] {
    let path = std::env::temp_dir().join(format!("zmq-interop-nobody-{}", std::process::id()));
    [free_endpoint(), format!("ipc://{}", path.display())]
}

fn our_push(reconnect: Reconnect) -> (zmq::Socket<Push>, Monitor) {
    our_push_to(&free_endpoint(), reconnect)
}

fn our_push_to(endpoint: &str, reconnect: Reconnect) -> (zmq::Socket<Push>, Monitor) {
    let context = Context::new().unwrap();
    let mut push = context.socket_with::<Push>(SocketOptions::new().reconnect(reconnect));
    let monitor = push.monitor();
    push.connect(endpoint).unwrap();
    (push, monitor)
}

fn libzmq_push(interval: i32, max: i32) -> (Socket, libzmq::Monitor) {
    libzmq_push_to(&free_endpoint(), interval, max)
}

fn libzmq_push_to(endpoint: &str, interval: i32, max: i32) -> (Socket, libzmq::Monitor) {
    let push = Socket::new(Kind::Push);
    push.set_reconnect(interval, max);
    let monitor = push.monitor();
    push.connect_str(endpoint);
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

/// Our events for as long as `window`.
fn our_events(monitor: &mut Monitor, window: Duration) -> Vec<EventKind> {
    let until = Instant::now() + window;
    std::iter::from_fn(|| {
        let left = until.checked_duration_since(Instant::now())?;
        monitor.recv_timeout(left).ok()?
    })
    .map(|event| event.kind)
    .collect()
}

#[test]
fn libzmq_never_retries_with_an_interval_of_zero() {
    let (_push, monitor) = libzmq_push(0, 0);
    let events = monitor.until_quiet(MS(300));
    assert!(
        events
            .iter()
            .all(|(event, _)| *event != libzmq::EVENT_CONNECT_RETRIED),
        "{events:?}"
    );
    assert!(!events.is_empty(), "the attempt itself is reported");
}

#[test]
fn we_never_retry_with_an_interval_of_zero() {
    for reconnect in [
        Reconnect::Fixed {
            interval: Duration::ZERO,
        },
        Reconnect::Backoff {
            initial: Duration::ZERO,
            max: MS(40),
        },
    ] {
        let (_push, mut monitor) = our_push(reconnect);
        let events = our_events(&mut monitor, MS(300));
        assert!(
            events
                .iter()
                .all(|kind| !matches!(kind, EventKind::ConnectRetried { .. })),
            "{reconnect:?}: {events:?}"
        );
    }
}

#[test]
fn libzmq_reports_closed_before_each_retry() {
    for endpoint in dead_endpoints() {
        let (_push, monitor) = libzmq_push_to(&endpoint, 10, 10);
        let events: Vec<u16> = monitor
            .during(MS(200))
            .into_iter()
            .map(|(event, _)| event)
            .filter(|event| [libzmq::EVENT_CLOSED, libzmq::EVENT_CONNECT_RETRIED].contains(event))
            .collect();
        assert!(events.len() >= 4, "{endpoint}: {events:?}");
        for pair in events.as_chunks::<2>().0 {
            assert_eq!(
                *pair,
                [libzmq::EVENT_CLOSED, libzmq::EVENT_CONNECT_RETRIED],
                "{endpoint}: {events:?}"
            );
        }
    }
}

#[test]
fn we_report_closed_before_each_retry() {
    for endpoint in dead_endpoints() {
        let (_push, mut monitor) = our_push_to(
            &endpoint,
            Reconnect::Backoff {
                initial: MS(10),
                max: MS(10),
            },
        );
        let events = our_events(&mut monitor, MS(200));
        assert!(events.len() >= 4, "{endpoint}: {events:?}");
        for pair in events.as_chunks::<2>().0 {
            assert!(
                matches!(pair, [EventKind::Closed, EventKind::ConnectRetried { .. }]),
                "{endpoint}: {events:?}"
            );
        }
    }
}
