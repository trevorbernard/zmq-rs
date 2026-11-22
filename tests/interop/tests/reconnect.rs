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

/// An `ipc` endpoint that refuses connections: a socket file nothing
/// listens on.
fn refusing_ipc(name: &str) -> String {
    let path = std::env::temp_dir().join(format!("zmq-interop-{name}-{}", std::process::id()));
    let _ = std::fs::remove_file(&path);
    drop(std::os::unix::net::UnixListener::bind(&path).unwrap());
    format!("ipc://{}", path.display())
}

/// How often libzmq retries each of `endpoints` in the 200 ms after it
/// has disconnected from the first, having connected to them all with
/// `stop` as its `ZMQ_RECONNECT_STOP`. With `driven`, the socket is called
/// once more after the disconnect.
fn libzmq_retries_after_disconnecting(endpoints: &[&str], stop: i32, driven: bool) -> Vec<usize> {
    let push = Socket::new(Kind::Push);
    push.set_reconnect(10, 10);
    push.set_reconnect_stop(stop);
    let monitor = push.monitor();
    for endpoint in endpoints {
        push.connect_str(endpoint);
    }
    monitor.values_of(libzmq::EVENT_CONNECT_RETRIED, 4);
    push.disconnect(endpoints[0]);
    // Attempts under way finish.
    monitor.during(MS(50));
    if driven {
        push.events();
        monitor.during(MS(50));
    }
    let retried = monitor.endpoints_of(libzmq::EVENT_CONNECT_RETRIED, MS(200));
    endpoints
        .iter()
        .map(|endpoint| retried.iter().filter(|seen| seen == endpoint).count())
        .collect()
}

/// libzmq stops retrying a disconnected endpoint when its socket next
/// runs, not when `zmq_disconnect` returns.
#[test]
fn libzmq_retries_a_disconnected_endpoint_until_its_socket_is_next_called() {
    if !libzmq::has_drafts() {
        return;
    }
    let [tcp, missing] = dead_endpoints();
    let refusing = refusing_ipc("until-called");
    for endpoints in [
        [&*tcp, &*refusing, &*missing],
        [&*refusing, &*missing, &*tcp],
        [&*missing, &*tcp, &*refusing],
    ] {
        let idle = libzmq_retries_after_disconnecting(&endpoints, 0, false);
        assert!(
            idle.iter().all(|retries| *retries > 0),
            "{endpoints:?}: {idle:?}"
        );
        let driven = libzmq_retries_after_disconnecting(&endpoints, 0, true);
        assert_eq!(driven[0], 0, "{endpoints:?}: {driven:?}");
        assert!(driven[1] > 0 && driven[2] > 0, "{endpoints:?}: {driven:?}");
    }
}

/// `ZMQ_RECONNECT_STOP_AFTER_DISCONNECT` ends the retrying of every `ipc`
/// endpoint that refuses, disconnected or not, once any endpoint has been
/// disconnected, and of nothing else.
#[test]
fn libzmq_with_stop_after_disconnect_gives_up_on_every_refusing_ipc_endpoint() {
    if !libzmq::has_drafts() {
        return;
    }
    let stop = libzmq::RECONNECT_STOP_AFTER_DISCONNECT;
    let [tcp, missing] = dead_endpoints();
    let (first, second) = (refusing_ipc("stop-first"), refusing_ipc("stop-second"));

    let retries =
        libzmq_retries_after_disconnecting(&[&first, &second, &tcp, &missing], stop, false);
    assert_eq!(retries[..2], [0, 0], "{retries:?}");
    assert!(retries[2] > 0 && retries[3] > 0, "{retries:?}");

    let retries =
        libzmq_retries_after_disconnecting(&[&tcp, &first, &second, &missing], stop, false);
    assert_eq!(retries[1..3], [0, 0], "{retries:?}");
    assert!(retries[0] > 0 && retries[3] > 0, "{retries:?}");
}

#[test]
fn we_stop_retrying_the_disconnected_endpoint_alone_and_at_once() {
    let [tcp, missing] = dead_endpoints();
    let refusing = refusing_ipc("ours");
    for endpoints in [
        [&*tcp, &*refusing, &*missing],
        [&*refusing, &*missing, &*tcp],
        [&*missing, &*tcp, &*refusing],
    ] {
        let context = Context::new().unwrap();
        let mut push = context.socket_with::<Push>(
            SocketOptions::new().reconnect(Reconnect::Fixed { interval: MS(10) }),
        );
        let mut monitor = push.monitor();
        for endpoint in endpoints {
            push.connect(endpoint).unwrap();
        }
        our_intervals(&mut monitor, 4);
        push.disconnect(endpoints[0]).unwrap();
        // Attempts under way finish.
        our_events(&mut monitor, MS(50));
        let until = Instant::now() + MS(200);
        let mut retried = Vec::new();
        while let Some(left) = until.checked_duration_since(Instant::now()) {
            if let Ok(Some(event)) = monitor.recv_timeout(left)
                && matches!(event.kind, EventKind::ConnectRetried { .. })
            {
                retried.push(event.endpoint);
            }
        }
        let retries =
            endpoints.map(|endpoint| retried.iter().filter(|seen| *seen == endpoint).count());
        assert_eq!(retries[0], 0, "{endpoints:?}: {retries:?}");
        assert!(
            retries[1] > 0 && retries[2] > 0,
            "{endpoints:?}: {retries:?}"
        );
    }
}
