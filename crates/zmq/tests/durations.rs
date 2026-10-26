//! Durations too long for an `Instant` to reach mean never, rather than
//! overflowing on the I/O thread and taking every connection with it.

use std::net::TcpListener;
use std::time::Duration;

use zmq::{Context, Heartbeat, Linger, Message, Pull, Push, Reconnect, SocketOptions};

const FIVE: Duration = Duration::from_secs(5);

fn forever() -> SocketOptions {
    SocketOptions {
        linger: Linger::For(Duration::MAX),
        handshake_timeout: Some(Duration::MAX),
        heartbeat: Some(Heartbeat {
            interval: Duration::MAX,
            timeout: Duration::MAX,
            ttl: Duration::MAX,
        }),
        reconnect: Reconnect::Backoff {
            initial: Duration::MAX,
            max: Duration::MAX,
        },
        ..SocketOptions::default()
    }
}

/// A PUSH and a PULL with `options`, connected over TCP, exchange a
/// message.
fn exchange(context: &Context, options: &SocketOptions) {
    let mut pull = context.socket_with::<Pull>(options.clone());
    let bound = pull.bind("tcp://127.0.0.1:*").unwrap();
    let mut push = context.socket_with::<Push>(options.clone());
    push.connect(&bound.to_string()).unwrap();
    push.send("through");
    assert_eq!(pull.recv_timeout(FIVE), Some(Message::from("through")));
}

#[test]
fn durations_too_long_to_reach_mean_never() {
    let context = Context::new().unwrap();
    // Refused, so it waits out a backoff too long to reach.
    let unused = TcpListener::bind("127.0.0.1:0")
        .unwrap()
        .local_addr()
        .unwrap();
    let mut refused = context.socket_with::<Push>(forever());
    refused.connect(&format!("tcp://{unused}")).unwrap();
    // Connected: it heartbeats, times its handshake and lingers.
    exchange(&context, &forever());
    // The I/O thread is still there for everyone else.
    exchange(&context, &SocketOptions::default());
}
