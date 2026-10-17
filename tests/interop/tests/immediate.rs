//! `ZMQ_IMMEDIATE` connections against libzmq.

use std::net::TcpListener;
use std::thread;
use std::time::{Duration, Instant};

use zmq::{Context, Push, Reconnect, SocketOptions};
use zmq_interop::libzmq::{Kind, Socket};

#[test]
fn our_immediate_push_sends_only_to_the_libzmq_pull_that_is_up() {
    let dead = TcpListener::bind("127.0.0.1:0")
        .unwrap()
        .local_addr()
        .unwrap();
    let pull = Socket::new(Kind::Pull);
    let alive = pull.bind_loopback();
    let context = Context::new().unwrap();
    let mut push = context.socket_with::<Push>(SocketOptions {
        immediate: true,
        reconnect: Reconnect::Fixed {
            interval: Duration::from_millis(10),
        },
        ..SocketOptions::default()
    });
    push.connect(&format!("tcp://{dead}")).unwrap();
    push.connect(&format!("tcp://{alive}")).unwrap();
    let started = Instant::now();
    while push.try_send("0").is_err() {
        assert!(
            started.elapsed() < Duration::from_secs(5),
            "never connected"
        );
        thread::sleep(Duration::from_millis(5));
    }
    for i in 1..10 {
        push.send(i.to_string());
    }
    for i in 0..10 {
        assert_eq!(pull.recv().unwrap(), [i.to_string().into_bytes()]);
    }
}
