//! Conflating sockets (`ZMQ_CONFLATE`) against libzmq.

use std::net::TcpListener;
use std::thread;
use std::time::{Duration, Instant};

use zmq::{Context, Message, Pull, Push, Sub};
use zmq_interop::libzmq::{Kind, Socket};

const FIVE: Duration = Duration::from_secs(5);

#[test]
fn our_conflating_pull_keeps_the_latest_from_a_libzmq_push() {
    let context = Context::new().unwrap();
    let mut pull = context.socket::<Pull>();
    pull.set_conflate(true);
    let bound = pull.bind("tcp://127.0.0.1:*").unwrap();
    let push = Socket::new(Kind::Push);
    push.connect(bound);
    for i in 1..=100 {
        push.send(&[i.to_string().as_bytes()]).unwrap();
    }
    // Time for all of them to arrive, each replacing the one before.
    thread::sleep(Duration::from_millis(200));
    assert_eq!(pull.try_recv(), Some(Message::from("100")));
    assert_eq!(pull.try_recv(), None);
}

#[test]
fn our_conflating_push_delivers_only_the_latest_to_a_libzmq_pull() {
    let port = TcpListener::bind("127.0.0.1:0")
        .unwrap()
        .local_addr()
        .unwrap()
        .port();
    let context = Context::new().unwrap();
    let mut push = context.socket::<Push>();
    push.set_conflate(true);
    push.connect(&format!("tcp://127.0.0.1:{port}")).unwrap();
    for i in 1..=10 {
        push.try_send(i.to_string()).expect("never full");
    }
    let pull = Socket::new(Kind::Pull);
    pull.bind(&format!("tcp://127.0.0.1:{port}"));
    assert_eq!(pull.recv().unwrap(), [b"10".to_vec()]);
    pull.set_receive_timeout(Duration::from_millis(100));
    assert!(pull.recv().is_err(), "nothing older follows");
}

#[test]
fn our_conflating_sub_keeps_every_subscription_with_a_libzmq_pub() {
    let publisher = Socket::new(Kind::Pub);
    let addr = publisher.bind_loopback();
    let context = Context::new().unwrap();
    let mut subscriber = context.socket::<Sub>();
    subscriber.set_conflate(true);
    subscriber.subscribe("a");
    subscriber.subscribe("b");
    subscriber.connect(&format!("tcp://{addr}")).unwrap();
    for topic in ["a", "b"] {
        let started = Instant::now();
        loop {
            assert!(started.elapsed() < FIVE, "{topic} never arrived");
            publisher.send(&[topic.as_bytes()]).unwrap();
            thread::sleep(Duration::from_millis(10));
            if subscriber.try_recv() == Some(Message::from(topic)) {
                break;
            }
        }
    }
}
