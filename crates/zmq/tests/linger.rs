//! Dropping the last context waits for queued messages, within each
//! socket's linger, as libzmq's `zmq_ctx_term`.

use std::net::TcpListener;
use std::sync::mpsc;
use std::thread;
use std::time::Duration;

use zmq::{Context, Hwm, Linger, Pull, Push, Rep, Req, SocketOptions};

const FIVE: Duration = Duration::from_secs(5);

fn within<T: Send + 'static>(limit: Duration, f: impl FnOnce() -> T + Send + 'static) -> T {
    let (done, result) = mpsc::channel();
    thread::spawn(move || {
        let _ = done.send(f());
    });
    result
        .recv_timeout(limit)
        .unwrap_or_else(|_| panic!("timed out after {limit:?}"))
}

/// A TCP endpoint nothing listens on.
fn unused_endpoint() -> String {
    let listener = TcpListener::bind("127.0.0.1:0").unwrap();
    format!("tcp://{}", listener.local_addr().unwrap())
}

#[test]
fn dropping_the_context_delivers_what_was_queued() {
    const COUNT: usize = 100_000;
    let receiving = Context::new().unwrap();
    let mut pull = receiving.socket::<Pull>();
    let endpoint = pull.bind("tcp://127.0.0.1:*").unwrap().to_string();

    let receiver = thread::spawn(move || {
        let mut count = 0;
        while pull.recv() != "END".into() {
            count += 1;
        }
        count
    });
    within(FIVE * 6, move || {
        let context = Context::new().unwrap();
        // So nearly everything is still queued when the context goes.
        let mut push = context.socket_with::<Push>(SocketOptions {
            send_hwm: Hwm::Unlimited,
            ..SocketOptions::default()
        });
        push.connect(&endpoint).unwrap();
        // Too much for the kernel's socket buffers to take at once.
        for _ in 0..COUNT {
            push.send(vec![0; 1000]);
        }
        push.send("END");
    });
    let received = within(FIVE * 6, move || receiver.join().unwrap());
    assert_eq!(received, COUNT);
}

#[test]
fn a_reply_sent_just_before_its_socket_drops_is_delivered() {
    // The session may look at its pipe, then the socket sends and drops,
    // then the session checks whether the socket is gone: it must not
    // take the earlier empty look for the last word. Racy by nature, so
    // tried many times.
    for _ in 0..1000 {
        let context = Context::new().unwrap();
        let mut rep = context.socket::<Rep>();
        let endpoint = rep.bind("tcp://127.0.0.1:*").unwrap().to_string();
        let replier = thread::spawn(move || rep.recv().reply("answer"));
        let mut req = context.socket::<Req>();
        req.connect(&endpoint).unwrap();
        let answer = within(FIVE, move || req.request("question"));
        assert_eq!(answer, Ok("answer".into()));
        replier.join().unwrap();
    }
}

#[test]
fn an_unconnected_socket_with_nothing_queued_does_not_hold_up_the_drop() {
    within(FIVE, || {
        let context = Context::new().unwrap();
        let mut push = context.socket::<Push>();
        push.connect(&unused_endpoint()).unwrap();
        let mut pull = context.socket::<Pull>();
        pull.connect(&unused_endpoint()).unwrap();
    });
}

#[test]
fn a_bounded_linger_gives_up_on_an_unreachable_peer() {
    within(FIVE, || {
        let context = Context::new().unwrap();
        let mut push = context.socket_with::<Push>(SocketOptions {
            linger: Linger::For(Duration::from_millis(100)),
            ..SocketOptions::default()
        });
        push.connect(&unused_endpoint()).unwrap();
        push.send("never delivered");
    });
}
