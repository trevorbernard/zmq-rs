//! PUSH and PULL (RFC 30) through the typed `zmq` API, against libzmq.

use std::collections::BTreeMap;
use std::sync::mpsc;
use std::thread;
use std::time::Duration;

use zmq::{Context, Message, Pull, Push};
use zmq_interop::libzmq::{Kind, Socket};

/// Runs `f` on another thread and fails the test if it takes longer than
/// `limit`, so a lost wakeup fails instead of hanging the suite.
fn within<T: Send + 'static>(limit: Duration, f: impl FnOnce() -> T + Send + 'static) -> T {
    let (done, result) = mpsc::channel();
    thread::spawn(move || {
        let _ = done.send(f());
    });
    result
        .recv_timeout(limit)
        .unwrap_or_else(|_| panic!("timed out after {limit:?}"))
}

fn text(message: &Message) -> String {
    String::from_utf8(message.get(0).unwrap().to_vec()).unwrap()
}

#[test]
fn our_push_feeds_a_libzmq_pull() {
    let context = Context::new().unwrap();
    let mut push = context.socket::<Push>();
    let addr = push.bind("tcp://127.0.0.1:*").unwrap();
    let pull = Socket::new(Kind::Pull);
    pull.connect(addr);
    for i in 0..10 {
        push.send(format!("task {i}"));
    }
    for i in 0..10 {
        assert_eq!(pull.recv().unwrap(), [format!("task {i}").into_bytes()]);
    }
}

#[test]
fn our_pull_drains_a_libzmq_push() {
    let push = Socket::new(Kind::Push);
    let addr = push.bind_loopback();
    let context = Context::new().unwrap();
    let mut pull = context.socket::<Pull>();
    pull.connect(&format!("tcp://{addr}")).unwrap();
    for i in 0..10 {
        push.send(&[format!("task {i}").as_bytes()]).unwrap();
    }
    let received = within(Duration::from_secs(5), move || {
        (0..10).map(|_| text(&pull.recv())).collect::<Vec<_>>()
    });
    let expected: Vec<String> = (0..10).map(|i| format!("task {i}")).collect();
    assert_eq!(received, expected);
}

#[test]
fn push_round_robins_across_pull_peers() {
    let pulls: Vec<Socket> = (0..3).map(|_| Socket::new(Kind::Pull)).collect();
    let context = Context::new().unwrap();
    let mut push = context.socket::<Push>();
    for pull in &pulls {
        push.connect(&format!("tcp://{}", pull.bind_loopback()))
            .unwrap();
    }
    for i in 0..30 {
        push.send(format!("{i}"));
    }
    for (peer, pull) in pulls.iter().enumerate() {
        let got: Vec<usize> = (0..10)
            .map(|_| {
                String::from_utf8(pull.recv().unwrap().remove(0))
                    .unwrap()
                    .parse()
                    .unwrap()
            })
            .collect();
        let expected: Vec<usize> = (0..10).map(|k| k * 3 + peer).collect();
        assert_eq!(got, expected, "peer {peer} gets every third message");
    }
}

#[test]
fn pull_fair_queues_across_push_peers() {
    let context = Context::new().unwrap();
    let mut pull = context.socket::<Pull>();
    let addr = pull.bind("tcp://127.0.0.1:*").unwrap();
    let pushes: Vec<Socket> = (0..3)
        .map(|peer| {
            let push = Socket::new(Kind::Push);
            push.connect(addr.clone());
            for i in 0..5 {
                push.send(&[format!("{peer}:{i}").as_bytes()]).unwrap();
            }
            push
        })
        .collect();
    // Let every peer's messages arrive before reading, so fairness shows.
    thread::sleep(Duration::from_millis(200));
    let received = within(Duration::from_secs(5), move || {
        (0..15).map(|_| text(&pull.recv())).collect::<Vec<_>>()
    });
    let first_three: std::collections::BTreeSet<&str> =
        received[..3].iter().map(|m| &m[..1]).collect();
    assert_eq!(
        first_three.len(),
        3,
        "one from each peer first: {received:?}"
    );
    let mut per_peer: BTreeMap<&str, Vec<&str>> = BTreeMap::new();
    for message in &received {
        let (peer, i) = message.split_once(':').unwrap();
        per_peer.entry(peer).or_default().push(i);
    }
    for (peer, order) in per_peer {
        assert_eq!(order, ["0", "1", "2", "3", "4"], "peer {peer} in order");
    }
    drop(pushes);
}

#[test]
fn try_operations_do_not_block() {
    let context = Context::new().unwrap();
    let mut push = context.socket::<Push>();
    assert_eq!(push.try_send("nowhere"), Err(Message::from("nowhere")));
    let mut pull = context.socket::<Pull>();
    assert_eq!(pull.try_recv(), None);
}

#[test]
fn async_push_and_pull_between_our_sockets() {
    let context = Context::new().unwrap();
    let mut pull = context.socket::<Pull>();
    let addr = pull.bind("tcp://127.0.0.1:*").unwrap();
    let mut push = context.socket::<Push>();
    push.connect(&addr.to_string()).unwrap();
    let received = within(Duration::from_secs(5), move || {
        block_on(async move {
            for i in 0..100 {
                push.send_async(format!("{i}")).await;
            }
            let mut received = Vec::new();
            for _ in 0..100 {
                received.push(text(&pull.recv_async().await));
            }
            received
        })
    });
    let expected: Vec<String> = (0..100).map(|i| format!("{i}")).collect();
    assert_eq!(received, expected);
}

#[test]
fn bind_and_connect_report_bad_endpoints() {
    let context = Context::new().unwrap();
    let mut push = context.socket::<Push>();
    assert!(matches!(
        push.bind("tcp://nonsense"),
        Err(zmq::Error::InvalidEndpoint(_))
    ));
    assert!(matches!(
        push.connect("pgm://eth0;239.192.1.1:5555"),
        Err(zmq::Error::UnsupportedTransport(_))
    ));
    let addr = push.bind("tcp://127.0.0.1:*").unwrap();
    assert!(matches!(
        push.bind(&addr.to_string()),
        Err(zmq::Error::Io(err)) if err.kind() == std::io::ErrorKind::AddrInUse
    ));
}

/// A minimal `block_on`: polls with a waker that unparks this thread.
fn block_on<F: Future>(future: F) -> F::Output {
    use std::sync::Arc;
    use std::task::{Context, Poll, Wake, Waker};

    struct Unpark(thread::Thread);
    impl Wake for Unpark {
        fn wake(self: Arc<Self>) {
            self.0.unpark();
        }
    }
    let waker = Waker::from(Arc::new(Unpark(thread::current())));
    let mut cx = Context::from_waker(&waker);
    let mut future = std::pin::pin!(future);
    loop {
        if let Poll::Ready(output) = future.as_mut().poll(&mut cx) {
            return output;
        }
        thread::park();
    }
}
