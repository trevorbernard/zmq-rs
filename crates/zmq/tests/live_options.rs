//! Options changed on a socket already made, as libzmq's `zmq_setsockopt`
//! on a live socket: the linger and the high-water marks reach the
//! connections it has, the rest the binds and connects made afterwards.

use std::sync::mpsc;
use std::thread;
use std::time::Duration;

use zmq::{
    Context, Dealer, Hwm, Identity, Linger, Pull, Push, Router, RoutingId, Socket, SocketOptions,
};
use zmq_test_ports::free_endpoint;

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

fn hwm(messages: usize) -> Hwm {
    Hwm::from(messages)
}

fn marks(send: usize, recv: usize) -> SocketOptions {
    SocketOptions::new().send_hwm(hwm(send)).recv_hwm(hwm(recv))
}

/// How many more messages `push` takes before it is full.
fn fill(push: &mut Socket<Push>) -> usize {
    let mut sent = 0;
    while push.try_send("x").is_ok() {
        sent += 1;
    }
    sent
}

fn drain(pull: &mut Socket<Pull>) -> usize {
    let mut received = 0;
    while pull.try_recv().unwrap().is_some() {
        received += 1;
    }
    received
}

#[test]
fn a_linger_set_to_zero_before_the_drop_abandons_what_is_queued() {
    within(FIVE, || {
        let context = Context::new().unwrap();
        let mut push = context.socket::<Push>();
        // Nothing listens there: the message would linger forever.
        push.connect(free_endpoint()).unwrap();
        push.send("never delivered").unwrap();
        push.set_linger(Linger::For(Duration::ZERO));
    });
}

#[test]
fn a_linger_raised_before_the_drop_delivers_what_is_queued() {
    const COUNT: usize = 20_000;
    let receiving = Context::new().unwrap();
    let mut pull = receiving.socket::<Pull>();
    let endpoint = pull.bind(free_endpoint()).unwrap().to_string();
    let receiver = thread::spawn(move || {
        let mut count = 0;
        while pull.recv().unwrap() != "END" {
            count += 1;
        }
        count
    });
    within(FIVE * 6, move || {
        let context = Context::new().unwrap();
        let options = SocketOptions::new()
            .send_hwm(Hwm::Unlimited)
            .linger(Linger::For(Duration::ZERO));
        let mut push = context.socket_with::<Push>(options);
        push.connect(&endpoint).unwrap();
        // Too much for the kernel's socket buffers to take at once.
        for _ in 0..COUNT {
            push.send(vec![0; 1000]).unwrap();
        }
        push.send("END").unwrap();
        push.set_linger(Linger::Forever);
    });
    let received = within(FIVE * 6, move || receiver.join().unwrap());
    assert_eq!(received, COUNT);
}

#[test]
fn a_send_mark_changed_on_a_connected_socket_is_the_connections() {
    let context = Context::new().unwrap();
    let mut push = context.socket_with::<Push>(marks(2, 2).linger(Linger::For(Duration::ZERO)));
    // Nothing listens there, so everything sent stays queued.
    push.connect(free_endpoint()).unwrap();
    assert_eq!(fill(&mut push), 2);
    push.set_send_hwm(hwm(5));
    assert_eq!(fill(&mut push), 3);
    push.set_send_hwm(hwm(1));
    assert_eq!(fill(&mut push), 0, "a lower mark keeps what is queued");
    push.set_send_hwm(Hwm::Unlimited);
    for _ in 0..1000 {
        push.try_send("x").unwrap();
    }
}

#[test]
fn a_send_mark_changed_is_that_of_later_connects() {
    let context = Context::new().unwrap();
    let mut push = context.socket_with::<Push>(marks(2, 2).linger(Linger::For(Duration::ZERO)));
    push.set_send_hwm(hwm(4));
    push.connect(free_endpoint()).unwrap();
    assert_eq!(fill(&mut push), 4);
}

#[test]
fn an_inproc_connection_holds_each_sockets_mark_as_it_is_now() {
    for bind_first in [true, false] {
        let context = Context::new().unwrap();
        let endpoint = format!("inproc://live-options-hwm-{bind_first}");
        let mut pull = context.socket_with::<Pull>(marks(1, 1));
        let mut push = context.socket_with::<Push>(marks(1, 1));
        if bind_first {
            pull.bind(&endpoint).unwrap();
            push.connect(&endpoint).unwrap();
        } else {
            push.connect(&endpoint).unwrap();
            pull.bind(&endpoint).unwrap();
        }
        assert_eq!(fill(&mut push), 2, "the sender's 1 and the receiver's 1");
        push.set_send_hwm(hwm(3));
        assert_eq!(fill(&mut push), 2, "3 and 1");
        pull.set_recv_hwm(hwm(4));
        assert_eq!(fill(&mut push), 3, "3 and 4");
        push.set_send_hwm(hwm(1));
        assert_eq!(fill(&mut push), 0, "1 and 4, with 7 queued");
        assert_eq!(drain(&mut pull), 7);
        assert_eq!(fill(&mut push), 5);
        pull.set_recv_hwm(Hwm::Unlimited);
        for _ in 0..1000 {
            push.try_send("x").unwrap();
        }
    }
}

#[test]
fn a_mark_set_before_the_inproc_name_is_bound_is_the_one_added_to() {
    let context = Context::new().unwrap();
    let endpoint = "inproc://live-options-hwm-pending";
    let mut push = context.socket_with::<Push>(marks(1, 1));
    push.connect(endpoint).unwrap();
    push.set_send_hwm(hwm(3));
    let mut pull = context.socket_with::<Pull>(marks(1, 2));
    pull.bind(endpoint).unwrap();
    assert_eq!(fill(&mut push), 5);
}

#[test]
fn an_identity_changed_between_connects_is_that_of_the_later_one() {
    let context = Context::new().unwrap();
    let mut first = context.socket::<Router>();
    let mut second = context.socket::<Router>();
    let first_endpoint = first.bind(free_endpoint()).unwrap().to_string();
    let second_endpoint = second.bind(free_endpoint()).unwrap().to_string();

    let named = |name: &str| Identity::new(name.to_owned()).unwrap();
    let mut dealer = context.socket_with::<Dealer>(SocketOptions::new().identity(named("one")));
    dealer.connect(&first_endpoint).unwrap();
    dealer.set_identity(named("two"));
    dealer.connect(&second_endpoint).unwrap();
    // One to each, in turn.
    dealer.send("hello").unwrap();
    dealer.send("hello").unwrap();

    for (router, name) in [(&mut first, "one"), (&mut second, "two")] {
        let (from, _) = router.recv_timeout(FIVE).unwrap().expect("a message");
        assert_eq!(from, RoutingId::new(name.to_owned()).unwrap());
    }
}
