//! In-process transport: sockets of one context talking through pipes,
//! with no TCP and no I/O thread in between.

use std::sync::mpsc;
use std::thread;
use std::time::Duration;

use zmq::{
    Context, Dealer, Endpoint, Error, Frame, Hwm, Identity, Message, Pull, Push, Rep, ReplyError,
    Req, Router, RoutingId, SocketOptions,
};
use zmq_test_ports::free_endpoint;

const FIVE: Duration = Duration::from_secs(5);

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

#[test]
fn bind_returns_the_endpoint() {
    let context = Context::new().unwrap();
    let mut pull = context.socket::<Pull>().unwrap();
    let bound = pull.bind("inproc://work").unwrap();
    assert_eq!(bound, Endpoint::Inproc("work".to_owned()));
    assert_eq!(bound.to_string(), "inproc://work");
    let tcp = pull.bind(free_endpoint()).unwrap();
    assert!(tcp.to_string().starts_with("tcp://127.0.0.1:"));
    assert!(tcp.tcp_addr().is_some());
    assert_eq!(bound.tcp_addr(), None);
}

#[test]
fn push_and_pull_over_inproc() {
    let context = Context::new().unwrap();
    let mut pull = context.socket::<Pull>().unwrap();
    pull.bind("inproc://pipeline").unwrap();
    let mut push = context.socket::<Push>().unwrap();
    push.connect("inproc://pipeline").unwrap();
    for i in 0..10 {
        push.send(format!("{i}")).unwrap();
    }
    let received = within(FIVE, move || {
        (0..10).map(|_| pull.recv().unwrap()).collect::<Vec<_>>()
    });
    let expected: Vec<Message> = (0..10).map(|i| Message::from(format!("{i}"))).collect();
    assert_eq!(received, expected);
}

#[test]
fn connect_may_come_before_bind() {
    let context = Context::new().unwrap();
    let mut push = context.socket::<Push>().unwrap();
    push.connect("inproc://later").unwrap();
    push.send("queued before bind").unwrap();
    let mut pull = context.socket::<Pull>().unwrap();
    pull.bind("inproc://later").unwrap();
    assert_eq!(
        within(FIVE, move || pull.recv().unwrap()),
        Message::from("queued before bind")
    );
}

#[test]
fn dealer_and_router_exchange_identities() {
    let context = Context::new().unwrap();
    let mut router = context.socket::<Router>().unwrap();
    router.bind("inproc://broker").unwrap();
    let named = SocketOptions::new().identity(Identity::new("worker").unwrap());
    let mut worker = context.socket_with::<Dealer>(named).unwrap();
    worker.connect("inproc://broker").unwrap();
    let mut anonymous = context.socket::<Dealer>().unwrap();
    anonymous.connect("inproc://broker").unwrap();
    worker.send("from worker").unwrap();
    anonymous.send("from anonymous").unwrap();

    let (received, mut router) = within(FIVE, move || {
        let received: Vec<(RoutingId, Message)> = (0..2).map(|_| router.recv().unwrap()).collect();
        (received, router)
    });
    let worker_id = RoutingId::new("worker").unwrap();
    let from_worker = received
        .iter()
        .find(|(id, _)| *id == worker_id)
        .expect("named");
    assert_eq!(from_worker.1, Message::from("from worker"));
    let (generated, _) = received.iter().find(|(id, _)| *id != worker_id).unwrap();
    assert_eq!(
        generated.as_bytes()[0],
        0,
        "anonymous peers get generated ids"
    );

    router.send(&worker_id, "to worker").unwrap();
    router.send(generated, "to anonymous").unwrap();
    assert_eq!(
        within(FIVE, move || worker.recv().unwrap()),
        Message::from("to worker")
    );
    assert_eq!(
        within(FIVE, move || anonymous.recv().unwrap()),
        Message::from("to anonymous")
    );
}

#[test]
fn req_and_rep_over_inproc() {
    let context = Context::new().unwrap();
    let mut rep = context.socket::<Rep>().unwrap();
    rep.bind("inproc://service").unwrap();
    let server = thread::spawn(move || {
        let request = rep.recv().unwrap();
        let answer = format!(
            "echo {}",
            String::from_utf8_lossy(request.body().get(0).unwrap())
        );
        request.reply(answer).unwrap();
    });
    let mut req = context.socket::<Req>().unwrap();
    req.connect("inproc://service").unwrap();
    assert_eq!(
        within(FIVE, move || req.request("hi")),
        Ok(Message::from("echo hi"))
    );
    server.join().unwrap();
}

#[test]
fn a_name_can_be_bound_once() {
    let context = Context::new().unwrap();
    let mut first = context.socket::<Pull>().unwrap();
    first.bind("inproc://taken").unwrap();
    let mut second = context.socket::<Pull>().unwrap();
    assert!(matches!(
        second.bind("inproc://taken"),
        Err(Error::Io(err)) if err.kind() == std::io::ErrorKind::AddrInUse
    ));
}

#[test]
fn a_name_is_free_again_once_its_socket_is_gone() {
    let context = Context::new().unwrap();
    let mut first = context.socket::<Pull>().unwrap();
    first.bind("inproc://reused").unwrap();
    drop(first);
    let mut second = context.socket::<Pull>().unwrap();
    second.bind("inproc://reused").unwrap();
    let mut push = context.socket::<Push>().unwrap();
    push.connect("inproc://reused").unwrap();
    push.send("to the new owner").unwrap();
    assert_eq!(
        within(FIVE, move || second.recv().unwrap()),
        Message::from("to the new owner")
    );
}

#[test]
fn any_two_socket_types_are_paired() {
    // As in libzmq, where only a handshake checks them.
    let context = Context::new().unwrap();
    let mut pull = context.socket::<Pull>().unwrap();
    pull.bind("inproc://any").unwrap();
    let mut dealer = context.socket::<Dealer>().unwrap();
    dealer.connect("inproc://any").unwrap();
    dealer.try_send("a dealer can feed a PULL").unwrap();
    assert_eq!(
        pull.recv_timeout(FIVE).unwrap(),
        Some(Message::from("a dealer can feed a PULL"))
    );
}

#[test]
fn contexts_do_not_share_inproc_names() {
    let first = Context::new().unwrap();
    let second = Context::new().unwrap();
    let mut pull = first.socket::<Pull>().unwrap();
    pull.bind("inproc://private").unwrap();
    let mut push = second.socket::<Push>().unwrap();
    push.connect("inproc://private").unwrap();
    push.send("never arrives").unwrap();
    thread::sleep(Duration::from_millis(100));
    assert_eq!(pull.try_recv().unwrap(), None);
}

#[test]
fn large_frames_cross_without_copying() {
    let context = Context::new().unwrap();
    let mut pull = context.socket::<Pull>().unwrap();
    pull.bind("inproc://zero-copy").unwrap();
    let mut push = context.socket::<Push>().unwrap();
    push.connect("inproc://zero-copy").unwrap();
    let frame = Frame::from(vec![7u8; 64 * 1024]);
    let address = frame.as_ptr() as usize;
    push.send(Message::new(frame)).unwrap();
    let received = within(FIVE, move || pull.recv().unwrap());
    assert_eq!(received.get(0).unwrap().as_ptr() as usize, address);
}

#[test]
fn the_high_water_mark_is_the_sum_of_both_sides() {
    let context = Context::new().unwrap();
    let hwm = |n| Hwm::from(n);
    let mut pull = context
        .socket_with::<Pull>(SocketOptions::new().recv_hwm(hwm(3)))
        .unwrap();
    pull.bind("inproc://bounded").unwrap();
    let mut push = context
        .socket_with::<Push>(SocketOptions::new().send_hwm(hwm(2)))
        .unwrap();
    push.connect("inproc://bounded").unwrap();
    for i in 0..5 {
        push.try_send(format!("{i}")).expect("2 + 3 messages fit");
    }
    assert!(push.try_send("6th").is_err(), "the sixth does not");
}

#[test]
fn empty_inproc_names_are_invalid() {
    let context = Context::new().unwrap();
    let mut pull = context.socket::<Pull>().unwrap();
    assert!(matches!(
        pull.bind("inproc://"),
        Err(Error::InvalidEndpoint(_))
    ));
}

#[test]
fn the_sum_holds_towards_the_connecting_side_too() {
    let context = Context::new().unwrap();
    let hwm = |n| Hwm::from(n);
    let mut push = context
        .socket_with::<Push>(SocketOptions::new().send_hwm(hwm(2)))
        .unwrap();
    push.bind("inproc://outbound").unwrap();
    let mut pull = context
        .socket_with::<Pull>(SocketOptions::new().recv_hwm(hwm(3)))
        .unwrap();
    pull.connect("inproc://outbound").unwrap();
    // The bound PUSH learns of its peer as it next sends.
    for i in 0..5 {
        push.try_send(format!("{i}")).expect("2 + 3 messages fit");
    }
    assert!(push.try_send("6th").is_err(), "the sixth does not");
}

/// A request as a REQ frames it, for a DEALER to send.
fn question() -> Message {
    let mut request = Message::new("");
    request.push("question");
    request
}

#[test]
fn a_reply_to_a_requester_that_left_is_dropped_and_says_so() {
    let context = Context::new().unwrap();
    let mut rep = context.socket::<Rep>().unwrap();
    rep.bind("inproc://left").unwrap();
    let mut dealer = context.socket::<Dealer>().unwrap();
    dealer.connect("inproc://left").unwrap();
    dealer.send(question()).unwrap();
    let request = rep.recv_timeout(FIVE).unwrap().expect("a request");
    drop(dealer);
    assert_eq!(request.reply("late"), Err(ReplyError::PeerGone));
}

#[test]
fn a_reply_to_a_requester_with_no_room_is_dropped_and_says_so() {
    let context = Context::new().unwrap();
    let mut rep = context
        .socket_with::<Rep>(SocketOptions::new().send_hwm(1))
        .unwrap();
    rep.bind("inproc://no-room").unwrap();
    let mut dealer = context
        .socket_with::<Dealer>(SocketOptions::new().recv_hwm(1))
        .unwrap();
    dealer.connect("inproc://no-room").unwrap();
    for _ in 0..3 {
        dealer.send(question()).unwrap();
    }
    // The two marks together are the room there is.
    for _ in 0..2 {
        let request = rep.recv_timeout(FIVE).unwrap().expect("a request");
        assert_eq!(request.reply("answer"), Ok(()));
    }
    let request = rep.recv_timeout(FIVE).unwrap().expect("a request");
    assert_eq!(request.reply("answer"), Err(ReplyError::PeerFull));
}
