//! Waiting on several sockets at once, as libzmq's `zmq_poll`.

use std::num::NonZeroUsize;
use std::thread;
use std::time::Duration;

use zmq::{Context, Dealer, Hwm, Message, Pull, Push, Rep, Req, Router, SocketOptions};

const FIVE: Duration = Duration::from_secs(5);
const BRIEFLY: Duration = Duration::from_millis(100);

/// A PULL bound at `name` and a PUSH connected to it.
fn pipeline(context: &Context, name: &str) -> (zmq::Socket<Push>, zmq::Socket<Pull>) {
    let mut pull = context.socket::<Pull>();
    pull.bind(&format!("inproc://{name}")).unwrap();
    let mut push = context.socket::<Push>();
    push.connect(&format!("inproc://{name}")).unwrap();
    (push, pull)
}

#[test]
fn only_sockets_with_a_message_are_readable() {
    let context = Context::new().unwrap();
    let (mut to_a, mut a) = pipeline(&context, "a");
    let (_to_b, mut b) = pipeline(&context, "b");
    to_a.send("for a");
    let [a_ready, b_ready] = zmq::poll([a.readable(), b.readable()]);
    assert!(a_ready);
    assert!(!b_ready);
    assert_eq!(a.try_recv(), Some(Message::from("for a")));
    assert_eq!(b.try_recv(), None);
}

#[test]
fn every_ready_socket_is_reported() {
    let context = Context::new().unwrap();
    let (mut to_a, mut a) = pipeline(&context, "a");
    let (mut to_b, mut b) = pipeline(&context, "b");
    to_a.send("for a");
    to_b.send("for b");
    // A message the poll took is not counted twice.
    for _ in 0..2 {
        assert_eq!(zmq::poll([a.readable(), b.readable()]), [true, true]);
    }
}

#[test]
fn what_a_poll_received_comes_first_and_in_order() {
    let context = Context::new().unwrap();
    let (mut push, mut pull) = pipeline(&context, "order");
    push.send("first");
    push.send("second");
    assert_eq!(zmq::poll([pull.readable()]), [true]);
    assert_eq!(pull.recv(), Message::from("first"));
    assert_eq!(pull.recv(), Message::from("second"));
}

#[test]
fn a_poll_waits_for_a_message() {
    let context = Context::new().unwrap();
    let (mut push, mut pull) = pipeline(&context, "later");
    let sender = thread::spawn(move || {
        thread::sleep(BRIEFLY);
        push.send("late");
        push
    });
    assert_eq!(zmq::poll_timeout([pull.readable()], FIVE), Some([true]));
    assert_eq!(pull.try_recv(), Some(Message::from("late")));
    drop(sender.join());
}

#[test]
fn a_poll_times_out_with_nothing_ready() {
    let context = Context::new().unwrap();
    let (_push, mut pull) = pipeline(&context, "quiet");
    assert_eq!(zmq::poll_timeout([pull.readable()], BRIEFLY), None);
}

#[test]
fn a_push_is_writable_only_with_room() {
    let one = Hwm::Messages(NonZeroUsize::MIN);
    let context = Context::new().unwrap();
    let mut push = context.socket_with::<Push>(SocketOptions {
        send_hwm: one,
        ..SocketOptions::default()
    });
    assert_eq!(
        zmq::poll_timeout([push.writable()], BRIEFLY),
        None,
        "no peer"
    );
    let mut pull = context.socket_with::<Pull>(SocketOptions {
        recv_hwm: one,
        ..SocketOptions::default()
    });
    pull.bind("inproc://room").unwrap();
    push.connect("inproc://room").unwrap();
    assert_eq!(zmq::poll([push.writable()]), [true]);
    while push.try_send("fill").is_ok() {}
    assert_eq!(zmq::poll_timeout([push.writable()], BRIEFLY), None, "full");
    pull.recv();
    assert_eq!(zmq::poll_timeout([push.writable()], FIVE), Some([true]));
    assert!(push.try_send("fits").is_ok());
}

#[test]
fn a_broker_polls_both_sides() {
    let context = Context::new().unwrap();
    let mut frontend = context.socket::<Router>();
    frontend.bind("inproc://frontend").unwrap();
    let mut backend = context.socket::<Dealer>();
    backend.bind("inproc://backend").unwrap();
    let mut client = context.socket::<Req>();
    client.connect("inproc://frontend").unwrap();
    let mut worker = context.socket::<Rep>();
    worker.connect("inproc://backend").unwrap();

    let asking = thread::spawn(move || client.request("ping").unwrap());
    let answering = thread::spawn(move || {
        let request = worker.recv();
        assert_eq!(request.body(), &Message::from("ping"));
        request.reply("pong");
    });
    let mut forwarded = 0;
    while forwarded < 2 {
        let [from_client, from_worker] = zmq::poll([frontend.readable(), backend.readable()]);
        if from_client {
            let (client, request) = frontend.try_recv().expect("readable");
            let mut message = Message::new(client.as_bytes().to_vec());
            message.extend(request);
            backend.send(message);
            forwarded += 1;
        }
        if from_worker {
            let reply = backend.try_recv().expect("readable");
            let (client, rest) = reply.into_parts();
            let routing_id = zmq::RoutingId::new(client).unwrap();
            frontend
                .try_send(&routing_id, Message::try_from(rest).unwrap())
                .unwrap();
            forwarded += 1;
        }
    }
    assert_eq!(asking.join().unwrap(), Message::from("pong"));
    answering.join().unwrap();
}

#[test]
fn a_readable_rep_lends_out_its_request() {
    let context = Context::new().unwrap();
    let mut rep = context.socket::<Rep>();
    rep.bind("inproc://rep").unwrap();
    let mut req = context.socket::<Req>();
    req.connect("inproc://rep").unwrap();
    let asking = thread::spawn(move || req.request("question").unwrap());
    assert_eq!(zmq::poll([rep.readable()]), [true]);
    let request = rep.try_recv().expect("readable");
    assert_eq!(request.body(), &Message::from("question"));
    request.reply("answer");
    assert_eq!(asking.join().unwrap(), Message::from("answer"));
}

#[cfg(feature = "draft")]
#[test]
fn draft_sockets_poll_too() {
    use zmq::{Client, Frame, Gather, Scatter, Server};

    let context = Context::new().unwrap();
    let mut server = context.socket::<Server>();
    server.bind("inproc://server").unwrap();
    let mut client = context.socket::<Client>();
    client.connect("inproc://server").unwrap();
    let mut gather = context.socket::<Gather>();
    gather.bind("inproc://gather").unwrap();
    let mut scatter = context.socket::<Scatter>();
    scatter.connect("inproc://gather").unwrap();

    assert_eq!(
        zmq::poll([client.writable(), scatter.writable()]),
        [true, true]
    );
    client.send("hello");
    scatter.send("piece");
    assert_eq!(
        zmq::poll([server.readable(), gather.readable()]),
        [true, true]
    );
    let (from, frame) = server.try_recv().expect("readable");
    assert_eq!(frame, Frame::from("hello"));
    assert_eq!(gather.try_recv(), Some(Frame::from("piece")));
    server.try_send(from, "hi").unwrap();
    assert_eq!(zmq::poll_timeout([client.readable()], FIVE), Some([true]));
    assert_eq!(client.try_recv(), Some(Frame::from("hi")));
}
