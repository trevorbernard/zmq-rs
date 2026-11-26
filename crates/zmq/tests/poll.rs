//! Waiting on several sockets at once, as libzmq's `zmq_poll`.

use std::thread;
use std::time::Duration;

use zmq::{Context, Dealer, Hwm, Message, Pair, Pull, Push, Rep, Req, Router, SocketOptions};

const FIVE: Duration = Duration::from_secs(5);
const BRIEFLY: Duration = Duration::from_millis(100);

/// A PULL bound at `name` and a PUSH connected to it.
fn pipeline(context: &Context, name: &str) -> (zmq::Socket<Push>, zmq::Socket<Pull>) {
    let mut pull = context.socket::<Pull>().unwrap();
    pull.bind(format!("inproc://{name}")).unwrap();
    let mut push = context.socket::<Push>().unwrap();
    push.connect(format!("inproc://{name}")).unwrap();
    (push, pull)
}

#[test]
fn only_sockets_with_a_message_are_readable() {
    let context = Context::new().unwrap();
    let (mut to_a, mut a) = pipeline(&context, "a");
    let (_to_b, mut b) = pipeline(&context, "b");
    to_a.send("for a").unwrap();
    let [a_ready, b_ready] = zmq::poll([a.readable(), b.readable()]).unwrap();
    assert!(a_ready);
    assert!(!b_ready);
    assert_eq!(a.try_recv().unwrap(), Some(Message::from("for a")));
    assert_eq!(b.try_recv().unwrap(), None);
}

#[test]
fn every_ready_socket_is_reported() {
    let context = Context::new().unwrap();
    let (mut to_a, mut a) = pipeline(&context, "a");
    let (mut to_b, mut b) = pipeline(&context, "b");
    to_a.send("for a").unwrap();
    to_b.send("for b").unwrap();
    // A message the poll took is not counted twice.
    for _ in 0..2 {
        assert_eq!(
            zmq::poll([a.readable(), b.readable()]).unwrap(),
            [true, true]
        );
    }
}

#[test]
fn what_a_poll_received_comes_first_and_in_order() {
    let context = Context::new().unwrap();
    let (mut push, mut pull) = pipeline(&context, "order");
    push.send("first").unwrap();
    push.send("second").unwrap();
    assert_eq!(zmq::poll([pull.readable()]).unwrap(), [true]);
    assert_eq!(pull.recv().unwrap(), Message::from("first"));
    assert_eq!(pull.recv().unwrap(), Message::from("second"));
}

#[test]
fn a_poll_waits_for_a_message() {
    let context = Context::new().unwrap();
    let (mut push, mut pull) = pipeline(&context, "later");
    let sender = thread::spawn(move || {
        thread::sleep(BRIEFLY);
        push.send("late").unwrap();
        push
    });
    assert_eq!(
        zmq::poll_timeout([pull.readable()], FIVE).unwrap(),
        Some([true])
    );
    assert_eq!(pull.try_recv().unwrap(), Some(Message::from("late")));
    drop(sender.join());
}

#[test]
fn a_poll_times_out_with_nothing_ready() {
    let context = Context::new().unwrap();
    let (_push, mut pull) = pipeline(&context, "quiet");
    assert_eq!(zmq::poll_timeout([pull.readable()], BRIEFLY).unwrap(), None);
}

#[test]
fn a_push_is_writable_only_with_room() {
    let one = Hwm::from(1);
    let context = Context::new().unwrap();
    let mut push = context
        .socket_with::<Push>(SocketOptions::new().send_hwm(one))
        .unwrap();
    assert_eq!(
        zmq::poll_timeout([push.writable()], BRIEFLY).unwrap(),
        None,
        "no peer"
    );
    let mut pull = context
        .socket_with::<Pull>(SocketOptions::new().recv_hwm(one))
        .unwrap();
    pull.bind("inproc://room").unwrap();
    push.connect("inproc://room").unwrap();
    assert_eq!(zmq::poll([push.writable()]).unwrap(), [true]);
    while push.try_send("fill").is_ok() {}
    assert_eq!(
        zmq::poll_timeout([push.writable()], BRIEFLY).unwrap(),
        None,
        "full"
    );
    pull.recv().unwrap();
    assert_eq!(
        zmq::poll_timeout([push.writable()], FIVE).unwrap(),
        Some([true])
    );
    assert!(push.try_send("fits").is_ok());
}

/// Queues of one message each way.
fn tiny() -> SocketOptions {
    let one = Hwm::from(1);
    SocketOptions::new().send_hwm(one).recv_hwm(one)
}

#[test]
fn a_pair_is_writable_only_with_a_peer_that_has_room() {
    let context = Context::new().unwrap();
    let mut left = context.socket_with::<Pair>(tiny()).unwrap();
    left.bind("inproc://pair-room").unwrap();
    assert_eq!(
        zmq::poll_timeout([left.writable()], BRIEFLY).unwrap(),
        None,
        "no peer"
    );
    let mut right = context.socket_with::<Pair>(tiny()).unwrap();
    right.connect("inproc://pair-room").unwrap();
    assert_eq!(zmq::poll([left.writable()]).unwrap(), [true]);
    while left.try_send("fill").is_ok() {}
    assert_eq!(
        zmq::poll_timeout([left.writable()], BRIEFLY).unwrap(),
        None,
        "full"
    );
    right.recv().unwrap();
    assert_eq!(
        zmq::poll_timeout([left.writable()], FIVE).unwrap(),
        Some([true])
    );
    assert!(left.try_send("fits").is_ok());

    while left.try_send("fill").is_ok() {}
    drop(right);
    assert_eq!(
        zmq::poll_timeout([left.writable()], BRIEFLY).unwrap(),
        None,
        "the peer is gone"
    );
    let mut next = context.socket_with::<Pair>(tiny()).unwrap();
    next.connect("inproc://pair-room").unwrap();
    assert_eq!(
        zmq::poll_timeout([left.writable()], FIVE).unwrap(),
        Some([true]),
        "another took its place"
    );
}

#[cfg(feature = "draft")]
#[test]
fn a_channel_is_writable_only_with_a_peer_that_has_room() {
    use zmq::Channel;

    let context = Context::new().unwrap();
    let mut left = context.socket_with::<Channel>(tiny()).unwrap();
    left.bind("inproc://channel-room").unwrap();
    assert_eq!(
        zmq::poll_timeout([left.writable()], BRIEFLY).unwrap(),
        None,
        "no peer"
    );
    let mut right = context.socket_with::<Channel>(tiny()).unwrap();
    right.connect("inproc://channel-room").unwrap();
    assert_eq!(zmq::poll([left.writable()]).unwrap(), [true]);
    while left.try_send("fill").is_ok() {}
    assert_eq!(
        zmq::poll_timeout([left.writable()], BRIEFLY).unwrap(),
        None,
        "full"
    );
    right.recv().unwrap();
    assert_eq!(
        zmq::poll_timeout([left.writable()], FIVE).unwrap(),
        Some([true])
    );
    assert!(left.try_send("fits").is_ok());
}

#[test]
fn a_broker_polls_both_sides() {
    let context = Context::new().unwrap();
    let mut frontend = context.socket::<Router>().unwrap();
    frontend.bind("inproc://frontend").unwrap();
    let mut backend = context.socket::<Dealer>().unwrap();
    backend.bind("inproc://backend").unwrap();
    let mut client = context.socket::<Req>().unwrap();
    client.connect("inproc://frontend").unwrap();
    let mut worker = context.socket::<Rep>().unwrap();
    worker.connect("inproc://backend").unwrap();

    let asking = thread::spawn(move || client.request("ping").unwrap());
    let answering = thread::spawn(move || {
        let request = worker.recv().unwrap();
        assert_eq!(request.body(), &Message::from("ping"));
        request.reply("pong").unwrap();
    });
    let mut forwarded = 0;
    while forwarded < 2 {
        let [from_client, from_worker] =
            zmq::poll([frontend.readable(), backend.readable()]).unwrap();
        if from_client {
            let (client, request) = frontend.try_recv().unwrap().expect("readable");
            let mut message = Message::new(client.as_bytes().to_vec());
            message.extend(request);
            backend.send(message).unwrap();
            forwarded += 1;
        }
        if from_worker {
            let reply = backend.try_recv().unwrap().expect("readable");
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
    let mut rep = context.socket::<Rep>().unwrap();
    rep.bind("inproc://rep").unwrap();
    let mut req = context.socket::<Req>().unwrap();
    req.connect("inproc://rep").unwrap();
    let asking = thread::spawn(move || req.request("question").unwrap());
    assert_eq!(zmq::poll([rep.readable()]).unwrap(), [true]);
    let request = rep.try_recv().unwrap().expect("readable");
    assert_eq!(request.body(), &Message::from("question"));
    request.reply("answer").unwrap();
    assert_eq!(asking.join().unwrap(), Message::from("answer"));
}

#[cfg(feature = "draft")]
#[test]
fn draft_sockets_poll_too() {
    use zmq::{Client, Frame, Gather, Scatter, Server};

    let context = Context::new().unwrap();
    let mut server = context.socket::<Server>().unwrap();
    server.bind("inproc://server").unwrap();
    let mut client = context.socket::<Client>().unwrap();
    client.connect("inproc://server").unwrap();
    let mut gather = context.socket::<Gather>().unwrap();
    gather.bind("inproc://gather").unwrap();
    let mut scatter = context.socket::<Scatter>().unwrap();
    scatter.connect("inproc://gather").unwrap();

    assert_eq!(
        zmq::poll([client.writable(), scatter.writable()]).unwrap(),
        [true, true]
    );
    client.send("hello").unwrap();
    scatter.send("piece").unwrap();
    assert_eq!(
        zmq::poll([server.readable(), gather.readable()]).unwrap(),
        [true, true]
    );
    let (from, frame) = server.try_recv().unwrap().expect("readable");
    assert_eq!(frame, Frame::from("hello"));
    assert_eq!(gather.try_recv().unwrap(), Some(Frame::from("piece")));
    server.try_send(from, "hi").unwrap();
    assert_eq!(
        zmq::poll_timeout([client.readable()], FIVE).unwrap(),
        Some([true])
    );
    assert_eq!(client.try_recv().unwrap(), Some(Frame::from("hi")));
}

#[test]
fn a_socket_polled_both_ways_says_which_is_ready() {
    let context = Context::new().unwrap();
    let mut a = context.socket::<Dealer>().unwrap();
    a.bind("inproc://both-ways").unwrap();
    // No peer: nothing to receive, and nowhere to send.
    let (readable, writable) = a.readable_and_writable();
    assert_eq!(
        zmq::poll_timeout([readable, writable], BRIEFLY).unwrap(),
        None
    );
    let mut b = context.socket::<Dealer>().unwrap();
    b.connect("inproc://both-ways").unwrap();
    let (readable, writable) = a.readable_and_writable();
    assert_eq!(
        zmq::poll_timeout([readable, writable], FIVE).unwrap(),
        Some([false, true])
    );
    b.send("hello").unwrap();
    // In either order, and with the other left out.
    let (readable, writable) = a.readable_and_writable();
    assert_eq!(
        zmq::poll_timeout([writable.when(false), readable], FIVE).unwrap(),
        Some([false, true])
    );
    let (readable, writable) = a.readable_and_writable();
    assert_eq!(
        zmq::poll_timeout([writable, readable], FIVE).unwrap(),
        Some([true, true])
    );
    assert_eq!(a.try_recv().unwrap(), Some(Message::from("hello")));
    assert!(a.try_send("back").is_ok());
    assert_eq!(b.recv_timeout(FIVE).unwrap(), Some(Message::from("back")));
}

/// `count` PULLs bound at `name-0`, `name-1` and so on, as a set whose size
/// is known only as the program runs.
fn pulls(context: &Context, name: &str, count: usize) -> Vec<zmq::Socket<Pull>> {
    (0..count)
        .map(|index| {
            let mut pull = context.socket::<Pull>().unwrap();
            pull.bind(format!("inproc://{name}-{index}")).unwrap();
            pull
        })
        .collect()
}

fn readable(pulls: &mut [zmq::Socket<Pull>]) -> Vec<zmq::PollItem<'_>> {
    pulls.iter_mut().map(|pull| pull.readable()).collect()
}

#[test]
fn a_vec_of_sockets_is_polled_as_an_array_is() {
    let context = Context::new().unwrap();
    let mut pulls = pulls(&context, "vec", 5);
    for ready in [1, 3] {
        let mut push = context.socket::<Push>().unwrap();
        push.connect(format!("inproc://vec-{ready}")).unwrap();
        push.send("job").unwrap();
    }
    // A message the poll took is not counted twice.
    for _ in 0..2 {
        assert_eq!(
            zmq::poll(readable(&mut pulls)).unwrap(),
            [false, true, false, true, false]
        );
    }
    assert_eq!(pulls[1].try_recv().unwrap(), Some(Message::from("job")));
    assert_eq!(pulls[3].try_recv().unwrap(), Some(Message::from("job")));
    assert_eq!(
        zmq::poll_timeout(readable(&mut pulls), BRIEFLY).unwrap(),
        None
    );
}

#[test]
fn a_vec_of_sockets_waits_for_a_message() {
    let context = Context::new().unwrap();
    let mut pulls = pulls(&context, "vec-later", 3);
    let mut push = context.socket::<Push>().unwrap();
    push.connect("inproc://vec-later-2").unwrap();
    let sender = thread::spawn(move || {
        thread::sleep(BRIEFLY);
        push.send("late").unwrap();
        push
    });
    assert_eq!(
        zmq::poll(readable(&mut pulls)).unwrap(),
        [false, false, true]
    );
    drop(sender.join());
}

#[test]
fn a_vec_holds_a_socket_polled_both_ways_and_one_left_out() {
    let context = Context::new().unwrap();
    let (mut push, mut pull) = pipeline(&context, "vec-mixed");
    let mut a = context.socket::<Dealer>().unwrap();
    a.bind("inproc://vec-both-ways").unwrap();
    let mut b = context.socket::<Dealer>().unwrap();
    b.connect("inproc://vec-both-ways").unwrap();
    push.send("job").unwrap();
    let (a_readable, a_writable) = a.readable_and_writable();
    let items = vec![a_readable, pull.readable().when(false), a_writable];
    assert_eq!(
        zmq::poll_timeout(items, FIVE).unwrap(),
        Some(vec![false, false, true])
    );
}

#[test]
fn polling_an_empty_vec_with_a_timeout_waits_it_out() {
    let start = std::time::Instant::now();
    assert_eq!(zmq::poll_timeout(Vec::new(), BRIEFLY).unwrap(), None);
    assert!(start.elapsed() >= BRIEFLY);
}
