//! A socket's file descriptor and events, as libzmq's `ZMQ_FD` and
//! `ZMQ_EVENTS`: edge-triggered, and readable when the events may have
//! changed.

use std::os::fd::{AsFd, AsRawFd};
use std::time::Duration;

use zmq::{
    Context, Dealer, Events, Hwm, Message, PollItem, Pull, Push, Router, SocketFd, SocketOptions,
    Terminated,
};

const FIVE: Duration = Duration::from_secs(5);
const BRIEFLY: Duration = Duration::from_millis(100);

const NOTHING: Events = Events {
    readable: false,
    writable: false,
};
const READABLE: Events = Events {
    readable: true,
    writable: false,
};
const WRITABLE: Events = Events {
    readable: false,
    writable: true,
};

/// A PULL bound at `name` and a PUSH connected to it.
fn pipeline(context: &Context, name: &str) -> (zmq::Socket<Push>, zmq::Socket<Pull>) {
    let mut pull = context.socket::<Pull>().unwrap();
    pull.bind(format!("inproc://socket-fd-{name}")).unwrap();
    let mut push = context.socket::<Push>().unwrap();
    push.connect(format!("inproc://socket-fd-{name}")).unwrap();
    (push, pull)
}

/// Whether `fd` turns readable within `timeout`, as an event loop would
/// find it.
fn readable_within(fd: &SocketFd, timeout: Duration) -> bool {
    let item = PollItem::fd_readable(fd.as_fd()).unwrap();
    zmq::poll_timeout([item], timeout).unwrap().is_some()
}

fn signalled(fd: &SocketFd) -> bool {
    readable_within(fd, FIVE)
}

fn quiet(fd: &SocketFd) -> bool {
    !readable_within(fd, BRIEFLY)
}

#[test]
fn a_new_descriptor_is_readable_until_the_socket_is_looked_at() {
    let context = Context::new().unwrap();
    let (_push, mut pull) = pipeline(&context, "new");
    let fd = pull.fd().unwrap();
    assert!(signalled(&fd));
    assert_eq!(pull.events().unwrap(), NOTHING);
    assert!(quiet(&fd));
}

#[test]
fn every_call_returns_the_same_descriptor() {
    let context = Context::new().unwrap();
    let (_push, mut pull) = pipeline(&context, "same");
    let fd = pull.fd().unwrap();
    assert_eq!(pull.fd().unwrap().as_raw_fd(), fd.as_raw_fd());
}

#[test]
fn a_message_makes_the_descriptor_readable() {
    let context = Context::new().unwrap();
    let (mut push, mut pull) = pipeline(&context, "message");
    let fd = pull.fd().unwrap();
    assert_eq!(pull.events().unwrap(), NOTHING);
    assert!(quiet(&fd));

    push.send("job").unwrap();
    assert!(signalled(&fd));
    assert_eq!(pull.events().unwrap(), READABLE);
    assert_eq!(pull.try_recv().unwrap(), Some(Message::from("job")));
}

#[test]
fn the_descriptor_is_quiet_until_the_socket_is_found_empty() {
    let context = Context::new().unwrap();
    let (mut push, mut pull) = pipeline(&context, "edge");
    let fd = pull.fd().unwrap();
    assert_eq!(pull.events().unwrap(), NOTHING);

    push.send("first").unwrap();
    assert!(signalled(&fd));
    assert_eq!(pull.events().unwrap(), READABLE);
    // Readable was reported, so more messages are not news.
    push.send("second").unwrap();
    assert!(quiet(&fd));

    assert_eq!(pull.try_recv().unwrap(), Some(Message::from("first")));
    assert_eq!(pull.try_recv().unwrap(), Some(Message::from("second")));
    assert_eq!(pull.try_recv().unwrap(), None);
    assert!(quiet(&fd));
    push.send("third").unwrap();
    assert!(signalled(&fd));
}

#[test]
fn receiving_until_there_is_no_more_needs_no_look_at_the_events() {
    let context = Context::new().unwrap();
    let (mut push, mut pull) = pipeline(&context, "drain");
    let fd = pull.fd().unwrap();
    for round in 0..3 {
        assert!(signalled(&fd), "round {round}");
        while pull.try_recv().unwrap().is_some() {}
        assert!(quiet(&fd), "round {round}");
        push.send("job").unwrap();
        push.send("job").unwrap();
    }
}

#[test]
fn a_blocking_receive_leaves_the_descriptor_to_the_next_look() {
    let context = Context::new().unwrap();
    let (mut push, mut pull) = pipeline(&context, "blocking");
    let fd = pull.fd().unwrap();
    assert_eq!(pull.events().unwrap(), NOTHING);
    push.send("job").unwrap();
    assert_eq!(pull.recv().unwrap(), Message::from("job"));

    assert_eq!(pull.events().unwrap(), NOTHING);
    assert!(quiet(&fd));
    push.send("job").unwrap();
    assert!(signalled(&fd));
}

#[test]
fn room_to_send_makes_the_descriptor_readable() {
    let one = Hwm::from(1);
    let context = Context::new().unwrap();
    let mut push = context
        .socket_with::<Push>(SocketOptions::new().send_hwm(one))
        .unwrap();
    let fd = push.fd().unwrap();
    assert_eq!(push.events().unwrap(), NOTHING, "no peer");
    assert!(quiet(&fd));

    let mut pull = context
        .socket_with::<Pull>(SocketOptions::new().recv_hwm(one))
        .unwrap();
    pull.bind("inproc://socket-fd-room").unwrap();
    push.connect("inproc://socket-fd-room").unwrap();
    assert_eq!(push.events().unwrap(), WRITABLE);

    while push.try_send("fill").is_ok() {}
    assert!(quiet(&fd), "full");
    pull.recv().unwrap();
    assert!(signalled(&fd));
    assert_eq!(push.events().unwrap(), WRITABLE);
    assert!(push.try_send("fits").is_ok());
}

#[test]
fn a_socket_that_sends_and_receives_reports_both() {
    let context = Context::new().unwrap();
    let mut a = context.socket::<Dealer>().unwrap();
    a.bind("inproc://socket-fd-both").unwrap();
    let fd = a.fd().unwrap();
    assert_eq!(a.events().unwrap(), NOTHING, "no peer");
    assert!(quiet(&fd));

    let mut b = context.socket::<Dealer>().unwrap();
    b.connect("inproc://socket-fd-both").unwrap();
    assert!(signalled(&fd));
    assert_eq!(a.events().unwrap(), WRITABLE);
    assert!(quiet(&fd));

    b.send("hello").unwrap();
    assert!(signalled(&fd));
    assert_eq!(
        a.events().unwrap(),
        Events {
            readable: true,
            writable: true
        }
    );
}

#[test]
fn a_type_says_what_it_never_or_always_is() {
    let context = Context::new().unwrap();
    let (mut push, mut pull) = pipeline(&context, "types");
    push.send("job").unwrap();
    assert_eq!(pull.events().unwrap(), READABLE, "a PULL never sends");
    assert_eq!(push.events().unwrap(), WRITABLE, "a PUSH never receives");
    let mut router = context.socket::<Router>().unwrap();
    assert_eq!(
        router.events().unwrap(),
        WRITABLE,
        "a ROUTER's sends never wait"
    );
}

#[test]
fn events_need_no_descriptor() {
    let context = Context::new().unwrap();
    let (mut push, mut pull) = pipeline(&context, "no-fd");
    assert_eq!(pull.events().unwrap(), NOTHING);
    push.send("job").unwrap();
    assert_eq!(pull.events().unwrap(), READABLE);
    assert_eq!(pull.try_recv().unwrap(), Some(Message::from("job")));
    assert_eq!(pull.events().unwrap(), NOTHING);
}

#[test]
fn a_shutdown_makes_the_descriptor_readable_and_the_events_fail() {
    let context = Context::new().unwrap();
    let (_push, mut pull) = pipeline(&context, "shutdown");
    let fd = pull.fd().unwrap();
    assert_eq!(pull.events().unwrap(), NOTHING);
    assert!(quiet(&fd));

    context.shutdown(FIVE).unwrap();
    assert!(signalled(&fd));
    assert_eq!(pull.events(), Err(Terminated));
}

#[test]
fn a_shutdown_makes_readable_a_descriptor_quieted_by_a_receive() {
    let context = Context::new().unwrap();
    let (_push, mut pull) = pipeline(&context, "shutdown-recv");
    let fd = pull.fd().unwrap();
    assert_eq!(pull.try_recv().unwrap(), None);
    assert!(quiet(&fd));

    context.shutdown(FIVE).unwrap();
    assert!(signalled(&fd));
    assert_eq!(pull.try_recv(), Err(Terminated));
}

#[cfg(feature = "draft")]
#[test]
fn draft_sockets_report_events_too() {
    use zmq::{Client, Server};

    let context = Context::new().unwrap();
    let mut server = context.socket::<Server>().unwrap();
    server.bind("inproc://socket-fd-draft").unwrap();
    let fd = server.fd().unwrap();
    assert_eq!(
        server.events().unwrap(),
        WRITABLE,
        "a SERVER's sends never wait"
    );
    assert!(quiet(&fd));

    let mut client = context.socket::<Client>().unwrap();
    client.connect("inproc://socket-fd-draft").unwrap();
    assert_eq!(client.events().unwrap(), WRITABLE);
    client.send("hello").unwrap();
    assert!(signalled(&fd));
    assert!(server.events().unwrap().readable);
}
