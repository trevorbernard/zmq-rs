//! A socket's file descriptor and events, compared with libzmq's `ZMQ_FD`
//! and `ZMQ_EVENTS`: what `zmq_getsockopt`'s manual says of them holds for
//! both, step for step.

use std::os::fd::{AsFd, BorrowedFd};
use std::time::Duration;

use zmq::{Context, Message, PollItem, Pull, Push};
use zmq_interop::libzmq::{Kind, Socket};
use zmq_test_ports::free_endpoint;

const FIVE: Duration = Duration::from_secs(5);
const BRIEFLY: Duration = Duration::from_millis(200);

/// libzmq's `ZMQ_POLLIN`.
const POLLIN: i32 = 1;

fn readable_within(fd: BorrowedFd<'_>, timeout: Duration) -> bool {
    let item = PollItem::fd_readable(fd).unwrap();
    zmq::poll_timeout([item], timeout).unwrap().is_some()
}

fn signalled(fd: BorrowedFd<'_>) -> bool {
    readable_within(fd, FIVE)
}

fn quiet(fd: BorrowedFd<'_>) -> bool {
    !readable_within(fd, BRIEFLY)
}

#[test]
fn libzmq_makes_its_descriptor_readable_when_the_events_change() {
    let endpoint = free_endpoint();
    let pull = Socket::new(Kind::Pull);
    pull.set_receive_timeout(Duration::ZERO);
    pull.bind(&endpoint);
    let push = Socket::new(Kind::Push);
    push.connect_str(&endpoint);
    let fd = pull.fd();

    // Looked at until the connection is made and nothing is left to say.
    while !quiet(fd) {
        assert_eq!(pull.events() & POLLIN, 0);
    }

    push.send(&[b"first"]).unwrap();
    assert!(signalled(fd));
    assert_eq!(pull.events() & POLLIN, POLLIN);
    // Readable was reported, so another message is not news.
    push.send(&[b"second"]).unwrap();
    assert!(quiet(fd));

    assert_eq!(pull.recv().unwrap(), [b"first"]);
    assert_eq!(pull.recv().unwrap(), [b"second"]);
    assert!(pull.recv().is_err(), "nothing more");
    assert_eq!(pull.events() & POLLIN, 0);
    assert!(quiet(fd));

    push.send(&[b"third"]).unwrap();
    assert!(signalled(fd));
    assert_eq!(pull.events() & POLLIN, POLLIN);
}

#[test]
fn we_make_our_descriptor_readable_when_the_events_change() {
    let context = Context::new().unwrap();
    let mut pull = context.socket::<Pull>().unwrap();
    let endpoint = pull.bind(free_endpoint()).unwrap().to_string();
    let mut push = context.socket::<Push>().unwrap();
    push.connect(&endpoint).unwrap();
    let handle = pull.fd().unwrap();
    let fd = handle.as_fd();

    // Looked at until the connection is made and nothing is left to say.
    while !quiet(fd) {
        assert!(!pull.events().unwrap().readable);
    }

    push.send("first").unwrap();
    assert!(signalled(fd));
    assert!(pull.events().unwrap().readable);
    // Readable was reported, so another message is not news.
    push.send("second").unwrap();
    assert!(quiet(fd));

    assert_eq!(pull.try_recv().unwrap(), Some(Message::from("first")));
    assert_eq!(pull.try_recv().unwrap(), Some(Message::from("second")));
    assert_eq!(pull.try_recv().unwrap(), None, "nothing more");
    assert!(!pull.events().unwrap().readable);
    assert!(quiet(fd));

    push.send("third").unwrap();
    assert!(signalled(fd));
    assert!(pull.events().unwrap().readable);
}
