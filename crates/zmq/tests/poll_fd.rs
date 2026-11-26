//! Waiting on file descriptors with sockets, as libzmq's `zmq_poll` with
//! an item's `fd` set.

use std::io::Write;
use std::os::fd::AsFd;
use std::os::unix::net::UnixStream;
use std::thread;
use std::time::{Duration, Instant};

use zmq::{Context, Message, PollItem, Pull, Push};

const FIVE: Duration = Duration::from_secs(5);
const BRIEFLY: Duration = Duration::from_millis(100);

/// A PULL bound at `name` and a PUSH connected to it.
fn pipeline(context: &Context, name: &str) -> (zmq::Socket<Push>, zmq::Socket<Pull>) {
    let mut pull = context.socket::<Pull>().unwrap();
    pull.bind(format!("inproc://fd-{name}")).unwrap();
    let mut push = context.socket::<Push>().unwrap();
    push.connect(format!("inproc://fd-{name}")).unwrap();
    (push, pull)
}

#[test]
fn only_a_descriptor_with_data_is_readable() {
    let context = Context::new().unwrap();
    let (_push, mut pull) = pipeline(&context, "data");
    let (mut ours, theirs) = UnixStream::pair().unwrap();
    let (_quiet_ours, quiet) = UnixStream::pair().unwrap();
    ours.write_all(b"data").unwrap();
    let ready = zmq::poll([
        pull.readable(),
        PollItem::fd_readable(theirs.as_fd()).unwrap(),
        PollItem::fd_readable(quiet.as_fd()).unwrap(),
    ]);
    assert_eq!(ready.unwrap(), [false, true, false]);
}

#[test]
fn a_message_ends_a_wait_on_a_quiet_descriptor() {
    let context = Context::new().unwrap();
    let (mut push, mut pull) = pipeline(&context, "message");
    let (_ours, theirs) = UnixStream::pair().unwrap();
    let sender = thread::spawn(move || {
        thread::sleep(BRIEFLY);
        push.send("late").unwrap();
        push
    });
    let ready = zmq::poll_timeout(
        [
            pull.readable(),
            PollItem::fd_readable(theirs.as_fd()).unwrap(),
        ],
        FIVE,
    );
    assert_eq!(ready.unwrap(), Some([true, false]));
    assert_eq!(pull.try_recv().unwrap(), Some(Message::from("late")));
    drop(sender.join());
}

#[test]
fn data_ends_a_wait_on_a_quiet_socket() {
    let context = Context::new().unwrap();
    let (_push, mut pull) = pipeline(&context, "late-data");
    let (mut ours, theirs) = UnixStream::pair().unwrap();
    let writer = thread::spawn(move || {
        thread::sleep(BRIEFLY);
        ours.write_all(b"late").unwrap();
        ours
    });
    let ready = zmq::poll_timeout(
        [
            pull.readable(),
            PollItem::fd_readable(theirs.as_fd()).unwrap(),
        ],
        FIVE,
    );
    assert_eq!(ready.unwrap(), Some([false, true]));
    drop(writer.join());
}

#[test]
fn a_ready_socket_does_not_hide_a_ready_descriptor() {
    let context = Context::new().unwrap();
    let (mut push, mut pull) = pipeline(&context, "both");
    let (mut ours, theirs) = UnixStream::pair().unwrap();
    push.send("job").unwrap();
    ours.write_all(b"data").unwrap();
    let ready = zmq::poll([
        pull.readable(),
        PollItem::fd_readable(theirs.as_fd()).unwrap(),
    ]);
    assert_eq!(ready.unwrap(), [true, true]);
}

#[test]
fn descriptors_are_polled_without_a_socket() {
    let (mut ours, theirs) = UnixStream::pair().unwrap();
    assert_eq!(
        zmq::poll_timeout([PollItem::fd_readable(theirs.as_fd()).unwrap()], BRIEFLY).unwrap(),
        None
    );
    ours.write_all(b"data").unwrap();
    assert_eq!(
        zmq::poll([PollItem::fd_readable(theirs.as_fd()).unwrap()]).unwrap(),
        [true]
    );
}

#[test]
fn a_descriptor_with_room_is_writable() {
    let (ours, _theirs) = UnixStream::pair().unwrap();
    let ready = zmq::poll([
        PollItem::fd_readable(ours.as_fd()).unwrap(),
        PollItem::fd_writable(ours.as_fd()).unwrap(),
    ]);
    assert_eq!(ready.unwrap(), [false, true]);
}

#[test]
fn a_descriptor_whose_peer_hung_up_is_reported_whatever_was_asked() {
    let (ours, theirs) = UnixStream::pair().unwrap();
    drop(theirs);
    let ready = zmq::poll_timeout(
        [
            PollItem::fd_readable(ours.as_fd()).unwrap(),
            PollItem::fd_writable(ours.as_fd()).unwrap(),
        ],
        FIVE,
    );
    assert_eq!(ready.unwrap(), Some([true, true]));
}

#[test]
fn a_poll_of_quiet_descriptors_waits_out_its_timeout() {
    let context = Context::new().unwrap();
    let (_push, mut pull) = pipeline(&context, "quiet");
    let (_ours, theirs) = UnixStream::pair().unwrap();
    let start = Instant::now();
    let ready = zmq::poll_timeout(
        [
            pull.readable(),
            PollItem::fd_readable(theirs.as_fd()).unwrap(),
        ],
        BRIEFLY,
    );
    assert_eq!(ready.unwrap(), None);
    assert!(start.elapsed() >= BRIEFLY);
}

#[test]
fn a_descriptor_left_out_is_never_ready() {
    let (mut ours, theirs) = UnixStream::pair().unwrap();
    ours.write_all(b"data").unwrap();
    let left_out = PollItem::fd_readable(theirs.as_fd()).unwrap().when(false);
    assert_eq!(zmq::poll_timeout([left_out], BRIEFLY).unwrap(), None);
}

#[test]
fn a_vec_holds_descriptors_and_sockets() {
    let context = Context::new().unwrap();
    let (mut push, mut pull) = pipeline(&context, "vec");
    let streams: Vec<_> = (0..3).map(|_| UnixStream::pair().unwrap()).collect();
    (&streams[1].0).write_all(b"data").unwrap();
    push.send("job").unwrap();
    let mut items = vec![pull.readable()];
    for (_, theirs) in &streams {
        items.push(PollItem::fd_readable(theirs.as_fd()).unwrap());
    }
    assert_eq!(zmq::poll(items).unwrap(), [true, false, true, false]);
}

#[test]
fn a_wake_left_by_an_earlier_poll_does_not_end_the_next() {
    let context = Context::new().unwrap();
    let (mut push, mut pull) = pipeline(&context, "stale");
    let (_ours, theirs) = UnixStream::pair().unwrap();
    let quiet = |pull: &mut zmq::Socket<Pull>| {
        zmq::poll_timeout(
            [
                pull.readable(),
                PollItem::fd_readable(theirs.as_fd()).unwrap(),
            ],
            BRIEFLY,
        )
    };
    assert_eq!(quiet(&mut pull).unwrap(), None);
    // The message wakes what that poll left registered.
    push.send("job").unwrap();
    assert_eq!(pull.recv().unwrap(), Message::from("job"));
    let start = Instant::now();
    assert_eq!(quiet(&mut pull).unwrap(), None);
    assert!(start.elapsed() >= BRIEFLY);
}

#[test]
fn a_thread_that_has_polled_descriptors_still_blocks_and_polls_sockets() {
    let context = Context::new().unwrap();
    let (mut push, mut pull) = pipeline(&context, "after");
    let (_ours, theirs) = UnixStream::pair().unwrap();
    let ready = zmq::poll_timeout(
        [
            pull.readable(),
            PollItem::fd_readable(theirs.as_fd()).unwrap(),
        ],
        Duration::ZERO,
    );
    assert_eq!(ready.unwrap(), None);
    let sender = thread::spawn(move || {
        for _ in 0..2 {
            thread::sleep(BRIEFLY);
            push.send("late").unwrap();
        }
        push
    });
    assert_eq!(
        zmq::poll_timeout([pull.readable()], FIVE).unwrap(),
        Some([true])
    );
    assert_eq!(pull.try_recv().unwrap(), Some(Message::from("late")));
    assert_eq!(
        pull.recv_timeout(FIVE).unwrap(),
        Some(Message::from("late"))
    );
    drop(sender.join());
}
