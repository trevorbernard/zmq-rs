//! RADIO and DISH on the wire: JOIN and LEAVE commands, `group | body`
//! messages, rejoining after a reconnection, and libzmq's RADIO and DISH
//! when libzmq was built with drafts.
#![cfg(feature = "draft")]

use std::net::TcpListener;
use std::sync::mpsc;
use std::thread;
use std::time::{Duration, Instant};

use zmq::{Context, Dish, Frame, Group, Radio, Reconnect, SocketOptions};
use zmq_interop::libzmq::{self, Kind, Socket};
use zmq_interop::{Peer, Security};
use zmtp::{Command, Identity, Incoming, Message, SocketType};

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

fn group(name: &'static str) -> Group {
    Group::new(name).unwrap()
}

/// Whether libzmq's draft sockets can be tested; says why not otherwise.
fn libzmq_has_drafts() -> bool {
    let has = libzmq::has("draft");
    if !has {
        eprintln!("skipped: this libzmq was built without draft sockets");
    }
    has
}

/// The next command from `peer`, skipping heartbeats.
fn next_command(peer: &mut Peer) -> Command {
    loop {
        match peer.recv().unwrap() {
            Incoming::Command(body) => match Command::parse(&body).unwrap() {
                Command::Ping(_) | Command::Pong(_) => {}
                command => return command,
            },
            Incoming::Message(message) => panic!("expected a command, got {message:?}"),
        }
    }
}

#[test]
fn our_dish_sends_join_and_leave_and_rejoins_after_reconnecting() {
    let listener = TcpListener::bind("127.0.0.1:0").unwrap();
    let context = Context::new().unwrap();
    let mut dish = context.socket_with::<Dish>(SocketOptions {
        reconnect: Reconnect::Fixed {
            interval: Duration::from_millis(20),
        },
        ..SocketOptions::default()
    });
    dish.join(&group("a"));
    dish.join(&group("b"));
    dish.connect(&format!("tcp://{}", listener.local_addr().unwrap()))
        .unwrap();
    let mut first = Peer::accept(
        &listener,
        SocketType::Radio,
        Identity::default(),
        Security::Null,
    )
    .unwrap();
    let mut joined = [next_command(&mut first), next_command(&mut first)];
    joined.sort_by_key(|command| format!("{command:?}"));
    assert_eq!(
        joined,
        [Command::Join(group("a")), Command::Join(group("b"))]
    );
    dish.leave(&group("a"));
    assert_eq!(next_command(&mut first), Command::Leave(group("a")));
    drop(first);

    let mut second = within(FIVE, move || {
        Peer::accept(
            &listener,
            SocketType::Radio,
            Identity::default(),
            Security::Null,
        )
        .unwrap()
    });
    assert_eq!(next_command(&mut second), Command::Join(group("b")));
    // Nothing else was replayed: the next command is the next one sent.
    dish.join(&group("c"));
    assert_eq!(next_command(&mut second), Command::Join(group("c")));
}

#[test]
fn our_radio_sends_group_and_body_to_a_dish_that_joined() {
    let context = Context::new().unwrap();
    let mut radio = context.socket::<Radio>();
    let bound = radio.bind("tcp://127.0.0.1:*").unwrap();
    let mut peer = Peer::connect(bound.tcp_addr().unwrap(), SocketType::Dish, b"").unwrap();
    peer.send_command(&Command::Join(group("news"))).unwrap();
    let started = Instant::now();
    while radio.send(&group("news"), "rain") == 0 {
        assert!(started.elapsed() < FIVE, "the JOIN never arrived");
        thread::sleep(Duration::from_millis(5));
    }
    let expected = Message::try_from(vec![Frame::from("news"), Frame::from("rain")]).unwrap();
    assert_eq!(peer.recv_message().unwrap(), expected);
}

#[test]
fn a_libzmq_radio_reaches_our_dish() {
    if !libzmq_has_drafts() {
        return;
    }
    let radio = Socket::new(Kind::Radio);
    let addr = radio.bind_loopback();
    let context = Context::new().unwrap();
    let mut dish = context.socket::<Dish>();
    dish.join(&group("weather"));
    dish.connect(&format!("tcp://{addr}")).unwrap();
    let started = Instant::now();
    let (heard_group, body) = loop {
        radio.send_to_group("sports", b"not joined").unwrap();
        radio.send_to_group("weather", b"sunny").unwrap();
        if let Some(received) = dish.try_recv() {
            break received;
        }
        assert!(started.elapsed() < FIVE, "nothing arrived");
        thread::sleep(Duration::from_millis(5));
    };
    assert_eq!(heard_group, group("weather"));
    assert_eq!(body, Frame::from("sunny"));
}

#[test]
fn our_radio_reaches_a_libzmq_dish() {
    if !libzmq_has_drafts() {
        return;
    }
    let context = Context::new().unwrap();
    let mut radio = context.socket::<Radio>();
    let bound = radio.bind("tcp://127.0.0.1:*").unwrap();
    let dish = Socket::new(Kind::Dish);
    dish.set_receive_timeout(FIVE);
    dish.join("weather");
    dish.connect(bound);
    let started = Instant::now();
    while radio.send(&group("weather"), "sunny") == 0 {
        assert!(started.elapsed() < FIVE, "libzmq's JOIN never arrived");
        thread::sleep(Duration::from_millis(5));
    }
    assert_eq!(radio.send(&group("sports"), "not joined"), 0);
    assert_eq!(
        dish.recv_from_group().unwrap(),
        ("weather".to_owned(), b"sunny".to_vec())
    );
}
