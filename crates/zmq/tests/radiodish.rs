//! RADIO and DISH (RFC 48): single-part messages to groups, matched
//! exactly. Draft sockets.
#![cfg(feature = "draft")]

use std::num::NonZeroUsize;
use std::sync::mpsc;
use std::thread;
use std::time::{Duration, Instant};

use zmq::{Context, Dish, Frame, Group, Hwm, Radio, SocketOptions};
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

fn group(name: &'static str) -> Group {
    Group::new(name).unwrap()
}

#[test]
fn a_dish_receives_only_the_groups_it_joined() {
    let context = Context::new().unwrap();
    let mut radio = context.socket::<Radio>();
    radio.bind("inproc://radio").unwrap();
    let mut dish = context.socket::<Dish>();
    dish.connect("inproc://radio").unwrap();
    assert!(dish.join(&group("news")));
    assert_eq!(radio.send(&group("newsflash"), "groups match exactly"), 0);
    assert_eq!(radio.send(&group("sports"), "not joined"), 0);
    assert_eq!(radio.send(&group("news"), "rain"), 1);
    assert_eq!(dish.try_recv(), Some((group("news"), Frame::from("rain"))));
    assert_eq!(dish.try_recv(), None);
}

#[test]
fn joining_twice_or_leaving_unjoined_is_refused() {
    let context = Context::new().unwrap();
    let mut dish = context.socket::<Dish>();
    assert!(dish.join(&group("a")));
    assert!(!dish.join(&group("a")), "already joined");
    assert!(dish.leave(&group("a")));
    assert!(!dish.leave(&group("a")), "not joined");
}

#[test]
fn leaving_stops_delivery_even_of_queued_messages() {
    let context = Context::new().unwrap();
    let mut radio = context.socket::<Radio>();
    radio.bind("inproc://leaving").unwrap();
    let mut dish = context.socket::<Dish>();
    dish.connect("inproc://leaving").unwrap();
    dish.join(&group("a"));
    assert_eq!(radio.send(&group("a"), "queued"), 1);
    dish.leave(&group("a"));
    assert_eq!(dish.try_recv(), None, "filtered on arrival");
    assert_eq!(radio.send(&group("a"), "after"), 0);
}

#[test]
fn a_radio_reaches_every_dish_in_the_group_over_tcp() {
    let context = Context::new().unwrap();
    let mut radio = context.socket::<Radio>();
    let bound = radio.bind(&free_endpoint()).unwrap();
    let dishes: Vec<_> = (0..3)
        .map(|_| {
            let mut dish = context.socket::<Dish>();
            dish.join(&group("all"));
            dish.connect(&bound.to_string()).unwrap();
            dish
        })
        .collect();
    let started = Instant::now();
    while radio.send(&group("all"), "warm up") < 3 {
        assert!(started.elapsed() < FIVE, "the joins never arrived");
        thread::sleep(Duration::from_millis(5));
    }
    assert_eq!(radio.send(&group("all"), "to everyone"), 3);
    for mut dish in dishes {
        let received = within(FIVE, move || {
            loop {
                let (group, body) = dish.recv();
                if body == Frame::from("to everyone") {
                    return group;
                }
            }
        });
        assert_eq!(received, Group::new("all").unwrap());
    }
}

#[test]
fn a_radio_drops_for_a_slow_dish_instead_of_blocking() {
    let context = Context::new().unwrap();
    let one = Hwm::Messages(NonZeroUsize::MIN);
    let mut radio = context.socket_with::<Radio>(SocketOptions {
        send_hwm: one,
        ..SocketOptions::default()
    });
    radio.bind("inproc://slow-dish").unwrap();
    let mut dish = context.socket_with::<Dish>(SocketOptions {
        recv_hwm: one,
        ..SocketOptions::default()
    });
    dish.join(&group("g"));
    dish.connect("inproc://slow-dish").unwrap();
    let sent: Vec<usize> = (0..4)
        .map(|i| radio.send(&group("g"), format!("{i}")))
        .collect();
    assert_eq!(sent, [1, 1, 0, 0]);
}

#[test]
fn joins_are_never_dropped_however_many() {
    let context = Context::new().unwrap();
    let mut radio = context.socket::<Radio>();
    radio.bind("inproc://many-groups").unwrap();
    let mut dish = context.socket::<Dish>();
    for i in 0..3000 {
        dish.join(&Group::new(format!("group {i}")).unwrap());
    }
    dish.connect("inproc://many-groups").unwrap();
    for i in 0..3000 {
        let name = Group::new(format!("group {i}")).unwrap();
        assert_eq!(radio.send(&name, "x"), 1, "group {i}");
        assert!(dish.try_recv().is_some());
    }
}

#[test]
fn leaving_drops_a_message_a_poll_already_took() {
    let context = Context::new().unwrap();
    let mut radio = context.socket::<Radio>();
    radio.bind("inproc://radio-held").unwrap();
    let mut dish = context.socket::<Dish>();
    dish.connect("inproc://radio-held").unwrap();
    assert!(dish.join(&group("news")));
    assert_eq!(radio.send(&group("news"), "rain"), 1);
    assert_eq!(zmq::poll_timeout([dish.readable()], FIVE), Some([true]));
    assert!(dish.leave(&group("news")));
    assert_eq!(dish.try_recv(), None);
}
