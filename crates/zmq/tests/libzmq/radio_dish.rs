//! libzmq's `tests/test_radio_dish.cpp`: RADIO sends to the DISH peers
//! that joined a message's group.
//!
//! Not ported, as zmq-rs has no UDP transport: `test_radio_bind_fails`,
//! `test_dish_connect_fails`, `test_radio_dish_udp`,
//! `test_radio_dish_mcast` and `test_radio_dish_no_loop`, each over IPv4
//! and IPv6.

use std::net::TcpListener;
use std::thread;
use std::time::Duration;

use zmq::{Context, Dish, Frame, Group, Radio, Socket};

use crate::support::{FIVE, settle, within};

fn group(name: &str) -> Group {
    Group::new(name.to_owned()).unwrap()
}

/// Receives the next message and checks its group and body.
fn recv_cmp(mut dish: Socket<Dish>, group_name: &str, body: &str) -> Socket<Dish> {
    let (received_group, frame, dish) = within(FIVE, move || {
        let (received_group, frame) = dish.recv();
        (received_group, frame, dish)
    });
    assert_eq!(received_group, group(group_name));
    assert_eq!(frame, Frame::from(body.to_owned()));
    dish
}

/// Whether this host can bind the IPv6 loopback, as libzmq's
/// `is_ipv6_available`.
fn ipv6_available() -> bool {
    TcpListener::bind("[::1]:0").is_ok()
}

#[test]
fn leave_unjoined_fails() {
    let context = Context::new().unwrap();
    let mut dish = context.socket::<Dish>();
    assert!(!dish.leave(&group("Movies")));
}

#[test]
fn join_too_long_fails() {
    // ZMQ_GROUP_MAX_LENGTH is 255: a group one octet longer cannot be
    // made, so it can never be joined.
    assert!(Group::new("A".repeat(256)).is_err());
}

#[test]
fn long_group() {
    let context = Context::new().unwrap();
    let mut radio = context.socket::<Radio>();
    let endpoint = radio.bind("tcp://127.0.0.1:*").unwrap().to_string();

    let mut dish = context.socket::<Dish>();
    let long = "0123456789ABCDEFGH";
    assert!(dish.join(&group(long)));
    dish.connect(&endpoint).unwrap();
    settle();

    radio.send(&group(long), "HELLO");
    recv_cmp(dish, long, "HELLO");
}

#[test]
fn join_twice_fails() {
    let context = Context::new().unwrap();
    let mut dish = context.socket::<Dish>();
    assert!(dish.join(&group("Movies")));
    assert!(!dish.join(&group("Movies")));
}

fn radio_dish_tcp_poll(loopback: &str) {
    let context = Context::new().unwrap();
    let mut radio = context.socket::<Radio>();
    let endpoint = radio
        .bind(&format!("tcp://{loopback}:*"))
        .unwrap()
        .to_string();

    let mut dish = context.socket::<Dish>();
    assert!(dish.join(&group("Movies")));
    dish.connect(&endpoint).unwrap();
    settle();

    // Only the Movies message is for the dish.
    radio.send(&group("TV"), "Friends");
    radio.send(&group("Movies"), "Godfather");
    let mut dish = recv_cmp(dish, "Movies", "Godfather");

    // Join a group while connected.
    assert!(dish.join(&group("TV")));
    thread::sleep(Duration::from_secs(1));
    radio.send(&group("TV"), "Friends");
    let mut dish = recv_cmp(dish, "TV", "Friends");

    assert!(dish.leave(&group("TV")));
    thread::sleep(Duration::from_secs(1));
    radio.send(&group("TV"), "Friends");
    radio.send(&group("Movies"), "Godfather");

    // libzmq polls the radio too and finds it not readable; RADIO never
    // receives, so here it has no `readable` to poll.
    assert_eq!(
        zmq::poll_timeout([dish.readable()], Duration::from_secs(2)),
        Some([true])
    );
    recv_cmp(dish, "Movies", "Godfather");
}

#[test]
fn radio_dish_tcp_poll_ipv4() {
    radio_dish_tcp_poll("127.0.0.1");
}

#[test]
fn radio_dish_tcp_poll_ipv6() {
    if !ipv6_available() {
        eprintln!("ipv6 is not available");
        return;
    }
    radio_dish_tcp_poll("[::1]");
}
