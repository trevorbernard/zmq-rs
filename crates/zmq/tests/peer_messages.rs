//! `ZMQ_HELLO_MSG`, `ZMQ_DISCONNECT_MSG` and `ZMQ_HICCUP_MSG` beyond what
//! libzmq's own tests cover: which side hears which, and reconnections.
#![cfg(feature = "draft")]

use std::time::Duration;

use zmq::{Context, Dealer, Frame, Keypair, Message, Reconnect, Router, Security, SocketOptions};
use zmq_test_ports::free_endpoint;

const FIVE: Duration = Duration::from_secs(5);
const QUIET: Duration = Duration::from_millis(100);

fn quick() -> SocketOptions {
    SocketOptions {
        reconnect: Reconnect::Fixed {
            interval: Duration::from_millis(10),
        },
        ..SocketOptions::default()
    }
}

fn frames(message: Message) -> Vec<Frame> {
    message.into_iter().collect()
}

#[test]
fn hello_is_sent_again_after_each_reconnection_and_a_hiccup_heard_between() {
    let context = Context::new().unwrap();
    let mut router = context.socket::<Router>();
    let endpoint = router.bind(&free_endpoint()).unwrap().to_string();
    let mut dealer = context.socket_with::<Dealer>(quick());
    dealer.set_hello_message("hello");
    dealer.set_hiccup_message("hiccup");
    dealer.connect(&endpoint).unwrap();

    let (_, hello) = router.recv_timeout(FIVE).unwrap().expect("the hello");
    assert_eq!(hello, Message::from("hello"));
    drop(router);
    let hiccup = dealer.recv_timeout(FIVE).unwrap().expect("the hiccup");
    assert_eq!(hiccup, Message::from("hiccup"));

    let mut router = context.socket::<Router>();
    router.bind(&endpoint).unwrap();
    let (_, hello) = router.recv_timeout(FIVE).unwrap().expect("a second hello");
    assert_eq!(hello, Message::from("hello"));
}

#[test]
fn hello_comes_before_what_was_queued() {
    let context = Context::new().unwrap();
    let endpoint = free_endpoint();
    let mut dealer = context.socket_with::<Dealer>(quick());
    dealer.set_hello_message("hello");
    dealer.connect(&endpoint).unwrap();
    dealer.send("queued").unwrap();

    let mut router = context.socket::<Router>();
    router.bind(&endpoint).unwrap();
    let (_, first) = router.recv_timeout(FIVE).unwrap().expect("the hello");
    let (_, second) = router.recv_timeout(FIVE).unwrap().expect("the message");
    assert_eq!(
        [first, second],
        [Message::from("hello"), Message::from("queued")]
    );
}

#[test]
fn a_router_hears_which_peer_left() {
    let context = Context::new().unwrap();
    let mut router = context.socket::<Router>();
    router.set_disconnect_message("left");
    let endpoint = router.bind(&free_endpoint()).unwrap().to_string();
    let mut dealer = context.socket::<Dealer>();
    dealer.connect(&endpoint).unwrap();
    dealer.send("last words").unwrap();
    drop(dealer);

    let (who, message) = router.recv_timeout(FIVE).unwrap().expect("the message");
    assert_eq!(frames(message), [Frame::from("last words")]);
    let (left, message) = router.recv_timeout(FIVE).unwrap().expect("the disconnect");
    assert_eq!(frames(message), [Frame::from("left")]);
    assert_eq!(left, who);
}

/// libzmq leaves a disconnect message only for a peer the socket accepted,
/// and a hiccup message only for one it connected to.
#[test]
fn each_message_is_for_one_side_only() {
    let context = Context::new().unwrap();
    let mut accepting = context.socket::<zmq::Peer>();
    accepting.set_hiccup_message("hiccup");
    let endpoint = accepting.bind(&free_endpoint()).unwrap().to_string();
    let mut connecting = context.socket_with::<zmq::Peer>(quick());
    connecting.set_disconnect_message("disconnect");
    connecting.set_hello_message("hello");
    connecting.connect_peer(&endpoint).unwrap();
    let (_, hello) = accepting.recv_timeout(FIVE).unwrap().expect("the hello");
    assert_eq!(hello, Frame::from("hello"));

    drop(accepting);
    assert_eq!(
        connecting.recv_timeout(QUIET).unwrap(),
        None,
        "no disconnect message for a peer it connected to"
    );

    let mut accepting = context.socket::<zmq::Peer>();
    accepting.set_hiccup_message("hiccup");
    accepting.bind(&endpoint).unwrap();
    let (_, hello) = accepting.recv_timeout(FIVE).unwrap().expect("the hello");
    assert_eq!(hello, Frame::from("hello"));
    drop(connecting);
    assert_eq!(
        accepting.recv_timeout(QUIET).unwrap(),
        None,
        "no hiccup message for a peer it accepted"
    );
}

#[test]
fn hello_and_disconnect_messages_over_curve() {
    let keys = Keypair::generate();
    let server_key = keys.public();
    let context = Context::new().unwrap();
    let mut router = context.socket_with::<Router>(SocketOptions {
        security: Security::curve_server(keys),
        ..SocketOptions::default()
    });
    router.set_hello_message("welcome");
    router.set_disconnect_message("left");
    let endpoint = router.bind(&free_endpoint()).unwrap().to_string();
    let mut dealer = context.socket_with::<Dealer>(SocketOptions {
        security: Security::curve_client(Keypair::generate(), server_key),
        ..SocketOptions::default()
    });
    dealer.set_hello_message("hello");
    dealer.connect(&endpoint).unwrap();

    let welcome = dealer.recv_timeout(FIVE).unwrap().expect("the welcome");
    assert_eq!(welcome, Message::from("welcome"));
    let (_, hello) = router.recv_timeout(FIVE).unwrap().expect("the hello");
    assert_eq!(hello, Message::from("hello"));
    drop(dealer);
    let (_, left) = router.recv_timeout(FIVE).unwrap().expect("the disconnect");
    assert_eq!(left, Message::from("left"));
}

/// Over `inproc` only the binder hears of the connector leaving: the
/// connector hears nothing when the binder goes.
#[test]
fn an_inproc_connector_hears_nothing_when_the_binder_leaves() {
    let context = Context::new().unwrap();
    let mut binder = context.socket::<Router>();
    binder.bind("inproc://binder-leaves").unwrap();
    let mut connector = context.socket::<Router>();
    connector.set_disconnect_message("left");
    connector.connect("inproc://binder-leaves").unwrap();
    drop(binder);
    assert_eq!(connector.recv_timeout(QUIET).unwrap(), None);
}

#[test]
fn an_empty_message_turns_each_off() {
    let context = Context::new().unwrap();
    let mut router = context.socket::<Router>();
    router.set_hello_message("hello");
    router.set_hello_message("");
    router.set_disconnect_message("left");
    router.set_disconnect_message("");
    let endpoint = router.bind(&free_endpoint()).unwrap().to_string();
    let mut dealer = context.socket::<Dealer>();
    dealer.connect(&endpoint).unwrap();
    dealer.send("only").unwrap();
    let (_, only) = router.recv_timeout(FIVE).unwrap().expect("the message");
    assert_eq!(only, Message::from("only"));
    assert_eq!(dealer.recv_timeout(QUIET).unwrap(), None, "no hello");
    drop(dealer);
    assert_eq!(router.recv_timeout(QUIET).unwrap(), None, "no disconnect");
}
