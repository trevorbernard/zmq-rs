//! libzmq's `tests/test_router_handover.cpp`: `ZMQ_ROUTER_HANDOVER`, which
//! lets a peer announcing a routing id already in use take it over.

use zmq::{Context, Dealer, Identity, Message, Router, RoutingId, Socket, SocketOptions};

use crate::support::{FIVE, SETTLE};

fn dealer_called_x(context: &Context, endpoint: &str) -> Socket<Dealer> {
    let mut dealer = context.socket_with::<Dealer>(SocketOptions {
        identity: Identity::new("X").unwrap(),
        ..SocketOptions::default()
    });
    dealer.connect(endpoint).unwrap();
    dealer
}

/// Sends from `dealer` and checks that `router` receives it from X, which
/// also shows the connection is up.
fn hello_from_x(dealer: &mut Socket<Dealer>, router: &mut Socket<Router>) {
    dealer.send("Hello");
    let (from, message) = router.recv_timeout(FIVE).expect("Hello from X");
    assert_eq!(from.as_bytes(), b"X");
    assert_eq!(message, Message::from("Hello"));
}

#[test]
fn with_handover() {
    let context = Context::new().unwrap();
    let mut router = context.socket::<Router>();
    router.set_handover(true);
    let endpoint = router.bind("tcp://127.0.0.1:*").unwrap().to_string();

    let mut dealer_one = dealer_called_x(&context, &endpoint);
    hello_from_x(&mut dealer_one, &mut router);

    // A second dealer with the same routing id takes it over.
    let mut dealer_two = dealer_called_x(&context, &endpoint);
    hello_from_x(&mut dealer_two, &mut router);

    let x = RoutingId::new("X").unwrap();
    router.send(&x, "Hello").unwrap();
    assert_eq!(dealer_one.recv_timeout(SETTLE), None);
    assert_eq!(dealer_two.recv_timeout(FIVE), Some(Message::from("Hello")));
}

#[test]
fn without_handover() {
    let context = Context::new().unwrap();
    let mut router = context.socket::<Router>();
    let endpoint = router.bind("tcp://127.0.0.1:*").unwrap().to_string();

    let mut dealer_one = dealer_called_x(&context, &endpoint);
    hello_from_x(&mut dealer_one, &mut router);

    // A second dealer with the same routing id is ignored.
    let mut dealer_two = dealer_called_x(&context, &endpoint);
    dealer_two.send("Hello");
    assert_eq!(router.recv_timeout(SETTLE), None);

    let x = RoutingId::new("X").unwrap();
    router.send(&x, "Hello").unwrap();
    assert_eq!(dealer_two.recv_timeout(SETTLE), None);
    assert_eq!(dealer_one.recv_timeout(FIVE), Some(Message::from("Hello")));
}
