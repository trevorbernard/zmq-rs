//! libzmq's `tests/test_reqrep_device.cpp`: a request and its reply
//! passed by hand through a ROUTER-DEALER device.

use zmq::{Context, Dealer, Frame, Message, Rep, Req, Router, RoutingId};

use crate::support::{FIVE, Requesting, msg};

#[test]
fn roundtrip() {
    // Create a req/rep device.
    let context = Context::new().unwrap();
    let mut dealer = context.socket::<Dealer>();
    let endpoint1 = dealer.bind("tcp://127.0.0.1:*").unwrap().to_string();
    let mut router = context.socket::<Router>();
    let endpoint2 = router.bind("tcp://127.0.0.1:*").unwrap().to_string();

    // Create a worker.
    let mut rep = context.socket::<Rep>();
    rep.connect(&endpoint1).unwrap();

    // Create a client.
    let mut req = context.socket::<Req>();
    req.connect(&endpoint2).unwrap();

    // Send a request.
    let requesting = Requesting::start(req, msg(&["ABC", "DEF"]));

    // Pass the request through the device: libzmq copies its four frames
    // one by one; a zmq-rs ROUTER hands over the sender's id apart, so it
    // goes back in front.
    let (client, request) = router.recv_timeout(FIVE).expect("the request");
    let mut forwarded = Message::new(Frame::from(client));
    forwarded.extend(request);
    assert_eq!(forwarded.len().get(), 4);
    dealer.send(forwarded);

    // Receive the request.
    let request = rep.recv_timeout(FIVE).expect("the request");
    assert_eq!(request.body(), &msg(&["ABC", "DEF"]));

    // Send the reply.
    request.reply(msg(&["GHI", "JKL"]));

    // Pass the reply through the device.
    let reply = dealer.recv_timeout(FIVE).expect("the reply");
    assert_eq!(reply.len().get(), 4);
    let (client, rest) = reply.into_parts();
    router
        .send(
            &RoutingId::new(client).unwrap(),
            Message::try_from(rest).unwrap(),
        )
        .unwrap();

    // Receive the reply.
    let (_req, reply) = requesting.finish();
    assert_eq!(reply, msg(&["GHI", "JKL"]));
}
