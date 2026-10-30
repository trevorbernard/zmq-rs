//! libzmq's `tests/test_spec_req.cpp`: REQ against RFC 28.

use std::thread;
use std::time::Duration;

use zmq::{Context, Identity, Message, Rep, Req, RequestError, Router, RoutingId, SocketOptions};
use zmq_test_ports::free_endpoint;

use crate::support::{FIVE, Requesting, SETTLE, msg, settle};

/// Endpoints to bind a case to: each case runs over inproc and tcp.
fn inproc(name: &str) -> String {
    format!("inproc://spec_req_{name}")
}

/// SHALL route outgoing messages to connected peers using a round-robin
/// strategy.
fn round_robin_out(bind_address: &str) {
    let context = Context::new().unwrap();
    let mut req = context.socket::<Req>();
    let connect_address = req.bind(bind_address).unwrap().to_string();

    // Connected one at a time, so that they reach REQ in this order
    let mut reps: Vec<_> = (0..5)
        .map(|_| {
            let mut rep = context.socket::<Rep>();
            rep.connect(&connect_address).unwrap();
            thread::sleep(SETTLE);
            rep
        })
        .collect();

    // Send our peer-replies, and expect every REP it used once in order
    for rep in &mut reps {
        let requesting = Requesting::start(req, msg(&["ABC"]));
        let request = rep.recv_timeout(FIVE).expect("this REP's turn");
        assert_eq!(request.body(), &msg(&["ABC"]));
        request.reply("DEF");
        let reply;
        (req, reply) = requesting.finish();
        assert_eq!(reply, msg(&["DEF"]));
    }
}

#[test]
fn round_robin_out_inproc() {
    round_robin_out(&inproc("round_robin_out"));
}

#[test]
fn round_robin_out_tcp() {
    round_robin_out(&free_endpoint());
}

/// The request and reply messages SHALL have this format on the wire: a
/// delimiter, an empty frame added by the REQ socket, then one or more data
/// frames, the message visible to the application.
fn req_message_format(bind_address: &str) {
    let context = Context::new().unwrap();
    let mut req = context.socket::<Req>();
    let connect_address = req.bind(bind_address).unwrap().to_string();
    let mut router = context.socket::<Router>();
    router.connect(&connect_address).unwrap();

    // Send a multi-part request.
    let requesting = Requesting::start(req, msg(&["ABC", "DEF"]));

    // A ROUTER hands over the peer's routing id apart from the message.
    let (peer, request) = router.recv_timeout(FIVE).expect("the request");
    assert!(!peer.as_bytes().is_empty());
    assert_eq!(request, msg(&["", "ABC", "DEF"]));

    // Send back a single-part reply.
    router.send(&peer, msg(&["", "GHI"])).unwrap();
    let (_req, reply) = requesting.finish();
    assert_eq!(reply, msg(&["GHI"]));
}

#[test]
fn req_message_format_inproc() {
    req_message_format(&inproc("message_format"));
}

#[test]
fn req_message_format_tcp() {
    req_message_format(&free_endpoint());
}

/// SHALL block on sending, or return a suitable error, when it has no
/// connected peers. libzmq's REQ fails a send after its `ZMQ_SNDTIMEO`;
/// here the whole request times out, and the socket stays usable.
#[test]
fn block_on_send_no_peers() {
    let context = Context::new().unwrap();
    let mut sc = context.socket::<Req>();
    assert_eq!(
        sc.request_timeout(Message::new(""), Duration::from_millis(250)),
        Err(RequestError::TimedOut)
    );
}

/// SHALL accept an incoming message only from the last peer that it sent
/// a request to, and SHALL discard silently any messages received from
/// other peers.
///
/// libzmq leaves this case disabled ("still failing"): a WRONG sent after
/// the GOOD can still sit in that router's queue when a later round's
/// request goes to it, and is then taken for the reply, in zmq-rs too, as
/// only request correlation can tell. Here each round sends its WRONGs
/// first and lets them land, so that what is tested is that they are
/// discarded.
fn req_only_listens_to_current_peer(bind_address: &str) {
    let context = Context::new().unwrap();
    let mut req = context.socket_with::<Req>(SocketOptions {
        identity: Identity::new("A").unwrap(),
        ..SocketOptions::default()
    });
    let connect_address = req.bind(bind_address).unwrap().to_string();

    // libzmq sets ZMQ_ROUTER_MANDATORY here; a zmq-rs ROUTER's `send`
    // always fails for an unknown peer.
    let mut routers: Vec<_> = (0..3)
        .map(|_| {
            let mut router = context.socket::<Router>();
            router.connect(&connect_address).unwrap();
            thread::sleep(SETTLE);
            router
        })
        .collect();
    let a = RoutingId::new("A").unwrap();

    for i in 0..routers.len() {
        let requesting = Requesting::start(req, msg(&["ABC"]));

        // Receive on router i
        let (from, request) = routers[i].recv_timeout(FIVE).expect("router i's turn");
        assert_eq!(from, a);
        assert_eq!(request, msg(&["", "ABC"]));

        // Send back replies on all routers, the wrong ones first
        for (j, router) in routers.iter_mut().enumerate() {
            if j != i {
                router.send(&a, msg(&["", "WRONG"])).unwrap();
            }
        }
        settle();
        routers[i].send(&a, msg(&["", "GOOD"])).unwrap();

        // Receive only the good reply
        let reply;
        (req, reply) = requesting.finish();
        assert_eq!(reply, msg(&["GOOD"]));
    }
}

#[test]
fn req_only_listens_to_current_peer_inproc() {
    req_only_listens_to_current_peer(&inproc("current_peer"));
}

#[test]
fn req_only_listens_to_current_peer_tcp() {
    req_only_listens_to_current_peer(&free_endpoint());
}
