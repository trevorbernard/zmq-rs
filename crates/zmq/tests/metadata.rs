//! The connection metadata each received message carries, as libzmq's
//! `zmq_msg_gets` reads it.

use std::time::Duration;

use zmq::{
    Context, Dealer, Identity, Message, MessageMetadata, MessageProperties, Pull, Push,
    SocketOptions,
};
use zmq_test_ports::free_endpoint;

const FIVE: Duration = Duration::from_secs(5);

fn named(identity: &'static str) -> SocketOptions {
    SocketOptions {
        identity: Identity::new(identity).unwrap(),
        ..SocketOptions::default()
    }
}

/// A message sent from a DEALER named `id` over `transport`, as received.
fn received_over(transport: &str) -> (Message, Message) {
    received_with(transport, SocketOptions::default())
}

fn received_with(transport: &str, options: SocketOptions) -> (Message, Message) {
    let context = Context::new().unwrap();
    let mut server = context.socket_with::<Dealer>(options);
    let endpoint = server.bind(transport).unwrap().to_string();
    let mut client = context.socket_with::<Dealer>(named("id"));
    client.connect(&endpoint).unwrap();
    client.send("first").unwrap();
    client.send("second").unwrap();
    let first = server
        .recv_timeout(FIVE)
        .unwrap()
        .expect("the first message");
    let second = server
        .recv_timeout(FIVE)
        .unwrap()
        .expect("the second message");
    (first, second)
}

#[test]
fn a_message_over_tcp_carries_its_connections_properties() {
    let (message, _) = received_over(&free_endpoint());
    assert_eq!(message.property("Socket-Type"), Some(b"DEALER".as_slice()));
    assert_eq!(message.property("Identity"), Some(b"id".as_slice()));
    assert_eq!(
        message.property("Peer-Address"),
        Some(b"127.0.0.1".as_slice())
    );
    assert_eq!(message.property("User-Id"), None, "no ZAP, no user");
}

#[test]
fn names_are_matched_exactly_as_libzmq_matches_them() {
    let (message, _) = received_over(&free_endpoint());
    assert_eq!(message.property("socket-type"), None);
    assert_eq!(message.property("SOCKET-TYPE"), None);
}

#[test]
fn every_message_from_a_connection_shares_its_metadata() {
    let (first, second) = received_over(&free_endpoint());
    let (first, second) = (first.metadata().unwrap(), second.metadata().unwrap());
    assert!(first.ptr_eq(second));
}

#[cfg(unix)]
#[test]
fn a_message_over_ipc_has_no_peer_address() {
    let (message, _) = received_over("ipc://*");
    assert_eq!(message.property("Socket-Type"), Some(b"DEALER".as_slice()));
    assert_eq!(message.property("Peer-Address"), None);
}

#[test]
fn a_message_over_inproc_has_no_metadata() {
    let (message, _) = received_over("inproc://metadata");
    assert!(message.metadata().is_none());
}

#[test]
fn a_socket_can_turn_metadata_off() {
    let options = SocketOptions {
        message_properties: MessageProperties::Off,
        ..SocketOptions::default()
    };
    let (first, second) = received_with(&free_endpoint(), options);
    assert!(first.metadata().is_none());
    assert!(second.metadata().is_none());
}

#[test]
fn routing_id_is_an_alias_for_identity_only_one_way() {
    let metadata =
        MessageMetadata::new([(zmq::Frame::from("Routing-Id"), zmq::Frame::from("rid"))]);
    assert_eq!(metadata.get("Identity"), Some(b"rid".as_slice()));
    let metadata = MessageMetadata::new([(zmq::Frame::from("Identity"), zmq::Frame::from("id"))]);
    assert_eq!(metadata.get("Routing-Id"), None, "as in libzmq");
}

#[test]
fn of_two_properties_with_one_name_the_first_is_kept() {
    let metadata = MessageMetadata::new([
        (zmq::Frame::from("User-Id"), zmq::Frame::from("from ZAP")),
        (
            zmq::Frame::from("User-Id"),
            zmq::Frame::from("from the peer"),
        ),
    ]);
    assert_eq!(metadata.get("User-Id"), Some(b"from ZAP".as_slice()));
    assert_eq!(metadata.iter().len(), 1);
}

/// libzmq resets a message's metadata as it sends it, so a message passed
/// on does not carry its first connection's properties.
#[test]
fn a_message_passed_on_drops_its_first_connections_metadata() {
    let context = Context::new().unwrap();
    let mut pull = context.socket::<Pull>();
    let endpoint = pull.bind(&free_endpoint()).unwrap().to_string();
    let mut push = context.socket::<Push>();
    push.connect(&endpoint).unwrap();
    push.send("passed on").unwrap();
    let message = pull.recv_timeout(FIVE).unwrap().expect("over tcp");
    assert!(message.metadata().is_some());

    let mut onward = context.socket::<Pull>();
    onward.bind("inproc://onward").unwrap();
    let mut forwarder = context.socket::<Push>();
    forwarder.connect("inproc://onward").unwrap();
    forwarder.send(message).unwrap();
    let message = onward.recv_timeout(FIVE).unwrap().expect("over inproc");
    assert!(message.metadata().is_none());
}

/// A conflated pipe keeps only the latest message, which still carries the
/// connection's metadata.
#[test]
fn a_conflated_message_carries_its_connections_metadata() {
    let context = Context::new().unwrap();
    let mut pull = context.socket::<Pull>();
    pull.set_conflate(true);
    let endpoint = pull.bind(&free_endpoint()).unwrap().to_string();
    let mut push = context.socket::<Push>();
    push.connect(&endpoint).unwrap();
    for i in 0..100 {
        push.send(format!("m{i}")).unwrap();
    }
    let message = pull.recv_timeout(FIVE).unwrap().expect("a message");
    assert_eq!(message.property("Socket-Type"), Some(b"PUSH".as_slice()));
}

/// `ZMQ_METADATA`: each mechanism announces the application's properties
/// from both of its sides, in its READY or INITIATE.
#[cfg(feature = "draft")]
#[test]
fn application_properties_are_announced_under_every_mechanism() {
    use zmq::{Credential, Keypair, Metadata, PlainCredentials, Security, ZapReply, ZapRequest};

    fn accept(_: &ZapRequest) -> ZapReply {
        ZapReply::Ok {
            user_id: zmq::Frame::from("user"),
            metadata: Metadata::new(),
        }
    }
    let keys = Keypair::generate();
    let server_key = keys.public();
    let mechanisms = [
        ("NULL", Security::Null, Security::Null),
        (
            "NULL with ZAP",
            Security::null_authenticated("domain", accept),
            Security::Null,
        ),
        (
            "PLAIN",
            Security::plain_server("domain", accept),
            Security::PlainClient(PlainCredentials {
                username: Credential::new("who").unwrap(),
                password: Credential::new("me").unwrap(),
            }),
        ),
        (
            "CURVE",
            Security::curve_server(keys),
            Security::curve_client(Keypair::generate(), server_key),
        ),
    ];
    for (mechanism, server_security, client_security) in mechanisms {
        let context = Context::new().unwrap();
        let mut server = context.socket_with::<Dealer>(SocketOptions {
            security: server_security,
            ..SocketOptions::default()
        });
        server.add_metadata("X-Side", "server").unwrap();
        let endpoint = server.bind(&free_endpoint()).unwrap().to_string();
        let mut client = context.socket_with::<Dealer>(SocketOptions {
            security: client_security,
            ..SocketOptions::default()
        });
        client.add_metadata("X-Side", "client").unwrap();
        client.connect(&endpoint).unwrap();

        client.send("ping").unwrap();
        let ping = server.recv_timeout(FIVE).unwrap().expect("the ping");
        assert_eq!(
            ping.property("X-Side"),
            Some(b"client".as_slice()),
            "{mechanism}"
        );
        server.send("pong").unwrap();
        let pong = client.recv_timeout(FIVE).unwrap().expect("the pong");
        assert_eq!(
            pong.property("X-Side"),
            Some(b"server".as_slice()),
            "{mechanism}"
        );
    }
}

/// libzmq snapshots a socket's options when it binds or connects, so a
/// property added afterwards reaches only what is bound or connected later.
#[cfg(feature = "draft")]
#[test]
fn a_property_added_after_connecting_is_not_announced_on_that_connection() {
    let context = Context::new().unwrap();
    let mut server = context.socket::<Dealer>();
    let endpoint = server.bind(&free_endpoint()).unwrap().to_string();
    let mut client = context.socket::<Dealer>();
    client.connect(&endpoint).unwrap();
    client.add_metadata("X-Late", "yes").unwrap();
    client.send("hi").unwrap();
    let message = server.recv_timeout(FIVE).unwrap().expect("the message");
    assert_eq!(message.property("X-Late"), None);
}

/// REQ and REP take the envelope off what they receive; the body keeps the
/// connection's properties, as each frame does in libzmq.
#[test]
fn a_request_and_its_reply_carry_their_connections_properties() {
    let context = Context::new().unwrap();
    let mut rep = context.socket::<zmq::Rep>();
    let endpoint = rep.bind(&free_endpoint()).unwrap().to_string();
    let mut req = context.socket::<zmq::Req>();
    req.connect(&endpoint).unwrap();
    let asking = std::thread::spawn(move || req.request("ping"));
    let request = rep.recv_timeout(FIVE).unwrap().expect("the request");
    assert_eq!(
        request.body().property("Socket-Type"),
        Some(b"REQ".as_slice())
    );
    let _ = request.reply("pong");
    let reply = asking.join().unwrap().unwrap();
    assert_eq!(reply.property("Socket-Type"), Some(b"REP".as_slice()));
}
