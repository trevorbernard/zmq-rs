//! libzmq's `tests/test_metadata.cpp`: metadata a ZAP handler attaches to
//! a connection, read from each message with `zmq_msg_gets`.
//!
//! `zmq_msg_gets` is [`Message::property`].

use std::sync::mpsc;
use std::thread;
use std::time::Duration;

use zmq::{
    Context, Dealer, Frame, Linger, Message, MessageProperties, Metadata, Req, Router, Security,
    Socket, SocketOptions, SocketType, ZapCredentials, ZapReply, ZapRequest,
};
use zmtp::PropertyName;

use crate::support::{FIVE, settle, within};

/// libzmq's test handler: in the domain DOMAIN, accepts the peer as
/// anonymous with the property Hello: World.
fn zap_handler(request: &ZapRequest) -> ZapReply {
    if request.domain != Frame::from("DOMAIN") {
        return ZapReply::AuthenticationFailure;
    }
    let mut metadata = Metadata::new();
    metadata
        .insert(PropertyName::new("Hello").unwrap(), "World")
        .unwrap();
    ZapReply::Ok {
        user_id: Frame::from("anonymous"),
        metadata,
    }
}

/// A server of type `T` asking `zap_handler` in DOMAIN, bound to TCP. Each
/// request goes to the returned receiver.
fn server<T: SocketType>(context: &Context) -> (Socket<T>, String, mpsc::Receiver<ZapRequest>) {
    let (seen, requests) = mpsc::channel();
    let mut server = context.socket_with::<T>(SocketOptions {
        message_properties: MessageProperties::Attach,
        security: Security::null_authenticated("DOMAIN", move |request: &ZapRequest| {
            let _ = seen.send(request.clone());
            zap_handler(request)
        }),
        linger: Linger::For(Duration::ZERO),
        ..SocketOptions::default()
    });
    let endpoint = server.bind("tcp://127.0.0.1:*").unwrap().to_string();
    (server, endpoint, requests)
}

#[test]
fn metadata() {
    let context = Context::new().unwrap();
    let (mut server, endpoint, requests) = server::<Dealer>(&context);
    let mut client = context.socket::<Dealer>();
    client.connect(&endpoint).unwrap();

    client.send("This is a message");
    let message = server.recv_timeout(FIVE).expect("the message");
    assert_eq!(message, Message::from("This is a message"));
    assert_eq!(message.property("Hello"), Some(b"World".as_slice()));
    assert_eq!(message.property("Socket-Type"), Some(b"DEALER".as_slice()));
    assert_eq!(message.property("User-Id"), Some(b"anonymous".as_slice()));
    assert_eq!(
        message.property("Peer-Address"),
        Some(b"127.0.0.1".as_slice())
    );
    assert_eq!(message.property("No Such"), None);

    let request = requests.recv_timeout(FIVE).unwrap();
    assert_eq!(request.credentials, ZapCredentials::Null);
    assert_eq!(request.credentials.mechanism(), "NULL");
    assert_eq!(request.address, "127.0.0.1");
}

/// libzmq checks that every frame of a message a ROUTER prefetched carries
/// the metadata; a zmq-rs message carries it once, for all its frames.
#[test]
fn router_prefetch_metadata() {
    let context = Context::new().unwrap();
    let (mut server, endpoint, _requests) = server::<Router>(&context);
    let mut client = context.socket::<Req>();
    client.connect(&endpoint).unwrap();

    let asking = thread::spawn(move || client.request("This is a message"));
    settle();
    let (from, request) = server.recv_timeout(FIVE).expect("the request");
    assert_eq!(request.property("Hello"), Some(b"World".as_slice()));
    let frames: Vec<Frame> = request.into_iter().collect();
    assert_eq!(frames, [Frame::new(), Frame::from("This is a message")]);

    let mut reply = Message::new(Frame::new());
    reply.push("reply");
    server.send(&from, reply).unwrap();
    let reply = within(FIVE, move || asking.join().unwrap()).unwrap();
    assert_eq!(reply, Message::from("reply"));
}
