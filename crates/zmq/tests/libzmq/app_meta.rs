//! libzmq's `tests/test_app_meta.cpp`: `ZMQ_METADATA`, the application's
//! `X-` properties a socket announces in its handshake, which its peer reads
//! from the messages it receives.
//!
//! libzmq takes one `name:value` string; ours takes the two apart, so its
//! strings with no colon have no counterpart here.

use std::thread;

use zmq::{Context, InvalidMetadata, InvalidName, Rep, Req};
use zmq_test_ports::free_endpoint;

use crate::support::FIVE;

#[test]
fn app_meta_reqrep() {
    let context = Context::new().unwrap();
    let mut rep = context.socket::<Rep>();
    let mut req = context.socket::<Req>();

    rep.add_metadata("X-hello", "world").unwrap();
    rep.add_metadata("X-connection", "backup").unwrap();

    let too_long = format!("X-{}", "KeyTooLong".repeat(26));
    for (name, value, error) in [
        ("", "", InvalidMetadata::NotApplication),
        ("key", "", InvalidMetadata::NotApplication),
        ("", "value", InvalidMetadata::NotApplication),
        ("key", "value", InvalidMetadata::NotApplication),
        (
            too_long.as_str(),
            "value",
            InvalidMetadata::Name(InvalidName::TooLong),
        ),
        ("X-key", "", InvalidMetadata::EmptyValue),
    ] {
        assert_eq!(rep.add_metadata(name, value), Err(error), "{name}:{value}");
    }

    let endpoint = rep.bind(&free_endpoint()).unwrap().to_string();

    req.add_metadata("X-hello", "hello").unwrap();
    req.add_metadata("X-connection", "primary").unwrap();
    req.add_metadata("X-bin", "009c6").unwrap();
    req.connect(&endpoint).unwrap();

    let asking = thread::spawn(move || req.request([1u8].as_slice()));

    let request = rep.recv_timeout(FIVE).unwrap().expect("the request");
    let message = request.body();
    assert_eq!(message.property("X-hello"), Some(b"hello".as_slice()));
    assert_eq!(
        message.property("X-connection"),
        Some(b"primary".as_slice())
    );
    let binary = message.property("X-bin").expect("the Z85 property");
    assert_eq!(
        zmtp::z85::decode(std::str::from_utf8(binary).unwrap()).unwrap(),
        [0, 1, 2, 3]
    );
    assert_eq!(message.property("X-foobar"), None);
    assert_eq!(message.property("foobar"), None);

    let reply = request.body().clone();
    let _ = request.reply(reply);

    let reply = asking.join().unwrap().unwrap();
    assert_eq!(reply.property("X-hello"), Some(b"world".as_slice()));
    assert_eq!(reply.property("X-connection"), Some(b"backup".as_slice()));
}

/// libzmq's `std::map::insert` leaves a name already set as it was.
#[test]
fn a_name_added_twice_keeps_its_first_value() {
    let context = Context::new().unwrap();
    let mut rep = context.socket::<Rep>();
    let endpoint = rep.bind(&free_endpoint()).unwrap().to_string();
    let mut req = context.socket::<Req>();
    req.add_metadata("X-hello", "first").unwrap();
    req.add_metadata("X-hello", "second").unwrap();
    req.connect(&endpoint).unwrap();

    let asking = thread::spawn(move || req.request("ping"));
    let request = rep.recv_timeout(FIVE).unwrap().expect("the request");
    assert_eq!(
        request.body().property("X-hello"),
        Some(b"first".as_slice())
    );
    let _ = request.reply("pong");
    asking.join().unwrap().unwrap();
}
