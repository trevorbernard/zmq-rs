//! Demonstrates request-reply identities: a ROUTER sees a generated
//! identity for an anonymous REQ and the chosen one for an identified REQ.
//!
//! REQ's `request` waits for the reply, so each request runs on its own
//! thread while the ROUTER dumps it and answers.

use std::thread;

use zguide::dump;
use zmq::{Context, Identity, Message, Req, Router, SocketOptions};

fn main() -> Result<(), Box<dyn std::error::Error>> {
    let context = Context::new()?;
    let mut sink = context.socket::<Router>();
    sink.bind("inproc://example")?;

    // First allow 0MQ to set the identity
    let mut anonymous = context.socket::<Req>();
    anonymous.connect("inproc://example")?;
    let request =
        thread::spawn(move || anonymous.request("ROUTER uses a generated 5 byte identity"));
    dump_and_answer(&mut sink)?;
    request.join().expect("request panicked")?;

    // Then set the identity ourselves
    let mut identified = context.socket_with::<Req>(SocketOptions {
        identity: Identity::new("PEER2")?,
        ..SocketOptions::default()
    });
    identified.connect("inproc://example")?;
    let request =
        thread::spawn(move || identified.request("ROUTER socket uses REQ's socket identity"));
    dump_and_answer(&mut sink)?;
    request.join().expect("request panicked")?;
    Ok(())
}

/// Dumps the next message with its identity frame, as libzmq's ROUTER
/// presents it, then replies so the REQ is released.
fn dump_and_answer(sink: &mut zmq::Socket<Router>) -> Result<(), zmq::UnknownPeer<zmq::Message>> {
    let (from, message) = sink.recv();
    let mut framed = Message::new(from.as_bytes().to_vec());
    framed.extend(message);
    dump(&framed);
    let mut reply = Message::new("");
    reply.push("OK");
    sink.send(&from, reply)
}
