//! Freelance server, model three: uses a ROUTER socket whose identity is
//! its connect endpoint, so clients can address it directly. Answers PING
//! with PONG and anything else with OK.

use std::env;

use zguide::dump;
use zmq::{Context, Identity, Message, Router, SocketOptions};

fn main() -> Result<(), Box<dyn std::error::Error>> {
    let verbose = env::args().nth(1).is_some_and(|arg| arg == "-v");

    // Prepare server socket with predictable identity
    let bind_endpoint = "tcp://*:5555";
    let connect_endpoint = "tcp://localhost:5555";
    let context = Context::new()?;
    let mut server = context.socket_with::<Router>(SocketOptions {
        identity: Identity::new(connect_endpoint)?,
        ..SocketOptions::default()
    });
    server.bind(bind_endpoint)?;
    println!("I: service is ready at {bind_endpoint}");

    loop {
        let (identity, request) = server.recv();
        if verbose {
            dump(&request);
        }

        // Frame 0: identity of client
        // Frame 1: PING, or client control frame
        // Frame 2: request body
        let (control, _) = request.split_first();
        let reply = if control.as_slice() == b"PING" {
            Message::new("PONG")
        } else {
            let mut reply = Message::new(control.clone());
            reply.push("OK");
            reply
        };
        if verbose {
            dump(&reply);
        }
        // A client that went away meanwhile gets no reply
        let _ = server.send(&identity, reply);
    }
}
