//! Freelance server, model two: does some work, replies OK, with the
//! request's sequence number.

use std::env;

use zmq::{Context, Message, Rep};

fn main() -> Result<(), Box<dyn std::error::Error>> {
    let Some(endpoint) = env::args().nth(1) else {
        println!("I: syntax: flserver2 <endpoint>");
        return Ok(());
    };
    let context = Context::new()?;
    let mut server = context.socket::<Rep>();
    server.bind(&endpoint)?;

    println!("I: service is ready at {endpoint}");
    loop {
        let request = server.recv();
        // Fail nastily if run against wrong client
        assert_eq!(request.body().len().get(), 2);

        let (sequence, _) = request.body().split_first();
        let mut reply = Message::new(sequence.clone());
        reply.push("OK");
        request.reply(reply);
    }
}
