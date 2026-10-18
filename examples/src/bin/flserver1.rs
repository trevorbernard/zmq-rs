//! Freelance server, model one: a trivial echo service.

use std::env;

use zmq::{Context, Rep};

fn main() -> Result<(), Box<dyn std::error::Error>> {
    let Some(endpoint) = env::args().nth(1) else {
        println!("I: syntax: flserver1 <endpoint>");
        return Ok(());
    };
    let context = Context::new()?;
    let mut server = context.socket::<Rep>();
    server.bind(&endpoint)?;

    println!("I: echo service is ready at {endpoint}");
    loop {
        let request = server.recv();
        let echo = request.body().clone();
        request.reply(echo);
    }
}
