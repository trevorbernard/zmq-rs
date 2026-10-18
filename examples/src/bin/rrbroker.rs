//! Simple request-reply broker: binds ROUTER to tcp://*:5559 for clients
//! and DEALER to tcp://*:5560 for services.
//!
//! The guide switches messages between the two sockets with `zmq_poll`,
//! which zmq-rs does not have yet, so this uses the built-in proxy, as
//! msgqueue does.

use zmq::{Context, Dealer, Proxy, Router};

fn main() -> Result<(), Box<dyn std::error::Error>> {
    let context = Context::new()?;
    let mut frontend = context.socket::<Router>();
    frontend.bind("tcp://*:5559")?;
    let mut backend = context.socket::<Dealer>();
    backend.bind("tcp://*:5560")?;

    Proxy::new(&mut frontend, &mut backend).run();
    Ok(())
}
