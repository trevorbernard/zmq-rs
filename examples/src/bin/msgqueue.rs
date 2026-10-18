//! Simple message queuing broker: same as the request-reply broker, but
//! using the built-in proxy.

use zmq::{Context, Dealer, Proxy, Router};

fn main() -> Result<(), Box<dyn std::error::Error>> {
    let context = Context::new()?;

    // Socket facing clients
    let mut frontend = context.socket::<Router>();
    frontend.bind("tcp://*:5559")?;

    // Socket facing services
    let mut backend = context.socket::<Dealer>();
    backend.bind("tcp://*:5560")?;

    Proxy::new(&mut frontend, &mut backend).run();
    Ok(())
}
