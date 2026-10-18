//! Request-reply client: connects REQ to tcp://localhost:5559 and sends
//! "Hello" to the broker, expecting "World" back.

use zguide::body;
use zmq::{Context, Req};

fn main() -> Result<(), Box<dyn std::error::Error>> {
    let context = Context::new()?;
    let mut requester = context.socket::<Req>();
    requester.connect("tcp://localhost:5559")?;

    for request_nbr in 0..10 {
        let reply = requester.request("Hello")?;
        println!("Received reply {request_nbr} [{}]", body(&reply));
    }
    Ok(())
}
