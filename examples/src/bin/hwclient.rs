//! Hello World client: connects REQ to tcp://localhost:5555, sends "Hello"
//! and expects "World" back.

use zmq::{Context, Req};

fn main() -> Result<(), Box<dyn std::error::Error>> {
    println!("Connecting to hello world server...");
    let context = Context::new()?;
    let mut requester = context.socket::<Req>();
    requester.connect("tcp://localhost:5555")?;
    for request_nbr in 0..10 {
        println!("Sending Hello {request_nbr}...");
        requester.request("Hello")?;
        println!("Received World {request_nbr}");
    }
    Ok(())
}
