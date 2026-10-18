//! Weather proxy device: subscribes to the weather server and republishes
//! its updates on another port.
//!
//! The guide's addresses are 192.168.55.210:5556 and 10.1.1.0:8100; these
//! use the local weather server instead.

use zmq::{Context, Proxy, XPub, XSub};

fn main() -> Result<(), Box<dyn std::error::Error>> {
    let context = Context::new()?;

    // This is where the weather server sits
    let mut frontend = context.socket::<XSub>();
    frontend.connect("tcp://localhost:5556")?;

    // This is our public endpoint for subscribers
    let mut backend = context.socket::<XPub>();
    backend.bind("tcp://*:8100")?;

    // Run the proxy until the user interrupts us
    Proxy::new(&mut frontend, &mut backend).run();
    Ok(())
}
