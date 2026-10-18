//! Majordomo Protocol client example: uses the mdcli API to hide all MDP
//! aspects.

use std::env;

use zguide::mdcliapi::MdClient;
use zmq::Context;

fn main() -> Result<(), Box<dyn std::error::Error>> {
    let verbose = env::args().nth(1).as_deref() == Some("-v");
    let context = Context::new()?;
    let mut session = MdClient::new(&context, "tcp://localhost:5555", verbose)?;

    let mut count = 0;
    while count < 100_000 {
        if session.send("echo", "Hello world".into()).is_none() {
            break; // Interrupt or failure
        }
        count += 1;
    }
    println!("{count} requests/replies processed");
    Ok(())
}
