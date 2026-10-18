//! Majordomo Protocol client example, asynchronous: uses the mdcli API to
//! hide all MDP aspects.

use std::env;

use zguide::mdcliapi2::MdClient;
use zmq::Context;

fn main() -> Result<(), Box<dyn std::error::Error>> {
    let verbose = env::args().nth(1).as_deref() == Some("-v");
    let context = Context::new()?;
    let mut session = MdClient::new(&context, "tcp://localhost:5555", verbose)?;

    for _ in 0..100_000 {
        session.send("echo", "Hello world".into());
    }
    let mut count = 0;
    while count < 100_000 {
        if session.recv().is_none() {
            break;
        }
        count += 1;
    }
    println!("{count} replies received");
    Ok(())
}
