//! MMI echo query example: asks the broker whether the echo service has
//! workers.

use std::env;

use zguide::mdcliapi::MdClient;
use zguide::text;
use zmq::Context;

fn main() -> Result<(), Box<dyn std::error::Error>> {
    let verbose = env::args().nth(1).as_deref() == Some("-v");
    let context = Context::new()?;
    let mut session = MdClient::new(&context, "tcp://localhost:5555", verbose)?;

    // This is the service we want to look up, sent to the lookup service
    match session.send("mmi.service", "echo".into()) {
        Some(reply) => {
            let reply_code = text(reply.split_first().0);
            println!("Lookup echo service: {reply_code}");
        }
        None => println!("E: no response from broker, make sure it's running"),
    }
    Ok(())
}
