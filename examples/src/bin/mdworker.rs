//! Majordomo Protocol worker example: uses the mdwrk API to hide all MDP
//! aspects.

use std::env;

use zguide::mdwrkapi::MdWorker;
use zmq::Context;

fn main() -> Result<(), Box<dyn std::error::Error>> {
    let verbose = env::args().nth(1).as_deref() == Some("-v");
    let context = Context::new()?;
    let mut session = MdWorker::new(&context, "tcp://localhost:5555", "echo", verbose)?;

    let mut reply = None;
    loop {
        let request = session.recv(reply.take());
        reply = Some(request); // Echo is complex... :-)
    }
}
