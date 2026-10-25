//! Simple Pirate worker: connects REQ to tcp://localhost:5556 and
//! implements the worker part of load-balancing, randomly simulating a
//! crash or CPU overload after a few cycles.

use zguide::{randof, random_identity, sleep_ms, text};
use zmq::{Context, Req, SocketOptions};

/// Signals worker is ready
const WORKER_READY: &[u8] = b"\x01";

fn main() -> Result<(), Box<dyn std::error::Error>> {
    let context = Context::new()?;

    // Set random identity to make tracing easier
    let identity = random_identity();
    let name = text(identity.as_bytes());
    let mut worker = context.socket_with::<Req>(SocketOptions {
        identity,
        ..SocketOptions::default()
    });
    worker.connect("tcp://localhost:5556")?;

    // Tell broker we're ready for work
    println!("I: ({name}) worker ready");
    let mut request = worker.request(WORKER_READY)?;

    let mut cycles = 0;
    loop {
        // Simulate various problems, after a few cycles
        cycles += 1;
        if cycles > 3 && randof(5) == 0 {
            println!("I: ({name}) simulating a crash");
            return Ok(());
        } else if cycles > 3 && randof(5) == 0 {
            println!("I: ({name}) simulating CPU overload");
            sleep_ms(3000);
        }
        println!("I: ({name}) normal reply");
        sleep_ms(1000); // Do some heavy work

        // The reply goes back along the request's envelope: the client's
        // routing id and an empty frame
        request = worker.request(request)?;
    }
}
