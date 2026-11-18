//! Synchronized publisher: waits for ten subscribers to check in over
//! REQ-REP before publishing a million updates.

use zmq::{Context, Pub, Rep, SocketOptions};

const SUBSCRIBERS_EXPECTED: usize = 10;

fn main() -> Result<(), Box<dyn std::error::Error>> {
    let context = Context::new()?;

    // Socket to talk to clients
    let mut publisher = context.socket_with::<Pub>(SocketOptions::new().send_hwm(1_100_000));
    publisher.bind("tcp://*:5561")?;

    // Socket to receive signals
    let mut syncservice = context.socket::<Rep>();
    syncservice.bind("tcp://*:5562")?;

    // Get synchronization from subscribers
    println!("Waiting for subscribers");
    for _ in 0..SUBSCRIBERS_EXPECTED {
        syncservice.recv()?.reply("");
    }

    // Now broadcast exactly 1M updates followed by END
    println!("Broadcasting messages");
    for _ in 0..1_000_000 {
        publisher.send("Rhubarb")?;
    }
    publisher.send("END")?;
    Ok(())
}
