//! Shows how a non-blocking send fails once a DEALER's queue to a peer
//! that is not there is full.

use std::time::Duration;

use zmq::{Context, Dealer, Linger, SocketOptions};

fn main() -> Result<(), Box<dyn std::error::Error>> {
    let context = Context::new()?;
    let mut mailbox = context.socket_with::<Dealer>(
        SocketOptions::new()
            .send_hwm(4)
            .linger(Linger::For(Duration::ZERO)),
    );
    mailbox.connect("tcp://localhost:9876")?;

    for count in 0..10 {
        println!("Sending message {count}");
        if mailbox.try_send(format!("message {count}")).is_err() {
            println!("Resource temporarily unavailable");
            break;
        }
    }
    Ok(())
}
