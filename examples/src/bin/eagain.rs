//! Shows how a non-blocking send fails once a DEALER's queue to a peer
//! that is not there is full.

use std::num::NonZeroUsize;
use std::time::Duration;

use zmq::{Context, Dealer, Hwm, Linger, SocketOptions};

fn main() -> Result<(), Box<dyn std::error::Error>> {
    let context = Context::new()?;
    let mut mailbox = context.socket_with::<Dealer>(SocketOptions {
        send_hwm: Hwm::Messages(NonZeroUsize::new(4).expect("nonzero")),
        linger: Linger::For(Duration::ZERO),
        ..SocketOptions::default()
    });
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
