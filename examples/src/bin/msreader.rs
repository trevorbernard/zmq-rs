//! Reading from multiple sockets: reads from both the task ventilator and
//! the weather server with a simple non-blocking loop, not a poller.

use zguide::sleep_ms;
use zmq::{Context, Pull, Sub};

fn main() -> Result<(), Box<dyn std::error::Error>> {
    let context = Context::new()?;
    let mut receiver = context.socket::<Pull>();
    receiver.connect("tcp://localhost:5557")?;
    let mut subscriber = context.socket::<Sub>();
    subscriber.connect("tcp://localhost:5556")?;
    subscriber.subscribe("10001 ");

    // Process messages from both sockets, prioritizing traffic from the
    // task ventilator
    loop {
        while let Some(_task) = receiver.try_recv() {
            // Process task
        }
        while let Some(_update) = subscriber.try_recv() {
            // Process weather update
        }
        sleep_ms(1);
    }
}
