//! Reading from multiple sockets: reads from both the task ventilator and
//! the weather server, waiting on both with a poller.

use zmq::{Context, Pull, Sub};

fn main() -> Result<(), Box<dyn std::error::Error>> {
    let context = Context::new()?;
    let mut receiver = context.socket::<Pull>();
    receiver.connect("tcp://localhost:5557")?;
    let mut subscriber = context.socket::<Sub>();
    subscriber.connect("tcp://localhost:5556")?;
    subscriber.subscribe("10001 ");

    // Process messages from both sockets
    loop {
        let [task, update] = zmq::poll([receiver.readable(), subscriber.readable()]);
        if task {
            let _task = receiver.recv();
            // Process task
        }
        if update {
            let _update = subscriber.recv();
            // Process weather update
        }
    }
}
