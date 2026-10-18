//! Task worker: connects PULL to tcp://localhost:5557 to collect workloads
//! from the ventilator, and PUSH to tcp://localhost:5558 to send results to
//! the sink.

use std::io::{self, Write};

use zguide::{body, sleep_ms};
use zmq::{Context, Pull, Push};

fn main() -> Result<(), Box<dyn std::error::Error>> {
    let context = Context::new()?;
    let mut receiver = context.socket::<Pull>();
    receiver.connect("tcp://localhost:5557")?;
    let mut sender = context.socket::<Push>();
    sender.connect("tcp://localhost:5558")?;

    loop {
        let workload = body(&receiver.recv());
        print!("{workload}.");
        io::stdout().flush()?;
        sleep_ms(workload.parse()?);
        sender.send("");
    }
}
