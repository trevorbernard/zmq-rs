//! Task ventilator: binds PUSH to tcp://*:5557 and sends a batch of tasks
//! to workers via that socket.

use std::io;

use zguide::randof;
use zmq::{Context, Push};

fn main() -> Result<(), Box<dyn std::error::Error>> {
    let context = Context::new()?;
    let mut sender = context.socket::<Push>();
    sender.bind("tcp://*:5557")?;
    let mut sink = context.socket::<Push>();
    sink.connect("tcp://localhost:5558")?;

    println!("Press Enter when the workers are ready: ");
    io::stdin().read_line(&mut String::new())?;
    println!("Sending tasks to workers...");

    // The first message is "0" and signals start of batch
    sink.send("0");

    let mut total_msec = 0;
    for _ in 0..100 {
        let workload = randof(100) + 1;
        total_msec += workload;
        sender.send(workload.to_string());
    }
    println!("Total expected cost: {total_msec} msec");
    Ok(())
}
