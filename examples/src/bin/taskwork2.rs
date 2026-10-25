//! Task worker, with kill signal: as taskwork, and also connects SUB to
//! tcp://localhost:5559 to hear the sink's KILL when the batch is done.

use std::io::{self, Write};

use zguide::{body, sleep_ms};
use zmq::{Context, Pull, Push, Sub};

fn main() -> Result<(), Box<dyn std::error::Error>> {
    let context = Context::new()?;
    let mut receiver = context.socket::<Pull>();
    receiver.connect("tcp://localhost:5557")?;
    let mut sender = context.socket::<Push>();
    sender.connect("tcp://localhost:5558")?;
    let mut controller = context.socket::<Sub>();
    controller.connect("tcp://localhost:5559")?;
    controller.subscribe("");

    // Process tasks forever, or until killed
    loop {
        let [task, kill] = zmq::poll([receiver.readable(), controller.readable()]);
        if task {
            let workload = body(&receiver.recv());
            print!("{workload}.");
            io::stdout().flush()?;
            sleep_ms(workload.parse()?);
            sender.send("");
        }
        // Any waiting controller command acts as 'KILL'
        if kill {
            break;
        }
    }
    println!();
    Ok(())
}
