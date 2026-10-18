//! Multithreaded relay: three threads signal each other in turn over
//! inproc PAIR sockets.

use std::thread;

use zmq::{Context, Pair};

fn step1(context: Context) -> Result<(), zmq::Error> {
    // Connect to step2 and tell it we're ready
    let mut xmitter = context.socket::<Pair>();
    xmitter.connect("inproc://step2")?;
    println!("Step 1 ready, signaling step 2");
    xmitter.send("READY");
    Ok(())
}

fn step2(context: Context) -> Result<(), zmq::Error> {
    // Bind inproc socket before starting step1
    let mut receiver = context.socket::<Pair>();
    receiver.bind("inproc://step2")?;
    let step1 = thread::spawn({
        let context = context.clone();
        move || step1(context)
    });

    // Wait for signal and pass it on
    receiver.recv();

    // Connect to step3 and tell it we're ready
    let mut xmitter = context.socket::<Pair>();
    xmitter.connect("inproc://step3")?;
    println!("Step 2 ready, signaling step 3");
    xmitter.send("READY");
    step1.join().expect("step 1 panicked")
}

fn main() -> Result<(), Box<dyn std::error::Error>> {
    let context = Context::new()?;

    // Bind inproc socket before starting step2
    let mut receiver = context.socket::<Pair>();
    receiver.bind("inproc://step3")?;
    let step2 = thread::spawn({
        let context = context.clone();
        move || step2(context)
    });

    // Wait for signal
    receiver.recv();
    step2.join().expect("step 2 panicked")?;
    println!("Test successful!");
    Ok(())
}
