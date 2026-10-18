//! Suicidal Snail: a subscriber that kills itself when it falls more than
//! a second behind its publisher.

use std::thread;
use std::time::{SystemTime, UNIX_EPOCH};

use zguide::{body, randof, sleep_ms};
use zmq::{Context, Pair, Pub, Socket, Sub};

const MAX_ALLOWED_DELAY: u128 = 1000; // msecs

fn now_ms() -> u128 {
    SystemTime::now()
        .duration_since(UNIX_EPOCH)
        .expect("after the epoch")
        .as_millis()
}

// The subscriber connects to the publisher and reads timestamps. If one
// is more than a second old, it gives up and tells the main thread so.
fn subscriber(context: Context, mut pipe: Socket<Pair>) -> Result<(), zmq::Error> {
    let mut subscriber = context.socket::<Sub>();
    subscriber.subscribe("");
    subscriber.connect("tcp://localhost:5556")?;

    loop {
        let string = body(&subscriber.recv());
        println!("{string}");
        let clock: u128 = string.parse().expect("a timestamp");

        // Suicide snail logic
        if now_ms() - clock > MAX_ALLOWED_DELAY {
            eprintln!("E: subscriber cannot keep up, aborting");
            break;
        }
        // Work for 1 msec plus some random additional time
        sleep_ms(1 + randof(2));
    }
    pipe.send("gone and died");
    Ok(())
}

// The publisher sends a timestamp every millisecond, until told to stop.
fn publisher(context: Context, mut pipe: Socket<Pair>) -> Result<(), zmq::Error> {
    let mut publisher = context.socket::<Pub>();
    publisher.bind("tcp://*:5556")?;

    loop {
        publisher.send(now_ms().to_string());
        if pipe.try_recv().is_some() {
            break;
        }
        sleep_ms(1);
    }
    Ok(())
}

// A pipe to a child thread: a pair of PAIR sockets over inproc, as CZMQ's
// zthread_fork makes.
fn pipe(context: &Context, name: &str) -> Result<(Socket<Pair>, Socket<Pair>), zmq::Error> {
    let mut parent = context.socket::<Pair>();
    parent.bind(&format!("inproc://{name}"))?;
    let mut child = context.socket::<Pair>();
    child.connect(&format!("inproc://{name}"))?;
    Ok((parent, child))
}

fn main() -> Result<(), Box<dyn std::error::Error>> {
    let context = Context::new()?;
    let (mut pubpipe, child) = pipe(&context, "publisher")?;
    let publisher = thread::spawn({
        let context = context.clone();
        move || publisher(context, child)
    });
    let (mut subpipe, child) = pipe(&context, "subscriber")?;
    thread::spawn({
        let context = context.clone();
        move || subscriber(context, child)
    });

    subpipe.recv();
    pubpipe.send("break");
    publisher.join().expect("publisher panicked")?;
    Ok(())
}
