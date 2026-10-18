//! Synchronized subscriber: subscribes, then checks in with the publisher
//! over REQ-REP and counts updates until "END".

use zguide::sleep_ms;
use zmq::{Context, Req, Sub};

fn main() -> Result<(), Box<dyn std::error::Error>> {
    let context = Context::new()?;

    // First, connect our subscriber socket
    let mut subscriber = context.socket::<Sub>();
    subscriber.connect("tcp://localhost:5561")?;
    subscriber.subscribe("");

    // 0MQ is so fast, we need to wait a while...
    sleep_ms(1000);

    // Second, synchronize with publisher
    let mut syncclient = context.socket::<Req>();
    syncclient.connect("tcp://localhost:5562")?;
    syncclient.request("")?;

    // Third, get our updates and report how many we got
    let mut update_nbr = 0;
    while subscriber.recv() != "END".into() {
        update_nbr += 1;
    }
    println!("Received {update_nbr} updates");
    Ok(())
}
