//! Decentralized chat: broadcasts lines from stdin on port 9000 of one
//! interface, and listens to port 9000 on every address of a /24 network.
//!
//! Usage: dechat ipaddress interface username
//!
//! The guide binds to an interface name such as eth0; zmq-rs binds to
//! addresses only, so give the interface's address, or `*`.

use std::{env, io, process, thread};

use zguide::body;
use zmq::{Context, Pub, Sub};

fn listener_task(context: Context, network: String) -> Result<(), zmq::Error> {
    let mut listener = context.socket::<Sub>();
    for address in 1..255 {
        listener.connect(&format!("tcp://{network}{address}:9000"))?;
    }
    listener.subscribe("");
    loop {
        print!("{}", body(&listener.recv()));
    }
}

fn main() -> Result<(), Box<dyn std::error::Error>> {
    let args: Vec<String> = env::args().collect();
    let [_, ipaddress, interface, username] = &args[..] else {
        println!("Usage: dechat ipaddress interface username");
        println!("Example: dechat 192.168.55.123 192.168.55.123 joe");
        process::exit(0);
    };
    let Some(dot) = ipaddress.rfind('.') else {
        println!("Usage: dechat ipaddress interface username");
        process::exit(0);
    };
    // Cut string after dot
    let network = ipaddress[..=dot].to_owned();

    let context = Context::new()?;
    thread::spawn({
        let context = context.clone();
        move || listener_task(context, network)
    });

    let mut broadcaster = context.socket::<Pub>();
    broadcaster.bind(&format!("tcp://{interface}:9000"))?;
    let mut message = String::new();
    while io::stdin().read_line(&mut message)? > 0 {
        broadcaster.send(format!("{username}: {message}"));
        message.clear();
    }
    Ok(())
}
