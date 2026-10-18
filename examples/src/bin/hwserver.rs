//! Hello World server: binds REP to tcp://*:5555 and replies "World" to
//! each "Hello".

use zguide::sleep_ms;
use zmq::{Context, Rep};

fn main() -> Result<(), Box<dyn std::error::Error>> {
    let context = Context::new()?;
    let mut responder = context.socket::<Rep>();
    responder.bind("tcp://*:5555")?;
    loop {
        let request = responder.recv();
        println!("Received Hello");
        sleep_ms(1000);
        request.reply("World");
    }
}
