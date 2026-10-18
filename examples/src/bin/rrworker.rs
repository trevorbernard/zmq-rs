//! Request-reply worker: connects REP to tcp://localhost:5560 and replies
//! "World" to each request from the broker.

use zguide::{body, sleep_ms};
use zmq::{Context, Rep};

fn main() -> Result<(), Box<dyn std::error::Error>> {
    let context = Context::new()?;
    let mut responder = context.socket::<Rep>();
    responder.connect("tcp://localhost:5560")?;

    loop {
        let request = responder.recv();
        println!("Received request: [{}]", body(request.body()));
        sleep_ms(1000);
        request.reply("World");
    }
}
