//! Lazy Pirate server: an echo service that randomly simulates a crash or
//! CPU overload after a few cycles.

use zguide::{body, randof, sleep_ms};
use zmq::{Context, Rep};

fn main() -> Result<(), Box<dyn std::error::Error>> {
    let context = Context::new()?;
    let mut server = context.socket::<Rep>();
    server.bind("tcp://*:5555")?;

    let mut cycles = 0;
    loop {
        let request = server.recv();
        cycles += 1;

        // Simulate various problems, after a few cycles
        if cycles > 3 && randof(3) == 0 {
            println!("I: simulating a crash");
            return Ok(());
        } else if cycles > 3 && randof(3) == 0 {
            println!("I: simulating CPU overload");
            sleep_ms(2000);
        }
        let text = body(request.body());
        println!("I: normal request ({text})");
        sleep_ms(1000); // Do some heavy work
        request.reply(text);
    }
}
