//! Titanic client example: implements the client side of
//! <http://rfc.zeromq.org/spec:9>.

use std::env;
use std::process;

use zguide::mdcliapi::MdClient;
use zguide::mdp::message;
use zguide::{sleep_ms, text};
use zmq::{Context, Frame, Message};

/// Calls a TSP service. The response, without its status, if the status
/// is 200 (OK).
fn service_call(session: &mut MdClient, service: &str, request: Message) -> Option<Message> {
    let Some(reply) = session.send(service, request) else {
        process::exit(0); // Interrupted or failed
    };
    let (status, rest) = reply.split_first();
    match &**status {
        b"200" => Message::try_from(rest.to_vec()).ok(),
        b"400" => {
            println!("E: client fatal error, aborting");
            process::exit(1);
        }
        b"500" => {
            println!("E: server fatal error, aborting");
            process::exit(1);
        }
        _ => None, // Didn't succeed; don't care why not
    }
}

fn main() -> Result<(), Box<dyn std::error::Error>> {
    let verbose = env::args().nth(1).as_deref() == Some("-v");
    let context = Context::new()?;
    let mut session = MdClient::new(&context, "tcp://localhost:5555", verbose)?;

    // 1. Send 'echo' request to Titanic
    let request = message(vec![Frame::from("echo"), Frame::from("Hello world")]);
    let reply = service_call(&mut session, "titanic.request", request).ok_or("no request UUID")?;
    let uuid = reply.split_first().0.clone();
    println!("I: request UUID {}", text(&uuid));

    // 2. Wait until we get a reply
    loop {
        sleep_ms(100);
        match service_call(&mut session, "titanic.reply", Message::new(uuid.clone())) {
            Some(reply) => {
                println!("Reply: {}", text(reply.last()));
                // 3. Close request
                service_call(&mut session, "titanic.close", Message::new(uuid));
                return Ok(());
            }
            None => {
                println!("I: no reply yet, trying again...");
                sleep_ms(5000); // Try again in 5 seconds
            }
        }
    }
}
