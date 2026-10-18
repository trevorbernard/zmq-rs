//! Lazy Pirate client: retries each request a few times, then abandons.
//!
//! `request_timeout` keeps the socket usable after a timeout, but the
//! unanswered request stays queued for the server; closing and reopening
//! the socket, as the guide does, discards it.

use std::time::Duration;

use zguide::body;
use zmq::{Context, Linger, Req, RequestError, Socket, SocketOptions};

const REQUEST_TIMEOUT: Duration = Duration::from_millis(2500);
const REQUEST_RETRIES: usize = 3;
const SERVER_ENDPOINT: &str = "tcp://localhost:5555";

fn connect(context: &Context) -> Result<Socket<Req>, zmq::Error> {
    let mut client = context.socket_with::<Req>(SocketOptions {
        linger: Linger::For(Duration::ZERO),
        ..SocketOptions::default()
    });
    client.connect(SERVER_ENDPOINT)?;
    Ok(client)
}

fn main() -> Result<(), Box<dyn std::error::Error>> {
    let context = Context::new()?;
    println!("I: connecting to server...");
    let mut client = connect(&context)?;

    let mut sequence = 0;
    let mut retries_left = REQUEST_RETRIES;
    while retries_left > 0 {
        sequence += 1;
        let request = sequence.to_string();
        loop {
            match client.request_timeout(request.clone(), REQUEST_TIMEOUT) {
                Ok(reply) => {
                    let reply = body(&reply);
                    if reply == request {
                        println!("I: server replied OK ({reply})");
                        retries_left = REQUEST_RETRIES;
                        break;
                    }
                    println!("E: malformed reply from server: {reply}");
                }
                Err(RequestError::TimedOut | RequestError::PeerGone) => {
                    retries_left -= 1;
                    if retries_left == 0 {
                        println!("E: server seems to be offline, abandoning");
                        break;
                    }
                    println!("W: no response from server, retrying...");
                    // Old socket is confused; close it and open a new one
                    println!("I: reconnecting to server...");
                    client = connect(&context)?;
                }
            }
        }
    }
    Ok(())
}
