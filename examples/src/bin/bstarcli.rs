//! Binary Star client proof-of-concept implementation. This client does no
//! real work; it just demonstrates the Binary Star failover model.

use std::thread;
use std::time::Duration;

use zguide::{body, sleep_ms};
use zmq::{Context, Linger, Req, RequestError, Socket, SocketOptions};

const REQUEST_TIMEOUT: Duration = Duration::from_millis(1000);
const SETTLE_DELAY: Duration = Duration::from_millis(2000); // Before failing over
const SERVERS: [&str; 2] = ["tcp://localhost:5001", "tcp://localhost:5002"];

fn connect(context: &Context, server: &str) -> Result<Socket<Req>, zmq::Error> {
    println!("I: connecting to server at {server}...");
    let mut client = context.socket_with::<Req>(SocketOptions {
        linger: Linger::For(Duration::ZERO),
        ..SocketOptions::default()
    });
    client.connect(server)?;
    Ok(client)
}

fn main() -> Result<(), Box<dyn std::error::Error>> {
    let context = Context::new()?;
    let mut server_nbr = 0;
    let mut client = connect(&context, SERVERS[server_nbr])?;

    let mut sequence = 0;
    loop {
        // We send a request, then we work to get a reply
        sequence += 1;
        let request = sequence.to_string();
        loop {
            match client.request_timeout(request.clone(), REQUEST_TIMEOUT) {
                // We got a reply from the server, must match sequence
                Ok(reply) => {
                    let reply = body(&reply);
                    if reply == request {
                        println!("I: server replied OK ({reply})");
                        sleep_ms(1000); // One request per second
                        break;
                    }
                    println!("E: bad reply from server: {reply}");
                }
                // We use a Lazy Pirate strategy in the client. If there's
                // no reply within our timeout, we close the socket and try
                // again. In Binary Star, it's the client vote that decides
                // which server is primary; the client must therefore try
                // to connect to each server in turn
                Err(RequestError::TimedOut | RequestError::PeerGone) => {
                    println!("W: no response from server, failing over");

                    // Old socket is confused; close it and open a new one
                    server_nbr = (server_nbr + 1) % SERVERS.len();
                    thread::sleep(SETTLE_DELAY);
                    client = connect(&context, SERVERS[server_nbr])?;
                }
            }
        }
    }
}
