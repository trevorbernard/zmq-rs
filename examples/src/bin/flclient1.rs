//! Freelance client, model one: tries one server several times, or each
//! of several servers once, with a fresh REQ socket per attempt.

use std::env;
use std::time::Duration;

use zmq::{Context, Linger, Message, Req, SocketOptions};

const REQUEST_TIMEOUT: Duration = Duration::from_millis(1000);
const MAX_RETRIES: usize = 3;

fn try_request(context: &Context, endpoint: &str, request: &Message) -> Option<Message> {
    println!("I: trying echo service at {endpoint}...");
    let mut client = context.socket_with::<Req>(SocketOptions {
        linger: Linger::For(Duration::ZERO),
        ..SocketOptions::default()
    });
    client.connect(endpoint).ok()?;
    client
        .request_timeout(request.clone(), REQUEST_TIMEOUT)
        .ok()
}

fn main() -> Result<(), Box<dyn std::error::Error>> {
    let context = Context::new()?;
    let request = Message::new("Hello world");
    let endpoints: Vec<String> = env::args().skip(1).collect();

    let reply = match endpoints.as_slice() {
        [] => {
            println!("I: syntax: flclient1 <endpoint> ...");
            None
        }
        // For one endpoint, we retry N times
        [endpoint] => (0..MAX_RETRIES).find_map(|_| {
            let reply = try_request(&context, endpoint, &request);
            if reply.is_none() {
                println!("W: no response from {endpoint}, retrying...");
            }
            reply
        }),
        // For multiple endpoints, try each at most once
        endpoints => endpoints.iter().find_map(|endpoint| {
            let reply = try_request(&context, endpoint, &request);
            if reply.is_none() {
                println!("W: no response from {endpoint}");
            }
            reply
        }),
    };
    if reply.is_some() {
        println!("Service is running OK");
    }
    Ok(())
}
