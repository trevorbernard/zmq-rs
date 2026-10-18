//! Freelance client, model two: a DEALER sends each request to every
//! server at once and takes the first reply with the right sequence
//! number.

use std::env;
use std::time::{Duration, Instant};

use zguide::text;
use zmq::{Context, Dealer, Linger, Message, Socket, SocketOptions};

const GLOBAL_TIMEOUT: Duration = Duration::from_millis(2500);

struct Flclient {
    socket: Socket<Dealer>,
    servers: usize,
    sequence: u32,
}

impl Flclient {
    fn new(context: &Context) -> Self {
        Self {
            socket: context.socket_with::<Dealer>(SocketOptions {
                linger: Linger::For(Duration::ZERO),
                ..SocketOptions::default()
            }),
            servers: 0,
            sequence: 0,
        }
    }

    fn connect(&mut self, endpoint: &str) -> Result<(), zmq::Error> {
        self.socket.connect(endpoint)?;
        self.servers += 1;
        Ok(())
    }

    /// Sends `request` to every server and returns the first reply to it,
    /// or `None` if none came within the timeout.
    fn request(&mut self, request: &str) -> Option<Message> {
        // Prefix request with sequence number and empty envelope
        self.sequence += 1;
        let sequence = self.sequence.to_string();
        let mut message = Message::new("");
        message.push(sequence.clone());
        message.push(request.to_owned());

        // Blast the request to all connected servers
        for _ in 0..self.servers {
            self.socket.send(message.clone());
        }

        // Wait for a matching reply to arrive from anywhere
        let endtime = Instant::now() + GLOBAL_TIMEOUT;
        loop {
            let remaining = endtime.checked_duration_since(Instant::now())?;
            let reply = self.socket.recv_timeout(remaining)?;
            // Reply is [empty][sequence][OK]
            assert_eq!(reply.len().get(), 3);
            if reply.get(1).is_some_and(|frame| text(frame) == sequence) {
                return Some(reply);
            }
        }
    }
}

fn main() -> Result<(), Box<dyn std::error::Error>> {
    let endpoints: Vec<String> = env::args().skip(1).collect();
    if endpoints.is_empty() {
        println!("I: syntax: flclient2 <endpoint> ...");
        return Ok(());
    }
    let context = Context::new()?;
    let mut client = Flclient::new(&context);
    for endpoint in &endpoints {
        client.connect(endpoint)?;
    }

    let requests = 10_000;
    let start = Instant::now();
    for _ in 0..requests {
        if client.request("random name").is_none() {
            println!("E: name service not available, aborting");
            break;
        }
    }
    println!(
        "Average round trip cost: {} usec",
        start.elapsed().as_micros() / requests
    );
    Ok(())
}
