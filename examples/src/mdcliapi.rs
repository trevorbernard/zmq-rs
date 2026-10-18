//! Majordomo Protocol client API, as the guide's `mdcliapi`. Implements
//! the MDP/Client spec at <http://rfc.zeromq.org/spec:7>.
//!
//! The guide uses REQ and, on timeout, a fresh REQ socket. zmq-rs's REQ
//! times out only with `request_timeout`, which tags requests with an id
//! frame a ROUTER broker sees in front of the delimiter, so this emulates
//! REQ on DEALER: it sends the empty delimiter itself.

use std::time::Duration;

use zmq::{Context, Dealer, Frame, Linger, Message, Socket, SocketOptions};

use crate::dump;
use crate::mdp::{MDPC_CLIENT, log, message};

#[derive(Debug)]
pub struct MdClient {
    context: Context,
    broker: String,
    client: Socket<Dealer>,
    verbose: bool,
    timeout: Duration,
    retries: usize,
}

impl MdClient {
    pub fn new(context: &Context, broker: &str, verbose: bool) -> Result<Self, zmq::Error> {
        let client = connect(context, broker, verbose)?;
        Ok(Self {
            context: context.clone(),
            broker: broker.to_owned(),
            client,
            verbose,
            timeout: Duration::from_millis(2500),
            retries: 3,
        })
    }

    pub fn set_timeout(&mut self, timeout: Duration) {
        self.timeout = timeout;
    }

    pub fn set_retries(&mut self, retries: usize) {
        self.retries = retries;
    }

    /// Sends `request` to `service` and waits for the reply, retrying on a
    /// fresh socket. `None` if no reply came after all retries.
    pub fn send(&mut self, service: &str, request: Message) -> Option<Message> {
        // Prefix request with protocol frames
        // Frame 0: empty (REQ emulation)
        // Frame 1: "MDPCxy" (six bytes, MDP/Client x.y)
        // Frame 2: Service name (printable string)
        let mut frames = vec![
            Frame::new(),
            Frame::from(MDPC_CLIENT),
            Frame::from(service.to_owned()),
        ];
        frames.extend(request);
        let request = message(frames);
        if self.verbose {
            log(&format!("I: send request to '{service}' service:"));
            dump(&request);
        }

        let mut retries_left = self.retries;
        while retries_left > 0 {
            self.client.send(request.clone());
            if let Some(reply) = self.client.recv_timeout(self.timeout) {
                if self.verbose {
                    log("I: received reply:");
                    dump(&reply);
                }
                // We would handle malformed replies better in real code
                let mut frames = reply.into_iter();
                assert!(frames.next().is_some_and(|f| f.is_empty()));
                assert!(frames.next().is_some_and(|f| *f == *MDPC_CLIENT.as_bytes()));
                assert!(frames.next().is_some_and(|f| *f == *service.as_bytes()));
                return Message::try_from(frames.collect::<Vec<_>>()).ok();
            }
            retries_left -= 1;
            if retries_left > 0 {
                if self.verbose {
                    log("W: no reply, reconnecting...");
                }
                self.client = connect(&self.context, &self.broker, self.verbose)
                    .expect("connected to this endpoint before");
            } else if self.verbose {
                log("W: permanent error, abandoning");
            }
        }
        None
    }
}

/// A new socket connected to `broker`. It discards what it could not send
/// when dropped, as CZMQ's sockets do.
fn connect(context: &Context, broker: &str, verbose: bool) -> Result<Socket<Dealer>, zmq::Error> {
    let mut client = context.socket_with::<Dealer>(SocketOptions {
        linger: Linger::For(Duration::ZERO),
        ..SocketOptions::default()
    });
    client.connect(broker)?;
    if verbose {
        log(&format!("I: connecting to broker at {broker}..."));
    }
    Ok(client)
}
