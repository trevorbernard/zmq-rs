//! Majordomo Protocol client API, asynchronous, as the guide's
//! `mdcliapi2`. Implements the MDP/Client spec at
//! <http://rfc.zeromq.org/spec:7>.
//!
//! It uses DEALER, so it can send any number of requests without waiting
//! for replies.

use std::time::Duration;

use zmq::{Context, Dealer, Frame, Linger, Message, Socket, SocketOptions};

use crate::dump;
use crate::mdp::{MDPC_CLIENT, log, message};

#[derive(Debug)]
pub struct MdClient {
    client: Socket<Dealer>,
    verbose: bool,
    timeout: Duration,
}

impl MdClient {
    pub fn new(context: &Context, broker: &str, verbose: bool) -> Result<Self, zmq::Error> {
        let mut client = context.socket_with::<Dealer>(SocketOptions {
            linger: Linger::For(Duration::ZERO),
            ..SocketOptions::default()
        });
        client.connect(broker)?;
        if verbose {
            log(&format!("I: connecting to broker at {broker}..."));
        }
        Ok(Self {
            client,
            verbose,
            timeout: Duration::from_millis(2500),
        })
    }

    pub fn set_timeout(&mut self, timeout: Duration) {
        self.timeout = timeout;
    }

    /// Sends `request` to `service` without waiting for the reply.
    pub fn send(&mut self, service: &str, request: Message) {
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
        self.client.send(request);
    }

    /// The next reply, or `None` if none came in time. Does not attempt to
    /// recover from a broker failure, which is not possible without storing
    /// all unanswered requests and resending them all.
    pub fn recv(&mut self) -> Option<Message> {
        let Some(reply) = self.client.recv_timeout(self.timeout) else {
            if self.verbose {
                log("W: permanent error, abandoning request");
            }
            return None;
        };
        if self.verbose {
            log("I: received reply:");
            dump(&reply);
        }
        // Don't try to handle errors, just assert noisily
        let mut frames = reply.into_iter();
        assert!(frames.next().is_some_and(|f| f.is_empty()));
        assert!(frames.next().is_some_and(|f| *f == *MDPC_CLIENT.as_bytes()));
        assert!(frames.next().is_some(), "service name");
        Message::try_from(frames.collect::<Vec<_>>()).ok()
    }
}
