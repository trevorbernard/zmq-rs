//! Majordomo Protocol worker API, as the guide's `mdwrkapi`. Implements
//! the MDP/Worker spec at <http://rfc.zeromq.org/spec:7>.

use std::thread;
use std::time::{Duration, Instant};

use zmq::{Context, Dealer, Frame, Linger, Message, Socket, SocketOptions};

use crate::dump;
use crate::mdp::{Command, MDPW_WORKER, log, message};

/// 3-5 is reasonable.
const HEARTBEAT_LIVENESS: usize = 3;

#[derive(Debug)]
pub struct MdWorker {
    context: Context,
    broker: String,
    service: String,
    worker: Socket<Dealer>,
    verbose: bool,

    // Heartbeat management
    heartbeat_at: Instant,
    liveness: usize,
    heartbeat: Duration,
    reconnect: Duration,

    expect_reply: bool,
    reply_to: Option<Frame>,
}

impl MdWorker {
    pub fn new(
        context: &Context,
        broker: &str,
        service: &str,
        verbose: bool,
    ) -> Result<Self, zmq::Error> {
        let heartbeat = Duration::from_millis(2500);
        let mut worker = Self {
            context: context.clone(),
            broker: broker.to_owned(),
            service: service.to_owned(),
            worker: connect(context, broker)?,
            verbose,
            heartbeat_at: Instant::now() + heartbeat,
            liveness: HEARTBEAT_LIVENESS,
            heartbeat,
            reconnect: Duration::from_millis(2500),
            expect_reply: false,
            reply_to: None,
        };
        worker.register();
        Ok(worker)
    }

    pub fn set_heartbeat(&mut self, heartbeat: Duration) {
        self.heartbeat = heartbeat;
    }

    pub fn set_reconnect(&mut self, reconnect: Duration) {
        self.reconnect = reconnect;
    }

    /// Sends `reply`, if any, to the broker and waits for the next request.
    pub fn recv(&mut self, reply: Option<Message>) -> Message {
        assert!(reply.is_some() || !self.expect_reply);
        if let Some(reply) = reply {
            let reply_to = self.reply_to.take().expect("a request to reply to");
            let mut frames = vec![reply_to, Frame::new()];
            frames.extend(reply);
            self.send_to_broker(Command::Reply, None, frames);
        }
        self.expect_reply = true;

        loop {
            if let Some(msg) = self.worker.recv_timeout(self.heartbeat) {
                if self.verbose {
                    log("I: received message from broker:");
                    dump(&msg);
                }
                self.liveness = HEARTBEAT_LIVENESS;

                // Don't try to handle errors, just assert noisily
                let mut frames = msg.into_iter();
                assert!(frames.next().is_some_and(|f| f.is_empty()));
                assert!(frames.next().is_some_and(|f| *f == *MDPW_WORKER.as_bytes()));
                let command = frames.next().as_deref().and_then(Command::parse);
                match command {
                    Some(Command::Request) => {
                        // We should pop and save as many addresses as there
                        // are up to a null part, but for now, just save one
                        self.reply_to = frames.next();
                        assert!(frames.next().is_some_and(|f| f.is_empty()));
                        // Here is where we actually have a message to
                        // process; we return it to the caller application
                        if let Ok(request) = Message::try_from(frames.collect::<Vec<_>>()) {
                            return request;
                        }
                        log("E: request without a body");
                    }
                    Some(Command::Heartbeat) => {}
                    Some(Command::Disconnect) => self.reconnect_to_broker(),
                    _ => log("E: invalid input message"),
                }
            } else {
                self.liveness -= 1;
                if self.liveness == 0 {
                    if self.verbose {
                        log("W: disconnected from broker - retrying...");
                    }
                    thread::sleep(self.reconnect);
                    self.reconnect_to_broker();
                }
            }
            // Send HEARTBEAT if it's time
            if Instant::now() > self.heartbeat_at {
                self.send_to_broker(Command::Heartbeat, None, Vec::new());
                self.heartbeat_at = Instant::now() + self.heartbeat;
            }
        }
    }

    /// Sends `command` with `option` and `body` to the broker.
    fn send_to_broker(&mut self, command: Command, option: Option<&str>, body: Vec<Frame>) {
        // Stack protocol envelope to start of message
        let mut frames = vec![Frame::new(), Frame::from(MDPW_WORKER), command.frame()];
        frames.extend(option.map(|option| Frame::from(option.to_owned())));
        frames.extend(body);
        let msg = message(frames);
        if self.verbose {
            log(&format!("I: sending {} to broker", command.name()));
            dump(&msg);
        }
        self.worker.send(msg);
    }

    fn reconnect_to_broker(&mut self) {
        self.worker =
            connect(&self.context, &self.broker).expect("connected to this endpoint before");
        self.register();
    }

    /// Registers our service with the broker.
    fn register(&mut self) {
        if self.verbose {
            log(&format!("I: connecting to broker at {}...", self.broker));
        }
        let service = self.service.clone();
        self.send_to_broker(Command::Ready, Some(&service), Vec::new());
        // If liveness hits zero, queue is considered disconnected
        self.liveness = HEARTBEAT_LIVENESS;
        self.heartbeat_at = Instant::now() + self.heartbeat;
    }
}

/// A new socket connected to `broker`. It discards what it could not send
/// when dropped, as CZMQ's sockets do.
fn connect(context: &Context, broker: &str) -> Result<Socket<Dealer>, zmq::Error> {
    let mut worker = context.socket_with::<Dealer>(SocketOptions {
        linger: Linger::For(Duration::ZERO),
        ..SocketOptions::default()
    });
    worker.connect(broker)?;
    Ok(worker)
}
