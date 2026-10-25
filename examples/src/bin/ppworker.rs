//! Paranoid Pirate worker: a DEALER that heartbeats the queue, so each
//! notices if the other dies, and reconnects with exponential backoff
//! when the queue goes quiet.

use std::thread;
use std::time::{Duration, Instant};

use zguide::{dump, randof, sleep_ms};
use zmq::{Context, Dealer, Linger, Socket, SocketOptions};

const HEARTBEAT_LIVENESS: u32 = 3; // 3-5 is reasonable
const HEARTBEAT_INTERVAL: Duration = Duration::from_millis(1000);
const INTERVAL_INIT: Duration = Duration::from_millis(1000); // Initial reconnect
const INTERVAL_MAX: Duration = Duration::from_millis(32000); // After exponential backoff

// Paranoid Pirate Protocol constants
const PPP_READY: &[u8] = b"\x01"; // Signals worker is ready
const PPP_HEARTBEAT: &[u8] = b"\x02"; // Signals worker heartbeat

/// A new socket connected to the Paranoid Pirate queue, which it has told
/// it is ready.
fn worker_socket(context: &Context) -> Result<Socket<Dealer>, zmq::Error> {
    let mut worker = context.socket_with::<Dealer>(SocketOptions {
        linger: Linger::For(Duration::ZERO),
        ..SocketOptions::default()
    });
    worker.connect("tcp://localhost:5556")?;

    // Tell queue we're ready for work
    println!("I: worker ready");
    worker.send(PPP_READY);
    Ok(worker)
}

fn main() -> Result<(), Box<dyn std::error::Error>> {
    let context = Context::new()?;
    let mut worker = worker_socket(&context)?;

    // If liveness hits zero, queue is considered disconnected
    let mut liveness = HEARTBEAT_LIVENESS;
    let mut interval = INTERVAL_INIT;

    // Send out heartbeats at regular intervals
    let mut heartbeat_at = Instant::now() + HEARTBEAT_INTERVAL;

    let mut cycles = 0;
    loop {
        if let Some(message) = worker.recv_timeout(HEARTBEAT_INTERVAL) {
            // Get message
            // - envelope + content -> request
            // - 1-part HEARTBEAT -> heartbeat
            if message.len().get() > 1 {
                // To test the robustness of the queue implementation
                // we simulate various typical problems, such as the
                // worker crashing or running very slowly. We do this
                // after a few cycles so that the architecture can get
                // up and running first
                cycles += 1;
                if cycles > 3 && randof(5) == 0 {
                    println!("I: simulating a crash");
                    return Ok(());
                } else if cycles > 3 && randof(5) == 0 {
                    println!("I: simulating CPU overload");
                    sleep_ms(3000);
                }
                println!("I: normal reply");
                worker.send(message);
                liveness = HEARTBEAT_LIVENESS;
                sleep_ms(1000); // Do some heavy work
            } else if **message.last() == *PPP_HEARTBEAT {
                // When we get a heartbeat message from the queue, it means
                // the queue was (recently) alive, so we must reset our
                // liveness indicator
                liveness = HEARTBEAT_LIVENESS;
            } else {
                println!("E: invalid message");
                dump(&message);
            }
            interval = INTERVAL_INIT;
        } else {
            // If the queue hasn't sent us heartbeats in a while, destroy
            // the socket and reconnect. This is the simplest most brutal
            // way of discarding any messages we might have sent in the
            // meantime
            liveness -= 1;
            if liveness == 0 {
                println!("W: heartbeat failure, can't reach queue");
                println!("W: reconnecting in {} msec...", interval.as_millis());
                thread::sleep(interval);

                if interval < INTERVAL_MAX {
                    interval *= 2;
                }
                worker = worker_socket(&context)?;
                liveness = HEARTBEAT_LIVENESS;
            }
        }
        // Send heartbeat to queue if it's time
        if Instant::now() > heartbeat_at {
            heartbeat_at = Instant::now() + HEARTBEAT_INTERVAL;
            println!("I: worker heartbeat");
            worker.send(PPP_HEARTBEAT);
        }
    }
}
