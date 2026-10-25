//! Helpers shared by the ZeroMQ Guide's examples, as the guide's
//! `zhelpers.h`.

pub mod bstar;
pub mod clone;
pub mod flcliapi;
pub mod kvmsg;
pub mod kvsimple;
pub mod mdcliapi;
pub mod mdcliapi2;
pub mod mdp;
pub mod mdwrkapi;

use std::hash::{BuildHasher, RandomState};
use std::thread;
use std::time::Duration;

use zmq::{Frame, Identity, Message, Router, RoutingId, Socket};

/// A random number in `0..n`, as the guide's `randof`.
pub fn randof(n: u64) -> u64 {
    RandomState::new().hash_one(()) % n
}

/// Sleeps for `msecs` milliseconds, as the guide's `s_sleep`.
pub fn sleep_ms(msecs: u64) {
    thread::sleep(Duration::from_millis(msecs));
}

/// A frame as text, replacing invalid UTF-8.
pub fn text(frame: &[u8]) -> String {
    String::from_utf8_lossy(frame).into_owned()
}

/// The message's last frame as text, for the guide's single-frame
/// `s_recv`.
pub fn body(message: &Message) -> String {
    text(message.last())
}

/// Prints each frame of `message`, as text if it is printable and as hex
/// otherwise, as the guide's `s_dump`.
pub fn dump(message: &Message) {
    println!("----------------------------------------");
    for frame in message {
        let printable = frame.iter().all(|&b| (32..127).contains(&b));
        let shown = if printable {
            text(frame)
        } else {
            frame.iter().map(|b| format!("{b:02X}")).collect()
        };
        println!("[{:03}] {shown}", frame.len());
    }
}

/// A random printable identity, as the guide's `s_set_id`.
pub fn random_identity() -> Identity {
    Identity::new(format!("{:04X}-{:04X}", randof(0x10000), randof(0x10000))).expect("short enough")
}

/// The message a ROUTER received, with its sender's routing id back in
/// front, as libzmq's ROUTER delivers it.
pub fn with_sender((from, message): (RoutingId, Message)) -> Message {
    let mut framed = Message::new(Frame::from(from));
    framed.extend(message);
    framed
}

/// Sends `message` to the peer its first frame names, as libzmq's ROUTER
/// does. Like it, drops the message if there is no such peer or it is
/// full.
pub fn route(
    router: &mut Socket<Router>,
    message: Message,
) -> Result<(), Box<dyn std::error::Error>> {
    let (to, rest) = message.into_parts();
    if let Ok(rest) = Message::try_from(rest) {
        let _ = router.try_send(&RoutingId::new(to)?, rest);
    }
    Ok(())
}
