//! Majordomo Protocol definitions, as the guide's `mdp.h`.

use std::time::{SystemTime, UNIX_EPOCH};

use zmq::{Frame, Message};

/// The version of MDP/Client we implement.
pub const MDPC_CLIENT: &str = "MDPC01";

/// The version of MDP/Worker we implement.
pub const MDPW_WORKER: &str = "MDPW01";

/// MDP/Worker commands.
#[derive(Debug, Clone, Copy, PartialEq, Eq)]
pub enum Command {
    Ready = 1,
    Request = 2,
    Reply = 3,
    Heartbeat = 4,
    Disconnect = 5,
}

impl Command {
    /// The command's one-octet frame.
    pub fn frame(self) -> Frame {
        Frame::from(vec![self as u8])
    }

    /// The command a frame holds, if any.
    pub fn parse(frame: &[u8]) -> Option<Self> {
        match frame {
            [1] => Some(Self::Ready),
            [2] => Some(Self::Request),
            [3] => Some(Self::Reply),
            [4] => Some(Self::Heartbeat),
            [5] => Some(Self::Disconnect),
            _ => None,
        }
    }

    pub fn name(self) -> &'static str {
        match self {
            Self::Ready => "READY",
            Self::Request => "REQUEST",
            Self::Reply => "REPLY",
            Self::Heartbeat => "HEARTBEAT",
            Self::Disconnect => "DISCONNECT",
        }
    }
}

/// A message of `frames`, which must not be empty.
pub fn message(frames: Vec<Frame>) -> Message {
    Message::try_from(frames).expect("at least one frame")
}

/// Prints `text` after the time of day (UTC), as CZMQ's `zclock_log`.
pub fn log(text: &str) {
    let secs = SystemTime::now()
        .duration_since(UNIX_EPOCH)
        .map_or(0, |elapsed| elapsed.as_secs());
    let (h, m, s) = (secs / 3600 % 24, secs / 60 % 60, secs % 60);
    println!("{h:02}:{m:02}:{s:02} {text}");
}
