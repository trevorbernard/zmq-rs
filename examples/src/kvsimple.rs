//! Simple key-value message class for the Clone pattern, as the guide's
//! `kvsimple.c`. A message is three frames: key, sequence number (8 octets,
//! big-endian) and body.

use std::collections::HashMap;

use zmq::{Frame, Message};

#[derive(Debug, Clone, Default, PartialEq, Eq)]
pub struct KvMsg {
    key: Option<String>,
    sequence: i64,
    body: Option<Frame>,
}

impl KvMsg {
    pub fn new(sequence: i64) -> Self {
        Self {
            sequence,
            ..Self::default()
        }
    }

    pub fn key(&self) -> Option<&str> {
        self.key.as_deref()
    }

    pub fn set_key(&mut self, key: impl Into<String>) {
        self.key = Some(key.into());
    }

    pub fn sequence(&self) -> i64 {
        self.sequence
    }

    pub fn set_sequence(&mut self, sequence: i64) {
        self.sequence = sequence;
    }

    pub fn body(&self) -> Option<&[u8]> {
        self.body.as_deref()
    }

    pub fn set_body(&mut self, body: impl Into<Frame>) {
        self.body = Some(body.into());
    }

    /// The body's size, 0 if there is none.
    pub fn size(&self) -> usize {
        self.body().map_or(0, <[u8]>::len)
    }

    /// The three-frame message to send. Missing frames are sent empty.
    pub fn to_message(&self) -> Message {
        let mut message = Message::new(self.key.clone().unwrap_or_default());
        message.push(self.sequence.to_be_bytes().to_vec());
        message.push(self.body.clone().unwrap_or_default());
        message
    }

    /// Parses a received message; `None` unless it has exactly three frames
    /// and an 8-octet sequence number.
    pub fn from_message(message: &Message) -> Option<Self> {
        let [key, sequence, body] = message.iter().collect::<Vec<_>>()[..] else {
            return None;
        };
        Some(Self {
            key: Some(String::from_utf8_lossy(key).into_owned()),
            sequence: i64::from_be_bytes(sequence.as_slice().try_into().ok()?),
            body: Some(body.clone()),
        })
    }

    /// Stores the message in `map` under its key, if it has both a key and
    /// a body.
    pub fn store(self, map: &mut HashMap<String, KvMsg>) {
        if let (Some(key), Some(_)) = (&self.key, &self.body) {
            map.insert(key.clone(), self);
        }
    }

    /// Prints the message to stderr, for debugging.
    pub fn dump(&self) {
        let body: String = self
            .body()
            .unwrap_or_default()
            .iter()
            .map(|b| format!("{b:02X}"))
            .collect();
        eprintln!(
            "[seq:{}][key:{}][size:{}] {body}",
            self.sequence,
            self.key().unwrap_or_default(),
            self.size()
        );
    }
}

#[cfg(test)]
mod tests {
    use super::*;
    use zmq::{Context, Dealer};

    #[test]
    fn round_trips_through_a_socket() {
        let context = Context::new().unwrap();
        let mut output = context.socket::<Dealer>();
        output.bind("inproc://kvmsg_selftest").unwrap();
        let mut input = context.socket::<Dealer>();
        input.connect("inproc://kvmsg_selftest").unwrap();

        let mut kvmap = HashMap::new();
        let mut kvmsg = KvMsg::new(1);
        kvmsg.set_key("key");
        kvmsg.set_body("body");
        output.send(kvmsg.to_message());
        kvmsg.clone().store(&mut kvmap);

        let received = KvMsg::from_message(&input.recv()).unwrap();
        assert_eq!(received, kvmsg);
        assert_eq!(received.key(), Some("key"));
        received.store(&mut kvmap);
        assert_eq!(kvmap.len(), 1);
    }
}
