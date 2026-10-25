//! Key-value message class for the Clone pattern, as the guide's
//! `kvmsg.c`: `kvsimple` with a UUID and properties. A message is five
//! frames: key, sequence number (8 octets, big-endian), UUID (16 octets),
//! properties (`name=value` lines) and body.

use std::collections::HashMap;

use zmq::{Frame, Message};

use crate::{randof, text};

#[derive(Debug, Clone, Default, PartialEq, Eq)]
pub struct KvMsg {
    key: Option<String>,
    sequence: i64,
    uuid: Option<Frame>,
    /// Properties as `(name, value)`, in the order first set.
    props: Vec<(String, String)>,
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

    /// The UUID, if the message has a 16-octet one.
    pub fn uuid(&self) -> Option<&[u8]> {
        self.uuid.as_deref().filter(|uuid| uuid.len() == 16)
    }

    /// Sets the UUID to a random value.
    pub fn set_uuid(&mut self) {
        let mut uuid = randof(u64::MAX).to_be_bytes().to_vec();
        uuid.extend(randof(u64::MAX).to_be_bytes());
        self.uuid = Some(uuid.into());
    }

    /// The property `name`, or `""` if it is not set.
    pub fn prop(&self, name: &str) -> &str {
        self.props
            .iter()
            .find(|(prop, _)| prop == name)
            .map_or("", |(_, value)| value)
    }

    /// Sets the property `name`, which must not contain `=`, replacing any
    /// value it had.
    pub fn set_prop(&mut self, name: &str, value: impl Into<String>) {
        assert!(!name.contains('='), "property names cannot contain '='");
        let value = value.into();
        match self.props.iter_mut().find(|(prop, _)| prop == name) {
            Some((_, existing)) => *existing = value,
            None => self.props.push((name.to_owned(), value)),
        }
    }

    /// The five-frame message to send. Missing frames are sent empty.
    pub fn to_message(&self) -> Message {
        let props: String = self
            .props
            .iter()
            .map(|(name, value)| format!("{name}={value}\n"))
            .collect();
        let mut message = Message::new(self.key.clone().unwrap_or_default());
        message.push(self.sequence.to_be_bytes().to_vec());
        message.push(self.uuid.clone().unwrap_or_default());
        message.push(props);
        message.push(self.body.clone().unwrap_or_default());
        message
    }

    /// Parses a received message; `None` unless it has exactly five frames
    /// and an 8-octet sequence number.
    pub fn from_message(message: &Message) -> Option<Self> {
        let [key, sequence, uuid, props, body] = message.iter().collect::<Vec<_>>()[..] else {
            return None;
        };
        let props = text(props)
            .lines()
            .filter_map(|line| line.split_once('='))
            .map(|(name, value)| (name.to_owned(), value.to_owned()))
            .collect();
        Some(Self {
            key: Some(text(key)),
            sequence: i64::from_be_bytes(sequence.as_slice().try_into().ok()?),
            uuid: Some(uuid.clone()),
            props,
            body: Some(body.clone()),
        })
    }

    /// Stores the message in `map` under its key, if it has both a key and
    /// a body. An empty body deletes the key instead.
    pub fn store(self, map: &mut HashMap<String, KvMsg>) {
        let Some(key) = &self.key else {
            return;
        };
        if self.size() == 0 {
            map.remove(key);
        } else if self.body.is_some() {
            map.insert(key.clone(), self);
        }
    }

    /// Prints the message to stderr, for debugging.
    pub fn dump(&self) {
        let props: String = self
            .props
            .iter()
            .map(|(name, value)| format!("{name}={value};"))
            .collect();
        let props = if props.is_empty() {
            props
        } else {
            format!("[{props}]")
        };
        let body: String = self
            .body()
            .unwrap_or_default()
            .iter()
            .map(|b| format!("{b:02X}"))
            .collect();
        eprintln!(
            "[seq:{}][key:{}][size:{}] {props}{body}",
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

        // Test send and receive of simple message
        let mut kvmap = HashMap::new();
        let mut kvmsg = KvMsg::new(1);
        kvmsg.set_key("key");
        kvmsg.set_uuid();
        kvmsg.set_body("body");
        output.send(kvmsg.to_message());
        kvmsg.clone().store(&mut kvmap);

        let received = KvMsg::from_message(&input.recv()).unwrap();
        assert_eq!(received, kvmsg);
        assert_eq!(received.uuid().map(<[u8]>::len), Some(16));
        received.store(&mut kvmap);
        assert_eq!(kvmap.len(), 1);

        // Test send and receive of message with properties
        let mut kvmsg = KvMsg::new(2);
        kvmsg.set_prop("prop1", "value1");
        kvmsg.set_prop("prop2", "value1");
        kvmsg.set_prop("prop2", "value2");
        kvmsg.set_key("key");
        kvmsg.set_uuid();
        kvmsg.set_body("body");
        assert_eq!(kvmsg.prop("prop2"), "value2");
        output.send(kvmsg.to_message());

        let received = KvMsg::from_message(&input.recv()).unwrap();
        assert_eq!(received.key(), Some("key"));
        assert_eq!(received.prop("prop1"), "value1");
        assert_eq!(received.prop("prop2"), "value2");
        assert_eq!(received.prop("missing"), "");
    }

    #[test]
    fn an_empty_body_deletes_its_key() {
        let mut kvmap = HashMap::new();
        let mut kvmsg = KvMsg::new(1);
        kvmsg.set_key("key");
        kvmsg.set_body("body");
        kvmsg.clone().store(&mut kvmap);
        kvmsg.set_body("");
        kvmsg.store(&mut kvmap);
        assert!(kvmap.is_empty());
    }
}
