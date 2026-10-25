#![no_main]

use libfuzzer_sys::arbitrary::{self, Arbitrary};
use libfuzzer_sys::fuzz_target;
use zmtp::{
    Command, Decoder, Encoder, Frame, Group, Incoming, Membership, Message, Subscription, Version,
};

// What we write, we read back as it was: messages and commands through the
// encoder and decoder at every version, subscriptions and memberships
// through both their forms.

#[derive(Arbitrary, Debug)]
enum Case {
    /// Encoded, sending frames of at least `apart` octets apart, then fed
    /// to the decoder `chunk` octets at a time.
    Message {
        version: u8,
        frames: Vec<Vec<u8>>,
        apart: u16,
        chunk: u8,
    },
    Command {
        minor: bool,
        body: Vec<u8>,
    },
    Subscription {
        subscribe: bool,
        topic: Vec<u8>,
    },
    Membership {
        join: bool,
        group: Vec<u8>,
    },
}

const VERSIONS: [Version; 4] = [Version::V1, Version::V2, Version::V3_0, Version::V3_1];

/// Feeds `wire` to a decoder `chunk` octets at a time.
fn decode(version: Version, wire: &[u8], chunk: usize) -> Vec<Incoming> {
    let mut decoder = Decoder::new(version, zmtp_fuzz::decoder_config());
    let mut items = Vec::new();
    for piece in wire.chunks(chunk) {
        decoder.extend_from_slice(piece);
        while let Some(item) = decoder.decode().expect("what we encoded") {
            items.push(item);
        }
    }
    items
}

/// A command's body as it travels, then parsed back.
fn reparse(command: &Command) -> Command {
    let mut body = Vec::new();
    command.encode(&mut body);
    Command::parse(&body).expect("what we encoded")
}

fuzz_target!(|case: Case| match case {
    Case::Message {
        version,
        frames,
        apart,
        chunk,
    } => {
        let frames: Vec<Frame> = frames.into_iter().map(Frame::from).collect();
        let Ok(message) = Message::try_from(frames) else {
            return;
        };
        if let Some(subscription) = Subscription::from_message(&message) {
            assert_eq!(subscription.to_message(), message);
        }
        if let Some(membership) = Membership::from_message(&message) {
            assert_eq!(membership.to_message(), message);
        }
        let version = VERSIONS[usize::from(version) % VERSIONS.len()];
        let encoder = Encoder::new(version);
        let mut wire = Vec::new();
        encoder.encode(&message, &mut wire);

        let mut parts = Vec::new();
        let mut reassembled = Vec::new();
        encoder.encode_parts(
            message.clone(),
            usize::from(apart),
            &mut parts,
            |at, shared| {
                reassembled.push((at, shared));
            },
        );
        for (at, shared) in reassembled.into_iter().rev() {
            parts.splice(at..at, shared);
        }
        assert_eq!(parts, wire, "encode_parts and encode differ");

        let items = decode(version, &wire, usize::from(chunk.max(1)));
        assert_eq!(items, [Incoming::Message(message)]);
    }
    Case::Command { minor, body } => {
        let version = if minor { Version::V3_1 } else { Version::V3_0 };
        let mut wire = Vec::new();
        Encoder::new(version)
            .encode_command(&body, &mut wire)
            .expect("ZMTP 3 has commands");
        let items = decode(version, &wire, wire.len().max(1));
        assert_eq!(items, [Incoming::Command(Frame::from(body))]);
    }
    Case::Subscription { subscribe, topic } => {
        let topic = Frame::from(topic);
        let subscription = if subscribe {
            Subscription::Subscribe(topic)
        } else {
            Subscription::Cancel(topic)
        };
        let message = subscription.to_message();
        assert_eq!(
            Subscription::from_message(&message).as_ref(),
            Some(&subscription)
        );
        let command = reparse(&subscription.to_command());
        assert_eq!(Subscription::from_command(command), Some(subscription));
    }
    Case::Membership { join, group } => {
        let Ok(group) = Group::new(group) else {
            return;
        };
        let membership = if join {
            Membership::Join(group)
        } else {
            Membership::Leave(group)
        };
        let message = membership.to_message();
        assert_eq!(
            Membership::from_message(&message).as_ref(),
            Some(&membership)
        );
        let command = reparse(&membership.to_command());
        assert_eq!(Membership::from_command(command), Some(membership));
    }
});
