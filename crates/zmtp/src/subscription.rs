//! Subscriptions, as SUB and XSUB sockets send them upstream (RFC 29).
//!
//! ZMTP 3.0 and earlier carry them as messages: one frame, `0x01` or `0x00`
//! then the topic. ZMTP 3.1 carries them as SUBSCRIBE and CANCEL commands
//! (RFC 37). Sockets use the message form; engines convert.

use crate::{Command, Frame, Message};

/// A change to a subscriber's topics. A topic matches every message whose
/// first frame starts with it; the empty topic matches everything.
#[derive(Debug, Clone, PartialEq, Eq, Hash)]
pub enum Subscription {
    /// Start receiving messages that start with the topic.
    Subscribe(Frame),
    /// Stop receiving them.
    Cancel(Frame),
}

impl Subscription {
    /// Reads the message form. Messages of more than one frame, and frames
    /// not starting with `0x01` or `0x00`, are not subscriptions.
    pub fn from_message(message: &Message) -> Option<Self> {
        let (frame, []) = message.split_first() else {
            return None;
        };
        let (&kind, topic) = frame.split_first()?;
        let topic = Frame::copy_from_slice(topic);
        match kind {
            1 => Some(Self::Subscribe(topic)),
            0 => Some(Self::Cancel(topic)),
            _ => None,
        }
    }

    /// The message form.
    pub fn to_message(&self) -> Message {
        let kind = match self {
            Self::Subscribe(_) => 1,
            Self::Cancel(_) => 0,
        };
        let topic = self.topic();
        let mut frame = Vec::with_capacity(1 + topic.len());
        frame.push(kind);
        frame.extend_from_slice(topic);
        Message::new(Frame::from(frame))
    }

    /// Reads a SUBSCRIBE or CANCEL command.
    pub fn from_command(command: Command) -> Option<Self> {
        match command {
            Command::Subscribe(topic) => Some(Self::Subscribe(topic)),
            Command::Cancel(topic) => Some(Self::Cancel(topic)),
            _ => None,
        }
    }

    /// The ZMTP 3.1 command form.
    pub fn to_command(&self) -> Command {
        match self {
            Self::Subscribe(topic) => Command::Subscribe(topic.clone()),
            Self::Cancel(topic) => Command::Cancel(topic.clone()),
        }
    }

    /// The topic subscribed to or cancelled.
    pub fn topic(&self) -> &Frame {
        match self {
            Self::Subscribe(topic) | Self::Cancel(topic) => topic,
        }
    }
}

#[cfg(test)]
mod tests {
    use super::*;

    #[test]
    fn message_form() {
        let subscribe = Subscription::Subscribe(Frame::from("news"));
        assert_eq!(subscribe.to_message(), Message::from(&b"\x01news"[..]));
        let cancel = Subscription::Cancel(Frame::new());
        assert_eq!(cancel.to_message(), Message::from(&b"\x00"[..]));
        for subscription in [subscribe, cancel] {
            assert_eq!(
                Subscription::from_message(&subscription.to_message()),
                Some(subscription)
            );
        }
    }

    #[test]
    fn other_messages_are_not_subscriptions() {
        let cases = [
            Message::from(&b""[..]),
            Message::from(&b"\x02news"[..]),
            Message::from("news"),
            Message::try_from(vec![Frame::from(&b"\x01a"[..]), Frame::from("b")]).unwrap(),
        ];
        for message in cases {
            assert_eq!(Subscription::from_message(&message), None, "{message:?}");
        }
    }

    #[test]
    fn command_form() {
        let subscribe = Subscription::Subscribe(Frame::from("news"));
        assert_eq!(
            subscribe.to_command(),
            Command::Subscribe(Frame::from("news"))
        );
        let cancel = Subscription::Cancel(Frame::from("news"));
        assert_eq!(cancel.to_command(), Command::Cancel(Frame::from("news")));
        for subscription in [subscribe, cancel] {
            assert_eq!(
                Subscription::from_command(subscription.to_command()),
                Some(subscription)
            );
        }
        assert_eq!(
            Subscription::from_command(Command::Pong(Default::default())),
            None
        );
    }

    #[test]
    fn long_topics_survive_both_forms() {
        let topic = Frame::from(vec![b't'; 300]);
        let subscription = Subscription::Subscribe(topic);
        let message = subscription.to_message();
        assert_eq!(message.get(0).unwrap().len(), 301);
        assert_eq!(Subscription::from_message(&message), Some(subscription));
    }
}
