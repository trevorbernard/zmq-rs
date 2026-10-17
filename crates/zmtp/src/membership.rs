//! Group membership, as DISH sockets send it to RADIO sockets (RFC 48):
//! JOIN and LEAVE commands on the wire. Sockets use a message form inside
//! the process, `0x01` or `0x00` then the group, and engines convert, as
//! for subscriptions.

use crate::{Command, Frame, Group, Message};

/// A change to a DISH's groups.
#[derive(Debug, Clone, PartialEq, Eq, Hash)]
pub enum Membership {
    /// Start receiving messages sent to the group.
    Join(Group),
    /// Stop receiving them.
    Leave(Group),
}

impl Membership {
    /// Reads the message form. Anything else, including a group that is
    /// not valid, is not a membership change.
    pub fn from_message(message: &Message) -> Option<Self> {
        let (frame, []) = message.split_first() else {
            return None;
        };
        let (&kind, name) = frame.split_first()?;
        let group = Group::new(Frame::copy_from_slice(name)).ok()?;
        match kind {
            1 => Some(Self::Join(group)),
            0 => Some(Self::Leave(group)),
            _ => None,
        }
    }

    /// The message form.
    pub fn to_message(&self) -> Message {
        let kind = match self {
            Self::Join(_) => 1,
            Self::Leave(_) => 0,
        };
        let name = self.group().as_slice();
        let mut frame = Vec::with_capacity(1 + name.len());
        frame.push(kind);
        frame.extend_from_slice(name);
        Message::new(Frame::from(frame))
    }

    /// Reads a JOIN or LEAVE command.
    pub fn from_command(command: Command) -> Option<Self> {
        match command {
            Command::Join(group) => Some(Self::Join(group)),
            Command::Leave(group) => Some(Self::Leave(group)),
            _ => None,
        }
    }

    /// The command form.
    pub fn to_command(&self) -> Command {
        match self {
            Self::Join(group) => Command::Join(group.clone()),
            Self::Leave(group) => Command::Leave(group.clone()),
        }
    }

    /// The group joined or left.
    pub fn group(&self) -> &Group {
        match self {
            Self::Join(group) | Self::Leave(group) => group,
        }
    }
}

#[cfg(test)]
mod tests {
    use super::*;

    fn group(name: &'static str) -> Group {
        Group::new(name).unwrap()
    }

    #[test]
    fn message_form() {
        let join = Membership::Join(group("news"));
        assert_eq!(join.to_message(), Message::from(&b"\x01news"[..]));
        let leave = Membership::Leave(group("news"));
        assert_eq!(leave.to_message(), Message::from(&b"\x00news"[..]));
        for change in [join, leave] {
            assert_eq!(Membership::from_message(&change.to_message()), Some(change));
        }
    }

    #[test]
    fn other_messages_are_not_membership_changes() {
        let cases = [
            Message::from(&b""[..]),
            Message::from(&b"\x02news"[..]),
            Message::from(&b"\x01bad\x00group"[..]),
            Message::from(vec![1u8; 257]),
            Message::try_from(vec![Frame::from(&b"\x01a"[..]), Frame::from("b")]).unwrap(),
        ];
        for message in cases {
            assert_eq!(Membership::from_message(&message), None, "{message:?}");
        }
    }

    #[test]
    fn command_form() {
        let join = Membership::Join(group("news"));
        assert_eq!(join.to_command(), Command::Join(group("news")));
        let leave = Membership::Leave(group("news"));
        assert_eq!(leave.to_command(), Command::Leave(group("news")));
        for change in [join, leave] {
            assert_eq!(Membership::from_command(change.to_command()), Some(change));
        }
        assert_eq!(
            Membership::from_command(Command::Subscribe(Frame::from("news"))),
            None
        );
    }
}
