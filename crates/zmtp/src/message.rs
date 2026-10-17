//! A whole multipart message.

use std::fmt;
use std::iter::FusedIterator;
use std::num::NonZeroUsize;

use crate::Frame;

/// A ZeroMQ message: one or more [`Frame`]s, sent and delivered atomically.
#[derive(Clone, PartialEq, Eq, Hash)]
pub struct Message {
    first: Frame,
    rest: Vec<Frame>,
}

/// Returned when building a [`Message`] from zero frames.
#[derive(Debug, Clone, Copy, PartialEq, Eq)]
pub struct EmptyMessage;

impl Message {
    /// A single-part message.
    pub fn new(frame: impl Into<Frame>) -> Self {
        Self {
            first: frame.into(),
            rest: Vec::new(),
        }
    }

    /// Appends a frame.
    pub fn push(&mut self, frame: impl Into<Frame>) {
        self.rest.push(frame.into());
    }

    /// The number of frames. Never zero.
    pub fn len(&self) -> NonZeroUsize {
        NonZeroUsize::MIN.saturating_add(self.rest.len())
    }

    /// The frame at `index`.
    pub fn get(&self, index: usize) -> Option<&Frame> {
        match index {
            0 => Some(&self.first),
            i => self.rest.get(i - 1),
        }
    }

    /// The first frame and the remaining frames.
    pub fn split_first(&self) -> (&Frame, &[Frame]) {
        (&self.first, &self.rest)
    }

    /// The last frame.
    pub fn last(&self) -> &Frame {
        self.rest.last().unwrap_or(&self.first)
    }

    /// Iterates over the frames in order.
    pub fn iter(&self) -> Iter<'_> {
        Iter {
            first: Some(&self.first),
            rest: self.rest.iter(),
        }
    }

    /// Takes the message apart into its first frame and the remaining frames.
    pub fn into_parts(self) -> (Frame, Vec<Frame>) {
        (self.first, self.rest)
    }
}

/// Iterator over the frames of a [`Message`].
#[derive(Debug, Clone)]
pub struct Iter<'a> {
    first: Option<&'a Frame>,
    rest: std::slice::Iter<'a, Frame>,
}

impl<'a> Iterator for Iter<'a> {
    type Item = &'a Frame;

    fn next(&mut self) -> Option<&'a Frame> {
        self.first.take().or_else(|| self.rest.next())
    }

    fn size_hint(&self) -> (usize, Option<usize>) {
        let len = usize::from(self.first.is_some()) + self.rest.len();
        (len, Some(len))
    }
}

impl ExactSizeIterator for Iter<'_> {}

impl FusedIterator for Iter<'_> {}

/// Owning iterator over the frames of a [`Message`].
#[derive(Debug)]
pub struct IntoIter {
    first: Option<Frame>,
    rest: std::vec::IntoIter<Frame>,
}

impl Iterator for IntoIter {
    type Item = Frame;

    fn next(&mut self) -> Option<Frame> {
        self.first.take().or_else(|| self.rest.next())
    }

    fn size_hint(&self) -> (usize, Option<usize>) {
        let len = usize::from(self.first.is_some()) + self.rest.len();
        (len, Some(len))
    }
}

impl ExactSizeIterator for IntoIter {}

impl FusedIterator for IntoIter {}

impl IntoIterator for Message {
    type Item = Frame;
    type IntoIter = IntoIter;

    fn into_iter(self) -> IntoIter {
        IntoIter {
            first: Some(self.first),
            rest: self.rest.into_iter(),
        }
    }
}

impl<'a> IntoIterator for &'a Message {
    type Item = &'a Frame;
    type IntoIter = Iter<'a>;

    fn into_iter(self) -> Iter<'a> {
        self.iter()
    }
}

/// A single-part message from anything that makes a [`Frame`], such as
/// `"text"`, a `Vec<u8>` or a `Frame`.
impl<T: Into<Frame>> From<T> for Message {
    fn from(frame: T) -> Self {
        Self::new(frame)
    }
}

impl TryFrom<Vec<Frame>> for Message {
    type Error = EmptyMessage;

    fn try_from(frames: Vec<Frame>) -> Result<Self, EmptyMessage> {
        let mut rest = frames.into_iter();
        let first = rest.next().ok_or(EmptyMessage)?;
        Ok(Self {
            first,
            rest: rest.collect(),
        })
    }
}

impl Extend<Frame> for Message {
    fn extend<I: IntoIterator<Item = Frame>>(&mut self, frames: I) {
        self.rest.extend(frames);
    }
}

impl fmt::Debug for Message {
    fn fmt(&self, f: &mut fmt::Formatter<'_>) -> fmt::Result {
        f.write_str("Message")?;
        f.debug_list().entries(self).finish()
    }
}

impl fmt::Display for EmptyMessage {
    fn fmt(&self, f: &mut fmt::Formatter<'_>) -> fmt::Result {
        f.write_str("a message must have at least one frame")
    }
}

impl std::error::Error for EmptyMessage {}

#[cfg(test)]
mod tests {
    use super::*;

    fn frames(parts: &[&'static str]) -> Vec<Frame> {
        parts.iter().map(|&p| Frame::from(p)).collect()
    }

    #[test]
    #[cfg(target_pointer_width = "64")]
    fn message_is_56_bytes() {
        assert_eq!(size_of::<Message>(), 56);
    }

    #[test]
    fn single_part_message_does_not_allocate_a_frame_list() {
        let msg = Message::new("hello");
        assert_eq!(msg.rest.capacity(), 0);
        assert_eq!(msg.len().get(), 1);
        assert_eq!(msg.split_first(), (&Frame::from("hello"), &[][..]));
    }

    #[test]
    fn an_empty_frame_is_a_valid_message() {
        let msg = Message::new(Frame::new());
        assert_eq!(msg.len().get(), 1);
        assert!(msg.last().is_empty());
    }

    #[test]
    fn push_appends_frames_in_order() {
        let mut msg = Message::new("a");
        msg.push("b");
        msg.push(Frame::from("c"));
        assert_eq!(msg.len().get(), 3);
        assert_eq!(
            msg.iter().cloned().collect::<Vec<_>>(),
            frames(&["a", "b", "c"])
        );
    }

    #[test]
    fn get_and_last_index_across_all_frames() {
        let msg = Message::try_from(frames(&["a", "b", "c"])).unwrap();
        assert_eq!(msg.get(0), Some(&Frame::from("a")));
        assert_eq!(msg.get(2), Some(&Frame::from("c")));
        assert_eq!(msg.get(3), None);
        assert_eq!(msg.last(), &Frame::from("c"));
        assert_eq!(Message::new("x").last(), &Frame::from("x"));
    }

    #[test]
    fn split_first_separates_the_head() {
        let msg = Message::try_from(frames(&["id", "", "body"])).unwrap();
        let (head, tail) = msg.split_first();
        assert_eq!(head, &Frame::from("id"));
        assert_eq!(tail, &frames(&["", "body"])[..]);
    }

    #[test]
    fn empty_frame_list_is_rejected() {
        assert_eq!(Message::try_from(Vec::<Frame>::new()), Err(EmptyMessage));
        assert_eq!(
            EmptyMessage.to_string(),
            "a message must have at least one frame"
        );
    }

    #[test]
    fn try_from_round_trips_through_into_parts() {
        let msg = Message::try_from(frames(&["a", "b"])).unwrap();
        let (first, rest) = msg.into_parts();
        assert_eq!(first, Frame::from("a"));
        assert_eq!(rest, frames(&["b"]));
    }

    #[test]
    fn iterators_report_exact_length() {
        let msg = Message::try_from(frames(&["a", "b", "c"])).unwrap();
        let mut iter = msg.iter();
        assert_eq!(iter.len(), 3);
        iter.next();
        assert_eq!(iter.len(), 2);
        assert_eq!(msg.clone().into_iter().len(), 3);
    }

    #[test]
    fn owned_and_borrowed_iteration_yield_all_frames() {
        let msg = Message::try_from(frames(&["a", "b"])).unwrap();
        let borrowed: Vec<&Frame> = (&msg).into_iter().collect();
        assert_eq!(borrowed, frames(&["a", "b"]).iter().collect::<Vec<_>>());
        let owned: Vec<Frame> = msg.into_iter().collect();
        assert_eq!(owned, frames(&["a", "b"]));
    }

    #[test]
    fn single_part_messages_convert_from_frame_sources() {
        assert_eq!(Message::from("hi"), Message::new(Frame::from("hi")));
        assert_eq!(Message::from(b"hi".to_vec()), Message::new("hi"));
        assert_eq!(Message::from(Frame::from("hi")), Message::new("hi"));
    }

    #[test]
    fn extend_appends_frames() {
        let mut msg = Message::from(Frame::from("a"));
        msg.extend(frames(&["b", "c"]));
        assert_eq!(
            msg.into_iter().collect::<Vec<_>>(),
            frames(&["a", "b", "c"])
        );
    }

    #[test]
    fn equality_follows_frames() {
        let a = Message::try_from(frames(&["a", "b"])).unwrap();
        let mut b = Message::new("a");
        b.push("b");
        assert_eq!(a, b);
        assert_ne!(a, Message::new("a"));
    }

    #[test]
    fn debug_lists_frames() {
        let mut msg = Message::new("a");
        msg.push("");
        assert_eq!(format!("{msg:?}"), r#"Message[Frame(b"a"), Frame(b"")]"#);
    }
}
