//! A whole multipart message.

use std::collections::HashSet;
use std::fmt;
use std::hash::{Hash, Hasher};
use std::iter::FusedIterator;
use std::num::NonZeroUsize;
use std::sync::Arc;
use std::{mem, slice};

use crate::Frame;

/// A ZeroMQ message: one or more [`Frame`]s, sent and delivered atomically.
///
/// A message received over a connection carries that connection's
/// [`MessageMetadata`], as libzmq's do. Comparing and hashing messages
/// looks only at their frames.
#[derive(Clone)]
pub struct Message {
    first: Frame,
    rest: Rest,
    metadata: Option<MessageMetadata>,
}

/// The properties of the connection a message arrived on, as libzmq's
/// `zmq_msg_gets` reads them: `Socket-Type` and `Identity` and any other
/// property the peer announced, `User-Id` and any metadata ZAP returned,
/// and the peer's IP address as `Peer-Address` over TCP. Shared by every
/// message from that connection.
#[derive(Clone)]
pub struct MessageMetadata(Arc<Vec<(Frame, Frame)>>);

impl MessageMetadata {
    /// Metadata of `properties`, in order of precedence: of two with the
    /// same name, the first is kept, as libzmq builds its map.
    pub fn new(properties: impl IntoIterator<Item = (Frame, Frame)>) -> Self {
        // A set, not a scan of what is kept: a peer chooses how many
        // properties it announces.
        let mut seen = HashSet::new();
        let kept = properties
            .into_iter()
            .filter(|(name, _)| seen.insert(name.clone()))
            .collect();
        Self(Arc::new(kept))
    }

    /// The value of the property `name`, matched exactly, case and all, as
    /// libzmq does. `Identity`, the name ZMTP 3.0 peers announce, is looked
    /// up as `Routing-Id` if absent, as libzmq does for its deprecated
    /// name.
    pub fn get(&self, name: &str) -> Option<&[u8]> {
        let exact = |name: &str| {
            self.0
                .iter()
                .find(|(property, _)| property.as_slice() == name.as_bytes())
                .map(|(_, value)| value.as_slice())
        };
        exact(name).or_else(|| (name == "Identity").then(|| exact("Routing-Id")).flatten())
    }

    /// Whether there are no properties.
    #[inline]
    pub fn is_empty(&self) -> bool {
        self.0.is_empty()
    }

    /// Every property, as names and values.
    pub fn iter(&self) -> impl ExactSizeIterator<Item = (&[u8], &[u8])> {
        self.0
            .iter()
            .map(|(name, value)| (name.as_slice(), value.as_slice()))
    }

    /// Whether `self` and `other` are the same connection's metadata.
    pub fn ptr_eq(&self, other: &Self) -> bool {
        Arc::ptr_eq(&self.0, &other.0)
    }
}

impl fmt::Debug for MessageMetadata {
    fn fmt(&self, f: &mut fmt::Formatter<'_>) -> fmt::Result {
        f.debug_map()
            .entries(
                self.iter()
                    .map(|(name, value)| (name.escape_ascii(), value.escape_ascii())),
            )
            .finish()
    }
}

/// The frames after the first. A second frame is held inline, so the
/// common two-frame message (a REQ's delimiter and body, a topic and its
/// data) allocates no list; a third moves them all to a `Vec`.
#[derive(Clone)]
enum Rest {
    /// None when empty, which allocates nothing either.
    Many(Vec<Frame>),
    One(Frame),
}

impl Rest {
    fn as_slice(&self) -> &[Frame] {
        match self {
            Self::Many(frames) => frames,
            Self::One(frame) => slice::from_ref(frame),
        }
    }

    fn push(&mut self, frame: Frame) {
        match self {
            Self::Many(frames) if frames.is_empty() => *self = Self::One(frame),
            Self::Many(frames) => frames.push(frame),
            Self::One(_) => {
                let Self::One(second) = mem::replace(self, Self::Many(Vec::new())) else {
                    unreachable!("matched One");
                };
                // Room for two more, so that a message of up to five frames
                // allocates its list once.
                let mut frames = Vec::with_capacity(4);
                frames.extend([second, frame]);
                *self = Self::Many(frames);
            }
        }
    }

    fn into_vec(self) -> Vec<Frame> {
        match self {
            Self::Many(frames) => frames,
            Self::One(frame) => vec![frame],
        }
    }
}

/// Returned when building a [`Message`] from zero frames.
#[derive(Debug, Clone, Copy, PartialEq, Eq)]
pub struct EmptyMessage;

impl Message {
    /// A single-part message.
    pub fn new(frame: impl Into<Frame>) -> Self {
        Self {
            first: frame.into(),
            rest: Rest::Many(Vec::new()),
            metadata: None,
        }
    }

    /// Appends a frame.
    pub fn push(&mut self, frame: impl Into<Frame>) {
        self.rest.push(frame.into());
    }

    /// The number of frames. Never zero.
    pub fn len(&self) -> NonZeroUsize {
        NonZeroUsize::MIN.saturating_add(self.rest.as_slice().len())
    }

    /// The frame at `index`.
    pub fn get(&self, index: usize) -> Option<&Frame> {
        match index {
            0 => Some(&self.first),
            i => self.rest.as_slice().get(i - 1),
        }
    }

    /// The first frame and the remaining frames.
    pub fn split_first(&self) -> (&Frame, &[Frame]) {
        (&self.first, self.rest.as_slice())
    }

    /// The last frame.
    pub fn last(&self) -> &Frame {
        self.rest.as_slice().last().unwrap_or(&self.first)
    }

    /// Iterates over the frames in order.
    pub fn iter(&self) -> Iter<'_> {
        Iter {
            first: Some(&self.first),
            rest: self.rest.as_slice().iter(),
        }
    }

    /// The metadata of the connection this message arrived on, if it came
    /// over one that has any: never for `inproc`, which has no connection,
    /// as in libzmq, nor if the receiving socket has turned it off.
    #[inline]
    pub fn metadata(&self) -> Option<&MessageMetadata> {
        self.metadata.as_ref()
    }

    /// The value of the property `name` of the connection this message
    /// arrived on, as libzmq's `zmq_msg_gets`: matched exactly, case and
    /// all. See [`MessageMetadata::get`].
    #[inline]
    pub fn property(&self, name: &str) -> Option<&[u8]> {
        self.metadata.as_ref()?.get(name)
    }

    /// Attaches the metadata of the connection the message arrived on, or
    /// takes it off, as a socket does before sending, so a message passed
    /// on does not carry its first connection's metadata.
    #[inline]
    pub fn set_metadata(&mut self, metadata: Option<MessageMetadata>) {
        self.metadata = metadata;
    }

    /// Takes the message apart into its first frame and the remaining frames.
    /// A two-frame message allocates the list here.
    pub fn into_parts(self) -> (Frame, Vec<Frame>) {
        (self.first, self.rest.into_vec())
    }
}

impl PartialEq for Message {
    fn eq(&self, other: &Self) -> bool {
        self.first == other.first && self.rest.as_slice() == other.rest.as_slice()
    }
}

impl Eq for Message {}

impl Hash for Message {
    fn hash<H: Hasher>(&self, state: &mut H) {
        self.first.hash(state);
        self.rest.as_slice().hash(state);
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
    /// A second frame held inline.
    second: Option<Frame>,
    rest: std::vec::IntoIter<Frame>,
}

impl Iterator for IntoIter {
    type Item = Frame;

    fn next(&mut self) -> Option<Frame> {
        self.first
            .take()
            .or_else(|| self.second.take())
            .or_else(|| self.rest.next())
    }

    fn size_hint(&self) -> (usize, Option<usize>) {
        let len = usize::from(self.first.is_some())
            + usize::from(self.second.is_some())
            + self.rest.len();
        (len, Some(len))
    }
}

impl ExactSizeIterator for IntoIter {}

impl FusedIterator for IntoIter {}

impl IntoIterator for Message {
    type Item = Frame;
    type IntoIter = IntoIter;

    fn into_iter(self) -> IntoIter {
        let (second, rest) = match self.rest {
            Rest::Many(frames) => (None, frames),
            Rest::One(frame) => (Some(frame), Vec::new()),
        };
        IntoIter {
            first: Some(self.first),
            second,
            rest: rest.into_iter(),
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
        let mut frames = frames.into_iter();
        let first = frames.next().ok_or(EmptyMessage)?;
        let rest = if frames.len() == 1 {
            Rest::One(frames.next().expect("one left"))
        } else {
            Rest::Many(frames.collect())
        };
        Ok(Self {
            first,
            rest,
            metadata: None,
        })
    }
}

impl Extend<Frame> for Message {
    fn extend<I: IntoIterator<Item = Frame>>(&mut self, frames: I) {
        for frame in frames {
            self.rest.push(frame);
        }
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
    fn message_is_80_bytes() {
        // A second frame held inline costs 16 octets over a bare `Vec`, and
        // the connection's metadata 8 more.
        assert_eq!(size_of::<Message>(), 80);
    }

    #[test]
    fn single_part_message_does_not_allocate_a_frame_list() {
        let msg = Message::new("hello");
        assert!(matches!(&msg.rest, Rest::Many(frames) if frames.capacity() == 0));
        assert_eq!(msg.len().get(), 1);
        assert_eq!(msg.split_first(), (&Frame::from("hello"), &[][..]));
    }

    #[test]
    fn a_second_frame_is_held_inline_and_a_third_moves_to_a_list() {
        let mut msg = Message::new("a");
        msg.push("b");
        assert!(matches!(msg.rest, Rest::One(_)));
        assert_eq!(msg.split_first().1, &frames(&["b"])[..]);
        msg.push("c");
        assert!(matches!(&msg.rest, Rest::Many(frames) if frames.len() == 2));
        assert_eq!(
            msg.iter().cloned().collect::<Vec<_>>(),
            frames(&["a", "b", "c"])
        );
        let two = Message::try_from(frames(&["a", "b"])).unwrap();
        assert!(matches!(two.rest, Rest::One(_)));
    }

    #[test]
    fn equality_and_hashing_ignore_how_frames_are_held() {
        use std::hash::BuildHasher;
        let inline = Message::try_from(frames(&["a", "b"])).unwrap();
        let listed = Message {
            first: Frame::from("a"),
            rest: Rest::Many(frames(&["b"])),
            metadata: None,
        };
        assert_eq!(inline, listed);
        let hasher = std::hash::RandomState::new();
        assert_eq!(hasher.hash_one(&inline), hasher.hash_one(&listed));
    }

    #[test]
    fn a_two_frame_message_iterates_owned() {
        let msg = Message::try_from(frames(&["a", "b"])).unwrap();
        let mut iter = msg.into_iter();
        assert_eq!(iter.len(), 2);
        assert_eq!(iter.next(), Some(Frame::from("a")));
        assert_eq!(iter.len(), 1);
        assert_eq!(iter.next(), Some(Frame::from("b")));
        assert_eq!(iter.next(), None);
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

    #[test]
    fn metadata_keeps_the_first_of_each_name_however_many_a_peer_sends() {
        let many = 100_000;
        let properties = (0..many)
            .map(|i| {
                (
                    Frame::from(format!("p{i}").into_bytes()),
                    Frame::from("first"),
                )
            })
            .chain([(Frame::from("p0"), Frame::from("second"))]);
        let metadata = MessageMetadata::new(properties);
        assert_eq!(metadata.iter().len(), many);
        assert_eq!(metadata.get("p0"), Some(&b"first"[..]));
    }
}
