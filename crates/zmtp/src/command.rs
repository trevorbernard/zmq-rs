//! Typed ZMTP 3.x commands (RFC 23, 37).
//!
//! A command body is `name-length name data`. Commands that belong to a
//! security mechanism (HELLO, WELCOME, INITIATE, ...) are kept as
//! [`Command::Other`] for the mechanism to interpret.

use std::fmt;
use std::hash::{Hash, Hasher};
use std::time::Duration;

use crate::Frame;

/// A ZMTP command.
#[derive(Debug, Clone, PartialEq, Eq)]
pub enum Command {
    /// Ends the handshake and carries the peer's metadata.
    Ready(Metadata),
    /// A fatal handshake error. Do not reconnect with the same credentials.
    Error(ErrorReason),
    /// Heartbeat request (ZMTP 3.1).
    Ping(Ping),
    /// Heartbeat reply echoing the PING context (ZMTP 3.1).
    Pong(PingContext),
    /// Subscribe to messages starting with this prefix (ZMTP 3.1).
    Subscribe(Frame),
    /// Cancel a subscription (ZMTP 3.1).
    Cancel(Frame),
    /// Join a RADIO/DISH group (ZMTP 3.1).
    Join(Group),
    /// Leave a RADIO/DISH group (ZMTP 3.1).
    Leave(Group),
    /// Any other command, such as a security mechanism's.
    Other { name: CommandName, data: Frame },
}

/// A heartbeat request.
#[derive(Debug, Clone, PartialEq, Eq)]
pub struct Ping {
    /// How long the peer may wait for traffic before closing the connection.
    pub ttl: Ttl,
    /// Echoed back in the PONG.
    pub context: PingContext,
}

/// A heartbeat time-to-live, in tenths of a second. Zero means none.
#[derive(Debug, Clone, Copy, PartialEq, Eq, Hash, PartialOrd, Ord, Default)]
pub struct Ttl(u16);

/// Up to 16 octets echoed from PING to PONG.
#[derive(Debug, Clone, PartialEq, Eq, Hash, Default)]
pub struct PingContext(Frame);

/// The reason carried by an ERROR command: up to 255 octets.
#[derive(Debug, Clone, PartialEq, Eq, Hash, Default)]
pub struct ErrorReason(Frame);

/// A RADIO/DISH group: up to 255 octets, none of them zero.
#[derive(Debug, Clone, PartialEq, Eq, Hash, Default)]
pub struct Group(Frame);

/// A command name: 1 to 255 octets.
#[derive(Debug, Clone, PartialEq, Eq, Hash)]
pub struct CommandName(Frame);

/// Connection metadata from a READY or INITIATE command, in wire order.
#[derive(Debug, Clone, PartialEq, Eq, Default)]
pub struct Metadata {
    properties: Vec<(PropertyName, Frame)>,
}

/// A metadata property name: 1 to 255 octets, compared case-insensitively.
///
/// Names we create must use RFC 37's characters (`A-Za-z0-9-_.+`). Names
/// received from peers only need the right length, because libzmq accepts
/// any `X-` name.
#[derive(Debug, Clone)]
pub struct PropertyName(Frame);

/// A value longer than its field allows.
#[derive(Debug, Clone, Copy, PartialEq, Eq)]
pub struct TooLong {
    /// The most octets the field holds.
    pub max: usize,
}

/// Why a [`Group`] was rejected.
#[derive(Debug, Clone, Copy, PartialEq, Eq)]
pub enum InvalidGroup {
    TooLong,
    ContainsNul,
}

/// Why a [`PropertyName`] or [`CommandName`] was rejected.
#[derive(Debug, Clone, Copy, PartialEq, Eq)]
pub enum InvalidName {
    Empty,
    TooLong,
    Character(u8),
}

/// Why a command body could not be parsed. The connection must be closed.
#[derive(Debug, Clone, PartialEq, Eq)]
pub enum CommandError {
    /// The body has no command name.
    MissingName,
    /// The named command's data is shorter than its fields require.
    Truncated { command: &'static str },
    /// A JOIN or LEAVE group is invalid.
    InvalidGroup(InvalidGroup),
    /// A metadata property has an empty name.
    EmptyPropertyName,
}

const MAX_SHORT: usize = u8::MAX as usize;
const MAX_PING_CONTEXT: usize = 16;
const MAX_PROPERTY_VALUE: usize = i32::MAX as usize;
const DECISECOND_NANOS: u128 = 100_000_000;

/// The wire length of a metadata value, which RFC 37 limits to 2^31 - 1.
fn property_value_len(len: usize) -> Result<u32, TooLong> {
    if len > MAX_PROPERTY_VALUE {
        return Err(TooLong {
            max: MAX_PROPERTY_VALUE,
        });
    }
    Ok(len as u32)
}

impl Command {
    /// Parses a command frame body.
    pub fn parse(body: &[u8]) -> Result<Self, CommandError> {
        let (&name_len, rest) = body.split_first().ok_or(CommandError::MissingName)?;
        let (name, data) = match rest.split_at_checked(usize::from(name_len)) {
            Some((name, data)) if !name.is_empty() => (name, data),
            _ => return Err(CommandError::MissingName),
        };
        match name {
            b"READY" => parse_metadata(data, "READY").map(Self::Ready),
            b"ERROR" => {
                let truncated = CommandError::Truncated { command: "ERROR" };
                let (&len, rest) = data.split_first().ok_or(truncated.clone())?;
                let reason = rest.get(..usize::from(len)).ok_or(truncated)?;
                Ok(Self::Error(ErrorReason(Frame::copy_from_slice(reason))))
            }
            b"PING" => {
                let (ttl, context) = data
                    .split_first_chunk()
                    .ok_or(CommandError::Truncated { command: "PING" })?;
                Ok(Self::Ping(Ping {
                    ttl: Ttl(u16::from_be_bytes(*ttl)),
                    context: PingContext::truncating(context),
                }))
            }
            b"PONG" => Ok(Self::Pong(PingContext::truncating(data))),
            b"SUBSCRIBE" => Ok(Self::Subscribe(Frame::copy_from_slice(data))),
            b"CANCEL" => Ok(Self::Cancel(Frame::copy_from_slice(data))),
            b"JOIN" => Group::new(Frame::copy_from_slice(data))
                .map(Self::Join)
                .map_err(CommandError::InvalidGroup),
            b"LEAVE" => Group::new(Frame::copy_from_slice(data))
                .map(Self::Leave)
                .map_err(CommandError::InvalidGroup),
            _ => Ok(Self::Other {
                name: CommandName(Frame::copy_from_slice(name)),
                data: Frame::copy_from_slice(data),
            }),
        }
    }

    /// The command name.
    pub fn name(&self) -> &[u8] {
        match self {
            Self::Ready(_) => b"READY",
            Self::Error(_) => b"ERROR",
            Self::Ping(_) => b"PING",
            Self::Pong(_) => b"PONG",
            Self::Subscribe(_) => b"SUBSCRIBE",
            Self::Cancel(_) => b"CANCEL",
            Self::Join(_) => b"JOIN",
            Self::Leave(_) => b"LEAVE",
            Self::Other { name, .. } => name.as_slice(),
        }
    }

    /// Appends the command body (name length, name, data) to `out`, ready
    /// for [`Encoder::encode_command`](crate::Encoder::encode_command).
    pub fn encode(&self, out: &mut Vec<u8>) {
        put_name(out, self.name());
        match self {
            Self::Ready(metadata) => encode_metadata(metadata, out),
            Self::Error(reason) => put_name(out, reason.as_slice()),
            Self::Ping(ping) => {
                out.extend_from_slice(&ping.ttl.deciseconds().to_be_bytes());
                out.extend_from_slice(ping.context.as_slice());
            }
            Self::Pong(context) => out.extend_from_slice(context.as_slice()),
            Self::Subscribe(topic) | Self::Cancel(topic) => out.extend_from_slice(topic),
            Self::Join(group) | Self::Leave(group) => out.extend_from_slice(group.as_slice()),
            Self::Other { data, .. } => out.extend_from_slice(data),
        }
    }
}

/// Appends `metadata` as `name-length name value-length value` properties,
/// the data of READY and INITIATE.
pub(crate) fn encode_metadata(metadata: &Metadata, out: &mut Vec<u8>) {
    for (name, value) in metadata.iter() {
        put_name(out, name.as_slice());
        out.extend_from_slice(&(value.len() as u32).to_be_bytes());
        out.extend_from_slice(value);
    }
}

/// Writes a short string: one length octet, then the octets. Every caller
/// passes a value whose type limits it to 255 octets.
fn put_name(out: &mut Vec<u8>, name: &[u8]) {
    out.push(name.len() as u8);
    out.extend_from_slice(name);
}

/// Parses `name-length name value-length value` properties until `data`
/// ends. Like libzmq, any leftover or truncated property is an error and the
/// first of duplicate names wins on lookup.
pub(crate) fn parse_metadata(
    mut data: &[u8],
    command: &'static str,
) -> Result<Metadata, CommandError> {
    let truncated = || CommandError::Truncated { command };
    let mut properties = Vec::new();
    while let Some((&name_len, rest)) = data.split_first() {
        if name_len == 0 {
            return Err(CommandError::EmptyPropertyName);
        }
        let (name, rest) = rest
            .split_at_checked(usize::from(name_len))
            .ok_or_else(truncated)?;
        let (value_len, rest) = rest.split_first_chunk().ok_or_else(truncated)?;
        let value_len = usize::try_from(u32::from_be_bytes(*value_len)).map_err(|_| truncated())?;
        let (value, rest) = rest.split_at_checked(value_len).ok_or_else(truncated)?;
        properties.push((
            PropertyName(Frame::copy_from_slice(name)),
            Frame::copy_from_slice(value),
        ));
        data = rest;
    }
    Ok(Metadata { properties })
}

fn validate_name(name: &[u8], valid: impl Fn(u8) -> bool) -> Result<(), InvalidName> {
    if name.is_empty() {
        Err(InvalidName::Empty)
    } else if name.len() > MAX_SHORT {
        Err(InvalidName::TooLong)
    } else if let Some(&c) = name.iter().find(|&&c| !valid(c)) {
        Err(InvalidName::Character(c))
    } else {
        Ok(())
    }
}

fn check_len(value: &[u8], max: usize) -> Result<(), TooLong> {
    if value.len() > max {
        Err(TooLong { max })
    } else {
        Ok(())
    }
}

impl Ttl {
    /// No time-to-live.
    pub const ZERO: Self = Self(0);

    /// A time-to-live of `deciseconds` tenths of a second.
    pub const fn from_deciseconds(deciseconds: u16) -> Self {
        Self(deciseconds)
    }

    /// Rounds `duration` up to whole deciseconds, saturating at the maximum.
    pub fn from_duration(duration: Duration) -> Self {
        let deciseconds = duration.as_nanos().div_ceil(DECISECOND_NANOS);
        Self(u16::try_from(deciseconds).unwrap_or(u16::MAX))
    }

    /// Tenths of a second.
    pub const fn deciseconds(self) -> u16 {
        self.0
    }

    /// The time-to-live as a `Duration`.
    pub fn as_duration(self) -> Duration {
        Duration::from_millis(u64::from(self.0) * 100)
    }
}

impl PingContext {
    /// Validates `context`: at most 16 octets.
    pub fn new(context: impl Into<Frame>) -> Result<Self, TooLong> {
        let context = context.into();
        check_len(&context, MAX_PING_CONTEXT)?;
        Ok(Self(context))
    }

    /// The context octets.
    pub fn as_slice(&self) -> &[u8] {
        &self.0
    }

    /// Keeps the first 16 octets, as libzmq does with a longer context.
    fn truncating(context: &[u8]) -> Self {
        Self(Frame::copy_from_slice(
            &context[..context.len().min(MAX_PING_CONTEXT)],
        ))
    }
}

impl ErrorReason {
    /// Validates `reason`: at most 255 octets.
    pub fn new(reason: impl Into<Frame>) -> Result<Self, TooLong> {
        let reason = reason.into();
        check_len(&reason, MAX_SHORT)?;
        Ok(Self(reason))
    }

    /// The reason octets.
    pub fn as_slice(&self) -> &[u8] {
        &self.0
    }

    /// A reason known to fit.
    pub(crate) fn from_static(reason: &'static str) -> Self {
        debug_assert!(reason.len() <= MAX_SHORT);
        Self(Frame::from(reason))
    }
}

impl Group {
    /// Validates `group`: at most 255 octets, none of them zero.
    pub fn new(group: impl Into<Frame>) -> Result<Self, InvalidGroup> {
        let group = group.into();
        if group.len() > MAX_SHORT {
            Err(InvalidGroup::TooLong)
        } else if group.contains(&0) {
            Err(InvalidGroup::ContainsNul)
        } else {
            Ok(Self(group))
        }
    }

    /// The group octets.
    pub fn as_slice(&self) -> &[u8] {
        &self.0
    }
}

impl CommandName {
    /// Validates `name`: 1 to 255 ASCII letters.
    pub fn new(name: impl Into<Frame>) -> Result<Self, InvalidName> {
        let name = name.into();
        validate_name(&name, |c| c.is_ascii_alphabetic())?;
        Ok(Self(name))
    }

    /// The name octets.
    pub fn as_slice(&self) -> &[u8] {
        &self.0
    }
}

impl PropertyName {
    /// Validates `name`: 1 to 255 of `A-Za-z0-9-_.+`.
    pub fn new(name: impl Into<Frame>) -> Result<Self, InvalidName> {
        let name = name.into();
        validate_name(&name, |c| c.is_ascii_alphanumeric() || b"-_.+".contains(&c))?;
        Ok(Self(name))
    }

    /// The name octets, as sent.
    pub fn as_slice(&self) -> &[u8] {
        &self.0
    }

    /// A name known to be valid.
    pub(crate) fn from_static(name: &'static str) -> Self {
        debug_assert!(Self::new(name).is_ok());
        Self(Frame::from(name))
    }
}

impl PartialEq for PropertyName {
    fn eq(&self, other: &Self) -> bool {
        self.0.eq_ignore_ascii_case(&other.0)
    }
}

impl Eq for PropertyName {}

impl Hash for PropertyName {
    fn hash<H: Hasher>(&self, state: &mut H) {
        state.write_usize(self.0.len());
        for octet in self.0.iter() {
            state.write_u8(octet.to_ascii_lowercase());
        }
    }
}

impl Metadata {
    /// Empty metadata.
    pub const fn new() -> Self {
        Self {
            properties: Vec::new(),
        }
    }

    /// Sets `name` to `value`, replacing any property with the same name.
    pub fn insert(&mut self, name: PropertyName, value: impl Into<Frame>) -> Result<(), TooLong> {
        let value = value.into();
        property_value_len(value.len())?;
        match self.properties.iter_mut().find(|(n, _)| *n == name) {
            Some((_, slot)) => *slot = value,
            None => self.properties.push((name, value)),
        }
        Ok(())
    }

    /// The value of the first property named `name`, ignoring case.
    pub fn get(&self, name: impl AsRef<[u8]>) -> Option<&Frame> {
        let name = name.as_ref();
        self.properties
            .iter()
            .find(|(n, _)| n.as_slice().eq_ignore_ascii_case(name))
            .map(|(_, value)| value)
    }

    /// Appends a property whose value is known to fit.
    pub(crate) fn push(&mut self, name: PropertyName, value: Frame) {
        debug_assert!(value.len() <= MAX_PROPERTY_VALUE);
        self.properties.push((name, value));
    }

    /// The properties in order.
    pub fn iter(&self) -> impl ExactSizeIterator<Item = (&PropertyName, &Frame)> {
        self.properties.iter().map(|(name, value)| (name, value))
    }
}

impl fmt::Display for TooLong {
    fn fmt(&self, f: &mut fmt::Formatter<'_>) -> fmt::Result {
        write!(f, "value is longer than {} octets", self.max)
    }
}

impl std::error::Error for TooLong {}

impl fmt::Display for InvalidGroup {
    fn fmt(&self, f: &mut fmt::Formatter<'_>) -> fmt::Result {
        f.write_str(match self {
            Self::TooLong => "group is longer than 255 octets",
            Self::ContainsNul => "group contains a zero octet",
        })
    }
}

impl std::error::Error for InvalidGroup {}

impl fmt::Display for InvalidName {
    fn fmt(&self, f: &mut fmt::Formatter<'_>) -> fmt::Result {
        match self {
            Self::Empty => f.write_str("name is empty"),
            Self::TooLong => f.write_str("name is longer than 255 octets"),
            Self::Character(c) => {
                write!(
                    f,
                    "name contains invalid character b'{}'",
                    [*c].escape_ascii()
                )
            }
        }
    }
}

impl std::error::Error for InvalidName {}

impl fmt::Display for CommandError {
    fn fmt(&self, f: &mut fmt::Formatter<'_>) -> fmt::Result {
        match self {
            Self::MissingName => f.write_str("command has no name"),
            Self::Truncated { command } => write!(f, "malformed {command} command"),
            Self::InvalidGroup(err) => write!(f, "invalid group: {err}"),
            Self::EmptyPropertyName => f.write_str("metadata property has an empty name"),
        }
    }
}

impl std::error::Error for CommandError {}

#[cfg(test)]
mod tests {
    use super::*;
    use std::hash::BuildHasher;

    fn encode(command: &Command) -> Vec<u8> {
        let mut out = Vec::new();
        command.encode(&mut out);
        out
    }

    fn name(s: &str) -> PropertyName {
        PropertyName::new(s.to_owned()).unwrap()
    }

    fn ready() -> Command {
        let mut metadata = Metadata::new();
        metadata.insert(name("Socket-Type"), "DEALER").unwrap();
        metadata.insert(name("Identity"), Frame::new()).unwrap();
        Command::Ready(metadata)
    }

    fn all_commands() -> Vec<Command> {
        vec![
            ready(),
            Command::Ready(Metadata::new()),
            Command::Error(ErrorReason::new("400").unwrap()),
            Command::Ping(Ping {
                ttl: Ttl::from_deciseconds(300),
                context: PingContext::new(&b"ctx"[..]).unwrap(),
            }),
            Command::Pong(PingContext::new(&b"ctx"[..]).unwrap()),
            Command::Subscribe(Frame::from("topic")),
            Command::Cancel(Frame::new()),
            Command::Join(Group::new("weather").unwrap()),
            Command::Leave(Group::default()),
            Command::Other {
                name: CommandName::new("HELLO").unwrap(),
                data: Frame::from(vec![0; 300]),
            },
        ]
    }

    // ---- encoding ----

    #[test]
    fn ready_encodes_properties_in_order() {
        let mut expected = b"\x05READY".to_vec();
        expected.extend_from_slice(b"\x0bSocket-Type\x00\x00\x00\x06DEALER");
        expected.extend_from_slice(b"\x08Identity\x00\x00\x00\x00");
        assert_eq!(encode(&ready()), expected);
    }

    #[test]
    fn simple_commands_encode() {
        let cases: Vec<(Command, &[u8])> = vec![
            (
                Command::Error(ErrorReason::new("400").unwrap()),
                b"\x05ERROR\x03400",
            ),
            (
                Command::Ping(Ping {
                    ttl: Ttl::from_deciseconds(0x0102),
                    context: PingContext::new(&b"ab"[..]).unwrap(),
                }),
                b"\x04PING\x01\x02ab",
            ),
            (Command::Pong(PingContext::default()), b"\x04PONG"),
            (Command::Subscribe(Frame::from("a")), b"\x09SUBSCRIBEa"),
            (Command::Cancel(Frame::from("a")), b"\x06CANCELa"),
            (Command::Join(Group::new("g").unwrap()), b"\x04JOINg"),
            (Command::Leave(Group::new("g").unwrap()), b"\x05LEAVEg"),
            (
                Command::Other {
                    name: CommandName::new("HELLO").unwrap(),
                    data: Frame::from("x"),
                },
                b"\x05HELLOx",
            ),
        ];
        for (command, bytes) in cases {
            assert_eq!(encode(&command), bytes, "{command:?}");
        }
    }

    // ---- parsing ----

    #[test]
    fn every_command_round_trips() {
        for command in all_commands() {
            assert_eq!(
                Command::parse(&encode(&command)),
                Ok(command.clone()),
                "{command:?}"
            );
        }
    }

    #[test]
    fn names() {
        let commands = all_commands();
        let names: Vec<&[u8]> = commands.iter().map(|c| c.name()).collect();
        let expected: [&[u8]; 10] = [
            b"READY",
            b"READY",
            b"ERROR",
            b"PING",
            b"PONG",
            b"SUBSCRIBE",
            b"CANCEL",
            b"JOIN",
            b"LEAVE",
            b"HELLO",
        ];
        assert_eq!(names, expected);
    }

    #[test]
    fn missing_or_truncated_name_is_rejected() {
        assert_eq!(Command::parse(b""), Err(CommandError::MissingName));
        assert_eq!(Command::parse(b"\x00"), Err(CommandError::MissingName));
        assert_eq!(Command::parse(b"\x05REA"), Err(CommandError::MissingName));
    }

    #[test]
    fn command_names_are_case_sensitive() {
        let parsed = Command::parse(b"\x05ready").unwrap();
        assert!(matches!(parsed, Command::Other { .. }), "{parsed:?}");
    }

    #[test]
    fn truncated_metadata_is_rejected() {
        let full = encode(&ready());
        // "\x05READY" then "\x0bSocket-Type" + u32 length + "DEALER".
        let first_property_end = 6 + 1 + 11 + 4 + 6;
        let Ok(Command::Ready(prefix)) = Command::parse(&full[..first_property_end]) else {
            panic!("a cut between properties is valid metadata");
        };
        assert_eq!(prefix.iter().len(), 1);
        for cut in (7..full.len()).filter(|&cut| cut != first_property_end) {
            assert_eq!(
                Command::parse(&full[..cut]),
                Err(CommandError::Truncated { command: "READY" }),
                "cut at {cut}"
            );
        }
        let mut trailing = full.clone();
        trailing.push(0x01);
        assert_eq!(
            Command::parse(&trailing),
            Err(CommandError::Truncated { command: "READY" })
        );
    }

    #[test]
    fn empty_property_name_is_rejected() {
        assert_eq!(
            Command::parse(b"\x05READY\x00\x00\x00\x00\x00"),
            Err(CommandError::EmptyPropertyName)
        );
    }

    #[test]
    fn peer_property_names_need_only_the_right_length() {
        let parsed = Command::parse(b"\x05READY\x0bX-hello key\x00\x00\x00\x01v").unwrap();
        let Command::Ready(metadata) = parsed else {
            panic!("expected READY");
        };
        assert_eq!(metadata.get("x-HELLO KEY"), Some(&Frame::from("v")));
    }

    #[test]
    fn first_duplicate_property_wins_like_libzmq() {
        let parsed =
            Command::parse(b"\x05READY\x01a\x00\x00\x00\x011\x01A\x00\x00\x00\x012").unwrap();
        let Command::Ready(metadata) = parsed else {
            panic!("expected READY");
        };
        assert_eq!(metadata.get("a"), Some(&Frame::from("1")));
        assert_eq!(metadata.iter().len(), 2, "both are kept in wire order");
    }

    #[test]
    fn ping_needs_a_ttl() {
        assert_eq!(
            Command::parse(b"\x04PING\x01"),
            Err(CommandError::Truncated { command: "PING" })
        );
    }

    #[test]
    fn long_ping_and_pong_contexts_are_truncated_like_libzmq() {
        let mut ping = b"\x04PING\x00\x0a".to_vec();
        ping.extend_from_slice(&[7; 20]);
        let Command::Ping(parsed) = Command::parse(&ping).unwrap() else {
            panic!("expected PING");
        };
        assert_eq!(parsed.context.as_slice(), &[7; 16]);
        assert_eq!(parsed.ttl, Ttl::from_deciseconds(10));

        let mut pong = b"\x04PONG".to_vec();
        pong.extend_from_slice(&[8; 20]);
        assert_eq!(
            Command::parse(&pong),
            Ok(Command::Pong(PingContext::new(vec![8; 16]).unwrap()))
        );
    }

    #[test]
    fn error_reason_must_fit_the_body() {
        assert_eq!(
            Command::parse(b"\x05ERROR"),
            Err(CommandError::Truncated { command: "ERROR" })
        );
        assert_eq!(
            Command::parse(b"\x05ERROR\x04400"),
            Err(CommandError::Truncated { command: "ERROR" })
        );
    }

    #[test]
    fn invalid_groups_are_rejected_not_fatal() {
        let mut join = b"\x04JOIN".to_vec();
        join.extend_from_slice(&[b'g'; 256]);
        assert_eq!(
            Command::parse(&join),
            Err(CommandError::InvalidGroup(InvalidGroup::TooLong))
        );
        assert_eq!(
            Command::parse(b"\x05LEAVEa\x00b"),
            Err(CommandError::InvalidGroup(InvalidGroup::ContainsNul))
        );
    }

    // ---- values ----

    #[test]
    fn ttl_converts_without_overflow() {
        assert_eq!(
            Ttl::from_deciseconds(u16::MAX).as_duration(),
            Duration::from_millis(6_553_500)
        );
        assert_eq!(Ttl::from_duration(Duration::ZERO), Ttl::ZERO);
        assert_eq!(
            Ttl::from_duration(Duration::from_millis(50)).deciseconds(),
            1
        );
        assert_eq!(
            Ttl::from_duration(Duration::from_millis(100)).deciseconds(),
            1
        );
        assert_eq!(
            Ttl::from_duration(Duration::from_millis(101)).deciseconds(),
            2
        );
        assert_eq!(
            Ttl::from_duration(Duration::from_secs(1 << 40)).deciseconds(),
            u16::MAX
        );
    }

    #[test]
    fn length_limits() {
        assert!(PingContext::new(vec![0; 16]).is_ok());
        assert_eq!(PingContext::new(vec![0; 17]), Err(TooLong { max: 16 }));
        assert!(ErrorReason::new(vec![b'x'; 255]).is_ok());
        assert_eq!(ErrorReason::new(vec![b'x'; 256]), Err(TooLong { max: 255 }));
        assert!(Group::new(vec![b'g'; 255]).is_ok());
        assert_eq!(Group::new(vec![b'g'; 256]), Err(InvalidGroup::TooLong));
        assert_eq!(Group::new(&b"a\x00"[..]), Err(InvalidGroup::ContainsNul));
    }

    #[test]
    fn property_value_limit() {
        assert_eq!(property_value_len(MAX_PROPERTY_VALUE), Ok(i32::MAX as u32));
        assert_eq!(
            property_value_len(MAX_PROPERTY_VALUE + 1),
            Err(TooLong {
                max: MAX_PROPERTY_VALUE
            })
        );
    }

    #[test]
    fn property_names_follow_rfc_37() {
        assert!(PropertyName::new("Az09-_.+").is_ok());
        assert_eq!(PropertyName::new(""), Err(InvalidName::Empty));
        assert_eq!(
            PropertyName::new("a".repeat(256)),
            Err(InvalidName::TooLong)
        );
        assert_eq!(PropertyName::new("a b"), Err(InvalidName::Character(b' ')));
    }

    #[test]
    fn command_names_are_letters() {
        assert!(CommandName::new("INITIATE").is_ok());
        assert_eq!(CommandName::new(""), Err(InvalidName::Empty));
        assert_eq!(CommandName::new("A".repeat(256)), Err(InvalidName::TooLong));
        assert_eq!(CommandName::new("A1"), Err(InvalidName::Character(b'1')));
    }

    #[test]
    fn property_names_compare_and_hash_ignoring_case() {
        let (a, b) = (name("Socket-Type"), name("socket-type"));
        assert_eq!(a, b);
        let hasher = std::hash::RandomState::new();
        assert_eq!(hasher.hash_one(&a), hasher.hash_one(&b));
        assert_eq!(a.as_slice(), b"Socket-Type", "original case is kept");
    }

    #[test]
    fn insert_replaces_ignoring_case() {
        let mut metadata = Metadata::new();
        metadata.insert(name("X-Key"), "1").unwrap();
        metadata.insert(name("x-key"), "2").unwrap();
        assert_eq!(metadata.iter().len(), 1);
        assert_eq!(metadata.get("X-KEY"), Some(&Frame::from("2")));
        assert_eq!(metadata.get("missing"), None);
    }

    #[test]
    fn error_messages() {
        assert_eq!(
            TooLong { max: 16 }.to_string(),
            "value is longer than 16 octets"
        );
        assert_eq!(
            InvalidGroup::ContainsNul.to_string(),
            "group contains a zero octet"
        );
        assert_eq!(
            InvalidName::Character(b' ').to_string(),
            "name contains invalid character b' '"
        );
        assert_eq!(
            CommandError::Truncated { command: "PING" }.to_string(),
            "malformed PING command"
        );
    }
}
