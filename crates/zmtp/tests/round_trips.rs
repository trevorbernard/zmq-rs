//! Round trips through the protocol's text and binary forms: whatever we
//! write parses back to itself, and whatever we accept writes back to what
//! was sent.

use proptest::prelude::*;
use zmtp::{
    Command, CommandName, ErrorReason, Frame, Group, Metadata, Ping, PingContext, PropertyName,
    PublicKey, SecretKey, SocketType, Subscription, Ttl, z85,
};

/// The names `Command::parse` gives a type of their own.
const TYPED: [&[u8]; 8] = [
    b"READY",
    b"ERROR",
    b"PING",
    b"PONG",
    b"SUBSCRIBE",
    b"CANCEL",
    b"JOIN",
    b"LEAVE",
];

fn bytes(max: usize) -> impl Strategy<Value = Vec<u8>> {
    prop::collection::vec(any::<u8>(), 0..=max)
}

fn property_name() -> impl Strategy<Value = PropertyName> {
    let alphabet = b"ABCXYZabcxyz0189-_.+".to_vec();
    prop::collection::vec(prop::sample::select(alphabet), 1..=20)
        .prop_map(|name| PropertyName::new(name).expect("valid name"))
}

fn metadata() -> impl Strategy<Value = Metadata> {
    prop::collection::vec((property_name(), bytes(300)), 0..5).prop_map(|properties| {
        let mut metadata = Metadata::new();
        for (name, value) in properties {
            metadata.insert(name, value).expect("short value");
        }
        metadata
    })
}

fn command() -> impl Strategy<Value = Command> {
    let other_name = prop::collection::vec(
        prop::sample::select(b"ABCDEFGHIJKLMNOPQRSTUVWXYZabcxyz".to_vec()),
        1..=255,
    )
    .prop_filter("not a typed command", |name| {
        !TYPED.contains(&name.as_slice())
    })
    .prop_map(|name| CommandName::new(name).expect("letters"));
    prop_oneof![
        metadata().prop_map(Command::Ready),
        bytes(255).prop_map(|reason| Command::Error(ErrorReason::new(reason).expect("fits"))),
        (any::<u16>(), bytes(16)).prop_map(|(ttl, context)| Command::Ping(Ping {
            ttl: Ttl::from_deciseconds(ttl),
            context: PingContext::new(context).expect("fits"),
        })),
        bytes(16).prop_map(|context| Command::Pong(PingContext::new(context).expect("fits"))),
        bytes(300).prop_map(|topic| Command::Subscribe(Frame::from(topic))),
        bytes(300).prop_map(|topic| Command::Cancel(Frame::from(topic))),
        prop::collection::vec(1u8..=255, 0..=255)
            .prop_map(|group| Command::Join(Group::new(group).expect("fits"))),
        prop::collection::vec(1u8..=255, 0..=255)
            .prop_map(|group| Command::Leave(Group::new(group).expect("fits"))),
        (other_name, bytes(300)).prop_map(|(name, data)| Command::Other {
            name,
            data: Frame::from(data),
        }),
    ]
}

fn encoded(command: &Command) -> Vec<u8> {
    let mut body = Vec::new();
    command.encode(&mut body);
    body
}

/// A command body as a peer might send it: an encoded command, mangled.
fn body() -> impl Strategy<Value = Vec<u8>> {
    prop_oneof![
        bytes(40),
        (
            command(),
            prop::collection::vec((any::<prop::sample::Index>(), any::<u8>()), 0..4),
            any::<prop::sample::Index>(),
            bytes(20),
        )
            .prop_map(|(command, changes, cut, extra)| {
                let mut body = encoded(&command);
                for (at, octet) in changes {
                    let at = at.index(body.len());
                    body[at] = octet;
                }
                body.truncate(cut.index(body.len() + 1));
                body.extend_from_slice(&extra);
                body
            }),
    ]
}

fn z85_text() -> impl Strategy<Value = String> {
    let alphabet: Vec<char> =
        "0123456789abcdefghijklmnopqrstuvwxyzABCDEFGHIJKLMNOPQRSTUVWXYZ.-:+=^!/*?&<>()[]{}@%$#"
            .chars()
            .collect();
    prop_oneof![
        3 => prop::collection::vec(prop::sample::select(alphabet), 0..40),
        1 => prop::collection::vec(any::<char>(), 0..12),
    ]
    .prop_map(|chars| chars.into_iter().collect())
}

proptest! {
    #![proptest_config(ProptestConfig::with_cases(1024))]

    /// Every command we can build parses back to itself.
    #[test]
    fn commands_round_trip(command in command()) {
        prop_assert_eq!(Command::parse(&encoded(&command)), Ok(command));
    }

    /// A command we accept writes back to exactly what was sent, except
    /// that, as in libzmq, ERROR ignores octets after its reason and PING
    /// and PONG keep only 16 octets of context: then what we write is a
    /// prefix of what was sent.
    #[test]
    fn accepted_commands_write_back(body in body()) {
        if let Ok(command) = Command::parse(&body) {
            let written = encoded(&command);
            let lenient = matches!(command, Command::Error(_) | Command::Ping(_) | Command::Pong(_));
            if lenient {
                prop_assert!(body.starts_with(&written), "{:?} wrote {:?}", body, written);
            } else {
                prop_assert_eq!(&written, &body);
            }
            prop_assert_eq!(Command::parse(&written), Ok(command));
        }
    }

    /// Z85 decodes what it encodes, encodes back what it decodes, and
    /// rejects anything else for a reason found in the input.
    #[test]
    fn z85_round_trips(data in bytes(40), text in z85_text()) {
        let whole = &data[..data.len() / 4 * 4];
        let encoded = z85::encode(whole).expect("whole groups");
        prop_assert_eq!(encoded.len(), whole.len() / 4 * 5);
        prop_assert_eq!(z85::decode(&encoded), Ok(whole.to_vec()));
        if whole.len() != data.len() {
            prop_assert_eq!(z85::encode(&data), Err(z85::Z85Error::Length));
        }
        match z85::decode(&text) {
            Ok(decoded) => prop_assert_eq!(z85::encode(&decoded), Ok(text)),
            Err(z85::Z85Error::Length) => prop_assert!(!text.len().is_multiple_of(5)),
            Err(z85::Z85Error::Character(c)) => {
                prop_assert!(text.contains(c), "{:?} is not in {:?}", c, text);
            }
            Err(z85::Z85Error::Overflow) => {}
        }
    }

    /// Keys survive their Z85 form.
    #[test]
    fn keys_round_trip(octets in any::<[u8; 32]>()) {
        let public = PublicKey::from_bytes(octets);
        let text = public.to_z85();
        prop_assert_eq!(text.len(), 40);
        prop_assert_eq!(PublicKey::from_z85(&text), Ok(public));
        let secret = SecretKey::from_z85(&text).expect("valid key");
        prop_assert_eq!(secret.expose(), &octets);
    }

    /// Subscriptions survive their message form.
    #[test]
    fn subscriptions_round_trip(topic in bytes(300), subscribe in any::<bool>()) {
        let topic = Frame::from(topic);
        let subscription = if subscribe {
            Subscription::Subscribe(topic)
        } else {
            Subscription::Cancel(topic)
        };
        prop_assert_eq!(Subscription::from_message(&subscription.to_message()), Some(subscription));
    }
}

#[test]
fn socket_type_names_round_trip() {
    for socket_type in SocketType::ALL {
        assert_eq!(
            SocketType::from_name(socket_type.name().as_bytes()),
            Some(socket_type)
        );
        let lower = socket_type.name().to_ascii_lowercase();
        assert_eq!(SocketType::from_name(lower.as_bytes()), None);
    }
}
