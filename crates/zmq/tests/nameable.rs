//! Types in `zmq`'s public signatures can be named from `zmq` alone.

use zmq::{
    EmptyMessage, Frame, InvalidName, KeyError, Message, Metadata, PropertyName, PublicKey,
    TooLong, Z85Error, message,
};

#[test]
fn an_authenticator_can_build_metadata() {
    let mut metadata = Metadata::default();
    let name: Result<PropertyName, InvalidName> = PropertyName::new("X-Role");
    let inserted: Result<(), TooLong> = metadata.insert(name.unwrap(), "admin");
    inserted.unwrap();
    assert_eq!(metadata.iter().count(), 1);
}

#[test]
fn a_message_from_no_frames_is_an_error_with_a_name() {
    let empty: Result<Message, EmptyMessage> = Vec::<Frame>::new().try_into();
    assert_eq!(empty, Err(EmptyMessage));
}

#[test]
fn a_messages_iterators_have_names() {
    let message = Message::from("frame");
    let borrowed: message::Iter<'_> = message.iter();
    assert_eq!(borrowed.len(), 1);
    let owned: message::IntoIter = message.into_iter();
    assert_eq!(owned.len(), 1);
}

#[test]
fn a_bad_key_says_what_z85_refused() {
    let Err(KeyError::Z85(error)) = PublicKey::from_z85(&" ".repeat(40)) else {
        panic!("a key of spaces");
    };
    let _: Z85Error = error;
}
