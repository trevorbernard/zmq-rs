//! Shared drivers for the fuzz targets.
//!
//! Wire targets take the raw octets a peer sends after connecting, the same
//! input shape as libzmq's `test_bind_*_fuzzer` harnesses, so their seed
//! corpora and `zmtp.dict` apply unchanged.

use zmtp::{Decoder, DecoderConfig, Greeting, GreetingConfig, Incoming, PeerGreeting};

/// libzmq's harnesses cap ZMQ_MAXMSGSIZE at 64 MiB to keep the sanitizer
/// allocator happy; so do we.
pub const MAX_MESSAGE_SIZE: u64 = 64 * 1024 * 1024;

pub fn decoder_config() -> DecoderConfig {
    DecoderConfig {
        max_message_size: Some(MAX_MESSAGE_SIZE),
    }
}

/// Runs version negotiation over `data`, returning the peer's greeting and a
/// decoder holding every octet after it, as the engine sets one up.
pub fn negotiate(config: GreetingConfig, data: &[u8]) -> Option<(PeerGreeting, Decoder)> {
    let mut greeting = Greeting::new(config);
    let feed = greeting.feed(data).ok()?;
    let peer = feed.peer?;
    let mut decoder = Decoder::new(peer.version(), decoder_config());
    if let PeerGreeting::Zmtp1Unversioned { replay } = &peer {
        decoder.extend_from_slice(replay);
    }
    decoder.extend_from_slice(&data[feed.consumed..]);
    Some((peer, decoder))
}

/// Decodes until the decoder needs more input or fails.
pub fn drain(decoder: &mut Decoder) -> Vec<Incoming> {
    let mut items = Vec::new();
    while let Ok(Some(item)) = decoder.decode() {
        items.push(item);
    }
    items
}

/// The long-term keys of libzmq's `test_bind_curve_fuzzer` and
/// `test_connect_curve_fuzzer`, so their seed corpora apply.
pub mod curve {
    use zmtp::{CurveCodec, Decoder, Incoming, Keypair, PublicKey, SecretKey};

    pub const CLIENT_SECRET: &str = "N?Gmik8R[2ACw{b7*[-$S6[4}aO#?DB?#=<OQPc7";
    pub const SERVER_PUBLIC: &str = "3.9-xXwy{g*w72TP*3iB9IJJRxlBH<ufTAvPd2>C";
    pub const SERVER_SECRET: &str = "T}t5GLq%&Qm1)y3ywu-}pY3KEA//{^Ut!M1ut+B4";

    pub fn client_keys() -> Keypair {
        Keypair::from_secret(SecretKey::from_z85(CLIENT_SECRET).expect("a key"))
    }

    pub fn server_keys() -> Keypair {
        Keypair::from_secret(SecretKey::from_z85(SERVER_SECRET).expect("a key"))
    }

    pub fn server_public() -> PublicKey {
        PublicKey::from_z85(SERVER_PUBLIC).expect("a key")
    }

    /// Decodes and unboxes until the decoder needs more input, or either
    /// fails.
    pub fn drain(decoder: &mut Decoder, codec: &mut CurveCodec) -> Result<Vec<Incoming>, ()> {
        let mut items = Vec::new();
        while let Some(item) = decoder.decode().map_err(drop)? {
            if let Some(item) = codec.decode(item).map_err(drop)? {
                items.push(item);
            }
        }
        Ok(items)
    }
}
