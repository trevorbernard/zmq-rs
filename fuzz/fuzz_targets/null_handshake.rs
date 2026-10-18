#![no_main]

use libfuzzer_sys::fuzz_target;
use zmtp::{GreetingConfig, Identity, LegacyPeers, Mechanism, NullHandshake, SocketType};

// A NULL PUB server, as in libzmq's test_bind_null_fuzzer: greeting,
// handshake, then whatever traffic follows.
fuzz_target!(|data: &[u8]| {
    let identity = Identity::new(Vec::new()).expect("empty identity");
    let config = GreetingConfig {
        socket_type: SocketType::Pub,
        identity: identity.clone(),
        mechanism: Mechanism::Null {
            legacy_peers: LegacyPeers::Allow,
        },
    };
    let Some((peer, mut decoder)) = zmtp_fuzz::negotiate(config, data) else {
        return;
    };
    let mut out = Vec::new();
    let Ok(handshake) = NullHandshake::start(SocketType::Pub, &identity, &peer, &mut out) else {
        return;
    };
    let Ok(Some(incoming)) = decoder.decode() else {
        return;
    };
    if handshake.receive(incoming, &mut out).is_ok() {
        zmtp_fuzz::drain(&mut decoder);
    }
});
