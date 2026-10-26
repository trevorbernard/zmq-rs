#![no_main]

use libfuzzer_sys::fuzz_target;
use zmtp::{
    PlainClientStep, Credential, GreetingConfig, Identity, Mechanism, PlainClient, PlainCredentials,
    Role, SocketType,
};

// A PLAIN DEALER client: the input is what the server sends.
fuzz_target!(|data: &[u8]| {
    let config = GreetingConfig {
        socket_type: SocketType::Dealer,
        identity: Identity::default(),
        mechanism: Mechanism::Plain(Role::Client),
    };
    let Some((peer, mut decoder)) = zmtp_fuzz::negotiate(config, data) else {
        return;
    };
    let credentials = PlainCredentials {
        username: Credential::new("user").expect("short"),
        password: Credential::new("pass").expect("short"),
    };
    let mut out = Vec::new();
    let Ok(mut client) = PlainClient::start(
        SocketType::Dealer,
        Identity::default(),
        &credentials,
        &peer,
        &mut out,
    ) else {
        return;
    };
    loop {
        let Ok(Some(incoming)) = decoder.decode() else {
            return;
        };
        match client.receive(incoming, &mut out) {
            Ok(PlainClientStep::Continue(next)) => client = next,
            Ok(PlainClientStep::Established(_)) => {
                zmtp_fuzz::drain(&mut decoder);
                return;
            }
            Err(_) => return,
        }
    }
});
