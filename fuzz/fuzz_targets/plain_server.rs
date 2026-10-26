#![no_main]

use libfuzzer_sys::fuzz_target;
use zmtp::{
    Frame, GreetingConfig, Identity, Mechanism, Metadata, PlainServer, Role, ServerConfig,
    PlainServerStep, SocketType, ZapReply,
};

// A PLAIN server whose authenticator accepts everyone, so the fuzzer can
// reach INITIATE and the traffic after it.
fuzz_target!(|data: &[u8]| {
    let identity = Identity::new(Vec::new()).expect("empty identity");
    let config = GreetingConfig {
        socket_type: SocketType::Router,
        identity: identity.clone(),
        mechanism: Mechanism::Plain(Role::Server),
    };
    let Some((peer, mut decoder)) = zmtp_fuzz::negotiate(config, data) else {
        return;
    };
    let config = ServerConfig {
        socket_type: SocketType::Router,
        identity,
        domain: Frame::new(),
        peer_address: String::new(),
    };
    let Ok(mut server) = PlainServer::start(config, &peer) else {
        return;
    };
    let mut out = Vec::new();
    loop {
        let Ok(Some(incoming)) = decoder.decode() else {
            return;
        };
        match server.receive(incoming, &mut out) {
            Ok(PlainServerStep::Authenticate(pending, _request)) => {
                let reply = ZapReply::Ok {
                    user_id: Frame::new(),
                    metadata: Metadata::new(),
                };
                match pending.reply(reply, &mut out) {
                    Ok(next) => server = next,
                    Err(_) => return,
                }
            }
            Ok(PlainServerStep::Established(_)) => {
                zmtp_fuzz::drain(&mut decoder);
                return;
            }
            Err(_) => return,
        }
    }
});
