#![no_main]

use libfuzzer_sys::fuzz_target;
use zmtp::{
    CurveServer, CurveServerConfig, CurveServerStep, Frame, GreetingConfig, Identity, Mechanism,
    Metadata, Role, SocketType, ZapReply,
};
use zmtp_fuzz::curve;

// A CURVE ROUTER server with libzmq's fixed keys, as in its
// test_bind_curve_fuzzer, whose ZAP accepts everyone. Without the client's
// transient secret the fuzzer cannot box anything, so this reaches parsing
// and rejection; curve_session reaches what lies past them.
fuzz_target!(|data: &[u8]| {
    let config = GreetingConfig {
        socket_type: SocketType::Router,
        identity: Identity::default(),
        mechanism: Mechanism::Curve(Role::Server),
    };
    let Some((peer, mut decoder)) = zmtp_fuzz::negotiate(config, data) else {
        return;
    };
    let config = CurveServerConfig {
        socket_type: SocketType::Router,
        identity: Identity::default(),
        domain: Frame::new(),
        peer_address: String::new(),
    };
    let Ok(mut server) = CurveServer::start(config, &curve::server_keys(), &peer) else {
        return;
    };
    let mut out = Vec::new();
    loop {
        let Ok(Some(incoming)) = decoder.decode() else {
            return;
        };
        match server.receive(incoming, &mut out) {
            Ok(CurveServerStep::Continue(next)) => server = next,
            Ok(CurveServerStep::Authenticate(pending, _request)) => {
                let reply = ZapReply::Ok {
                    user_id: Frame::new(),
                    metadata: Metadata::new(),
                };
                if let Ok((_, mut codec)) = pending.reply(reply, &mut out) {
                    let _ = curve::drain(&mut decoder, &mut codec);
                }
                return;
            }
            Err(_) => return,
        }
    }
});
