#![no_main]

use libfuzzer_sys::fuzz_target;
use zmtp::{CurveClient, CurveClientStep, GreetingConfig, Identity, Mechanism, Role, SocketType};
use zmtp_fuzz::curve;

// A CURVE DEALER client with libzmq's fixed keys, as in its
// test_connect_curve_fuzzer: the input is what the server sends.
fuzz_target!(|data: &[u8]| {
    let config = GreetingConfig {
        socket_type: SocketType::Dealer,
        identity: Identity::default(),
        mechanism: Mechanism::Curve(Role::Client),
    };
    let Some((peer, mut decoder)) = zmtp_fuzz::negotiate(config, data) else {
        return;
    };
    let mut out = Vec::new();
    let Ok(mut client) = CurveClient::start(
        SocketType::Dealer,
        Identity::default(),
        &curve::client_keys(),
        curve::server_public(),
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
            Ok(CurveClientStep::Continue(next)) => client = next,
            Ok(CurveClientStep::Established(_, mut codec)) => {
                let _ = curve::drain(&mut decoder, &mut codec);
                return;
            }
            Err(_) => return,
        }
    }
});
