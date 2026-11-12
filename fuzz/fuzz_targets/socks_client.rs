#![no_main]

use libfuzzer_sys::fuzz_target;
use zmq_core::{SocksClient, SocksError, SocksProxy};
use zmtp::Credential;

/// What the input's first octet picks: whether we have a user name (bit
/// 0), and the destination asked for (bits 1 and 2).
fn client(setup: u8) -> SocksClient {
    let username = if setup & 1 == 0 { "" } else { "user" };
    let proxy = SocksProxy {
        host: "proxy".to_owned(),
        port: 1080,
        source: None,
        username: Credential::new(username).expect("short"),
        password: Credential::new("pass").expect("short"),
    };
    let host = match (setup >> 1) & 3 {
        0 => "127.0.0.1",
        1 => "::1",
        2 => "example.org",
        _ => "a",
    };
    SocksClient::new(&proxy, host, 5555).expect("askable")
}

/// What a negotiation came to: how it ended, if it did, how many of the
/// proxy's octets it took, and what we sent.
type Outcome = (Option<Result<(), SocksError>>, usize, Vec<u8>);

/// Feeds `data` as a stream would give it, at most `chunk` octets at a
/// time and never more than are wanted.
fn negotiate(setup: u8, data: &[u8], chunk: usize) -> Outcome {
    let mut client = client(setup);
    let mut out = Vec::new();
    client.greet(&mut out);
    let mut taken = 0;
    while taken < data.len() {
        let wanted = client.wanted();
        assert!(wanted > 0, "an unfinished negotiation wants octets");
        let end = data.len().min(taken + wanted.min(chunk));
        let fed = client.feed(&data[taken..end], &mut out);
        taken = end;
        match fed {
            Ok(false) => {}
            Ok(true) => return (Some(Ok(())), taken, out),
            Err(err) => return (Some(Err(err)), taken, out),
        }
    }
    (None, taken, out)
}

// A SOCKS5 client: the input is what the proxy sends. However the stream
// splits it, the negotiation must end the same way, having sent the same
// and left the same octets for the peer.
fuzz_target!(|data: &[u8]| {
    let Some((&setup, data)) = data.split_first() else {
        return;
    };
    let whole = negotiate(setup, data, usize::MAX);
    let by_octet = negotiate(setup, data, 1);
    if whole.0 != Some(Err(SocksError::Malformed)) {
        assert_eq!(whole, by_octet);
    } else {
        // A wrong octet is refused when it is read, so sooner by octet.
        assert_eq!(by_octet.0, whole.0);
        assert!(by_octet.1 <= whole.1);
        assert_eq!(by_octet.2, whole.2);
    }
    // The proxy's longest reply is to CONNECT, with a 255-octet name.
    let longest = 2 + 2 + (4 + 1 + 255 + 2);
    assert!(whole.1 <= longest, "took {} octets", whole.1);
    if whole.0 == Some(Ok(())) {
        // The last thing we sent was the CONNECT, which ends with the port.
        assert!(
            whole.2.ends_with(&5555u16.to_be_bytes()),
            "connected unasked"
        );
    }
});
