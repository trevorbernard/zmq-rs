#![no_main]

use libfuzzer_sys::fuzz_target;
use zmtp::Command;

// Encoding may normalize the body, so compare parses rather than octets.
fuzz_target!(|body: &[u8]| {
    let Ok(command) = Command::parse(body) else {
        return;
    };
    let mut encoded = Vec::new();
    command.encode(&mut encoded);
    assert_eq!(Command::parse(&encoded).as_ref(), Ok(&command));
});
