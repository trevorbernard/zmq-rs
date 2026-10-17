// Links the system libzmq (the reference implementation) found by pkg-config.
// The Nix dev shell and package provide it; elsewhere install libzmq's
// development package.
fn main() {
    if let Err(err) = pkg_config::probe_library("libzmq") {
        panic!("the interop tests need libzmq and pkg-config: {err}");
    }
}
