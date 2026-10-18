//! Reports the zmq crate's version. zmq-rs is a port, not a binding, so
//! there is no libzmq version to ask for.

fn main() {
    println!("Current zmq-rs version is {}", env!("CARGO_PKG_VERSION"));
}
