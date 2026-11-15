# zmtp

The ZeroMQ Message Transport Protocol (ZMTP 1.0 to 3.1) as a sans-IO state
machine.

This crate is an internal of [`zmq`](https://docs.rs/zmq) and makes no
stability promise: any release may break its API. Depend on `zmq`, which
re-exports what applications need.
