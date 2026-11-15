# zmq-pipe

Lock-free single-producer, single-consumer message pipes with high-water
marks.

This crate is an internal of [`zmq`](https://docs.rs/zmq) and makes no
stability promise: any release may break its API. Depend on `zmq`, which
re-exports what applications need.
