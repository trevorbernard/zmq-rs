//! Idiomatic, type-safe ZeroMQ for Rust.
//!
//! Socket types expose only the operations their pattern allows, so invalid
//! states such as receiving on a PUB socket or replying twice on REP are
//! compile errors rather than runtime `EFSM`/`ENOTSUP` errors.
