# zmq-rs

- Crates, and which file holds each piece: `docs/architecture.md`
- Every source file opens with a `//!` summary: `head -n3 crates/<crate>/src/*.rs`
- Spec coverage: `docs/rfc-compliance.md`. Benchmarks: `docs/benchmarks.md`
- Tests:
  - unit: inline `mod tests` (zmq-core has no `tests/` dir)
  - property tests: `crates/zmtp/tests/`, `crates/zmq-pipe/tests/`; libzmq advisories: `crates/zmtp/tests/advisories.rs`
  - socket behaviour: `crates/zmq/tests/`
  - libzmq's ported tests: `crates/zmq/tests/libzmq/` (its README lists what's left out)
  - against real libzmq: `tests/interop/tests/`
- Commands: `just --list`; `just ci` matches CI
- Config: `.config/nextest.toml`, `.cargo/mutants.toml`, `.github/workflows/`
- The dev shell (`flake.nix`, via direnv) has nextest, cargo-mutants, cargo-llvm-cov, libzmq, gdb, python3
