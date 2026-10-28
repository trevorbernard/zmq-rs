# Run inside the dev shell (`nix develop` or direnv), which provides the
# toolchain, nextest, libzmq and the legacy libzmq peers.

[private]
default:
    @just --list

# Everything CI checks.
ci: fmt-check clippy test

# Tests; arguments filter, as for nextest. Doctests run only unfiltered.
test *args:
    cargo nextest run --workspace --all-features {{args}}
    {{ if args == "" { "cargo test --workspace --all-features --doc" } else { "" } }}

# zmq-pipe's queue on loom: every interleaving of its models. Its own
# target directory, as `--cfg loom` rebuilds everything.
loom *args:
    RUSTFLAGS="--cfg loom" CARGO_TARGET_DIR=target/loom cargo test -p zmq-pipe --lib --release {{args}}

# zmq-pipe's unit tests under Miri, for undefined behavior and data races.
miri *args:
    nix develop .#nightly -c cargo miri test -p zmq-pipe --lib {{args}}

# zmq-pipe's tests and the shutdown stress tests under ThreadSanitizer.
# Doctests are left out: rustdoc builds them without the sanitizer.
tsan:
    #!/usr/bin/env bash
    set -euo pipefail
    export RUSTFLAGS="-Zsanitizer=thread" CARGO_TARGET_DIR=target/tsan
    target=$(rustc -vV | sed -n 's/host: //p')
    tsan="nix develop .#nightly -c cargo test -Zbuild-std --target $target"
    $tsan -p zmq-pipe --lib --tests
    $tsan -p zmq --all-features --test shutdown_stress
    $tsan -p zmq --all-features --test libzmq shutdown_stress

# Mutation testing of one crate: the changes to its code that no test
# notices, in target/mutants/<crate>/mutants.out; `--iterate` retests only
# what earlier runs left. zmq-core is tested mostly from above, so its
# mutants run zmq's tests too; the interop tests would double each run.
# cargo-mutants 27 ignores --test-package under --package, or without
# --test-workspace=false, so the crate is picked by its files. Its baseline
# still runs only the crate's own tests, so zmq-core's timeout, which would
# follow from those, is set.
mutants crate *args:
    cargo mutants --file 'crates/{{crate}}/**' --test-workspace=false \
        {{ if crate == "zmq-core" { "--test-package zmq-core --test-package zmq --timeout 90" } else { "--test-package " + crate } }} \
        --test-tool nextest --profile mutants --all-features \
        --output target/mutants/{{crate}} {{args}}

# Interop tests against libzmq 2.2 (ZMTP 1.0) and 3.2 (ZMTP 2.0).
test-legacy *args:
    cargo nextest run -p zmq-interop --test legacy_peers {{args}}

fmt:
    cargo fmt --all

fmt-check:
    cargo fmt --all --check

clippy:
    cargo clippy --workspace --all-targets --all-features -- -D warnings

audit:
    cargo audit

# Tests, clippy and rustfmt, as Nix builds them.
flake-check:
    nix flake check

# Throughput and latency next to libzmq; `just perf -- --quick` to smoke test.
perf *args:
    cargo bench -p zmq-interop --bench perf {{args}}

# Patterns under load next to libzmq.
scenarios *args:
    cargo bench -p zmq-interop --bench scenarios {{args}}

# Micro benchmarks: pipe (zmq-pipe), fanout or fairqueue (zmq-core).
bench name *args:
    cargo bench -p {{ if name == "pipe" { "zmq-pipe" } else { "zmq-core --bench " + name } }} {{args}}

# A guide example, e.g. `just example hwserver`.
example name *args:
    cargo run -p zguide --bin {{name}} -- {{args}}

# Writes the fuzz seed corpora; `just fuzz-seed --fetch` adds libzmq's.
fuzz-seed *args:
    nix develop .#fuzz -c ./fuzz/seed.sh {{args}}

# Fuzzes one target, e.g. `just fuzz null_handshake`.
fuzz target *args:
    cd fuzz && nix develop ..#fuzz -c cargo fuzz run {{target}} -- -dict=zmtp.dict {{args}}

# Fuzzes every target at once, for `seconds` each.
fuzz-all seconds="60":
    nix develop .#fuzz -c ./fuzz/run-all.sh {{seconds}}

# Line coverage of zmtp and the sockets over the fuzz corpora.
fuzz-coverage *args:
    nix develop .#fuzz -c ./fuzz/coverage.sh {{args}}
