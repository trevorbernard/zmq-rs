#!/usr/bin/env bash
# Line coverage of zmtp and the sockets over every target's corpus,
# combined: run the targets first so the corpora hold what they found.
# Needs llvm-tools, which the fuzz dev shell has.
set -euo pipefail
cd "$(dirname "$0")"

bin=$(echo "$(rustc --print sysroot)"/lib/rustlib/*/bin)
host=$(rustc -vV | sed -n 's/host: //p')
objects=()
for target in $(cargo fuzz list); do
  cargo fuzz coverage --target "$host" "$target" >/dev/null 2>&1
  objects+=(-object "target/$host/coverage/$host/release/$target")
done
"$bin/llvm-profdata" merge -sparse coverage/*/coverage.profdata -o coverage/all.profdata
"$bin/llvm-cov" report "${objects[@]}" -instr-profile=coverage/all.profdata \
  --ignore-filename-regex='cargo/registry|rustc/|/fuzz/' "$@"
