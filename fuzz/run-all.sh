#!/usr/bin/env bash
# Runs every target at once for SECONDS (default 60) each, then reports
# those that failed with their logs' last lines. Logs go to target/logs/.
set -euo pipefail
cd "$(dirname "$0")"

seconds=${1:-60}
# rustc's host, not cargo-fuzz's own: a prebuilt cargo-fuzz may be a musl
# build, and sanitizers refuse musl's static libc.
host=$(rustc -vV | sed -n 's/host: //p')
cargo fuzz build --target "$host"
mkdir -p target/logs
declare -A pids
for target in $(cargo fuzz list); do
  cargo fuzz run --target "$host" "$target" -- -dict=zmtp.dict -max_total_time="$seconds" \
    >"target/logs/$target.log" 2>&1 &
  pids[$target]=$!
done
failed=0
for target in "${!pids[@]}"; do
  if wait "${pids[$target]}"; then
    echo "$target: $(grep '^Done' "target/logs/$target.log")"
  else
    echo "$target: FAILED"
    tail -30 "target/logs/$target.log"
    failed=1
  fi
done
exit $failed
