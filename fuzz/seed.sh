#!/usr/bin/env bash
# Writes seed inputs into corpus/<target>/: hand-built ZMTP traffic, the
# reproducers from libzmq's OSS-Fuzz security advisories, and (with
# --fetch) libzmq's seed corpora and ClusterFuzz's public z85 corpus.
set -euo pipefail
cd "$(dirname "$0")"

seed() { # target name hex...
  local target=$1 name=$2
  shift 2
  mkdir -p "corpus/$target"
  printf "$(printf '%s' "$@" | sed 's/../\\x&/g')" >"corpus/$target/$name"
}

zeros() { printf '00%.0s' $(seq "$1"); }

# Signature, version 3.1, mechanism padded to 20 octets, as-server, filler.
greeting() { # mechanism-hex as-server
  printf 'ff%s7f0301%s%s%s%s' "$(zeros 8)" "$1" "$(zeros $((20 - ${#1} / 2)))" "$2" "$(zeros 31)"
}

NULL=4e554c4c
PLAIN=504c41494e
SOCKET_TYPE=0b536f636b65742d54797065
READY_SUB=055245414459${SOCKET_TYPE}00000003535542
SUBSCRIBE_ALL=000101

seed null_handshake v3_sub "$(greeting $NULL 00)" 0419 $READY_SUB $SUBSCRIBE_ALL
seed null_handshake v2_sub ff "$(zeros 8)" 7f 01 02 0000 $SUBSCRIBE_ALL
seed null_handshake v1_unversioned 0100 020001
# GHSA-4p5v-h92w-6wxw: PUB metadata leak (test_bind_null_fuzzer).
seed null_handshake ghsa-4p5v ff00000000000000017f03014e554c4c "$(zeros 48)" \
  04190552454144590b536f636b65742d5400006500000003040000000000657468300000
# GHSA-wfr2-29gj-5w87: client leak (test_connect_null_fuzzer).
seed null_handshake ghsa-wfr2-a fffefefd0101010108ff01010108ff010101010101c0a15b0100010001000100010001000100010001000100000000000000010001000100010001000100010001000100010001000100ffff010001000100010001000100616161616161616161616161
seed null_handshake ghsa-wfr2-b 01f301033672630000c0a15b01000100010001000100010001000100 "$(printf '61%.0s' $(seq 72))"
# GHSA-fc3w-qxf5-7hp6: ZMTP 1.0 heap overflow.
seed null_handshake ghsa-fc3w ff000000000000000800000000000000000000000000

seed plain_server hello_initiate "$(greeting $PLAIN 00)" \
  0410 0548454c4c4f 0475736572 0470617373 \
  041f 08494e495449415445 ${SOCKET_TYPE} 00000006 4445414c4552 \
  000568656c6c6f

seed plain_client welcome_ready "$(greeting $PLAIN 01)" \
  0408 0757454c434f4d45 \
  041c 055245414459 ${SOCKET_TYPE} 00000006 524f55544552 \
  000568656c6c6f

seed command_parse ready $READY_SUB
seed command_parse error 054552524f5203343030
seed command_parse ping 0450494e47000a6869
seed command_parse subscribe 09535542534352494245746f706963

seed frame_decoder v3_command 03 0419 $READY_SUB
seed frame_decoder v3_multipart 03 0103666f6f 0003626172
seed frame_decoder v3_long 03 02 0000000000000003 616263
seed frame_decoder v2_message 01 0003616263
seed frame_decoder v1_message 00 0400616263

# engine: the first octet picks the socket type (low four bits: PUB SUB
# XPUB XSUB PULL PUSH PAIR DEALER ROUTER REP), connecting rather than
# binding (0x10), the mechanism (0x20 PLAIN, 0x40 NULL with ZAP, 0x60
# CURVE) and heartbeats (0x80); the rest is the raw peer's traffic.
ready() { # socket-type-name
  local name
  name=$(printf '%s' "$1" | od -An -tx1 | tr -d ' \n')
  printf '04%02x055245414459%s%08x%s' $((22 + ${#1})) $SOCKET_TYPE ${#1} "$name"
}
NULL_V3="$(greeting $NULL 00)"
HELLO_FRAME=000568656c6c6f
seed engine pub_bind 00 "$NULL_V3" "$(ready SUB)" $SUBSCRIBE_ALL
seed engine pub_bind_v2 00 ff "$(zeros 8)" 7f 01 02 0000 $SUBSCRIBE_ALL
seed engine pub_bind_v1 00 0100 020001
seed engine sub_bind 01 "$NULL_V3" "$(ready PUB)" 000a746f70696320626f6479
seed engine xpub_bind 02 "$NULL_V3" "$(ready SUB)" 0005016e657773 0005006e657773
seed engine pull_bind 04 "$NULL_V3" "$(ready PUSH)" 0103666f6f 0003626172
seed engine pair_bind 06 "$NULL_V3" "$(ready PAIR)" $HELLO_FRAME
seed engine router_bind 08 "$NULL_V3" "$(ready DEALER)" $HELLO_FRAME
seed engine rep_bind 09 "$NULL_V3" "$(ready REQ)" 0100 $HELLO_FRAME
seed engine dealer_connect 17 "$NULL_V3" "$(ready ROUTER)" $HELLO_FRAME
seed engine router_connect 18 "$NULL_V3" "$(ready DEALER)" $HELLO_FRAME
seed engine router_bind_plain 28 "$(greeting $PLAIN 00)" \
  0410 0548454c4c4f 0475736572 0470617373 \
  041f 08494e495449415445 ${SOCKET_TYPE} 00000006 4445414c4552 \
  $HELLO_FRAME
seed engine dealer_connect_plain 37 "$(greeting $PLAIN 01)" \
  0408 0757454c434f4d45 \
  041c 055245414459 ${SOCKET_TYPE} 00000006 524f55544552 \
  $HELLO_FRAME
seed engine router_bind_zap 48 "$NULL_V3" "$(ready DEALER)" $HELLO_FRAME
# PING with a TTL and context, under heartbeats.
seed engine pull_bind_ping 84 "$NULL_V3" "$(ready PUSH)" 0409 0450494e47000a6869

if [[ ${1:-} == --fetch ]]; then
  tmp=$(mktemp -d)
  trap 'rm -rf "$tmp"' EXIT
  git clone --quiet --depth 1 https://github.com/zeromq/libzmq-fuzz-corpora.git "$tmp/libzmq"
  cp "$tmp"/libzmq/test_bind_null_fuzzer_seed_corpus/* corpus/null_handshake/
  cp "$tmp"/libzmq/test_connect_null_fuzzer_seed_corpus/* corpus/null_handshake/
  mkdir -p corpus/curve_server corpus/curve_client
  cp "$tmp"/libzmq/test_bind_curve_fuzzer_seed_corpus/* corpus/curve_server/
  cp "$tmp"/libzmq/test_connect_curve_fuzzer_seed_corpus/* corpus/curve_client/
  # The same traffic to the engine, behind the octet for libzmq's setup:
  # PUB, binding or connecting, NULL or CURVE.
  for pair in bind_null:00 connect_null:10 bind_curve:60 connect_curve:70; do
    for file in "$tmp/libzmq/test_${pair%:*}_fuzzer_seed_corpus"/*; do
      { printf "\\x${pair#*:}"; cat "$file"; } >"corpus/engine/${pair%:*}-$(basename "$file")"
    done
  done
  mkdir -p corpus/z85_decode
  cp "$tmp"/libzmq/test_z85_decode_fuzzer_seed_corpus/* corpus/z85_decode/
  curl -fsSL -o "$tmp/z85.zip" \
    https://storage.googleapis.com/libzmq-backup.clusterfuzz-external.appspot.com/corpus/libFuzzer/libzmq_test_z85_decode_fuzzer/public.zip
  unzip -q -o -d corpus/z85_decode "$tmp/z85.zip"
fi
