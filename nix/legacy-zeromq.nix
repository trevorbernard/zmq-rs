# libzmq releases that speak the older wire protocols, as peers for the
# downgrade interop tests: 2.2 speaks ZMTP 1.0 and 3.2 speaks ZMTP 2.0.
{
  lib,
  stdenv,
  fetchurl,
  libuuid,
}: let
  # The interop tests' peer, `tests/interop/legacy/peer.c`, built against
  # `zeromq`: one process per old libzmq, as two cannot share one.
  peer = zeromq:
    stdenv.mkDerivation {
      pname = "zmq-legacy-peer";
      inherit (zeromq) version;
      src = ../tests/interop/legacy;
      buildInputs = [zeromq];
      buildPhase = "cc -O2 -Wall -Werror -o zmq-legacy-peer peer.c -lzmq";
      installPhase = "install -Dm755 zmq-legacy-peer $out/bin/zmq-legacy-peer";
    };

  zeromq = {
    version,
    repo,
    sha256,
    buildInputs ? [],
    postPatch ? "",
  }:
    stdenv.mkDerivation {
      pname = "zeromq";
      inherit version buildInputs postPatch;
      src = fetchurl {
        url = "https://github.com/zeromq/${repo}/releases/download/v${version}/zeromq-${version}.tar.gz";
        inherit sha256;
      };
      # Written for C++98, which later standards reject in places.
      CXXFLAGS = "-std=gnu++98";
      configureFlags = ["--disable-static" "--without-documentation"];
      enableParallelBuilding = true;
      meta.license = lib.licenses.lgpl3Plus;
    };
in rec {
  peer2 = peer zeromq2;
  peer3 = peer zeromq3;
  zeromq2 = zeromq {
    version = "2.2.0";
    repo = "zeromq2-x";
    sha256 = "0ds6c244wik1lyfrfr2f4sha30fly4xybr02r9r4z156kvmlm423";
    buildInputs = [libuuid];
  };
  zeromq3 = zeromq {
    version = "3.2.5";
    repo = "zeromq3-x";
    sha256 = "0911r7q4i1x9gnfinj39vx08fnz59mf05vl75zdkws36lib3wr89";
    # Its tests call nanosleep without including time.h, as older C
    # libraries let them.
    postPatch = "sed -i '1i #include <time.h>' tests/test_connect_delay.cpp";
  };
}
