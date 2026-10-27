{
  inputs = {
    nixpkgs.url = "github:NixOS/nixpkgs/nixos-26.05-small";
    rust-overlay.url = "github:oxalica/rust-overlay";
    rust-overlay.inputs.nixpkgs.follows = "nixpkgs";
  };

  outputs = {
    self,
    nixpkgs,
    rust-overlay,
    ...
  }: let
    supportedSystems = [
      "x86_64-linux"
      "aarch64-darwin"
    ];
    forEachSupportedSystem = f:
      nixpkgs.lib.genAttrs supportedSystems (
        system:
          f {
            pkgs = import nixpkgs {
              inherit system;
              overlays = [
                rust-overlay.overlays.default
                # The reference libzmq with its draft socket types, for the
                # draft interop tests. Its draft sockets abort on their first
                # handshake when built without them.
                (final: prev: {
                  zeromq = prev.zeromq.override {enableDrafts = true;};
                })
              ];
            };
          }
      );
    toolchainFor = pkgs: pkgs.rust-bin.fromRustupToolchainFile ./rust-toolchain.toml;
    workspaceFor = pkgs: let
      rust = toolchainFor pkgs;
      rustPlatform = pkgs.makeRustPlatform {
        cargo = rust;
        rustc = rust;
      };
    in
      pkgs.callPackage ./default.nix {inherit rustPlatform;};
  in {
    formatter = forEachSupportedSystem ({pkgs}: pkgs.alejandra);

    packages = forEachSupportedSystem ({pkgs}: {
      default = workspaceFor pkgs;
    });

    checks = forEachSupportedSystem (
      {pkgs}: let
        rust = toolchainFor pkgs;
        workspace = workspaceFor pkgs;
      in {
        # Builds the workspace and runs `cargo test`.
        test = workspace;

        clippy = workspace.overrideAttrs (_: {
          pname = "zmq-rs-clippy";
          buildPhase = ''
            runHook preBuild
            cargo clippy --workspace --all-targets --all-features --offline -- -D warnings
            runHook postBuild
          '';
          doCheck = false;
          installPhase = "touch $out";
        });

        fmt = pkgs.runCommand "zmq-rs-fmt" {nativeBuildInputs = [rust];} ''
          cd ${pkgs.lib.cleanSource ./.}
          cargo fmt --all --check
          touch $out
        '';
      }
    );

    devShells = forEachSupportedSystem (
      {pkgs}: let
        rust = toolchainFor pkgs;
        legacy = pkgs.callPackage ./nix/legacy-zeromq.nix {};
      in {
        default = pkgs.mkShell {
          # Peers on libzmq 2.2 (ZMTP 1.0) and 3.2 (ZMTP 2.0), for the
          # interop tests of downgrading. They skip without these.
          ZMQ_LEGACY_PEER_2 = "${legacy.peer2}/bin/zmq-legacy-peer";
          ZMQ_LEGACY_PEER_3 = "${legacy.peer3}/bin/zmq-legacy-peer";
          # The shell has the legacy peers and a libzmq with drafts, so an
          # interop test that would skip for want of them fails instead.
          ZMQ_INTEROP_STRICT = "1";
          nativeBuildInputs = [
            rust
            pkgs.cargo-audit
            pkgs.cargo-nextest
            pkgs.just
            pkgs.git
            pkgs.pkg-config
            pkgs.zeromq # reference libzmq for interop tests
          ];
          shellHook = ''
            echo "Rust $(rustc --version)"
            echo "Cargo $(cargo --version)"
          '';
        };
        # cargo-fuzz needs nightly for -Zsanitizer, and llvm-tools for
        # `cargo fuzz coverage`.
        fuzz = pkgs.mkShell {
          nativeBuildInputs = [
            (pkgs.rust-bin.selectLatestNightlyWith (toolchain:
              toolchain.minimal.override {extensions = ["llvm-tools-preview"];}))
            pkgs.cargo-fuzz
          ];
        };
        # Miri, nightly only, checks zmq-pipe's unsafe queue for undefined
        # behavior and data races.
        miri = pkgs.mkShell {
          nativeBuildInputs = [
            (pkgs.rust-bin.selectLatestNightlyWith (toolchain:
              toolchain.minimal.override {extensions = ["miri" "rust-src"];}))
            pkgs.just
          ];
        };
      }
    );
  };
}
