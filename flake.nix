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
              overlays = [rust-overlay.overlays.default];
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
      in {
        default = pkgs.mkShell {
          nativeBuildInputs = [
            rust
            pkgs.cargo-audit
            pkgs.cargo-nextest
            pkgs.git
            pkgs.pkg-config
            pkgs.zeromq # reference libzmq for interop tests
          ];
          shellHook = ''
            echo "Rust $(rustc --version)"
            echo "Cargo $(cargo --version)"
          '';
        };
      }
    );
  };
}
