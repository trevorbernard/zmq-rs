{
  pkgs,
  rustPlatform ? pkgs.rustPlatform,
}: let
  cargoToml = builtins.fromTOML (builtins.readFile ./Cargo.toml);
in
  rustPlatform.buildRustPackage {
    pname = "zmq-rs";
    version = cargoToml.workspace.package.version;
    src = pkgs.lib.cleanSource ./.;
    cargoLock = {
      lockFile = ./Cargo.lock;
    };
    cargoBuildFlags = ["--workspace"];
    cargoTestFlags = ["--workspace" "--all-features"];
    meta = with pkgs.lib; {
      description = "Idiomatic, type-safe ZeroMQ for Rust";
      homepage = "https://github.com/trevorbernard/zmq-rs";
      license = licenses.mpl20;
      platforms = platforms.unix;
      maintainers = [
        {
          github = "trevorbernard";
          name = "Trevor Bernard";
          email = "trevor.bernard@pm.me";
        }
      ];
    };
  }
