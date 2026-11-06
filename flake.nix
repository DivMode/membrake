{
  description = "membrake: a memory brake for macOS";

  inputs.nixpkgs.url = "github:NixOS/nixpkgs/nixpkgs-26.05-darwin";

  outputs =
    { self, nixpkgs }:
    let
      systems = [
        "aarch64-darwin"
        "x86_64-darwin"
      ];
      forAllSystems = f: nixpkgs.lib.genAttrs systems (system: f nixpkgs.legacyPackages.${system});
      cargoToml = builtins.fromTOML (builtins.readFile ./Cargo.toml);
    in
    {
      packages = forAllSystems (pkgs: {
        default = pkgs.rustPlatform.buildRustPackage {
          pname = cargoToml.package.name;
          version = cargoToml.package.version;
          src = pkgs.lib.fileset.toSource {
            root = ./.;
            fileset = pkgs.lib.fileset.unions [
              ./Cargo.toml
              ./Cargo.lock
              # tests/readme_hook.rs runs the example hook out of the README.
              ./README.md
              ./src
              ./tests
            ];
          };
          cargoLock.lockFile = ./Cargo.lock;
          # The same test runs the hook, which needs jq.
          nativeCheckInputs = [ pkgs.jq ];
          meta = {
            description = cargoToml.package.description;
            license = pkgs.lib.licenses.mit;
            platforms = systems;
            mainProgram = "membrake";
          };
        };
      });

      devShells = forAllSystems (pkgs: {
        default = pkgs.mkShell {
          packages = with pkgs; [
            cargo
            clippy
            rustc
            rustfmt
          ];
        };
      });
    };
}
