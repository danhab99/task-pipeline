{
  inputs = {
    nixpkgs.url = "github:NixOS/nixpkgs";
    flake-utils.url = "github:numtide/flake-utils";
  };

  outputs = { self, nixpkgs, flake-utils }:
    (flake-utils.lib.eachSystem flake-utils.lib.defaultSystems (system:
      let
        pkgs = import nixpkgs {
          inherit system;
          config.allowUnfree = true;
        };
        lib = pkgs.lib;

        grit = import ./lib.nix { };
      in
      {
        packages = {
          default = pkgs.buildGoModule {
            pname = "task-pipeline";
            version = "0.1.0";
            src = self;
            vendorHash = "sha256-lyPVR2ZXBaelbsk/zNxjxgOnrKMUm8shdXW7mXU4ndM=";
            subPackages = [ "." ];

            GO_PATH = "${self.outPath}/.go";
            CGO_CFLAGS = "-U_FORTIFY_SOURCE";
            CGO_CPPFLAGS = "-U_FORTIFY_SOURCE";
          };
        };

        devShells.default = pkgs.mkShell {
          buildInputs = with pkgs; [
            go
            gopls
            delve
            sqlite
            sqlite-web
            pandoc
            texliveFull
          ];

          shellHook = ''
            export OUTPUT_DIR=$(mktemp -d)
          '';

          CGO_CFLAGS = "-U_FORTIFY_SOURCE";
          CGO_CPPFLAGS = "-U_FORTIFY_SOURCE";
        };

        checks = import ./checks.nix { inherit pkgs grit; };
      }
    )) // {
      lib = import ./lib.nix { };
    };
}
