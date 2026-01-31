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

        grit = import ./lib.nix { inherit lib; };
      in
      {
        packages = {
          default = pkgs.buildGoModule {
            pname = "grit";
            version = "0.2.1";
            src = self;
            vendorHash = "sha256-NEWUHUio0oPZdSB9obpZEOD5RQcIsAwnosQg2yESXME=";
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

        checks = import ./checks.nix { inherit pkgs grit lib; };
      })) // (let
        pkgs = import nixpkgs {
          system = "x86_64-linux";
          config.allowUnfree = true;
        };
        lib = pkgs.lib;
      in {
        lib = import ./lib.nix { inherit lib pkgs; };
      });
}
