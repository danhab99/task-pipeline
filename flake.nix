{
  inputs = {
    nixpkgs.url = "github:NixOS/nixpkgs";
    flake-utils.url = "github:numtide/flake-utils";
  };

  outputs = { self, nixpkgs, flake-utils }:
    flake-utils.lib.eachSystem flake-utils.lib.defaultSystems (system:
      let
        pkgs = import nixpkgs {
          inherit system;
          config.allowUnfree = true;
        };
        lib = pkgs.lib;

      in {
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
          ];

          shellHook = ''
            echo "Task Pipeline development environment"
            echo "Go version: $(go version)"
            echo ""
            echo "Available commands:"
            echo "  go build      - Build the project"
            echo "  go test       - Run tests"
            echo "  dlv debug     - Debug with Delve"
          '';

          CGO_CFLAGS = "-U_FORTIFY_SOURCE";
          CGO_CPPFLAGS = "-U_FORTIFY_SOURCE";
        };
      });
}
