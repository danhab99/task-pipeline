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
        devShells = {
          default = pkgs.mkShell {
            packages = with pkgs; [
              gnumake
              go
              gopls
              delve
              sqlite
              texliveTeTeX
              pandoc
            ];

            GO_PATH="${self.outPath}/.go";
            CGO_CFLAGS="-U_FORTIFY_SOURCE";
            CGO_CPPFLAGS="-U_FORTIFY_SOURCE";
          };
        };
      });
}
