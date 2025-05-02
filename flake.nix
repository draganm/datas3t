{
  description = "Datas3t";
  inputs = {

    nixpkgs = { url = "github:NixOS/nixpkgs/nixos-24.11"; };
    nixpkgs-unstable = { 
      url = "github:NixOS/nixpkgs/f771eb401a46846c1aebd20552521b233dd7e18b"; 
    };
    systems.url = "github:nix-systems/default";

  };

  outputs = { self, nixpkgs, nixpkgs-unstable, systems, ... }@inputs:
    let
      eachSystem = f:
        nixpkgs.lib.genAttrs (import systems) (system:
          let
            pkgs = import nixpkgs {
              inherit system;
              config = { allowUnfree = true; };
            };
            unstable = import nixpkgs-unstable {
              inherit system;
              config = { allowUnfree = true; };
            };
          in f { inherit pkgs unstable; });
    in {
      devShells = eachSystem ({ pkgs, unstable }: {
        default = pkgs.mkShell {
          shellHook = ''
            # Set here the env vars you want to be available in the shell
          '';
          hardeningDisable = [ "all" ];

          packages = with pkgs; [
            go
            shellcheck
            sqlc
            sqlite
            overmind
            minio
            docker
            unstable.claude-code
          ];
        };
      });
    };
}

