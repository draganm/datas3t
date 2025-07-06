{
  description = "Datas3t";
  inputs = {

    nixpkgs = { url = "github:NixOS/nixpkgs/nixos-25.05"; };
    nixpkgs-unstable = { url = "github:NixOS/nixpkgs/nixos-unstable"; };
    
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
            pkgs-unstable = import nixpkgs-unstable {
              inherit system;
              config = { allowUnfree = true; };
            };
          in f { inherit pkgs pkgs-unstable; });
    in {
      devShells = eachSystem ({ pkgs, pkgs-unstable }: {
        default = pkgs.mkShell {
          shellHook = ''
            # Set here the env vars you want to be available in the shell
          '';
          hardeningDisable = [ "all" ];

          packages = with pkgs; [
            go
            shellcheck
            sqlc
            minio
            docker
            pkgs-unstable.claude-code
          ];
        };
      });
    };
}

