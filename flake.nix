{
  outputs = { nixpkgs, ... }: 
  let
    pkgs = nixpkgs.legacyPackages.x86_64-linux;
    visualvm-wrapped = pkgs.writeShellApplication {
      name = "visualvm";
      text = ''
        exec ${pkgs.visualvm}/bin/visualvm -J-Dawt.useSystemAAFontSettings=on "$@"
      '';
    };
  in {
    devShell.x86_64-linux = pkgs.mkShell {
      packages = with pkgs; [ cmake zlib gmp openjdk visualvm-wrapped ];
    };
  };
}
