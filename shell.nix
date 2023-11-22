let
  pkgs = import <nixpkgs> {};
in
pkgs.mkShell {
  nativeBuildInputs = with pkgs; [
    bash
    nodejs
    awscli2
    nodePackages.aws-cdk
    jdk17
    jdk11
    python3
    git
    maven
    k9s
    cmake
    gcc
    rustup
    cargo-cross
  ];
  shellHook = ''
    rustup default stable
  '';
}
