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
    pkg-config # Used to find openssl install
    openssl # Needed by git2 module in Rust
  ];
  shellHook = ''
    rustup default stable
  '';
}
