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
    git
    maven
    k9s
  ];
  packages = [
    (pkgs.python3.withPackages(python-pkgs: [
      python-pkgs.wheel
      python-pkgs.pip
      python-pkgs.setuptools
      python-pkgs.pyarrow
    ]))
  ];
}
