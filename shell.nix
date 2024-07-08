let
  pkgs = import <nixpkgs> {};
in
pkgs.mkShell {
  nativeBuildInputs = with pkgs; [
    bash
    jq
    zip
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
      # PyArrow requires native code that isn't accessible through Nix with pip install
      python-pkgs.pyarrow
      # Note that including boto3 or botocore here may break the AWS CLI.
      # Nix adds Python dependencies for all Python installs, including the one in the AWS CLI package.
    ]))
  ];
}
