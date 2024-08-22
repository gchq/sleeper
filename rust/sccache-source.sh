#!/bin/bash

set -x
set -euo pipefail

# shellcheck disable=SC1091
. lib.sh

main() {
    local triple
    local tag
    local td
    local url="https://github.com/mozilla/sccache"
    triple="${1}"

    install_packages ca-certificates curl unzip

    # install rust and cargo to build sccache
    export RUSTUP_HOME=/tmp/rustup
    export CARGO_HOME=/tmp/cargo
    curl --retry 3 -sSfL https://sh.rustup.rs -o rustup-init.sh
    sh rustup-init.sh -y --no-modify-path
    rm rustup-init.sh
    export PATH="${CARGO_HOME}/bin:${PATH}"
    rustup target add "${triple}"

    # download the source code from the latest sccache release
    td="$(mktemp -d)"
    pushd "${td}"
    tag=$(git ls-remote --tags --refs --exit-code \
        "${url}" \
        | cut -d/ -f3 \
        | grep -E '^v[0-9]+\.[0-9]+\.[0-9]+$' \
        | sort --version-sort \
        | tail -n1)
    curl -LSfs "${url}/archive/refs/tags/${tag}.zip" \
        -o sccache.zip
    unzip sccache.zip
    mv "sccache-${tag//v/}" sccache
    rm sccache.zip

    # build from source for the desired architecture
    # you can also use additional features here
    cd sccache
    cargo build --release --target "${triple}" \
        --features=all,"openssl/vendored"
    cp "target/${triple}/release/sccache" "/usr/bin/sccache"

    # clean up our install
    rm -r "${RUSTUP_HOME}" "${CARGO_HOME}"
    purge_packages
    popd
    rm -rf "${td}"
    rm "${0}"
}

main "${@}"