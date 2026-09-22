#!/usr/bin/env bash
# Copyright 2022-2026 Crown Copyright
#
# Licensed under the Apache License, Version 2.0 (the "License");
# you may not use this file except in compliance with the License.
# You may obtain a copy of the License at
#
#     http://www.apache.org/licenses/LICENSE-2.0
#
# Unless required by applicable law or agreed to in writing, software
# distributed under the License is distributed on an "AS IS" BASIS,
# WITHOUT WARRANTIES OR CONDITIONS OF ANY KIND, either express or implied.
# See the License for the specific language governing permissions and
# limitations under the License.

# Checks that a native library we have built can be loaded on Amazon Linux 2023.
#
# Amazon Linux 2023 includes glibc 2.34, and if a library is built against a newer version it can
# fail to dynamically link to the version available in Amazon Linux. We scan the library file for
# which versions of glibc it links against.
#
# Usage: check-native-lib.sh <library> <x86_64|aarch64>

set -euo pipefail
unset CDPATH

# See https://docs.aws.amazon.com/linux/al2023/ug/core-glibc.html
MAX_GLIBC="2.34"
MAX_GLIBCXX="3.4.29"

if [ $# -ne 2 ]; then
  echo "Usage: $0 <library> <x86_64|aarch64>" >&2
  exit 1
fi

LIBRARY=$1
ARCH=$2

case "$ARCH" in
  x86_64) EXPECTED_MACHINE="Advanced Micro Devices X86-64" ;;
  aarch64) EXPECTED_MACHINE="AArch64" ;;
  *) echo "Architecture not recognised, expected x86_64 or aarch64: $ARCH"; exit 1 ;;
esac

if [ ! -f "$LIBRARY" ]; then
  echo "Library not found: $LIBRARY"
  exit 1
fi

READELF_OUTPUT=$(readelf -V "$LIBRARY")

echo "Output from readelf -V for $LIBRARY:"
echo "$READELF_OUTPUT"
echo

# Highest version required from one symbol version family, e.g. max_required_version GLIBC.
# Reads the ELF version requirements section, which shows each dynamic dependency.
# The underscore in the pattern distinguishes GLIBC from GLIBCXX.
# Empty output means the library requires nothing from that family.
max_required_version() {
  local family=$1
  echo "$READELF_OUTPUT" |
    sed -n "s/.*Name: ${family}_\([0-9.]*\).*/\1/p" |
    sort -Vu |
    tail -1
}

# True when the first version is no higher than the second, compared as dotted version numbers.
version_at_most() {
  [ "$(printf '%s\n%s\n' "$1" "$2" | sort -V | tail -1)" = "$2" ]
}

FAILED=false

MACHINE=$(readelf -h "$LIBRARY" | sed -n 's/^ *Machine: *//p')
echo "Found library is built for for $MACHINE"
if [ "$MACHINE" != "$EXPECTED_MACHINE" ]; then
  echo "FAILED: Expected $EXPECTED_MACHINE for $ARCH" >&2
  FAILED=true
fi

GLIBC=$(max_required_version GLIBC)
if [ -z "$GLIBC" ]; then
  echo "FAILED: Found no GLIBC version requirements"
  FAILED=true
elif ! version_at_most "$GLIBC" "$MAX_GLIBC"; then
  echo "Library requires GLIBC_$GLIBC, limit is GLIBC_$MAX_GLIBC"
  echo "FAILED: Requires a version not present in Amazon Linux 2023."
  FAILED=true
else
  echo "Library requires GLIBC_$GLIBC, limit is GLIBC_$MAX_GLIBC"
fi

GLIBCXX=$(max_required_version GLIBCXX)
if [ -z "$GLIBCXX" ]; then
  echo "FAILED: Found no GLIBCXX version requirements"
  FAILED=true
elif ! version_at_most "$GLIBCXX" "$MAX_GLIBCXX"; then
  echo "Library requires GLIBCXX_$GLIBCXX, limit is GLIBCXX_$MAX_GLIBCXX"
  echo "FAILED: Requires a version not present in Amazon Linux 2023."
  FAILED=true
else
  echo "Library requires GLIBCXX_$GLIBCXX, limit is GLIBCXX_$MAX_GLIBCXX"
fi

if [ "$FAILED" = true ]; then
  exit 1
fi
