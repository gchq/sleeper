#!/usr/bin/env bash
set -ex

if [ "$#" -lt 1 ]; then
  echo "Usage: $0 <dockerRepositoryPrefix>"
  exit 1
fi

SCRIPTS_DIR=$(cd "$(dirname "$0")" && cd .. && pwd)

{
    echo "{"
    echo "  \"dockerImageLocation\": \"repository\","
    echo "  \"dockerRepositoryPrefix\": \"$1\""
    echo "}"
} > "$SCRIPTS_DIR/templates/deployConfig.json"
