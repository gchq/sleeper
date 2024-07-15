#!/usr/bin/env bash

# Tell the builder docker container image to take it's host name from the actual docker container ID, not the assigned host name
# This is a workaround for how cross-rs works when it's running in "docker-in-docker" mode. In DinD mode, this line:
# https://github.com/cross-rs/cross/blob/3bfc6d54c817a2991f610d258f3290906c97474f/src/docker/shared.rs#L1354 causes it to read
# the HOSTNAME environment variable so it can call "docker inspect" to read some mount paths. Docker normally sets a container's
# HOSTNAME to be the ID of the running container, but as this builder image usually runs with --network=host then the container
# receives the host's hostname causing cross-rs to call docker inspect and get an error back. To workaround, we configure this
# builder image to save it's container ID in a specific file which we can now read back in here. See scripts/cli/runInDocker.sh for
# the other part of this workaround.
export HOSTNAME=$(cat /tmp/container.id)

if [ "$#" -lt 1 ]; then
  bash
else
  bash -c "$*"
fi
