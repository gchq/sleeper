#!/usr/bin/env bash

docker run -v $HOME/.cache/trivy:/root/.cache -v /var/run/docker.sock/:/var/run/docker.sock aquasec/trivy "$@"
