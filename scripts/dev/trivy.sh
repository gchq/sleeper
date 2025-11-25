#!/usr/bin/env bash

docker run -v $HOME/.trivy/cache:/root/.cache -v /var/run/docker.sock/:/var/run/docker.sock aquasec/trivy "$@"
