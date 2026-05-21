Sleeper Docker tools
====================

We have a CLI available with some tools that can help you work with Sleeper. For now this just contains commands to run
a couple of Docker containers. In the future we may merge the scripts for interacting with Sleeper into this, but for
now it's more of a toolkit than a CLI to interact with Sleeper.

These tools run a Docker container that contains everything needed to build and deploy Sleeper. They can give you a
command line inside a container with the dependencies pre-installed, or run commands in such a container. This
way you can avoid needing to install any dependencies other than Docker on your machine.

## Dependencies

These tools have the following dependencies, please install these first:

* [Bash](https://www.gnu.org/software/bash/): Minimum v3.2. Use `bash --version`.
* [Docker](https://docs.docker.com/get-docker/)

## Installation

You can run the following commands to install the latest version from GitHub:

```bash
curl "https://raw.githubusercontent.com/gchq/sleeper/develop/scripts/cli/install.sh" -o ./sleeper-install.sh
chmod +x ./sleeper-install.sh
./sleeper-install.sh
```

Relaunch your terminal and check that the command `sleeper version` gives a version number. Note that this will be the
version of the CLI, rather than the version of Sleeper you will deploy.

### Commands

The CLI consists of a `sleeper` command with sub-commands. You can use `sleeper aws` or `sleeper cdk` to run `aws` or
`cdk` commands without needing to install the AWS or CDK CLI on your machine. If you set AWS environment variables or
configuration on the host machine, that will be propagated to the Docker container when you use `sleeper`.

The `sleeper builder` command gives you a command line in a Docker container with all the necessary tools to build
Sleeper, and a workspace folder persisted in the host at `~/.sleeper/builder`. You can use this to deploy and interact
with Sleeper.

You can upgrade to the latest version of the CLI using `sleeper cli upgrade`. This should be done regularly to keep the
build and deployment tools up to date.

There's a `sleeper environment` command that you can use to prepare your AWS account to deploy Sleeper into it. This is
documented in [Sleeper environment tool](environment-tool.md).
