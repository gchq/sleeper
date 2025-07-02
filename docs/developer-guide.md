Developer Guide
===============

This is a brief guide to developing Sleeper.

## Get your environment setup

Before you do any dev work on Sleeper it is worth reading the "Get your environment setup" section in
the [deployment guide](deployment-guide.md). Once you've built the system, exactly the same will apply here with a
copy that you built yourself.

### Install prerequisite software

There are a number of dependencies for building Sleeper, and a few options to set up a development environment with
these available.

#### Dev container

The Sleeper Git repository includes configuration for a dev container based on the `sleeper builder` Docker image from
the CLI. This includes all the same dependencies. If your IDE supports Dev Containers, it can work against this Docker
image based on this configuration.

The dev container configuration will mount any AWS CLI, Maven and SSH configuration from your host machine into the
container.

On Mac or Linux this should just work. On Windows it should be launched from inside Windows Subsystem for Linux (WSL).
Working in WSL will also let you use the project's Bash scripts from Windows.

Here are some example steps to set this up in Windows:

1. Install Visual Studio Code in Windows
2. Install Ubuntu from the Windows Store
3. Clone the Git repository in an Ubuntu terminal
4. Run `code` in an Ubuntu terminal, which will install a VS Code server and connect it to Windows
5. In the VS Code window that opened, open the Git repository
6. Click the prompt to open the dev container, or use ctrl+shift+P, Dev Containers: Reopen in Container
7. The dev container will build, open, and install VS Code extensions

#### Nix shell

You can use the [Nix package manager](https://nixos.org/download.html) to get up to date versions of all the
dependencies except Docker and Bash. When you have Nix installed, an easy way to get a development environment is to run
`nix-shell` at the root of the Sleeper Git repository. This will start a shell with all the Sleeper dependencies
installed, without installing them in your system. If you run your IDE from that shell, the dependencies will be
available in your IDE. You can run `nix-shell` again whenever you want to work with Sleeper.

**This has problems working with Python code.** The Nix package for the AWS CLI adds a number of libraries to the
system Python, and pins them to specific versions. It's not possible to override this in a virtual environment, so it's
likely there will be conflicts with the AWS library used in the Python code for Sleeper. This may prevent execution of
the Sleeper Python code. If you change the Python dependencies in the Nix shell, this may break the AWS CLI.

You can also download [shell.nix](/shell.nix) directly if you'd like to avoid installing Git. You can then `git clone`
the repository from the Nix shell. Here's an example to get the latest release:

```bash
curl "https://raw.githubusercontent.com/gchq/sleeper/main/shell.nix" -o ./shell.nix
nix-shell ./shell.nix
git clone https://github.com/gchq/sleeper.git
cd sleeper
git checkout --track origin/main
```

#### Sleeper CLI builder image

If you installed the Sleeper CLI from GitHub as described in the [getting started guide](getting-started.md), you can
use `sleeper builder` to get a shell inside a Docker container with the dependencies pre-installed. This is the same
container image that's used for the Dev Containers setup above. It may be useful if you want to work inside Docker
without using Dev Containers.

If you're in an EC2 deployed with `sleeper environment`, the Sleeper CLI was pre-installed and the repository was
already checked out when you created the EC2. Otherwise, you'll need to clone the repository in the container. You can
use the commands below to do this:

```bash
sleeper builder
git clone https://github.com/gchq/sleeper.git
cd sleeper
```

Everything in the repository will be persisted between executions of `sleeper builder`.

If you have AWS CLI installed in the host, the same configuration will be used in the builder container. Otherwise, any
configuration you set in the container will be persisted in the host home directory. AWS authentication environment
variables will be propagated to the container as well.

The host Docker environment will be propagated to the container via the Docker socket.

The files generated for the Sleeper instance will be persisted in the host home directory under `~/.sleeper`, so that
if you run the Docker container multiple times you will still have details of the last Sleeper instance you worked with.

If you add a command on the end, you can run a specific script like this:

```shell
sleeper builder sleeper/scripts/test/deployAll/deployTest.sh myinstanceid myvpc mysubnet
```

#### Manual dependency installation

You will need the following software:

* [AWS CDK](https://docs.aws.amazon.com/cdk/latest/guide/cli.html)
* [AWS CLI](https://docs.aws.amazon.com/cli/latest/userguide/install-cliv2.html)
* [Bash](https://www.gnu.org/software/bash/): Minimum v3.2. Use `bash --version`.
* [Docker](https://docs.docker.com/get-docker/)
* [Java](https://openjdk.java.net/install/): Requires version 17, to match the version used by AWS EMR
* [Maven](https://maven.apache.org/)
* [NodeJS / NPM](https://github.com/nvm-sh/nvm#installing-and-updating)
* [Rust](https://rustup.rs/)
* [Cross-rs](https://github.com/cross-rs/cross)

## Building

Provided script (recommended) - this builds the code and copies the jars into the scripts directory so that the scripts
work. Starting from the root of the Git repository:

```bash
./scripts/build/buildForTest.sh
```

### Sleeper CLI

To build the Sleeper CLI, you can run this script:

```bash
./scripts/cli/buildAll.sh
```

Use `./scripts/cli/runInDocker.sh` to run the built CLI. This will act the same as running the `sleeper`
command after installing the CLI. You can manually install it if you copy that script somewhere, rename it to `sleeper`,
and put it on the system path. Then `sleeper ...` commands will work as though you'd installed it normally.

If you have the CLI installed already it will be replaced with the version that is built. If the `runInDocker.sh` script
is different in the version you installed before, it will not be replaced. You can find it
at `$HOME/.local/bin/sleeper`, and manually overwrite it with the contents of `./scripts/cli/runInDocker.sh`.

### Java

To build the Java code only, without installing it for the scripts:

```bash
cd java
mvn clean install -Pquick
```

Removing the '-Pquick' option will cause the unit and integration tests to run.

### Disabling Rust component

You can disable the building of the Rust modules with:

```bash
cd java
mvn clean install -Pquick -DskipRust=true
```

## Using the codebase

The codebase is structured around the components explained in the [design document](design.md). The elements of the
design largely correspond to Maven modules. We'll look at the module architecture in more detail below.

If you'd like to look at how the modules relate to one another in terms of their dependencies, there is a script in
the [development scripts section](#development-scripts) that can display the dependency structure as a graph. There's
also a document with information on past and current [dependency conflicts](development/dependency-conflicts.md).

If you'd like to raise or pick up an open issue, see the [contributing guide](/CONTRIBUTING.md) for more information.

### IDE setup

Configuration is available for various development environments.

For VS Code there's [a separate setup guide](/.vscode/README.md).

For IntelliJ, these settings are available to import:

* Code style scheme at [code-style/intellij-style.xml](/code-style/intellij-style.xml)
* Inspection profile at [code-style/intellij-inspection-profile.xml](/code-style/intellij-inspection-profile.xml)
* Copyright profile for license header
  at [code-style/intellij-copyright-profile.xml](/code-style/intellij-copyright-profile.xml)
* Checkstyle plugin settings in [code-style/checkstyle-idea](/code-style/checkstyle-idea)

For Eclipse, these settings are available to import:

* Code style at [code-style/eclipse-style.xml](/code-style/eclipse-style.xml)
* Import order at [code-style/eclipse-import-order.importorder](/code-style/eclipse-import-order.importorder)
* License header at [code-style/licenseHeader.txt](/code-style/licenseHeader.txt)
* Code templates at [code-style/eclipse-codetemplates.xml](/code-style/eclipse-codetemplates.xml)
* Editor templates at [code-style/eclipse-templates.xml](/code-style/eclipse-templates.xml)

### Maven module architecture

Most Maven modules map to features of Sleeper, and we also have "core" modules, "common" modules, and some other modules
to do with the build, deployment with the CDK, and system tests.

The "core" modules make up the main application code independent of infrastructure or external dependencies. These are
the module `core`, and other modules with "core" in the name nested against specific features. These core modules
represent the "application" part of a ports and adapters, or hexagonal architecture. They do not contain external
dependencies such as the AWS SDK, Parquet or other client libraries. They do contain dependencies for logging, and some
utilities for serialisation/deserialisation.

The `core` module contains shared code for things like configuring a Sleeper instance, interacting with the state of a
Sleeper table, and some common logic to track operations for reporting. Each of these have adapters that connect these
things to AWS, but the adapter code sits in other, non-core modules that are specific to those features.

The other modules with "core" in the name involve core application code that we felt was more peripheral to the system.
For example, the specifics of how we model a compaction job is in the module `compaction-core`, but the high level
tracking of compaction, and the interactions of compaction with the state of a Sleeper table, are both part of the
`core` module. The `compaction-core` module sits alongside other non-core modules that contain the adapter code that
links the application code for compaction to AWS.

The "common" modules are shared utilities for interacting with external dependencies in common ways. They sit under the
directory `java/common`. This includes tools for working with DynamoDB, testing code using AWS clients against
LocalStack, and some infrastructure code to do with invoking lambdas, running jobs in a task, and starting tasks that
will run jobs. Here a job is some process that needs to run, and a task is a piece of infrastructure that can run jobs,
e.g. an AWS ECS task.

### Linting

The Maven project includes Checkstyle and Spotbugs. These are run on every pull request. You can run them locally with
the Maven checkstyle:check and spotbugs:check goals. Your IDE may have plugins available to alert you of violations.

You can run both plugins together:

```bash
cd java
mvn clean compile checkstyle:check spotbugs:check
```

### Testing

See the [test strategy](development/test-strategy.md) for how and when to write tests, as well as information on the
testing tools used in the project.

### Coding conventions

See the [coding conventions document](development/conventions.md) for practices we try to adhere to when working on
Sleeper.

## Standalone deployment

See the [deployment guide](deployment-guide.md) for notes on how to deploy Sleeper, and
the [system test guide](development/system-tests.md) to deploy instances specifically set up for development.

## Release process

See the [release process guide](development/release-process.md) for instructions on how to publish a release of Sleeper.

## Development scripts

In the `/scripts/dev` folder are some scripts that can assist you while working on Sleeper:

#### `showInternalDependencies.sh`

This will display a graph of the dependencies between Sleeper's Maven modules. You can use this to explore how the
modules relate to one another.

#### `generateDocumentation.sh`

This will regenerate the examples and templates for Sleeper configuration properties files. Use this if you've made any
changes to Sleeper configuration properties. This will propagate any changes to property descriptions, ordering,
grouping, etc.

#### `cleanupLogGroups.sh`

When deploying multiple instances (or running multiple system tests), many log groups will be generated. This can make
it difficult to find the logs you need to view. This script will delete any log groups that meet all of the following
criteria:

* Its name does not contain the name of any deployed CloudFormation stack
* Either it's empty, or it has no retention period and is older than 30 days

This can be used to limit the number of log groups in your AWS account, particularly if all your log groups are
deployed by the CDK or CloudFormation, with the stack name in the log group name.

Note that this will not delete log groups for recently deleted instances of Sleeper, so you will still need a different
instance ID when deploying a new instance to avoid naming collisions with existing log groups.

#### `updateVersionNumber.sh`

This is used during the release process to update the version number across the project (see below).
