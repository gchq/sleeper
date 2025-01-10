Developer Guide
===============

This is a brief guide to developing Sleeper.

## Get your environment setup

Before you do any dev work on Sleeper it is worth reading the "Get your environment setup" section in
the [deployment guide](02-deployment-guide.md). Once you've built the system, exactly the same will apply here with a
copy that you built yourself.

### Install Prerequisite Software

There are a number of dependencies for building Sleeper, and a few options to set up a development environment with
these available.

#### Dev Containers

The Sleeper Git repository includes configuration for a dev container based on the `sleeper builder` Docker image from
the CLI. This includes all the same dependencies. If your IDE supports Dev Containers, it can work against this Docker
image based on this configuration.

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

If you installed the Sleeper CLI from GitHub as described in the [getting started guide](01-getting-started.md), you can
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

* [AWS CDK](https://docs.aws.amazon.com/cdk/latest/guide/cli.html): Tested with v2.39.1
* [AWS CLI](https://docs.aws.amazon.com/cli/latest/userguide/install-cliv2.html): Tested with v2.7.27
* [Bash](https://www.gnu.org/software/bash/): Tested with v3.2. Use `bash --version`.
* [Docker](https://docs.docker.com/get-docker/): Tested with v20.10.17
* [Java 11/17](https://openjdk.java.net/install/)
* [Maven](https://maven.apache.org/): Tested with v3.8.6
* [NodeJS / NPM](https://github.com/nvm-sh/nvm#installing-and-updating): Tested with NodeJS v16.16.0 and npm v8.11.0
* [Rust](https://rustup.rs/): Tested with Rust v1.77
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

The codebase is structured around the components explained in the [design document](14-design.md). The elements of the
design largely correspond to Maven modules. Core or common modules contain shared model code. Other modules contain
integrations with libraries which are not needed by all components of the system, eg. AWS API clients.

If you'd like to look at how the modules relate to one another in terms of their dependencies, there is a script in
the [development scripts section](#development-scripts) that can display the dependency structure as a graph.

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

### Linting

The Maven project includes Checkstyle and Spotbugs. These are run on every pull request. You can run them locally with
the Maven checkstyle:check and spotbugs:check goals. Your IDE may have plugins available to alert you of violations.

You can run both plugins together:

```bash
cd java
mvn clean compile checkstyle:check spotbugs:check
```

### Javadoc

We try to ensure that all classes have Javadoc. Most methods should also have Javadoc. Private methods, as well as
getters and setters can be skipped unless there's something important to know.

See Oracle's standards for Javadoc:
<https://www.oracle.com/technical-resources/articles/java/javadoc-tool.html>

Note that the first sentence in a Javadoc comment will be used as a summary fragment in generated documentation. This
should not contain any links or formatting, to read normally as an item in a list.

Checkstyle checks for most of our criteria.

A notable omission from the Checkstyle checks is capitalisation of descriptions under tags, eg. parameter tags for
methods. Following the Oracle standards, these should be either a short phrase in all lower case, or a full sentence
with the first word capitalised and a full stop. For example:

```java
/**
 * Processes a foo and a bar.
 *
 * @param foo the foo
 * @param bar This is the bar. It must not be null or an empty string.
 */
public void process(String foo, String bar) {
}
```

### Testing

The Maven project includes unit tests, integration tests and system tests. We use JUnit 5, with AssertJ for assertions.
We also have a setup for manual testing against a deployed instance of Sleeper, documented in
the [system tests guide](15-system-tests.md#manual-testing).

A unit test is any test that runs entirely in-memory without any I/O operations (eg. file system or network calls).
If you configure your IDE to run all unit tests at once, they should finish in less than a minute. The unit of a test
should be a particular behaviour or scenario, rather than eg. a specific method.

A system test is a test that works with a deployed instance of Sleeper. These can be found in the
module `system-test/system-test-suite`. They use the class `SleeperSystemTest` as the entry point to work with an
instance of Sleeper. This is the acceptance test suite we use to define releasability of the system. This is documented
in the [system tests guide](15-system-tests.md#acceptance-tests). If you add a new feature, please add one or two simple
cases to this test suite, as a complement to more detailed unit testing.

An integration test is any test which does not meet the definition of a unit test or a system test. Usually it uses
external dependencies with TestContainers, tests network calls with WireMock, or uses the local file system.

Unit tests should be in a class ending with Test, like MyFeatureTest. Integration tests should be in a class ending with
IT, like MyFeatureIT. Classes named this way will be picked up by Maven's Surefire plugin for unit tests, and Failsafe
for integration tests. System tests should be in a class ending with ST, like CompactionPerformanceST, and must be
tagged with the annotation `SystemTest`. This means they will only be run as part of a system test suite, or directly.
See the [system tests guide](15-system-tests.md#acceptance-tests).

We avoid mocking wherever possible, and prefer to use test fakes, eg. implement an interface to a database with a
wrapper around a HashMap. Use test helper methods to make tests as readable as possible, and as close as possible to a
set of English given/when/then statements.

### Development scripts

In the `/scripts/dev` folder are some scripts that can assist you while working on Sleeper:

#### `showInternalDependencies.sh`

This will display a graph of the dependencies between Sleeper's Maven modules. You can use this to explore how the
modules relate to one another.

#### `generatePropertiesTemplates.sh`

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

## Standalone deployment

See the [deployment guide](02-deployment-guide.md) for notes on how to deploy Sleeper.

## Release Process

See the [release process guide](16-release-process.md) for instructions on how to publish a release of Sleeper.
