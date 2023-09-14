Developer Guide
===============

This is a brief guide to developing Sleeper.

## Get your environment setup

Before you do any dev work on Sleeper it is worth reading the "Get your environment setup" section in
the [deployment guide](02-deployment-guide.md) as exactly the same will apply here, especially for running the system
tests.

### Install Prerequisite Software

You will need the following software:

* [AWS CDK](https://docs.aws.amazon.com/cdk/latest/guide/cli.html): Tested with v2.39.1
* [AWS CLI](https://docs.aws.amazon.com/cli/latest/userguide/install-cliv2.html): Tested with v2.7.27
* [Bash](https://www.gnu.org/software/bash/): Tested with v3.2. Use `bash --version`.
* [Docker](https://docs.docker.com/get-docker/): Tested with v20.10.17
* [Java 11/17](https://openjdk.java.net/install/)
* [Maven](https://maven.apache.org/): Tested with v3.8.6
* [NodeJS / NPM](https://github.com/nvm-sh/nvm#installing-and-updating): Tested with NodeJS v16.16.0 and npm v8.11.0

You can use the [Nix package manager](https://nixos.org/download.html) to get up to date versions of all of these. When
you have Nix installed, an easy way to get a development environment is to run `nix-shell` at the root of the Sleeper
Git repository. This will start a shell with all the Sleeper dependencies installed, without installing them in your
system. If you run your IDE from that shell, the dependencies will be available in your IDE. You can run `nix-shell`
again whenever you want to work with Sleeper.

You can also download [shell.nix](/shell.nix) directly and run `nix-shell shell.nix` if you'd like to get a shell
without running Git. You can then `git clone` the repository from there.

If you're working with the Sleeper CLI, you can use `sleeper builder` to get a shell inside a Docker container with
the dependencies pre-installed. You'll need to clone the Git repository, and this will be persisted between executions
of `sleeper builder`. Use the commands below:

```bash
sleeper builder
git clone https://github.com/gchq/sleeper.git
cd sleeper
```

## Building

Provided script (recommended) - this builds the code and copies the jars
into the scripts directory so that the scripts work.

```bash
./scripts/build/buildForTest.sh
```

Maven (removing the '-Pquick' option will cause the unit and integration tests
to run):

```bash
cd java
mvn clean install -Pquick
```

## Using the codebase

The codebase is structured around the components explained in the [design document](10-design.md). The elements of the
design largely correspond to Maven modules. Core or common modules contain shared model code. Other modules contain
integrations with libraries which are not needed by all components of the system, eg. AWS API clients.

If you'd like to look at how the modules relate to one another in terms of their dependencies, there is a script in
the [development scripts section](#development-scripts) that can display the dependency structure as a graph.

If you'd like to raise or pick up an open issue, see the [contributing guide](/CONTRIBUTING.md) for more information.

### IDE setup

Configuration is available for various development environments.

For VS Code there's [a separate setup guide](/.vscode/README.md).

For IntelliJ, these settings are available to import:

- Code style scheme at [code-style/intellij-style.xml](/code-style/intellij-style.xml)
- Copyright profile for license header
  at [code-style/intellij-copyright-profile.xml](/code-style/intellij-copyright-profile.xml)
- Checkstyle plugin settings in [code-style/checkstyle-idea](/code-style/checkstyle-idea)

For Eclipse, these settings are available to import:

- Code style at [code-style/eclipse-style.xml](/code-style/eclipse-style.xml)
- Import order at [code-style/eclipse-import-order.importorder](/code-style/eclipse-import-order.importorder)
- License header at [code-style/licenseHeader.txt](/code-style/licenseHeader.txt)

### Linting

The Maven project includes Checkstyle and Spotbugs. These are run on every pull request. You can run them locally with
the Maven checkstyle:check and spotbugs:check goals. Your IDE may have plugins available to alert you of violations.

You can run both plugins together:

```bash
cd java
mvn clean compile checkstyle:check spotbugs:check
```

### Testing

The Maven project includes unit tests, integration tests and system tests. We use JUnit 5, with AssertJ for assertions.
We also have a setup for manual testing against a deployed instance of Sleeper, documented
in [12-system-tests.md](12-system-tests.md#manual-testing).

A unit test is any test that runs entirely in-memory without any I/O operations (eg. file system or network calls).
If you configure your IDE to run all unit tests at once, they should finish in less than a minute. The unit of a test
should be a particular behaviour or scenario, rather than eg. a specific method.

A system test is a test that works with a deployed instance of Sleeper. These can be found in the
module `system-test/system-test-suite`. They use the class `SleeperSystemTest` as the entry point to work with an
instance of Sleeper. This is the acceptance test suite we use to define releasability of the system. This is documented
in [12-system-tests.md](12-system-tests.md#acceptance-tests). If you add a new feature, please add one or two simple
cases to this test suite, as a complement to more detailed unit testing.

An integration test is any test which does not meet the definition of a unit test or a system test. Usually it uses
external dependencies with TestContainers, tests network calls with WireMock, or uses the local file system.

Unit tests should be in a class ending with Test, like MyFeatureTest. Integration tests should be in a class ending with
IT, like MyFeatureIT. Classes named this way will be picked up by Maven's Surefire plugin for unit tests, and Failsafe
for integration tests. System tests should follow the same naming as integration tests, but should be annotated with
`@Tag("SystemTest")`. This means they will only be run as part of a specific system test setup.

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

- Its name does not contain the name of any deployed CloudFormation stack
- Either it's empty, or it has no retention period and is older than 30 days

This can be used to limit the number of log groups in your AWS account, particularly if all your log groups are
deployed by the CDK or CloudFormation, with the stack name in the log group name.

Note that this will not delete log groups for recently deleted instances of Sleeper, so you will still need a different
instance ID when deploying a new instance to avoid naming collisions with existing log groups.

#### `updateVersionNumber.sh`

This is used during the release process to update the version number across the project (see below).

## Standalone deployment

See the [deployment guide](02-deployment-guide.md) for notes on how to deploy Sleeper.

## Release Process

See the [release process guide](16-release-process.md) for instructions on how to publish a release of Sleeper
