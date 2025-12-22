## Development scripts

In the `/scripts/dev` folder are some scripts that can assist you while working on Sleeper:

#### `buildDockerImage.sh`

This will build a single Docker image, and can be used to prepare for execution of the local Docker image tests, e.g.
`QueryLambdaDockerImageST`. You can run it like this:

```bash
./scripts/dev/buildDockerImage.sh query-lambda test
```

The first parameter is the name of the image, as listed in the documentation
of [Docker images](../deployment/docker-images.md). The second parameter is the tag, usually "test" for an automated
Docker image test.

#### `checkNotices.sh`

This will check whether all managed Maven dependencies have been included in the NOTICES file at the root of the
repository.

#### `checkRustStyle.sh`

This runs linting on the Rust code.

#### `checkSpotBugs.sh`

This runs SpotBugs on specified Java modules. This is separated from the other linting as SpotBugs is quite slow.
Here's an example of how to run it on multiple modules (note no spaces between the modules):

```bash
./scripts/dev/checkSpotBugs.sh core,ingest/ingest-core
```

#### `checkStyle.sh`

This runs linting on the Java code, except for SpotBugs as that is quite slow.

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

#### `compileModule.sh`

Recompiles a Maven module and its internal dependencies.

#### `copyRustToJava.sh`

This builds the Rust code and copies the binaries into the Maven project, so that the Rust code should be available
when you run from inside your IDE. This is intended for use when running integration tests that call from Java into
Rust.

#### `generateDocumentation.sh`

This will regenerate the examples and templates for Sleeper configuration properties files. Use this if you've made any
changes to Sleeper configuration properties. This will propagate any changes to property descriptions, ordering,
grouping, etc.

#### `publishDocker.sh`

Publishes Docker images to a remote repository, see [publishing artefacts](publishing.md).

#### `publishMaven.sh`

Publishes Maven artifacts to a remote repository, see [publishing artefacts](publishing.md).

#### `runTest.sh`

Runs a single integration or unit test. Cannot be used with system tests.

#### `showInternalDependencies.sh`

This will display a graph of the dependencies between Sleeper's Maven modules. You can use this to explore how the
modules relate to one another.

#### `trivy.sh`

This will run the Trivy security scanning tool in a Docker container: https://github.com/aquasecurity/trivy

#### `updateVersionNumber.sh`

This is used during the release process to update the version number across the project, see
the [release process guide](release-process.md).

#### `validateProjectChunks.sh`

Checks the configuration of the build for GitHub Actions. This compares `.github/config/chunks.yaml` against the Maven
project and the GitHub Actions workflows under `.github/workflows/chunk-<id>.yaml`.

This is a split build where different Maven modules are built in different GitHub Actions workflows. Each workflow
builds a single "chunk", with a number of modules. The `chunks.yaml` file defines the chunks, and therefore which
modules should be built together in the same workflow.

This script checks that all Maven modules are included in the build. There are also build triggers that need to be hard
coded in the GitHub Actions workflow. The script checks that triggers are set to run each workflow on any changes to
files under its modules, or modules that they depend on.
