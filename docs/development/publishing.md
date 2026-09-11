Publishing artefacts
====================

We have scripts to publish built artefacts to shared repositories, and to install them locally to avoid the need to
build Sleeper yourself. We do not currently publish artefacts publicly.

### Publishing Maven artifacts

There is a script [`scripts/dev/publishMaven.sh`](/scripts/dev/publishMaven.sh) to publish the Maven artifacts,
including all modules and the fat jars used to deploy from and run scripts.

This accepts options to pass through to Maven, including `-DaltDeploymentRepository`, documented
against [the Maven plugin](https://maven.apache.org/plugins/maven-deploy-plugin/deploy-mojo.html). If you don't set a
deployment repository it will publish the files to the local file system at `/tmp/sleeper/m2`.

Here's an example of running this script:

```bash
./scripts/dev/publishMaven.sh -DaltDeploymentRepository=my-repo-id::https://my.repository.com/path
```

Your Maven settings file will need to have this repository declared, with a matching ID and any necessary
authentication. Here's a guide to set this up: https://www.baeldung.com/maven-settings-xml#5-servers

This can be tested locally by using a repository url similar to file:/path/to/output that will publish these files to
the local file system.

### Publishing Docker images

There is a script [`scripts/dev/publishDocker.sh`](/scripts/dev/publishDocker.sh) to publish the Docker images to a
registry. It can be used like this:

```bash
./scripts/dev/publishDocker.sh my.registry.com/path
```

The first argument is the prefix that will begin each Docker image name. It should include the hostname and any path
that you want to be used before the path component for each image. In this example images will be pushed like
`my.registry.com/path/ingest`, `my.registry.com/path/query-lambda`.

You can pass an optional second argument for whether to create a new Docker builder. By default a Docker builder will be
created that is capable of publishing multiplatform images, like this:

```bash
docker buildx create --name sleeper-host-network --driver docker-container --driver-opt network=host --use
```

This may not be suitable for all use cases. You can disable this by passing "false" as the second argument. In that
case, you will need to ensure a Docker builder is set that can build multiplatform images before calling this script.

You can also configure options for the build with `scripts/deploy/setDeployConfig.sh`. For options,
see [building in a custom environment](custom-environment.md). If it's set to deploy images from a remote repository,
this publishing will fail.

### Publishing Docker tools images

The [Sleeper Docker tools](../deployment/docker-tools.md) CLI uses its own images, which are not published by
`publishDocker.sh`. The ones we publish are built and pushed to `ghcr.io/gchq` with the tag `latest` by the "Docker CLI
Deployment" GitHub Actions workflow, in [`.github/workflows/docker-cli-main.yaml`](/.github/workflows/docker-cli-main.yaml).

To publish them to your own registry, build them as described
in [the developer guide](../developer-guide.md#sleeper-docker-tools), then tag and push them:

```bash
for IMAGE in sleeper-builder sleeper-local; do
  docker tag "$IMAGE:current" "my.registry.com/path/$IMAGE:latest"
  docker push "my.registry.com/path/$IMAGE:latest"
done
```

The CLI pulls each image as `<registry>/<image name>:<tag>`, so the image names must be kept as they are, and the tag
must be `latest` unless you set the CLI to use the version in a local repository checkout, as described below.

### Configuring the Docker tools CLI to use a custom registry

If you've published Docker tools images to your own registry, as described above, you can point the Sleeper CLI at it
instead of the default Sleeper registry. Pass `--registry` when installing the CLI. You must first log in to that
registry with Docker, otherwise the image pull will fail:

```bash
docker login your.registry.example.com
./scripts/cli/install.sh --registry your.registry.example.com/sleeper
```

This can also be changed later, without reinstalling, using `sleeper cli set-registry <registry>`.

By default, images are pulled with the tag `latest`, regardless of which registry you're using. If you installed from
a local repository checkout, you can instead pull the version of Sleeper currently checked out there, using
`--useLocalVersion`:

```bash
./scripts/cli/install.sh --registry your.registry.example.com/sleeper --useLocalVersion
```

This reads the version from the repository's `pom.xml` each time images are pulled, so switching branches locally
will be picked up automatically, and the pulled images match the version you're working with.

This only works against a registry that publishes images tagged with the version, so you must set a custom registry
alongside it, either with `--registry` as above or from a previous installation. The default Sleeper registry only
publishes the tag `latest`, so the install script will fail immediately if you use `--useLocalVersion` without a
registry, or if you're not installing from a local repository checkout.

Use of the local version can also be toggled later using `sleeper cli set-use-local-version <true|false>`, but requires
the CLI to have been installed from a local repository checkout.

### Installing published artefacts

We have scripts to install Sleeper from published artefacts. We have not yet published Sleeper to Maven Central or
Docker Hub. To install your own artefacts published as in the sections above, you can follow these steps:

1. Prepare a clone of this Git repository.
2. Use `scripts/deploy/installJarsFromMaven.sh` to retrieve the jars from Maven.
3. Use `scripts/deploy/setDeployConfig.sh` to configure the Sleeper scripts to pull published Docker images.
4. Use the Sleeper scripts as though you had built from scratch.

The `installJarsFromMaven.sh` script can be used like this:

```bash
./scripts/deploy/installJarsFromMaven.sh <version> ./scripts/jars -DremoteRepositories=my-repo-id::https://my.repository.com/path
```

Your Maven settings file will need to have your repository declared, with a matching ID and any necessary
authentication. Here's a guide to set this up: https://www.baeldung.com/maven-settings-xml#5-servers

The version must be the Maven version as it was in the Sleeper `java/pom.xml` when it was published to the repository.

The `setDeployConfig.sh` script can be used like this:

```bash
./scripts/deploy/setDeployConfig.sh --image-repository-prefix my.registry.com/path
```

The argument to the script must be the prefix you used to publish the images. This script will create a configuration
file under the templates directory that will adjust the way Docker images are pushed to AWS ECR during deployment, to
pull them from your registry instead of building them locally.

If your Docker registry requires authentication, use the `--help` option on this script for information on how to
authenticate.
