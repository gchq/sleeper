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
repository. It can be used like this:

```bash
./scripts/dev/publishDocker.sh my.registry.com/path
```

The first argument is the prefix that will begin each Docker image name. It should include the hostname and any path
that you want to be used before the path component for each image. In this example images will be pushed like
`my.registry.com/path/ingest`, `my.registry.com/path/query-lambda`.

You can pass an optional second argument for whether to create a new Docker builder. By default a Docker builder will be
created that is capable of publishing multiplatform images, like this:

```bash
docker buildx create --name sleeper --use
```

This may not be suitable for all use cases. You can disable this by passing "false" as the second argument. In that
case, you will need to ensure a Docker builder is set that can build multiplatform images before calling this script.

### Installing published artefacts

We have scripts to install Sleeper from published artefacts. We have not yet published Sleeper to Maven Central or
Docker Hub. To install your own artefacts published as in the sections above, you can follow these steps:

1. Prepare a clone of this Git repository.
2. Use `scripts/deploy/installJarsFromMaven.sh` to retrieve the jars from Maven.
3. Use `scripts/deploy/setDeployFromRemoteDocker.sh` to configure the Sleeper scripts to pull published Docker images.
4. Use the Sleeper scripts as though you had built from scratch.

The `installJarsFromMaven.sh` script can be used like this:

```bash
./scripts/deploy/installJarsFromMaven.sh <version> ./scripts/jars -DremoteRepositories=my-repo-id::https://my.repository.com/path
```

Your Maven settings file will need to have your repository declared, with a matching ID and any necessary
authentication. Here's a guide to set this up: https://www.baeldung.com/maven-settings-xml#5-servers

The version must be the Maven version as it was in the Sleeper `java/pom.xml` when it was published to the repository.

The `setDeployFromRemoteDocker.sh` script can be used like this:

```bash
./scripts/deploy/setDeployFromRemoteDocker.sh my.registry.com/path
```

The argument to the script must be the prefix you used to publish the images. This script will create a configuration
file under the templates directory that will adjust the way Docker images are pushed to AWS ECR during deployment, to
pull them from your repository instead of building them locally.
