Publishing the Docker tools CLI
===============================

The [Sleeper Docker tools](../deployment/docker-tools.md) CLI uses its own images, which are not published by the
scripts described in [publishing artefacts](publishing.md). This guide covers publishing those images to your own
registry, and pointing the CLI at it.

### Publishing Docker tools images

The images we publish are built and pushed to `ghcr.io/gchq` with the tag `latest` by the "Docker CLI Deployment"
GitHub Actions workflow, in [`.github/workflows/docker-cli-main.yaml`](/.github/workflows/docker-cli-main.yaml).

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

By default, images are pulled with the tag `latest`, regardless of which registry you're using. If your pipeline
publishes images tagged with the version, you can instead pull the version of Sleeper in a local repository checkout.
This needs two options, `--useLocalRepo` to tie the installation to the checkout you're running the install script
from, and `--useLocalVersion` to read the version from there:

```bash
./scripts/cli/install.sh --registry your.registry.example.com/sleeper --useLocalRepo --useLocalVersion
```

This reads the version from the repository's `pom.xml` each time images are pulled, so switching branches locally
will be picked up automatically, and the pulled images match the version you're working with. Note that the version
covers a whole release line, so images tagged with it will not necessarily have been built from the commit you have
checked out.

The install script will fail immediately if these options can't be satisfied, either because `--useLocalVersion` was
used without `--useLocalRepo`, because you're not running the script from a local repository checkout, or because no
custom registry is set. The default Sleeper registry only publishes the tag `latest`, so a version tag would never be
found there.

Use of the local version can also be toggled later using `sleeper cli set-use-local-version <true|false>`, which needs
the CLI to have been installed with `--useLocalRepo`.

### Installing the Docker tools CLI from a local repository

`--useLocalRepo` also changes where the CLI itself comes from. Normally the install script downloads the `sleeper`
command from GitHub, and `sleeper cli upgrade` downloads it again from the `develop` branch each time you upgrade.
With `--useLocalRepo`, the command is taken from `scripts/cli/runInDocker.sh` in the checkout you installed from, and
upgrades are taken from there too, along with the CLI runner Dockerfile.

This is what you want when you're developing the CLI itself, or installing from a checkout that matches what your
pipeline publishes. It does mean upgrades no longer track GitHub, so the CLI only changes when that checkout does. To
make that visible, `sleeper cli upgrade` reports the repository it's updating from and the branch and commit it has
checked out, and pulling images reports the registry and tag being used. To go back to tracking GitHub, install again
without `--useLocalRepo`.
