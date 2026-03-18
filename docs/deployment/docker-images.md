Docker images deployed in Sleeper
=================================

A deployment of Sleeper includes components that run in Docker containers. This document lists the Docker images that
are used in Sleeper, how to build them, and how to make them available for deployment.

The easiest way to build and deploy these images is with our automated scripts. See
the [deployment guide](../deployment-guide.md) and [deployment with the CDK](./deploy-with-cdk.md) for more information.
The information below may be useful if you prefer to replicate this yourself.

## Docker deployment images

A build of Sleeper outputs several directories under `scripts/docker`. Each is the directory to build a Docker image,
with a Dockerfile. Some of these are used for parts of Sleeper that are always deployed from Docker images, and those
are listed here.

* Deployment name - This is both the name of its directory under `scripts/docker`, and the name of the image when it's
  built and the repository it's uploaded to.
* Optional Stack - They're each associated with an optional stack, and will only be used when that optional stack is
  deployed in an instance of Sleeper.
* Multiplatform - Compaction job execution is built as a multiplatform image, so it can be deployed in both x86 and ARM
  architectures.

| Deployment Name            | Optional Stack     | Multiplatform |
|----------------------------|--------------------|---------------|
| ingest                     | IngestStack        | false         |
| bulk-import-runner         | EksBulkImportStack | false         |
| compaction-job-execution   | CompactionStack    | true          |
| bulk-export-task-execution | BulkExportStack    | false         |
| statestore-committer       |                    | true          |


## Lambda images

Most lambdas are usually deployed from a jar in the jars bucket. Some need to be deployed as a Docker container, as
there's a limit on the size of a jar that can be deployed as a lambda. We also have an option to deploy all lambdas as
Docker containers as well.

All lambda Docker images are built from the Docker build directory that's output during a build of Sleeper
at `scripts/docker/lambda`. To build a Docker image for a lambda, we copy its jar file from `scripts/jars`
to `scripts/docker/lambda/lambda.jar`, and then run the Docker build for that directory. This results in a separate
Docker image for each lambda jar.

* Filename - This is the name of the jar file that's output by the build in `scripts/jars`. It includes the version
  number you've built, which we've included as a placeholder here.
* Image name - This is the name of the Docker image that's built, and the name of the repository it's uploaded to.
* Always Docker deploy - This means that that lambda will always be deployed with Docker, usually because the jar is too
  large to deploy directly.

| Filename                                            | Image Name                        | Always Docker deploy |
|-----------------------------------------------------|-----------------------------------|----------------------|
| athena-`<version-number>`.jar                       | athena-lambda                     | true                 |
| bulk-import-starter-`<version-number>`.jar          | bulk-import-starter-lambda        | false                |
| bulk-export-planner-`<version-number>`.jar          | bulk-export-planner               | false                |
| bulk-export-task-creator-`<version-number>`.jar     | bulk-export-task-creator          | false                |
| ingest-taskrunner-`<version-number>`.jar            | ingest-task-creator-lambda        | false                |
| ingest-batcher-submitter-`<version-number>`.jar     | ingest-batcher-submitter-lambda   | false                |
| ingest-batcher-job-creator-`<version-number>`.jar   | ingest-batcher-job-creator-lambda | false                |
| lambda-garbagecollector-`<version-number>`.jar      | garbage-collector-lambda          | false                |
| lambda-jobSpecCreationLambda-`<version-number>`.jar | compaction-job-creator-lambda     | false                |
| runningjobs-`<version-number>`.jar                  | compaction-task-creator-lambda    | false                |
| lambda-splitter-`<version-number>`.jar              | partition-splitter-lambda         | false                |
| query-`<version-number>`.jar                        | query-lambda                      | true                 |
| cdk-custom-resources-`<version-number>`.jar         | custom-resources-lambda           | false                |
| metrics-`<version-number>`.jar                      | metrics-lambda                    | false                |
| statestore-lambda-`<version-number>`.jar            | statestore-lambda                 | false                |


## Building and pushing

See the [deployment guide](../deployment-guide.md) and [deployment with the CDK](./deploy-with-cdk.md) for information
on available scripts and code to automate building of these images. This is done automatically in any of the deployment
scripts. We'll look at some examples of how to match the behaviour of those scripts.

We'll start by creating some environment variables for convenience:

```bash
INSTANCE_ID=<insert-a-unique-id-for-the-sleeper-instance-here>
ACCOUNT=<your-account-id>
REGION=eu-west-2
DOCKER_REGISTRY=$ACCOUNT.dkr.ecr.$REGION.amazonaws.com
REPO_PREFIX=${DOCKER_REGISTRY}/${INSTANCE_ID}
SCRIPTS_DIR=./scripts # This is from the root of the Sleeper Git repository
VERSION=$(cat "$SCRIPTS_DIR/templates/version.txt")
```

Then log into ECR:

```bash
aws ecr get-login-password --region $REGION | docker login --username AWS --password-stdin $DOCKER_REGISTRY
```

The value of the REPO_PREFIX environment variable could later be used as the value of the instance
property [`sleeper.ecr.repository.prefix`](../usage/properties/instance/user/common.md).

### Docker deployments

Here's an example of commands to build and push a non-multiplatform image from the `scripts/docker` directory:

```bash
TAG=$REPO_PREFIX/ingest:$VERSION
aws ecr create-repository --repository-name $INSTANCE_ID/ingest
docker build -t $TAG $SCRIPTS_DIR/docker/ingest
docker push $TAG
```

### Multiplatform images

For a multiplatform image, e.g. to run on AWS Graviton on the ARM64 architecture, we need a Docker builder suitable for
this.

These commands will create or recreate a builder:

```bash
docker buildx rm sleeper || true
docker buildx create --name sleeper --use
```

This also requires a slightly different command to build and push. This must be done as a single command as the builder
does not automatically add the image to the Docker Engine image store:

```bash
TAG=$REPO_PREFIX/ingest:$VERSION
aws ecr create-repository --repository-name $INSTANCE_ID/compaction-job-execution
docker buildx build --platform linux/amd64,linux/arm64 -t $TAG --push $SCRIPTS_DIR/docker/compaction-job-execution
```

### Lambdas

For a lambda the jar must be copied into the build directory before the build. Provenance must also be disabled for the
image to be supported by AWS Lambda. Here's an example:

```bash
TAG=$REPO_PREFIX/query-lambda:$VERSION
aws ecr create-repository --repository-name $INSTANCE_ID/query-lambda
cp $SCRIPTS_DIR/jars/query-$VERSION.jar $SCRIPTS_DIR/docker/lambda/lambda.jar
docker build --provenance=false -t $TAG $SCRIPTS_DIR/docker/query-lambda
docker push $TAG
```
