Building and deploying Sleeper
==============================

This contains instructions on how to deploy Sleeper.

If you just want a local instance for testing, see the documentation
on [deploying to localstack](deployment/deploy-to-localstack.md). This has very limited functionality compared to a
deployed instance.

## Get your environment set up

You will need to get your environment set up correctly so that you can deploy a Sleeper instance to AWS and then
interact with it. See [environment setup](deployment/environment-setup.md) for how to install the Sleeper CLI and create
an environment suitable for deploying Sleeper.

If you set up the [Sleeper CLI deployment environment](deployment/cli-deployment-environment.md), you can connect to it
and build Sleeper like this:

```bash
sleeper environment connect # Get a shell in the EC2 you deployed
sleeper builder             # Get a shell in a builder Docker container (hosted in the EC2)
cd sleeper                  # Change directory to the root of the Git repository
./scripts/build/build.sh
```

If you used the system test deployment described in the getting started guide, you will have already built Sleeper from
the Git repository in a `sleeper builder` container. If you deploy from outside of AWS this will involve lengthy uploads
of build artefacts, which you can avoid with the environment EC2, or your own EC2 instance. If you deploy from your own
EC2, you will need to check out the Git repository inside a `sleeper builder` container yourself.

The `sleeper builder` command gets you a shell inside a Docker container with all the dependencies required to build and
deploy an instance of Sleeper.  The container will be deleted after you exit. You will start in a directory mounted into
the container from a folder in the host home directory under `~/.sleeper`. This workspace will persist after the
container exits, and will be reused by future calls to `sleeper builder`. It also inherits the AWS and Docker
configuration from the host.

## Deployment

There are two ways to deploy Sleeper: you can use the automated scripts or a more manual approach.

Either approach should be done from within an EC2 instance set up as described above, to avoid lengthy uploads of large
jar files and Docker images.

### Automated Deployment

The automated deployment creates an instance of Sleeper either from your own configuration files, or from templates.
This also pre-populates certain properties for you, e.g. from your AWS configuration, and handles uploading the
necessary deployment artifacts to AWS.

We have planned to improve this by adding support for deploying a published version of Sleeper. We also plan to extend
support for declarative deployment with infrastructure as code, by simplifying the process of versioning an instance
configuration, and by moving some steps into the CDK that are currently done separately. Please see the article
on [potential deployment improvements](development/deployment-improvements.md).

Please ensure Sleeper has been built successfully before using this. This guide assumes you are in a `sleeper builder`
container in an EC2 deployed with `sleeper environment`.

Properties that are set to "changeme" in the templates will be overwritten and should not be set manually during
automated deployment.

You can find the template files [here](../scripts/templates). It is recommended that you change these templates to
configure Sleeper in the way that you want before you run the automated script. At the very least you will want to
change the tags.template file. See the Configuration section below for further details. In that guide, ignore the
properties that are set to "changeme" in the templates as they are overwritten by the automated deployment.

If you deploy from the templates, it will create an instance with no tables:

```bash
cd scripts
editor templates/instanceproperties.template
editor templates/tags.template
./deploy/deployNew.sh <instance-id> <vpc-id> <subnet-ids>
```

Here `vpc-id` and `subnet-ids` are the ids of the VPC and subnets that some components of Sleeper will be deployed into.
Multiple subnet ids can be specified with commas in between, e.g. `subnet-a,subnet-b`.

You can also create your own configuration, including tables, and deploy that:

```bash
cd scripts
mkdir my-instance
cp templates/instanceproperties.template my-instance/instance.properties
cp templates/tags.template my-instance/tags.properties
cp templates/tableproperties.template my-instance/tables/my-table/table.properties
cp templates/schema.template my-instance/tables/my-table/schema.json
# Edit configuration files as above
./deploy/deployNew.sh <instance-id> <vpc-id> <subnet-ids> ./my-instance/instance.properties
```

This script will upload the necessary jars to a bucket in S3 and push the Docker container images to respositories in
ECR.

The deployment scripts will create all of the required configuration files in a folder called `generated` in the scripts
directory.

### Manual Deployment

For Sleeper to be deployed manually, some resources have to be uploaded to AWS first. The jar files need to be uploaded
to an S3 bucket, and some Docker images need to be uploaded to an ECR repository.

#### Upload the Docker images to ECR

There are multiple ECR images that need to be created and pushed to an ECR repo, depending on the stacks you want to
deploy. There's one for ingest, one for compaction and two for bulk import (for EKS and EMR Serverless). You may not
wish to use the bulk import stacks so don't upload the images if you aren't. There's also an image for data generation
for system tests.

Next, create some environment variables for convenience:

```bash
INSTANCE_ID=<insert-a-unique-id-for-the-sleeper-instance-here>
VERSION=$(cat "./scripts/templates/version.txt")
DOCKER_REGISTRY=<insert-your-account-id-here>.dkr.ecr.eu-west-2.amazonaws.com
REPO_PREFIX=${DOCKER_REGISTRY}/${INSTANCE_ID}
DOCKER_BASE_DIR=./scripts/docker
```

Then log in to ECR:

```bash
aws ecr get-login-password --region eu-west-2 | docker login --username AWS --password-stdin ${DOCKER_REGISTRY}
```

Upload the container for ingest:

```bash
TAG=$REPO_PREFIX/compaction-job-execution:$VERSION
aws ecr create-repository --repository-name $INSTANCE_ID/ingest
docker build -t $TAG $DOCKER_BASE_DIR/ingest
docker push $TAG
```

Upload the container for compaction:

```bash
TAG=$REPO_PREFIX/compaction-job-execution:$VERSION
aws ecr create-repository --repository-name $INSTANCE_ID/compaction-job-execution
docker build -t $TAG $DOCKER_BASE_DIR/compaction-job-execution
docker push $TAG
```

If you will be using bulk import on EMR Serverless then upload the container as follows:

```bash
TAG=$REPO_PREFIX/bulk-import-runner-emr-serverless:$VERSION
aws ecr create-repository --repository-name $INSTANCE_ID/bulk-import-runner-emr-serverless
docker build -t $TAG $DOCKER_BASE_DIR/bulk-import-runner-emr-serverless
docker push $TAG
```

If you will be using the experimental bulk import using EKS then upload the container as
follows (note this container will take around 35 minutes to build and it is not needed for bulk
importing data using EMR):

```bash
TAG=$REPO_PREFIX/bulk-import-runner:$VERSION
aws ecr create-repository --repository-name $INSTANCE_ID/bulk-import-runner
docker build -t $TAG $DOCKER_BASE_DIR/bulk-import-runner
docker push $TAG
```

If you will be using the data generation that's used in system tests then upload the container as follows:

```bash
TAG=$REPO_PREFIX/system-test:$VERSION
aws ecr create-repository --repository-name $INSTANCE_ID/system-test
docker build -t $TAG $DOCKER_BASE_DIR/system-test
docker push $TAG
```

#### Building for Graviton

If you'd like to run operations in AWS Graviton-based instances, on the ARM64 architecture, you can use Docker BuildX to
build multiplatform images.

These commands will create or recreate a builder:

```bash
docker buildx rm sleeper || true
docker buildx create --name sleeper --use
```

This command should replace the `docker build` and `docker push` commands documented above:

```bash
docker buildx build --platform linux/amd64,linux/arm64 -t $TAG --push $DOCKER_BASE_DIR/<image directory>
```

#### Upload the jars to a bucket

We need to upload jars to a S3 bucket so that they can be used by various resources. The code below assumes you start
in the project root directory, and you've already built the system with `scripts/build/buildForTest.sh`.

```bash
INSTANCE_ID=<insert-a-unique-id-for-the-sleeper-instance-here>
JARS_BUCKET=sleeper-${INSTANCE_ID}-jars
REGION=<insert-the-AWS-region-you-want-to-use-here>
./scripts/deploy/syncJars.sh $JARS_BUCKET $REGION
```

#### Configuration

Before we can use CDK to deploy Sleeper, we need to create some configuration files:

* An `instance.properties` file - containing information about your Sleeper instance, as well as
  default values used by tables if not specified.
* A `tags.properties` file which lists the tags you want all of your Sleeper infrastructure to be tagged with.
* A `table.properties` file which contains information about a table and a link to its schema file.
* A `schema.json` file which describes the data stored in a Sleeper table.
* A `splits.txt` file which allows you to pre-split partitions in a Sleeper table.

The `.properties` files are Java properties files. You can find descriptions of all properties in the
system [here](usage/property-master.md). Details of this configuration are available
under [Sleeper instance configuration](deployment/instance-configuration.md).

You can start by copying the basic configuration example [here](../example/basic/). There's also an example of a full
configuration [here](../example/full/).

You will need to set sensible values for the following instance properties, which are set for you if you use the
automated deployment script:

* `sleeper.id`
* `sleeper.jars.bucket` - if you followed the steps above for uploading the jars this needs to be set to
  `sleeper-${INSTANCE_ID}-jars`
* `sleeper.account`
* `sleeper.region`
* `sleeper.vpc`
* `sleeper.subnets` - multiple subnet ids can be specified with commas in between, e.g. `subnet-a,subnet-b`.
* `sleeper.retain.infra.after.destroy` - set to false to cause resources such as the S3
  buckets and Dynamo tables to be destroyed after running CDK destroy.

You will also need to ensure your Docker images are in ECR repositories with the correct names. If you followed the
steps above this will already be correct. Each repository must have the expected name appended to a prefix,
e.g. `my-prefix/ingest`, `my-prefix/compaction`. The prefix is the Sleeper instance ID by default, but it can be set
in the instance property `sleeper.ecr.repository.prefix`. The image names are the same as the directory names in
the `scripts/docker` folder that is created when the system is built.

Note, if you do not set the property `sleeper.retain.infra.after.destroy` to false when deploying then however you
choose to tear down Sleeper later on you will also need to destroy some further S3 buckets and DynamoDB tables manually.
This is because by default they are kept.

Please ensure you predefine split points for your table. See [tables](../usage/tables.md#pre-split-partitions) for how
to do this. If you decide not to set split points, your state store will be initialised with a single root partition.
Note that pre-splitting a table is important for any large-scale use of Sleeper, and is essential for running bulk
import jobs.

#### Deploy with the CDK

Now you have your configuration in place and your environment set up, you can deploy your Sleeper instance using AWS
CDK.

```bash
INSTANCE_PROPERTIES=/path/to/instance.properties
VERSION=$(cat "./scripts/templates/version.txt")
cdk -a "java -cp scripts/jars/cdk-${VERSION}.jar sleeper.cdk.SleeperCdkApp" deploy -c propertiesfile=${INSTANCE_PROPERTIES} -c newinstance=true "*"
```

To avoid having to explicitly give approval for deploying all the stacks,
add "--require-approval never" to the command.

If you'd like to include data generation for system tests, use the system test CDK app instead.

```bash
INSTANCE_PROPERTIES=/path/to/instance.properties
VERSION=$(cat "./scripts/templates/version.txt")
cdk -a "java -cp scripts/jars/system-test-${VERSION}-utility.jar sleeper.systemtest.cdk.SystemTestApp" deploy -c propertiesfile=${INSTANCE_PROPERTIES} -c newinstance=true "*"
```

## Scripts to edit an instance

There are clients and scripts in the `scripts/deploy` and `scripts/utility` directories that can be used to adjust an
existing instance.

See the [usage guide](usage-guide.md) for information on how to interact with the instance. The admin client described
there can be used to adjust the configuration of an instance by setting instance properties.

See the [tables documentation](usage/tables.md#addedit-a-table) for how to add/edit Sleeper tables.

### Update Existing Instance

The `deployExisting.sh` script can be used to bring an existing instance up to date. This will upload any jars
that have changed, update all the docker images, and perform a `cdk deploy`.

```bash
./scripts/deploy/deployExisting.sh <instance-id>
```

We are planning to add support to this script for declarative deployment, so that you can set your full instance and
tables configuration in a folder structure and pass it to this script to apply any changes. Currently such changes must
be done with the admin client.

### Pausing and Restarting the System

If there is no ingest in progress, and all compactions have completed, then Sleeper will go to sleep, i.e. the only
significant ongoing charges are for data storage. However, there are several lambda functions that are scheduled to
run periodically using EventBridge rules. These lambda functions look for work to do, such as compactions to run.
The execution of these should have very small cost, but it is best practice to pause the system,
i.e. turn these rules off, if you will not be using it for a while. Note that the system can still be queried when
it is paused.

```bash
# Pause the System
./scripts/utility/pauseSystem.sh ${INSTANCE_ID}

# Restart the System
./scripts/utility/restartSystem.sh ${INSTANCE_ID}
```

### Tear Down

Once your finished with your Sleeper instance, you can delete it, i.e. remove all the resources
associated with it.

Again there are two options regarding teardown, the automatic and the manual options. The automatic option
will only work if you deployed Sleeper automatically and you still have the `generated` folder
in the project root directory. If you do you can simply run:

```bash
./scripts/deploy/tearDown.sh
```

To delete the resources manually use the following commands from the project root directory:

```bash
INSTANCE_PROPERTIES=/path/to/instance.properties
cdk -a "java -cp scripts/jars/cdk-${VERSION}.jar sleeper.cdk.SleeperCdkApp" \
destroy -c propertiesfile=${INSTANCE_PROPERTIES} -c validate=false "*"
```

To delete the jars bucket and all the jars in it:

```bash
aws s3 rb s3://${JARS_BUCKET} --force
```

To delete the ECR repositories use the following where INSTANCE_ID is the instance id of the cluster.

```bash
aws ecr delete-repository --repository-name=${INSTANCE_ID}/ingest --force
aws ecr delete-repository --repository-name=${INSTANCE_ID}/compaction-job-execution --force
aws ecr delete-repository --repository-name=${INSTANCE_ID}/bulk-import-runner --force
```
