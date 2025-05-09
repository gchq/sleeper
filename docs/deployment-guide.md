Building and deploying Sleeper
==============================

This contains instructions on how to deploy Sleeper.

If you just want a local instance for testing, see the documentation
on [deploying to localstack](deployment/deploy-to-localstack.md). This has very limited functionality compared to a
deployed instance.

## Get your environment set up

You will need to get your environment set up correctly so that you can deploy a Sleeper instance to AWS and then
interact with it. See [getting started](getting-started.md) for how to install the Sleeper CLI. The information below
provides more detail on how to create an environment to deploy Sleeper into, and how to get set up to deploy into AWS.

Currently it's necessary to build Sleeper before any deployment. With the `sleeper environment` setup described in the
getting started guide, you get an EC2 with the Sleeper CLI installed, and the Git repository checked out. Once this is
deployed, you can connect to it and build Sleeper like this:

```bash
sleeper environment connect # Get a shell in the EC2 you deployed
sleeper builder             # Get a shell in a builder Docker container (hosted in the EC2)
cd sleeper                  # Change directory to the root of the Git repository
./scripts/build/build.sh
```

If you used the system test deployment described in the getting started guide, you will have already built Sleeper.

### Sleeper CLI Docker environment

The Sleeper CLI runs commands inside a Docker container. This way you can avoid needing to install anything other than
Docker on your machine.

The `sleeper builder` command gets you a shell inside a container with all the dependencies required to build and deploy
an instance of Sleeper. Note that when you run this inside an environment EC2, the Sleeper Git repository will have been
cloned into the working directory of the container. If you are not using an environment EC2, you will need to manually
clone the repository. If you deploy from outside of AWS this will involve lengthy uploads of build artifacts, which you
can avoid with the environment EC2.

If you have AWS CLI installed, it will use your configuration from the host. Otherwise, any configuration you set in
the container will be persisted in the host home directory. AWS authentication environment variables will be propagated
to the container as well.

The host Docker environment will be propagated to the container via the Docker socket.

The files generated for the Sleeper instance will be persisted in the host home directory under `~/.sleeper`, so that
if you run the Docker container multiple times you will still have details of the last Sleeper instance you worked with.

If you add a command on the end, you can run a specific script like this:

```shell
sleeper builder sleeper/scripts/test/deployAll/deployTest.sh myinstanceid myvpc mysubnet
```

### Configure AWS

When you configure AWS on your machine or in the environment EC2, if you do it outside the Sleeper CLI, the
configuration will be passed on to any Sleeper CLI commands.

The following configuration should allow the SDKs, the CLI and CDK to all access AWS:

~/.aws/credentials:

```ini
[named-profile-123456789]
aws_access_key_id = abcd12345
aws_secret_access_key = defg12345
aws_session_token = hijK12345
```

~/.aws/config

```ini
[profile named-profile-123456789]
region = eu-west-2
```

~/.bashrc:

```bash
export AWS_PROFILE=named-profile-123456789
export AWS_REGION=eu-west-2
```

Also see
the [AWS IAM guide for CLI access](https://docs.aws.amazon.com/singlesignon/latest/userguide/howtogetcredentials.html).

### Bootstrapping CDK

To deploy Sleeper into your AWS account you will need to have bootstrapped CDK in the
account. Bootstrapping installs all the resources that CDK needs to do deployments. Note
that bootstrapping CDK is a one-time action for the account that is nothing to do with
Sleeper itself. See
[this link](https://docs.aws.amazon.com/cdk/latest/guide/bootstrapping.html) for guidance
on how to bootstrap CDK in your account. Note that the `cdk bootstrap` command should
not be run from inside the sleeper directory. You can run `cdk bootstrap` in a Sleeper CLI
Docker container, as described in [getting started](getting-started.md#deployment-environment).

### Lambda Reserved Concurrency

When deploying Sleeper, depending on the stacks you need, it will deploy a few Lambda
functions into your account. Some of these Lambda functions are configured to run
with reserved concurrency of 1. In order to allow this you will need to make
sure you have enough free reserved concurrency in your account.

You will need a reserved account concurrency of at most 6 for all the Sleeper stacks
to be deployed. In order to check how many you have, go to the Lambda section in your
AWS Console and check the dashboard. It should say at the top "full account concurrency = X"
(usually 1000) and "unreserved account concurrency = Y". You can't use the last 100 of your
limit. So if Y is greater than or equal to X-100 you won't be able to deploy Sleeper
and you will have to see if you can adjust your existing lambdas to free some up.

You're now ready to build and deploy Sleeper.

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
* A `table.properties` file which contains information about a table and a link to its schema file.
* A `schema.json` file which describes the data stored in a Sleeper table.
* A `tags.properties` file which lists the tags you want all of your Sleeper infrastructure to be tagged with.

The `instance.properties` and `table.properties` files are Java properties files. You can find descriptions of all
properties in the system [here](usage/property-master.md).

There's an example of a basic instance properties file [here](../example/basic/instance.properties) and an example of a
full instance properties file [here](../example/full/instance.properties). This latter file shows all the instance
properties that you can set. You can use one of these as your starting point. Examples of the other files listed above
can also be found alongside those.

You will need to set sensible values for the following instance properties:

* `sleeper.id`
* `sleeper.jars.bucket` - if you followed the steps above for uploading the jars this needs to be set to
  `sleeper-${INSTANCE_ID}-jars`
* `sleeper.account`
* `sleeper.region`
* `sleeper.vpc`
* `sleeper.subnets` - multiple subnet ids can be specified with commas in between, e.g. `subnet-a,subnet-b`.
* `sleeper.retain.infra.after.destroy` - set to false to cause resources such as the S3
  buckets and Dynamo tables to be destroyed after running CDK destroy.

You will also need to set values for whichever ECR repositories you have uploaded Docker images to. These should be set
to the ECR repository name, eg. `my-instance-id/ingest`.

* `sleeper.ingest.repo`
* `sleeper.compaction.repo`
* `sleeper.bulk.import.emr.serverless.repo`
* `sleeper.bulk.import.eks.repo`
* `sleeper.systemtest.repo`

To include a table in your instance, your `table.properties` file can be in the same folder as
your `instance.properties` file. You can add more than one by creating a `tables` directory in the same folder, with a
subfolder for each table.

See [tables](usage/tables.md) for more information on creating and working with Sleeper tables.

Each table will also need a `schema.json` file next to the `table.properties` file.
See [create a schema](usage/schema.md) for how to create a schema.

You can optionally create a `tags.properties` file next to your `instance.properties`, to apply tags to AWS resources
deployed by Sleeper. An example tags.properties file can be found [here](../example/full/tags.properties).

Here's a full example with two tables:

```
instance.properties
tags.properties
tables/table-1/table.properties
tables/table-1/schema.json
tables/table-2/table.properties
tables/table-2/schema.json
```

Note, if you do not set the property `sleeper.retain.infra.after.destroy` to false
when deploying then however you choose to tear down Sleeper later on
you will also need to destroy some further S3 buckets and DynamoDB tables manually.
This is because by default they are kept.

You may optionally want to predefine your split points for a given table.
You can do this by setting the `sleeper.table.splits.file` property in the
table properties file. There's an example of this in the
[full example](../example/full/table.properties). If you decide not to set
this, your state store will be initialised with a single root partition. Note that
pre-splitting a table is important for any large-scale use of Sleeper, and is essential
for running bulk import jobs.

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

#### Customising the Stacks

By default all the stacks are deployed. However, if you don't need them, you can customise which stacks are deployed.

Mandatory components are the configuration bucket and data bucket, the index of Sleeper tables, the state store,
policies and roles to interact with the instance, and the `TopicStack` which creates an SNS topic used by other stacks
to report errors.

That leaves the following stacks as optional:

* `CompactionStack` - for running compactions (in practice this is essential)
* `GarbageCollectorStack` - for running garbage collection (in practice this is essential)
* `IngestStack` - for ingesting files using the "standard" ingest method
* `PartitionSplittingStack` - for splitting partitions when they get too large
* `QueryStack` - for handling queries via SQS
* `WebSocketQueryStack` - for handling queries via a web socket
* `KeepLambdaWarmStack` - for sending dummy queries to avoid waiting for lambdas to start up during queries
* `EmrServerlessBulkImportStack` - for running bulk import jobs using Spark running on EMR Serverless
* `EmrStudioStack` - to create an EMR Studio containing the EMR Serverless application
* `EmrBulkImportStack` - for running bulk import jobs using Spark running on an EMR cluster that is created on demand
* `PersistentEmrBulkImportStack` - for running bulk import jobs using Spark running on a persistent EMR cluster, i.e. one
  that is always running (and therefore always costing money). By default, this uses EMR's managed scaling to scale up
  and down on demand.
* `IngestBatcherStack` - for gathering files to be ingested or bulk imported in larger jobs
* `TableMetricsStack` - for creating CloudWatch metrics showing statistics such as the number of records in a table over
  time
* `DashboardStack` - to create a CloudWatch dashboard showing recorded metrics
* `BulkExportStack` - to export a whole table as parquet files

The following stacks are optional and experimental:

* `AthenaStack` - for running SQL analytics over the data
* `EksBulkImportStack` - for running bulk import jobs using Spark running on EKS

By default most of the optional stacks are included but to customise it, set the `sleeper.optional.stacks` sleeper
property to a comma separated list of stack names, for example:

```properties
sleeper.optional.stacks=CompactionStack,IngestStack,QueryStack
```

Note that the system test stacks do not need to be specified. They will be included if you use the system test CDK app.

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
