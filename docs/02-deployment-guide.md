Building and deploying Sleeper
==============================

This contains instructions on how to build and deploy Sleeper.

## Get your environment set up
You will need to get your environment set up correctly so that you can deploy a Sleeper instance
to AWS and then interact with it.

### Install Prerequisite Software 
You will need the following software:
* [AWS CLI](https://docs.aws.amazon.com/cli/latest/userguide/install-cliv2.html)
* [NodeJS / Npm](https://github.com/nvm-sh/nvm#installing-and-updating)
* [AWS CDK](https://docs.aws.amazon.com/cdk/latest/guide/cli.html)
* [Java 8](https://openjdk.java.net/install/)
* [maven](https://maven.apache.org/)

### Configure AWS
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

### Bootstrapping CDK
To deploy Sleeper into your AWS account you will need to have bootstrapped CDK in the
account. Bootstrapping installs all the resources that CDK needs to do deployments. Note
that bootstrapping CDK is a one-time action for the account that is nothing to do with
Sleeper itself. See
[this link](https://docs.aws.amazon.com/cdk/latest/guide/bootstrapping.html) for guidance
on how to bootstrap CDK in your account. Note that the `cdk bootstrap` command should
not be run from inside the sleeper directory.

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

### Deployment environment

See [getting started](01-getting-started.md#deployment-environment) for information on setting up a VPC and EC2 instance
to deploy Sleeper. You may want to follow the remaining instructions from within the EC2 instance.

## Building
The first step is to build Sleeper. We have a section in our 
[developer guide](./09-dev-guide.md#Building) for how to do that but the easiest way is:

```bash
./scripts/build/build.sh
```

## Deployment
There are two ways to deploy Sleeper: you can use the automated scripts or a more manual
approach.

### Automated Deployment
The automated script uses template files to provide a default 
configuration for Sleeper. It also deploys only one table
into Sleeper with the schema provided in these template files. 
You can find the template files [here](../scripts/templates).

It is recommended that you change these templates to configure Sleeper
in the way that you want before you run the automated script. At
the very least you will want to change the schema.template and 
tags.template files. See the Configuration section below
for further details. 

Note that anything property in the templates with "changeme"
as its value will be overwritten automatically by the scripts.

You can then use the automated script by running the following command 
from the root directory:

```bash
./scripts/deploy/buildAndDeploy.sh <sleeper-instance-unique-id> <vpc-id> <subnet-id> <table-name>
```
Here vpc-id and subnet-id are the ids of the VPC and subnet that some components of Sleeper
will be deployed into.

This script will build Sleeper and upload the necessary jars to a bucket in S3
and push the Docker container images to respositories in ECR.

The deployment scripts will create all of the required configuration files in a 
folder called "generated" under the root directory. It is recommended you keep
this folder somewhere safe.

### Manual Deployment
For Sleeper to be deployed manually, some resources have to be uploaded to AWS first:
the jar files need to be uploaded to an S3 bucket, and some Docker images
need to be uploaded to an ECR repository.

#### Upload the Docker images to ECR
There are potentially three ECR images that need to be created and pushed
to an ECR repo, depending on the stacks you want to deploy. One is for 
ingest, one is for compaction and the last is for bulk import. You may 
not wish to use the bulk import stack so don't upload the image if you 
aren't.

The below assumes you start in the project root directory. First create some
environment variables for convenience:

```bash
cd java
INSTANCE_ID=<insert-a-unique-id-for-the-sleeper-instance-here>
VERSION=$(mvn -q -DforceStdout help:evaluate -Dexpression=project.version)
DOCKER_REGISTRY=<insert-your-account-id-here>.dkr.ecr.eu-west-2.amazonaws.com
REPO_PREFIX=${DOCKER_REGISTRY}/${INSTANCE_ID}
```

Then log in to ECR:

```bash
aws ecr get-login-password --region eu-west-2 | docker login --username AWS --password-stdin ${DOCKER_REGISTRY}
```

Upload the container for ingest:

```bash
aws ecr create-repository --repository-name ${INSTANCE_ID}/ingest
cp ingest/target/ingest-${VERSION}-utility.jar ingest/docker/ingest.jar
docker build -t ${REPO_PREFIX}/ingest:${VERSION} ./ingest/docker
docker push ${REPO_PREFIX}/ingest:${VERSION}
```

Upload the container for compaction:

```bash
aws ecr create-repository --repository-name ${INSTANCE_ID}/compaction-job-execution
cp compaction-job-execution/target/compaction-job-execution-${VERSION}-utility.jar compaction-job-execution/docker/compaction-job-execution.jar
docker build -t ${REPO_PREFIX}/compaction-job-execution:${VERSION} ./compaction-job-execution/docker
docker push ${REPO_PREFIX}/compaction-job-execution:${VERSION}
```

If you will be using the experimental bulk import using EKS then upload the container as
follows (note this container will take around 35 minutesto build and it is not needed for bulk
importing data using EMR):

```bash
aws ecr create-repository --repository-name ${INSTANCE_ID}/bulk-import-runner
cp bulk-import/bulk-import-runner/target/bulk-import-runner-${VERSION}-utility.jar bulk-import/bulk-import-runner/docker/bulk-import-runner.jar
docker build -t ${REPO_PREFIX}/bulk-import-runner:${VERSION} ./bulk-import/bulk-import-runner/docker
docker push ${REPO_PREFIX}/bulk-import-runner:${VERSION}

cd ..
```

#### Upload the jars to a bucket
We need to upload jars to a S3 bucket so that they can be used by various resources. The code
below assumes you start in the project root directory.

```bash
cd java
INSTANCE_ID=<insert-a-unique-id-for-the-sleeper-instance-here>
VERSION=$(mvn -q -DforceStdout help:evaluate -Dexpression=project.version)
SLEEPER_JARS=sleeper-${INSTANCE_ID}-jars
REGION=<insert-the-AWS-region-you-want-to-use-here>

aws s3api create-bucket --acl private --bucket ${SLEEPER_JARS} --region ${REGION} --create-bucket-configuration LocationConstraint=${REGION}

rm -rf temp-jars
mkdir -p temp-jars
cp distribution/target/distribution-${VERSION}-bin/scripts/jars/* temp-jars/
aws s3 sync --size-only temp-jars s3://${SLEEPER_JARS}
rm -rf temp-jars
cd ..
```

#### Configuration

Before we can use CDK to deploy Sleeper, we need to create some configuration files:

* An instance.properties file - containing information about your Sleeper instance, as well as
default values used by tables if not specified.
* A table.properties file which contains information about a table and a link to its schema file.
* A schema.json file which describes the data stored in a Sleeper table.
* A tags.properties file which lists the tags you want all of your Sleeper infrastructure to be tagged with.

There's an example of a basic instance properties file [here](../example/basic/instance.properties)
and an example of a full instance properties file [here](../example/full/instance.properties).
This latter file shows all the instance properties that you can set. Whichever of these two
files you use as your starting point, you will need to set sensible values for the following
properties:

* sleeper.id
* sleeper.table.properties - see below
* sleeper.jars.bucket - if you followed the steps above for uploading the jars this needs to be set to
sleeper-${INSTANCE_ID}-jars
* sleeper.tags.file - see below
* sleeper.account
* sleeper.region
* sleeper.version - set this to the value of ${VERSION}
* sleeper.vpc
* sleeper.subnet
* sleeper.retain.infra.after.destroy - set to false to cause resources such as the S3
buckets and Dynamo tables to be destroyed after running CDK destroy.

To include a table in your instance you need to set the `sleeper.table.properties`
property in your instance properties file to point to a table propertes file.
You can add more than one, separating each one with a comma, for example:
```properties
sleeper.table.properties=/path/to/first.properties,/path/to/second.properties
```
Alternatively if you have all your table properties in one directory, you
can just specify the directory. This is handy if you have more than one or two
tables. If you want, you can specify more than one directory. Be sure that all
the files in these directories are table properties.
```properties
sleeper.table.properties=/sleeper/config/tables
```
Note, if you do not set the property "sleeper.retain.infra.after.destroy" to false
when deploying then however you choose to tear down Sleeper later on
you will also need to destroy some further S3 buckets and DynamoDB tables manually.
This is because by default they are kept.

The table properties file needs to contain a link to a file containing the schema
for that table. See [create a schema](03-schema.md) for how to create a schema.
Once you've got your schema, add a link to it in the table properties file under
`sleeper.schema.file`.

You may optionally want to predefine your split points for a given table.
You can do this by setting the `sleeper.table.splits.file` property in the
table properties file. There's an example of this in the 
[full example](../example/full/table.properties). If you decide not to set
this, your statestore will be initialised with a single root partition.

To make sure you tag the Sleeper infrastructure correctly you will need set
the following property in your instance.properties. An example tags.properties file
can be found [here](../example/full/tags.properties).

#### Deploy with the CDK
Now you have your configuration in place and your environment set
up, you can deploy your Sleeper instance using AWS CDK.

```bash
INSTANCE_PROPERTIES=/path/to/instance.properties
cd java
VERSION=$(mvn -q -DforceStdout help:evaluate -Dexpression=project.version)
cd ..
cdk -a "java -cp scripts/jars/cdk-${VERSION}.jar sleeper.cdk.SleeperCdkApp" deploy -c propertiesfile=${INSTANCE_PROPERTIES} -c validate=true "*"
```

To avoid having to explicitly give approval for deploying all the stacks,
add "--require-approval never" to the command.

#### Customising the Stacks
By default all the stacks are deployed. However, if you don't 
need them, you can customise which stacks are deployed. 

The mandatory ones are the TableStack which deploys the statestore and 
data bucket for each table you specify, the TopicStack which creates an 
SNS topic used by other stacks to send errors and finally the
ConfigurationStack and PropertiesStack which writes the instance properties 
to the configuration bucket.

That leaves the following stacks as optional:
* CompactionStack - for running compactions
* GarbageCollectorStack - for running garbage collection
* IngestStack - for ingesting files using the "standard" ingest method
* PartitionSplittingStack - for splitting partitions when they get too large
* QueryStack - for handling queries
* EmrBulkImportStack - for running BulkImport jobs using Spark running on an EMR cluster that is created on demand
* PersistentEmrBulkImportStack - for running BulkImport jobs using Spark running on a persistent EMR cluster, i.e. one that
is always running (and therefore always costing money). By default, this uses EMR's managed scaling to scale up and down on
demand.

The following stacks are optional and experimental:
* AthenaStack - for running SQL analytics over the data
* EksBulkImportStack - for running bulk import jobs using Spark running on EKS

By default all the optional stacks are included but to customise
it, set the `sleeper.optional.stacks` sleeper property to a
comma separated list of stack names, for example:
```properties
sleeper.optional.stacks=CompactionStack,IngestStack,QueryStack
```

## Tear Down
Once your finished with your Sleeper instance, you can delete it, i.e. remove all the resources
associated with it. 

Again there are two options regarding teardown, the automatic and the manual options. The automatic option
will only work if you deployed Sleeper automatically and you still have the "generated" folder
in the project root directory. If you do you can simply run:

```bash
./scripts/deploy/teardown.sh
```

To delete the resources manually use the following commands from the project root directory:

```bash
INSTANCE_PROPERTIES=/path/to/instance.properties
cdk -a "java -cp scripts/jars/cdk-${VERSION}.jar sleeper.cdk.SleeperCdkApp" \
destroy -c propertiesfile=${INSTANCE_PROPERTIES} "*"
```

To delete the jars bucket and all the jars in it:

```bash
aws s3 rb s3://${SLEEPER_JARS} --force
```

To delete the ECR repositories use the following where INSTANCE_ID is the instance id of the cluster.

```bash
aws ecr delete-repository --repository-name=${INSTANCE_ID}/ingest --force
aws ecr delete-repository --repository-name=${INSTANCE_ID}/compaction-job-execution --force
aws ecr delete-repository --repository-name=${INSTANCE_ID}/bulk-import-runner --force
```
