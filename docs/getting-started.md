Getting started
===============

There are 2 ways of deploying Sleeper and interacting with an instance. You can deploy to AWS, or to Docker on your
local machine. The Docker version has limited functionality and will only work with small volumes of data, but will
allow you to deploy an instance, ingest some files, and run reports and scripts against the instance.

In either case, currently you'll need to start by building the system. If you want to deploy locally, you'll need to
build Sleeper locally. If you want to deploy to AWS you'll build Sleeper on a machine in AWS. The Sleeper CLI contains
tools for either of these, built into Docker images. In the future we may publish pre-built artefacts that will make
manual building unnecessary.

## Install Sleeper CLI

### Dependencies

The Sleeper CLI has the following dependencies, please install these first:

* [Bash](https://www.gnu.org/software/bash/): Minimum v3.2. Use `bash --version`.
* [Docker](https://docs.docker.com/get-docker/)

### Install script

You can run the following commands to install the latest version of the CLI from GitHub:

```bash
curl "https://raw.githubusercontent.com/gchq/sleeper/develop/scripts/cli/install.sh" -o ./sleeper-install.sh
chmod +x ./sleeper-install.sh
./sleeper-install.sh
```

The CLI consists of a `sleeper` command with sub-commands. You can use `sleeper aws` or `sleeper cdk` to run `aws` or
`cdk` commands without needing to install the AWS or CDK CLI on your machine. If you set AWS environment variables or
configuration on the host machine, that will be propagated to the Docker container when you use `sleeper`.

You can upgrade to the latest version of the CLI using `sleeper cli upgrade`. This should be done regularly to keep the
build and deployment tools up to date.

## Deploy locally

The quickest way to get an instance of Sleeper is to deploy to LocalStack in Docker on your local machine. Note that the
LocalStack version has very limited functionality in comparison to the AWS version, and can only handle small volumes of
data. See the documentation on [deploying to localstack](deployment/deploy-to-localstack.md) for more information.

## Deploy to AWS

The easiest way to deploy a full instance of Sleeper and interact with it is to use the "system test" functionality.
This deploys a Sleeper instance with a simple schema, and writes some random data into a table in the instance. You can
then use the status scripts to see how much data is in the system, run some example queries, and view logs to help
understand what the system is doing. It is best to do this from an EC2 instance as a significant amount of code needs to
be uploaded to AWS.

### Environment setup

You'll need a VPC that is suitable for deploying Sleeper. You'll also want an EC2 instance to deploy from, to avoid
lengthy uploads of large jar files and Docker images from outside AWS. You can use the Sleeper CLI to create both of
these, see the documentation for the [Sleeper CLI deployment environment](deployment/cli-deployment-environment.md).

If you prefer to use your own EC2, it should run on an x86_64 architecture, with Bash and Docker, and have enough
resources to build code for Maven and Rust. We've tested with 8GB RAM and 2 vCPUs, with `t3.large`. We recommend 4 vCPUs
(`t3.xlarge`), as that takes the build from over 40 minutes with 2 vCPUs, to around 20 minutes for the first build.

If you prefer to use your own VPC, you'll need to ensure it meets Sleeper's requirements. It should ideally have
multiple private subnets in different availability zones. Those subnets should have egress, e.g. via a NAT gateway. The
VPC should have gateway endpoints for S3 and DynamoDB.

The [Sleeper CLI deployment environment](deployment/cli-deployment-environment.md) includes options to deploy an EC2 to
an existing VPC, or a VPC on its own.

Once you've got a suitable VPC, and an EC2 with the Sleeper CLI installed, you should be able to run the manual system
test deployment script from there.

### System test

The easiest way to deploy an instance of Sleeper is to use the scripts for manual system testing. First, set the
environment variable `ID` to be a globally unique string. This is the instance id. It will be used as part of the name
of various AWS resources, such as an S3 bucket, lambdas, etc., and therefore should conform to the naming requirements
of those services. In general stick to lowercase letters, numbers, and hyphens. We use the instance id as part of the
name of all the resources that are deployed. This makes it easy to find the resources that Sleeper has deployed within
each service (go to the service in the AWS console and type the instance id into the search box).

Avoid reusing the same instance id, as log groups from a deleted instance will still be present unless you delete them.
An instance will fail to deploy if it would replace log groups from a deleted instance.

Create an environment variable called `VPC` which is the id of the VPC you want to deploy Sleeper to, and create an
environment variable called `SUBNETS` with the ids of subnets you wish to deploy Sleeper to (note that this is only
relevant to the ephemeral parts of Sleeper - all of the main components use services which naturally span availability
zones). Multiple subnet ids can be specified with commas in between, e.g. `subnet-a,subnet-b`.

The VPC _must_ have an S3 Gateway endpoint associated with it otherwise the `cdk deploy` step will fail.

Before you can run any scripts, you need to build the project. From the root of the Git repository, run:

```bash
./scripts/build/buildForTest.sh
```

Then you can deploy the system test instance by running the following command:

```bash
./scripts/test/deployAll/deployTest.sh ${ID} ${VPC} ${SUBNETS}
```

An S3 bucket will be created for the jars, and ECR repos will be created and Docker images pushed to them.
Note that this script currently needs to be run from an x86_64 machine as we do not yet have cross-architecture Docker
builds. Then CDK will be used to deploy a Sleeper instance. This will take around 20 minutes. Once that is complete,
some tasks are started on an ECS cluster. These tasks generate some random data and write it to Sleeper. 11 ECS tasks
will be created. Each of these will write 40 million records. As all writes to Sleeper are asynchronous, it will take a
while before the data appears (around 8 minutes).

You can watch what the ECS tasks that are writing data are doing by going to the ECS cluster named
sleeper-${ID}-system-test-cluster, finding a task and viewing the logs.

Run the following command to see how many records are currently in the system:

```bash
./scripts/utility/filesStatusReport.sh ${ID} system-test
```

The randomly generated data in the table conforms to the schema given in the file `scripts/templates/schema.template`.
This has a key field called `key` which is of type string. The code that randomly generates the data will generate keys
which are random strings of length 10. To run a query, use:

```bash
./scripts/utility/query.sh ${ID}
```

As the data that went into the table is randomly generated, you will need to query for a range of keys, rather than a
specific key. The above script can be used to run a range query (i.e. a query for all records where the key is in a
certain range) - press 'r' and then enter a minimum and a maximum value for the query. Don't choose too large a range or
you'll end up with a very large amount of data sent to the console (e.g a min of 'aaaaaaaaaa' and a max of
'aaaaazzzzz'). Note that the first query is slower than the others due to the overhead of initialising some libraries.
Also note that this query is executed directly from a Java class. Data is read directly from S3 to wherever the script
is run. It is also possible to execute queries using lambda and have the results written to either S3 or to SQS. The
lambda-based approach allows for a much greater degree of parallelism in the queries. Use `lambdaQuery.sh` instead of
`query.sh` to experiment with this.

Be careful that if you specify SQS as the output, and query for a range containing a large number of records, then a
large number of results could be posted to SQS, and this could result in significant charges.

Over time you will see the number of active files (as reported by the `filesStatusReport.sh` script) decrease. This is
due to compaction tasks merging files together. These are executed in an ECS cluster (named
`sleeper-${ID}-compaction-cluster`).

You will also see the number of leaf partitions increase. This functionality is performed using lambdas called
`sleeper-${ID}-find-partitions-to-split` and `sleeper-${ID}-split-partition`.

To ingest more random data, run:

```bash
java -cp ./scripts/jars/system-test-*-utility.jar  sleeper.systemtest.drivers.ingest.RunWriteRandomDataTaskOnECS ${ID} system-test
```

To tear all the infrastructure down, run

```bash
./scripts/test/tearDown.sh
```

It is possible to run variations on this system-test by editing the system test properties, like this:

```bash
cd ./scripts/test/deployAll
editor system-test-instance.properties
./buildDeployTest.sh  ${ID} ${VPC} ${SUBNETS}
```

To deploy your own instance of Sleeper with a particular schema, follow the [deployment guide](deployment-guide.md).
