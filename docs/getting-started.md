Getting started
===============

There are 2 ways of deploying Sleeper and interacting with an instance. You can deploy to AWS, or to Docker on your
local machine. The Docker version has limited functionality and will only work with small volumes of data, but will
allow you to deploy an instance, ingest some files, and run reports and scripts against the instance.

## Sleeper in LocalStack

The quickest way to get an instance of Sleeper is to deploy to LocalStack in Docker on your local machine. Note that the
LocalStack version has very limited functionality in comparison to the AWS version, and can only handle small volumes of
data. See the documentation on [deploying to localstack](deployment/deploy-to-localstack.md) for more information.

The rest of this guide will deal with Sleeper in AWS.

## Sleeper in AWS

This Git repository contains scripts that let you build and/or deploy Sleeper with a single command, to minimise setup.
The Sleeper CLI lets you run these scripts in a Docker container, with only Docker as a pre-installed dependency.

It's currently necessary to build the system yourself to deploy or interact with Sleeper. In the future we may publish
pre-built artefacts that will make this unnecessary.

## Install Sleeper CLI

The Sleeper CLI runs a Docker container that contains all the necessary tools to build and deploy Sleeper. This can give
you a command line inside a container with these tools pre-installed, or run commands in such a container. This way you
can avoid needing to install any dependencies other than Docker on your machine.

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

## Environment setup

If you already have an instance of Sleeper, please see the [usage guide](usage-guide.md) for how to interact with it.
If you're deploying your own, you'll need a VPC that is suitable for deploying Sleeper. You'll also want an EC2 instance
to deploy from, to avoid lengthy uploads of large jar files and Docker images from outside AWS. You can use the Sleeper
CLI to create both of these, see the documentation for
the [Sleeper CLI deployment environment](deployment/cli-deployment-environment.md).

If you prefer to use your own VPC, you'll need to ensure it meets Sleeper's requirements. It should ideally have
multiple private subnets in different availability zones. Those subnets should have egress, e.g. via a NAT gateway. The
VPC should have gateway endpoints for S3 and DynamoDB. If there is no gateway endpoint for S3, deployment of a Sleeper
instance will fail in the CDK.

If you prefer to use your own EC2, it should run on an x86_64 architecture, with Bash and Docker, and have enough
resources to build code for Maven and Rust. We've tested with 8GB RAM and 2 vCPUs, with `t3.large`. We recommend 4 vCPUs
(`t3.xlarge`), as that takes the build from over 40 minutes with 2 vCPUs, to around 20 minutes for the first build.

The [Sleeper CLI deployment environment](deployment/cli-deployment-environment.md) includes options to deploy an EC2 to
an existing VPC, or a VPC on its own. If you don't use that, you will need to have bootstrapped CDK in your AWS account.
See [environment setup](deployment/environment-setup.md) for how to do that, and for more detail if you run into
trouble.

Once you've got a suitable VPC, and an EC2 with the Sleeper CLI installed, you can either use our deployment scripts,
or invoke the CDK yourself as described in the [deployment guide](deployment-guide.md).

## Deployment

The easiest way to deploy a full instance of Sleeper and interact with it is to use the "system test" functionality.
This deploys a Sleeper instance with a simple schema, and writes some random data into a table in the instance. You can
then use the status scripts to see how much data is in the system, run some example queries, and view logs to help
understand what the system is doing.

If you'd prefer to match how you would deploy to production, see the [deployment guide](deployment-guide.md).

The Git repository includes a manual system test deployment script that builds and deploys Sleeper, and starts random
data generation in a separate "system test" ECS cluster. By default this generates 40 million records per ECS task. This
script takes a globally unique Sleeper instance ID, and IDs of the VPC and subnets you want to deploy the instance to.

The instance ID must be 20 characters or less, and should consist of lower case letters, numbers, and hyphens. We use
the instance ID as part of the name of all AWS resources that are deployed.

Avoid reusing the same instance ID, as log groups from a deleted instance will still be present unless you delete them.
An instance will fail to deploy if it would replace log groups from a deleted instance.

Subnets should be specified with commas in between the IDs, e.g. `subnet-a,subnet-b`.

We will run the deployment in a Docker container in your EC2 instance, using the `sleeper builder` command. This Docker
container will be deleted after you exit. This contains a workspace mounted from a folder in the host home directory,
which will persist after the container exits, and will be reused by future calls to `sleeper builder`. It also inherits
the AWS and Docker configuration from the host.

From a command line in your EC2 instance, if you set environment variables for the instance `ID`, `VPC` and `SUBNETS`,
you can run the script like this:

```bash
sleeper builder # Create a Docker container with a workspace mounted in from the host directory ~/.sleeper/builder
git clone --branch main https://github.com/gchq/sleeper.git # If you haven't checked out the Git repository yet
cd sleeper
./scripts/test/deployAll/buildDeployTest.sh ${ID} ${VPC} ${SUBNETS}
```

You may prefer to run this script in the background and redirect output to a file:

```bash
./scripts/test/deployAll/buildDeployTest.sh ${ID} ${VPC} ${SUBNETS} &> test.log &
less -R test.log # Press shift+F to follow the output in less
```

This should take around 20 minutes to build the code, and another 20 minutes to deploy the instance with the CDK. Once
it finishes, you can watch the random data generation tasks in the AWS console by finding the ECS cluster
named `sleeper-${ID}-system-test-cluster`. You can view logs for tasks in the cluster. It takes around 10 minutes to
generate data. The data will appear in Sleeper in large batches as the tasks finish.

### Interacting with Sleeper

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

It is possible to run variations on this system test by editing the system test properties, like this:

```bash
cd ./scripts/test/deployAll
editor system-test-instance.properties
./buildDeployTest.sh  ${ID} ${VPC} ${SUBNETS}
```

To deploy your own instance of Sleeper with a particular schema, follow the [deployment guide](deployment-guide.md).
