Getting started
===============

There are 2 ways of deploying Sleeper and interacting with an instance. You can deploy to AWS, or to Docker on your
local machine. The Docker version has limited functionality and will only work with small volumes of data, but will
allow you to deploy an instance, ingest some files, and run reports and scripts against the instance.

To get started we'll use the Sleeper CLI, which runs in Docker on your local machine.

### Dependencies

The Sleeper CLI has the following dependencies:

* [Bash](https://www.gnu.org/software/bash/): Tested with v3.2 to v5.2. Use `bash --version`.
* [Docker](https://docs.docker.com/get-docker/): Tested with v24.0.2

### Installation

The Sleeper CLI contains Docker images with the necessary dependencies and scripts to work with Sleeper. Run the
following commands to install the latest development version of the CLI.

```bash
curl "https://raw.githubusercontent.com/gchq/sleeper/develop/scripts/cli/install.sh" -o ./sleeper-install.sh
chmod +x ./sleeper-install.sh
./sleeper-install.sh develop
```

This will install the latest development version. This will not have been as fully tested as released versions and may
not work as expected. Most users should use the latest released version. You can find a list
of released versions [here](https://github.com/gchq/sleeper/tags), and the change log [here](../CHANGELOG.md).

**Due to a bug in GitHub, it is not currently possible to install released versions this way.** See the following
issue: https://github.com/gchq/sleeper/issues/2494

To use a released version, please follow the [developer guide](11-dev-guide.md) to build the CLI from source.

If the bug is fixed, you can replace `develop` with `main` for the latest release, or a release in the format `v0.20.0`.
These correspond to a branch or tag in the GitHub repository. If you do not specify a version on the command line, it
will default to the latest release.

```bash
curl "https://raw.githubusercontent.com/gchq/sleeper/[version]/scripts/cli/install.sh" -o ./sleeper-install.sh
chmod +x ./sleeper-install.sh
./sleeper-install.sh [version]
```

This installs a `sleeper` command to run other commands inside a Docker container. You can use `sleeper aws` or
`sleeper cdk` to run `aws` or `cdk` commands without needing to install the AWS or CDK CLI on your machine. If you set
AWS environment variables or configuration on the host machine, that will be propagated to the Docker container when
you use `sleeper`.

You can also upgrade the CLI to a different version with `sleeper cli upgrade`.

## Deploy to Docker

The quickest way to get an instance of Sleeper is to deploy to LocalStack in Docker on your local machine. Note that the
LocalStack version has very limited functionality in comparison to the AWS version, and can only handle small volumes of
data. See the documentation on [deploying to localstack](10-deploy-to-localstack.md) for more information.

## Deploy to AWS

The easiest way to deploy a full instance of Sleeper and interact with it is to use the "system test" functionality.
This deploys a Sleeper instance with a simple schema, and writes some random data into a table in the instance. You can
then use the status scripts to see how much data is in the system, run some example queries, and view logs to help
understand what the system is doing. It is best to do this from an EC2 instance as a significant amount of code needs to
be uploaded to AWS.

### Authentication

To use the Sleeper CLI against AWS, you need to authenticate against your AWS account. You can do this by running
`sleeper aws configure`, or other `sleeper aws` commands according to your AWS setup. AWS Environment variables
will also be propagated to the Sleeper CLI.

Most Sleeper clients also require you to set the default region.

### Deployment environment

If the CDK has never been bootstrapped in your AWS account, this must be done first. This only needs to be done
once in a given AWS account.

```bash
sleeper cdk bootstrap
```

Next, you'll need a VPC that is suitable for deploying Sleeper. You'll also want an EC2 instance to deploy from, to
avoid lengthy uploads of large jar files and Docker images. You can use the Sleeper CLI to create both of these.

If you'd prefer to use your own, you'll need to install the Sleeper CLI on your EC2, which should run on an x86_64
architecture. You'll need to authenticate with AWS as described above. You'll need to ensure your VPC meets Sleeper's
requirements, but you can also deploy a fresh VPC with the CLI. This is documented in
the [deployment guide](02-deployment-guide.md#deployment-environment).

#### Sleeper CLI environment

The Sleeper CLI can create an EC2 instance in a VPC that is suitable for deploying Sleeper. This will automatically
configure authentication such that once you're in the EC2 instance you'll have administrator access to your AWS account.

```bash
sleeper environment deploy TestEnvironment
```

The `sleeper environment deploy` command will wait for the EC2 instance to be deployed.
You can then SSH to it with EC2 Instance Connect and SSM Session Manager, using this command:

```bash
sleeper environment connect
```

Immediately after it's deployed, commands will run on this instance to install the Sleeper CLI. Once you're connected,
you can check the progress of those commands like this:

```bash
cloud-init status
```

You can check the output like this (add `-f` if you'd like to follow the progress):

```bash
tail /var/log/cloud-init-output.log
```

Once it has finished the EC2 will restart. Once it's restarted you can use the Sleeper CLI. Reconnect to the EC2
with `sleeper environment connect`.

You can access a built copy of the Sleeper scripts by running `sleeper deployment` in the EC2. That will get you a shell
inside a Docker container inside the EC2. You can run all the deployment scripts there as explained below. If you run it
outside of the EC2, you'll get the same thing but in your local Docker host. Use the one in the EC2 to avoid the
deployment being slow uploading jars and Docker images.

The Sleeper Git repository will also be cloned, and you can access it by running `sleeper builder` in the EC2.
That will get you a shell inside a Docker container similar to the `sleeper deployment` one, but with the dependencies
for building Sleeper. The whole working directory will be persisted between executions of `sleeper builder`.

If you want someone else to be able to access the same environment EC2, they can run `sleeper environment add <id>`
with the same environment ID. To begin with you'll both log on as the same user and share a single `screen` session. You
can set up separate users with `sleeper environment adduser <username>`, and switch users with
`sleeper environment setuser <username>`. If you call `sleeper environment setuser` with no arguments, you'll switch
back to the original default user for the EC2.

### System test

To run the system test, set the environment variable `ID` to be a globally unique string. This is the instance id. It
will be used as part of the name of various AWS resources, such as an S3 bucket, lambdas, etc., and therefore should
conform to the naming requirements of those services. In general stick to lowercase letters, numbers, and hyphens. We
use the instance id as part of the name of all the resources that are deployed. This makes it easy to find the resources
that Sleeper has deployed within each service (go to the service in the AWS console and type the instance id into the
search box).

Avoid reusing the same instance id, as log groups from a deleted instance will still be present unless you delete them.
An instance will fail to deploy if it would replace log groups from a deleted instance.

Create an environment variable called `VPC` which is the id of the VPC you want to deploy Sleeper to, and create an
environment variable called `SUBNETS` with the ids of subnets you wish to deploy Sleeper to (note that this is only
relevant to the ephemeral parts of Sleeper - all of the main components use services which naturally span availability
zones). Multiple subnet ids can be specified with commas in between, e.g. `subnet-a,subnet-b`.

The VPC _must_ have an S3 Gateway endpoint associated with it otherwise the `cdk deploy` step will fail.

While connected to your EC2 instance run:

```bash
sleeper deployment test/deployAll/deployTest.sh ${ID} ${VPC} ${SUBNETS}
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
sleeper deployment utility/filesStatusReport.sh ${ID} system-test
```

The randomly generated data in the table conforms to the schema given in the file `scripts/templates/schema.template`.
This has a key field called `key` which is of type string. The code that randomly generates the data will generate keys
which are random strings of length 10. To run a query, use:

```bash
sleeper deployment utility/query.sh ${ID}
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
sleeper deployment java -cp jars/system-test-*-utility.jar  sleeper.systemtest.drivers.ingest.RunWriteRandomDataTaskOnECS ${ID} system-test
```

To tear all the infrastructure down, run

```bash
sleeper deployment test/tearDown.sh
```

It is possible to run variations on this system-test by editing the system test properties, like this:

```bash
sleeper deployment
cd test/deployAll
editor system-test-instance.properties
./buildDeployTest.sh  ${ID} ${VPC} ${SUBNETS}
```

To deploy your own instance of Sleeper with a particular schema, go to the [deployment guide](02-deployment-guide.md).
