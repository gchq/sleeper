Getting started
===============

If you have access to our organisation's internal systems, search there for how to install Sleeper. After that
installation you'll be able to use any of our deployment methods, as well as the scripts to interact with Sleeper.

We do not currently publish pre-built versions of Sleeper publicly. If you're an external user you can build it
yourself, or use your own publishing. See the [developer guide](developer-guide.md) for how to build it,
and [publishing](development/publishing.md) for tools to set up your own.

## Deployment environment setup

You'll need to prepare your AWS account to deploy Sleeper into. See [deployment environment setup](deployment/environment-setup.md)
for how to do this. This includes bootstrapping the CDK, configuring or creating a VPC with endpoints for relevant AWS
services, and creating an EC2 instance to avoid lengthy uploads of large jar files and Docker images from outside AWS.
The setup documentation includes tools to automate most of this.

For the rest of this guide we'll assume you're working in an EC2 instance in an AWS account that's configured
appropriately.

## Dependencies

Once you've installed or built Sleeper, here are the dependencies you will need installed in your EC2, in order to
deploy and interact with an instance:

* [Bash](https://www.gnu.org/software/bash/): Minimum v3.2. Use `bash --version`.
* [Docker](https://docs.docker.com/get-docker/)
* Java: Minimum version 17, we recommend and test against [Amazon Corretto JDK 17](https://docs.aws.amazon.com/corretto/latest/corretto-17-ug/downloads-list.html).
* [NodeJS / NPM](https://github.com/nvm-sh/nvm#installing-and-updating)
* [AWS CDK](https://docs.aws.amazon.com/cdk/latest/guide/cli.html)
* [AWS CLI](https://docs.aws.amazon.com/cli/latest/userguide/install-cliv2.html)

If your EC2 runs Amazon Linux, some features such as direct queries may not work. We compile our native code against
a recent version of Ubuntu, and Amazon Linux uses an old version of glibc which is not compatible. We recommend using
Ubuntu.

We also have tools to run a Docker container with these pre-installed, so you only need Docker installed in your EC2.
See [Sleeper Docker tools](deployment/docker-tools.md).

## Deployment

This Git repository contains scripts that let you build and/or deploy Sleeper with a single command, to minimise setup.
Other scripts are provided to interact with Sleeper.

The easiest way to deploy a full instance of Sleeper and interact with it is to use the demonstration deployment.
This deploys a Sleeper instance and a table with a simple schema, and comes with a data generation ECS cluster that will
write some random data into the table. You can then use the status scripts to see how much data is in the system,
run some example queries, and view logs to help understand what the system is doing.

If you'd prefer to match how you would deploy to production, see the [deployment guide](deployment-guide.md). If you
deploy your own instance, or if you have your own Parquet files already that you'd like to add, you can follow the
documentation for [tables](usage/tables.md) and [ingest](usage/ingest.md) to add your data, before moving on to the
section below for interacting with Sleeper.

### Demonstration deployment

The demonstration deployment consists of a script that takes a globally unique Sleeper instance ID, and IDs of the VPC
and subnets you want to deploy the instance to. By default it runs 10 data generation ECS tasks, that each generate 40
million rows.

The instance ID must be 20 characters or less, and should consist of lower case letters, numbers, and hyphens. We use
the instance ID as part of the name of all AWS resources that are deployed.

Avoid reusing the same instance ID, as log groups from a deleted instance will still be present unless you delete them.
An instance will fail to deploy if it would replace log groups from a deleted instance.

Subnets should be specified with commas in between the IDs, e.g. `subnet-a,subnet-b`.

Currently the demonstration deployment is not included in a normal build of the system. If you've installed a pre-built
version, see the [developer guide](developer-guide.md) for how to set up to build the system.

From a command line in your EC2 instance with the dependencies to build the system available, you can run the script
like this:

```bash
cd sleeper # Navigate to this Git repository, which was checked out during installation or building
./scripts/test/deployAll/buildDeployTest.sh <instance-id> <vpc> <subnets>
```

You may prefer to run this script in the background and redirect output to a file:

```bash
./scripts/test/deployAll/buildDeployTest.sh <instance-id> <vpc> <subnets> &> test.log &
less -R test.log # Press shift+F to follow the output in less
```

This should take around 20 minutes to build the code, and another 20 minutes to deploy the instance with the CDK. Once
it finishes, you can watch the random data generation tasks in the AWS console by finding the ECS cluster
named `sleeper-${ID}-system-test-cluster`. You can view logs for tasks in the cluster. It takes around 10 minutes to
generate data. The data will appear in Sleeper in large batches as the tasks finish.

Note that you can still use the method described in the deployment guide to upgrade a demonstration instance to a later
version of Sleeper: [Upgrade/redeploy existing instance](deployment-guide.md#upgraderedeploy-existing-instance)

### Interacting with Sleeper

Run the following command to see how many rows are currently in the system:

```bash
./scripts/utility/filesStatusReport.sh <instance-id> system-test
```

The randomly generated data in the table conforms to the schema given in the file `scripts/templates/schema.template`.
This has a key field called `key` which is of type string. The code that randomly generates the data will generate keys
which are random strings of length 10. To run a query, use:

```bash
./scripts/utility/query.sh <instance-id>
```

As the data that went into the table is randomly generated, you will need to query for a range of keys, rather than a
specific key. The above script can be used to run a range query (i.e. a query for all rows where the key is in a
certain range) - press 'r' and then enter a minimum and a maximum value for the query. Don't choose too large a range or
you'll end up with a very large amount of data sent to the console (e.g a min of 'aaaaaaaaaa' and a max of
'aaaaazzzzz'). Note that the first query is slower than the others due to the overhead of initialising some libraries.
Also note that this query is executed directly from a Java class. Data is read directly from S3 to wherever the script
is run. It is also possible to execute queries using lambda and have the results written to either S3 or to SQS. The
lambda-based approach allows for a much greater degree of parallelism in the queries. Use `lambdaQuery.sh` instead of
`query.sh` to experiment with this.

Be careful that if you specify SQS as the output, and query for a range containing a large number of rows, then a
large number of results could be posted to SQS, and this could result in significant charges.

Over time you will see the number of files (as reported by the `filesStatusReport.sh` script) decrease. This is due to
compaction tasks merging files together. These are executed in an ECS cluster (named
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
./buildDeployTest.sh  <instance-id> <vpc> <subnets>
```

To deploy your own instance of Sleeper with a particular schema, follow the [deployment guide](deployment-guide.md).
