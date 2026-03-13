Getting started
===============

There are 2 ways of deploying Sleeper and interacting with an instance. You can deploy to AWS, or to Docker on your
local machine. The Docker version has limited functionality and will only work with small volumes of data, but will
allow you to deploy an instance, ingest some files, and run reports and scripts against the instance.

## Sleeper in AWS

This Git repository contains scripts that let you build and/or deploy Sleeper with a single command, to minimise setup.
Other scripts are provided to interact with Sleeper.

We do not currently publish pre-built versions of Sleeper publicly. Internally we have automated publishing of Sleeper,
with a scripted installation. Search internally for documentation on this. If you are in another organisation you may
need to build it yourself. In that case, see the [developer guide](developer-guide.md) for how to build it,
and [publishing](development/publishing.md) for tools to set up your own pre-build.

## Environment setup

You'll need to prepare your AWS account to deploy Sleeper into. See [environment setup](deployment/environment-setup.md)
for how to do this. For the rest of this guide we'll assume you're working in an EC2 instance in an AWS account that's
configured appropriately.

## Dependencies

Once you've installed or built Sleeper, here are the dependencies you will need installed in your EC2, in order to
deploy and interact with an instance:

* [Bash](https://www.gnu.org/software/bash/): Minimum v3.2. Use `bash --version`.
* [Docker](https://docs.docker.com/get-docker/)
* Java: Minimum version 17, we recommend the [Amazon Corretto JDK](https://docs.aws.amazon.com/corretto/latest/corretto-21-ug/downloads-list.html).
* [NodeJS / NPM](https://github.com/nvm-sh/nvm#installing-and-updating)
* [AWS CDK](https://docs.aws.amazon.com/cdk/latest/guide/cli.html)
* [AWS CLI](https://docs.aws.amazon.com/cli/latest/userguide/install-cliv2.html)

We also have tools to run a Docker container with these pre-installed, so you only need Docker installed in your EC2.
See [Sleeper Docker tools](deployment/docker-tools.md).

## Deployment

The easiest way to deploy a full instance of Sleeper and interact with it is to use the "system test" functionality.
This deploys a Sleeper instance with a simple schema, and writes some random data into a table in the instance. You can
then use the status scripts to see how much data is in the system, run some example queries, and view logs to help
understand what the system is doing.

If you'd prefer to match how you would deploy to production, see the [deployment guide](deployment-guide.md).

The Git repository includes a manual system test deployment script that builds and deploys Sleeper, and starts random
data generation in a separate "system test" ECS cluster. By default this generates 40 million rows per ECS task. This
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
git clone --branch main https://github.com/gchq/sleeper.git # Get the latest release version of Sleeper
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

Run the following command to see how many rows are currently in the system:

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
./buildDeployTest.sh  ${ID} ${VPC} ${SUBNETS}
```

To deploy your own instance of Sleeper with a particular schema, follow the [deployment guide](deployment-guide.md).
