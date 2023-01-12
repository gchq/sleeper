Getting started
===============

The easiest way to deploy an instance of Sleeper and interact with it is to use the "system test" functionality. This
deploys a Sleeper instance with a simple schema, and writes some random data into a table in the instance. You can then
use the status scripts to see how much data is in the system, run some example queries, and view logs to help understand
what the system is doing. It is best to do this from an EC2 instance as a significant amount of code needs to be
uploaded to AWS.

Before running this demo functionality, you will need the following installed (see
the [deployment guide](02-deployment-guide.md) for more information on getting set up correctly) and you will need your
CLI to be logged into your AWS account:

* [AWS CDK](https://docs.aws.amazon.com/cdk/latest/guide/cli.html): Tested with v2.39.1
* [AWS CLI](https://docs.aws.amazon.com/cli/latest/userguide/install-cliv2.html): Tested with v2.7.27
* [Bash](https://www.gnu.org/software/bash/): Tested with v3.2. Use `bash --version`.
* [Docker](https://docs.docker.com/get-docker/): Tested with v20.10.17
* [Java 8](https://openjdk.java.net/install/)
* [Maven](https://maven.apache.org/): Tested with v3.8.6
* [NodeJS / NPM](https://github.com/nvm-sh/nvm#installing-and-updating): Tested with NodeJS v16.16.0 and npm v8.11.0

If CDK has not previously been bootstrapped in this account, then it needs bootstrapping. The `cdk bootstrap ...`
command cannot be run from the `sleeper` directory directly as the Java CDK classes are not yet compiled into the JAR
named in `cdk.json`. See [this link](https://docs.aws.amazon.com/cdk/latest/guide/bootstrapping.html) for guidance on
how to bootstrap CDK in your account.

### Deployment environment

You can use the CDK to create an EC2 instance in a VPC that is suitable for deploying Sleeper. Once the CLI is logged in
to your AWS, and the CDK has been bootstrapped, run these commands in the directory `java/cdk-environment` to build and
deploy the environment:

```bash
mvn clean install -Pquick
cdk deploy --all
```

This will create an SSH key locally, and output a command to connect to the EC2 instance with that key. It should look
like this:

```bash
ssh -i SleeperEnvironment-BuildEC2.pem ubuntu@[ec2-public-ip]
```

Immediately after it's deployed, commands will run on this instance to install development tools. Once you're connected,
you can check the progress of those commands like this:

```bash
cloud-init status
```

You can check the output like this (add `-f` if you'd like to follow the progress):

```bash
tail /var/log/cloud-init-output.log
```

Once it has finished the instance might restart. The Sleeper Git repository will be checked out at `~/sleeper`.

To deploy Sleeper or run the system tests from this instance, you'll need to add your own credentials for the AWS CLI.
See
the [AWS IAM guide for CLI access](https://docs.aws.amazon.com/singlesignon/latest/userguide/howtogetcredentials.html).

#### Managing environments

You can deploy either the VPC or the EC2 independently, and specify an instance ID when deploying multiple
instances, or an existing VPC to deploy the EC2 to:

```bash
cdk deploy -c instanceId=MyEnvironment --all
cdk deploy -c instanceId=EmptyEnvironment "*-Networking"
cdk deploy -c instanceId=MyEnvironment -c vpcId=[vpc-id] "*-BuildEC2"
```

You can tear down the deployed environment with `cdk destroy`. If you used an `instanceId` or only deployed certain
parts, you'll need to specify those. Here are some examples:

```bash
cdk destroy --all
cdk destroy -c instanceId=MyEnvironment --all
cdk destroy -c instanceId=EmptyEnvironment "*-Networking"
```

### System test

To run the system test, set the environment variable `ID` to be a globally unique string. This is the instance id. It
will be used as part of the name of various AWS resources, such as an S3 bucket, lambdas, etc., and therefore should
conform to the naming requirements of those services. In general stick to lowercase letters, numbers, and hyphens. We
use the instance id as part of the name of all the resources that are deployed. This makes it easy to find the resources
that Sleeper has deployed within each service (go to the service in the AWS console and type the instance id into the
search box).

Create an environment variable called `VPC` which is the id of the VPC you want to deploy Sleeper to, and create an
environment variable called `SUBNET` with the id of the subnet you wish to deploy Sleeper to (note that this is only
relevant to the ephemeral parts of Sleeper - all of the main components use services which naturally span availability
zones).

The VPC _must_ have an S3 Gateway endpoint associated with it otherwise the `cdk deploy` step will fail.

Then run:

```bash
./scripts/test/buildDeployTest.sh ${ID} ${VPC} ${SUBNET}
```

This will use Maven to build Sleeper (this will take around 3 minutes, and the script will be silent during this time).
After that, an S3 bucket will be created for the jars, and ECR repos will be created and Docker images pushed to them.
Note that this script currently needs to be run from an x86 machine as we do not yet have cross-architecture Docker
builds. Then CDK will be used to deploy a Sleeper instance. This will take around 10 minutes. Once that is complete,
some code is run to start some tasks running on an ECS cluster. These tasks generate some random data and write it to
Sleeper. 11 ECS tasks will be created. Each of these will write 40 million records. As all writes to Sleeper are
asynchronous it will take a while before the data appears (around 8 minutes).

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
due to compaction tasks merging files together. These are executed in ECS clusters (named
`sleeper-${ID}-merge-compaction-cluster` and `sleeper-${ID}-splitting-merge-compaction-cluster`).

You will also see the number of leaf partitions increase. This functionality is performed using lambdas called
`sleeper-${ID}-find-partitions-to-split` and `sleeper-${ID}-split-partition`.

To ingest more random data, run:

```bash
java -cp java/system-test/target/system-test-*-utility.jar sleeper.systemtest.ingest.RunWriteRandomDataTaskOnECS ${ID} system-test
```

To tear all the infrastructure down, run

```bash
./scripts/test/tearDown.sh
```

Note that this will sometimes fail if there are ECS tasks running. Ensure that there are no compaction tasks running
before doing this.

It is possible to run variations on this system-test by editing the following files:
`scripts/test/system-test-instance.properties` and `scripts/templates/instanceproperties.template`.

To deploy your own instance of Sleeper with a particular schema, go to the [deployment guide](02-deployment-guide.md).
