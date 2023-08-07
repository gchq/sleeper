Developer Guide
===============

This is a brief guide to developing Sleeper.

## Get your environment setup

Before you do any dev work on Sleeper it is worth reading the "Get your environment setup" section in
the [deployment guide](02-deployment-guide.md) as exactly the same will apply here, especially for running the system
tests.

### Install Prerequisite Software

You will need the following software:

* [AWS CDK](https://docs.aws.amazon.com/cdk/latest/guide/cli.html): Tested with v2.39.1
* [AWS CLI](https://docs.aws.amazon.com/cli/latest/userguide/install-cliv2.html): Tested with v2.7.27
* [Bash](https://www.gnu.org/software/bash/): Tested with v3.2. Use `bash --version`.
* [Docker](https://docs.docker.com/get-docker/): Tested with v20.10.17
* [Java 11/17](https://openjdk.java.net/install/)
* [Maven](https://maven.apache.org/): Tested with v3.8.6
* [NodeJS / NPM](https://github.com/nvm-sh/nvm#installing-and-updating): Tested with NodeJS v16.16.0 and npm v8.11.0

If you're working with the Sleeper CLI, you can use `sleeper builder` to get a shell inside a Docker container with
these pre-installed. You'll need to clone the Git repository, and this will be persisted between executions of
`sleeper builder`. Use the commands below:

```bash
sleeper builder
git clone https://github.com/gchq/sleeper.git
cd sleeper
```

## Building

Provided script (recommended) - this builds the code and copies the jars
into the scripts directory so that the scripts work.

```bash
./scripts/build/buildForTest.sh
```

Maven (removing the '-Pquick' option will cause the unit and integration tests
to run):

```bash
cd java
mvn clean install -Pquick
```

## Exploring the code base

Look in the [design document](10-design.md) for an idea of what to expect in the project. The elements of the design
largely correspond to Maven modules. Core or common modules are used to separate shared model code from integrations
with libraries which are not needed by all components of the system.

If you'd like to look at how the modules relate to one another in terms of their dependencies, in the `build` module
there's a main method in the class `sleeper.build.maven.ShowInternalDependencies` to display the internal dependencies
as a graph.

## System Tests

Sleeper's system tests can be used to measure the performance of the standard ingest and compaction components of
Sleeper. This is useful to ensure that performance degradations have not been introduced when we release new versions.
More information about the performance tests can be found in [12-performance-test.md](12-performance-test.md).

They can also be used to test the functionality of different components, and provide a way to create an instance quickly
for testing purposes, which generates random test data for you to work with.

The easiest test to run if you are not sure about what stacks you need deployed is the deployAll system test. This test
deploys most of the stacks. To run the deployAll system test, run the following command:

```bash
./scripts/test/deployAll/buildDeployTest.sh <unique-identifier> <vpc-id> <subnet-id>
```

This will generate everything for you including:

* An S3 Bucket containing all the necessary jars
* ECR repositories for ingest, compaction and system test images
* The Sleeper properties file
* Random test data in the `system-test` table.

Once generated, it deploys Sleeper using CDK.

Each system test has an instance properties file next to the deploy script called `system-test-instance.properties`.
When running the deploy script, the `scripts/test/<test-name>` directory is scanned for `table.properties`,
`tags.properties`, and `schema.properties` files. If they are found, they are picked up and used by the test.
If they are not present, then the template files in `scripts/templates` are used. Note that properties in these
files with the value `changeme` will be overwritten by the script.

You can also change any system test specific properties in the
file `scripts/test/<test-name>/system-test-instance.properties`.
This includes the optional stacks property - you may want to customise this to experiment with different stacks.

All resources are tagged with the tags defined in the file `deploy/templates/tags.template`, or a `tags.properties` file
placed next to the `system-test-instance.properties`.

You can get a report of your instance by running:

```bash
./scripts/test/systemTestStatusReport.sh
```

Finally, when you are ready to tear down the instance, run:

```bash
./scripts/test/tearDown.sh
```

Note you will still need the files in the `/generated` folder that are created during deployment for this tear down
script to work correctly.

This will remove your deployment, including any ECR repos, S3 buckets and local files that have been generated.

## Standalone deployment

See the [deployment guide](02-deployment-guide.md) for notes on how to deploy Sleeper.

## Release Process

1. Update CHANGELOG.md with a summary of the issues fixed and improvements made in this version.

2. Create an issue for the release, and create a branch for that issue.

3. Set the new version number using `./scripts/dev/updateVersionNumber.sh`, e.g.

```bash
VERSION=0.12.0
./scripts/dev/updateVersionNumber.sh ${VERSION}
```

4. Push the branch to github and open a pull request so that the tests run. If there are any failures, fix them.

5. Run a deployment of the compactionPerformance system test to get the performance figures:

```bash
ID=<a-unique-id>
VPC=<your-vpc-id>
SUBNETS=<your-subnet-ids>
./scripts/test/compactionPerformance/buildDeployTest.sh ${ID} ${VPC} ${SUBNETS}
```

Wait for the deployment to finish. Then wait until the ingest processes have run and the compactions have completed.
Use the following command to check the status of the table called 'system-test':

```bash
./scripts/utility/filesStatusReport.sh ${ID} system-test
```

Run this occasionally until it reports that there are 440 million records in the table.

6. Publish the performance statistics.

Record the ingest and compaction rate in the [performance figures](12-performance-test.md) documentation.

```bash
# Ingest performance figures can be found by running the following reports
./scripts/utility/ingestTaskStatusReport.sh ${ID} standard -a
./scripts/utility/ingestJobStatusReport.sh ${ID} system-test standard -a

# Compaction performance figures can be found by running the following reports
./scripts/utility/compactionTaskStatusReport.sh ${ID} standard -a
./scripts/utility/compactionJobStatusReport.sh ${ID} system-test standard -a
```

7. Tear down the compactionPerformance system test

```bash
./scripts/test/tearDown.sh ${ID}
```

8. Run a deployment of the deployAll system test to test the functionality of the system. Note that it is best to
   provide an instance ID that's different from the compactionPerformance test:

```bash
ID=<a-unique-id>
VPC=<your-vpc-id>
SUBNETS=<your-subnet-ids>
./scripts/test/deployAll/buildDeployTest.sh ${ID} ${VPC} ${SUBNETS}
```

The following tests can be used as a quick check that all is working correctly. They are not intended to fully test
all aspects of the system. Any changes made by pull requests should be tested by doing a system test deployment on AWS
if they are likely to either affect performance or involve changes to the way the system is deployed to AWS.

9. Test a simple query:

```bash
./scripts/utility/query.sh ${ID}
```

Choose a range query, choose 'y' for the first two questions and then choose a range such as 'aaaaaa' to 'aaaazz'.
As the data that is ingested is random, it is not possible to say exactly how many results will be returned, but it
should be in the region of 900 results.

10. Test a query that will be executed by lambda:

```bash
./scripts/utility/lambdaQuery.sh ${ID}
```

Choose the S3 results bucket option and then choose the same options as above. It should say "COMPLETED".
The first query executed by lambda is a little slower than subsequent ones due to the start-up costs. The second query
should be quicker.

11. Test a query that will be executed by lambda with the results being returned over a websocket:

```bash
./scripts/utility/webSocketQuery.sh ${ID}
```

Choose the same options as above, and results should be returned.

12. Test the Python API:

```bash
cd python
pip install .
```

Then

```python
from sleeper.sleeper import SleeperClient

s = SleeperClient("instance-id")
region = {"key": ["aaaaaa", True, "aaaazz", False]}
s.range_key_query("system-test", [region])
```

Around 900 results should be returned.

13. Once the above tests have been done, merge the pull request into main. Then checkout the main branch,
    set the tag to `v${VERSION}` and push the tag using `git push --tags`.
