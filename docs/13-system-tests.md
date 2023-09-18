System tests
============

The following notes describe how to use the system tests that work with a deployed instance of the system.

## Manual testing

There's a script to create an instance quickly for manual testing purposes, which generates random test data for
you to work with. This deployAll test deploys most of the stacks. Note that this instance can't be reused by the
automated acceptance test suite. To run it, use the following command:

```bash
./scripts/test/deployAll/buildDeployTest.sh <instance-id> <vpc-id> <subnet-id>
```

This will generate everything for you including:

* An S3 Bucket containing all the necessary jars
* ECR repositories for ingest, compaction and system test images
* The Sleeper properties file
* Random test data in the `system-test` table.

Once generated, it deploys Sleeper using CDK.

This test has an instance properties file next to the deploy script called `system-test-instance.properties`.
When running the deploy script, the `scripts/test/deployAll` directory is scanned for `table.properties`,
`tags.properties`, and `schema.properties` files. If they are found, they are picked up and used by the test.
If they are not present, then the template files in `scripts/templates` are used. Note that properties in these
files with the value `changeme` will be overwritten by the script.

There's an optional stacks property which determines which components will be deployed - you may want to customise this
to experiment with different stacks.

There are also properties specific to system tests, which can also be changed in `system-test-instance.properties`.
These can be used to configure how much test data will be generated, and how much that data can vary.

All resources are tagged with the tags defined in the file `scripts/templates/tags.template`, or a `tags.properties`
file placed next to the `system-test-instance.properties`.

You can get a report of your instance by running:

```bash
./scripts/test/systemTestStatusReport.sh
```

Finally, when you are ready to tear down the instance, run:

```bash
./scripts/test/tearDown.sh
```

This will check the `scripts/generated` folder for the instance to tear down. That gets created during deployment, or
with `scripts/utility/downloadConfig.sh`. Alternatively, you can skip reading the generated folder by giving an instance
ID:

```bash
./scripts/test/tearDown.sh <instance-id>
```

This will remove your deployment, including any ECR repos, S3 buckets and local files that have been generated.

## Acceptance tests

We use an acceptance test suite to judge releasability of the system. These tell us whether the system can perform its
functions in a deployed environment, and help us understand whether changes to the code have increased or decreased the
performance. The tests sit in the Maven module `system-test/system-test-suite`. There are scripts to run these tests
under `scripts/test/maven`. We run this test suite nightly.

These tests run in JUnit, and use the class `SleeperSystemTest` as the entry point. That class defines the domain
specific language (DSL) for working with a deployed instance of Sleeper in JUnit. Please review the comment at the top
of that class, and look at the tests in that module for examples.

This test suite contains both feature tests, and performance tests which work with a larger bulk of data. By default,
the suite skips the performance tests. You can run the default test suite, with just the feature tests, like this:

```bash
./scripts/test/maven/buildDeployTest.sh <short-id> <vpc> <subnets>
```

The short ID will be used to generate the instance IDs of Sleeper instances deployed for the tests. The feature tests
will run on one Sleeper instance, but the performance tests will deploy more. A separate system test CDK stack will also
be deployed with `SystemTestStandaloneApp`, for resources shared between tests. The system test stack will use the short
ID as its stack name.

After the tests, the instances will remain deployed. If you run again with the same short ID, the same instances will
be used. Usually they will not be redeployed, but if configuration has changed which requires it, they will be updated
automatically. If you've rebuilt the system, this will not automatically cause redeployment. You can force redeployment
by setting the Maven property `sleeper.system.test.force.redeploy` for the system test stack,
and `sleeper.system.test.instances.force.redeploy` for the Sleeper instances, like this:

```bash
./scripts/test/maven/buildDeployTest.sh <short-id> <vpc> <subnets> \
  -Dsleeper.system.test.force.redeploy=true \
  -Dsleeper.system.test.instances.force.redeploy=true
```

If you've only changed test code and just want to re-run the tests against the same instance, you can avoid rebuilding
the whole system by using `deployTest.sh` instead of `buildDeployTest.sh`.

You can tear down all resources associated with a short ID like this:

```bash
./scripts/test/maven/tearDown.sh <short-id> <instance-ids>
```

This requires you to know which instance IDs are deployed. These will be in the form `<short-id>-<test-id>`. You can
find the different test IDs in the SystemTestInstance class. These are the identifiers associated with each enum value,
and you can find references to those enum values to find the tests that use them. When a test runs, it deploys an
instance with the associated instance ID if one does not exist.

### Performance tests

You can run specific performance tests like this:

```bash
./scripts/build/buildForTest.sh # This is not necessary if you used buildDeployTest.sh or have already built the system
./scripts/test/maven/performanceTest.sh <short-id> <vpc> <subnets> CompactionPerformanceIT,IngestPerformanceIT
```

Performance tests use an ECS cluster for generating data, which we call the system test cluster. This is deployed in the
system test CDK stack with `SystemTestStandaloneApp`. This is a CloudFormation stack with the short ID as its name. For
non-performance tests, the system test stack is still deployed, but the system test cluster is not.

When a system test has already deployed with the same short ID, the test suite will add the system test cluster to the
stack if it's needed. This will also redeploy all instances to give the system test cluster access to them.

The test suite decides whether to run performance tests or not by whether the system test cluster is enabled.
The `performanceTest.sh` script enables the system test cluster. You can also run all tests in one execution, including
performance tests, like this:

```bash
./scripts/test/maven/buildDeployTest.sh <short-id> <vpc> <subnets> -Dsleeper.system.test.cluster.enabled=true
```

### Results

Performance tests will output reports of jobs and tasks that were run, and statistics about their performance.

By default you'll get results on the command line, but there might be too much output to find what you're interested in.
You could redirect the output to a file to make it a bit easier to navigate, or you can set an output directory where
the test suite will write reports in separate files. Here are some examples:

```bash
# Set an output directory for reports
./scripts/test/maven/performanceTest.sh <short-id> <vpc> <subnets> IngestPerformanceIT \
  -Dsleeper.system.test.output.dir=/tmp/sleeper/output
less /tmp/sleeper/output/IngestPerformanceIT.shouldMeetIngestPerformanceStandardsAcrossManyPartitions.report.log

# Redirect output to a file
./scripts/test/maven/buildDeployTest.sh <short-id> <vpc> <subnets> &> test.log &
less -R test.log # The -R option presents colours correctly. When it opens, use shift+F to follow output as it's written to the file.
```

### Nightly test scripts

We run the acceptance test suite nightly. The scripts and crontab we use for this are available
in `scripts/test/nightly`. This uploads the output to an S3 bucket, including an HTML site with Maven Failsafe reports
on which tests were run, including information about failures. This will deploy fresh instances, and tear them down
afterwards.

If you want to run this manually you can use the Sleeper CLI. Once you've checked out Sleeper in a
builder as documented in the [developer guide](11-dev-guide.md#install-prerequisite-software), you can run this from the
host machine:

```bash
sleeper cli upgrade main && sleeper builder ./sleeper/scripts/test/nightly/updateAndRunTests.sh "<vpc>" "<subnet>" <output-bucket-name> "performance" &> /tmp/sleeperTests.log
```

This will take 4 hours or so. You can check output in `/tmp/sleeperTests.log`, but once the suite starts that file will
only update once the suite finishes. If you connect to the Docker container that's running the tests you can find the
output of the test suite:

```bash
docker images # Find the image ID of the sleeper-builder image with the 'current' tag
docker ps # Find the container ID running the updateAndRunTests command with that image
docker exec -it <container ID> bash
cd /tmp/sleeper/performanceTests
ls # Find the directory named by the start time of the test suite
less -R <directory>/performance.log # Once this opens, use shift+F to follow the output of the test
```

Once the tests have finished, you can get the performance figures from the results S3 bucket. First, check the tests
passed in the summary with this S3 key:

```
<results-bucket>/summary.txt
```

The performance reports can be found here:

```
<results-bucket>/<date>/CompactionPerformanceIT.shouldMeetCompactionPerformanceStandards.report.log
<results-bucket>/<date>/IngestPerformanceIT.shouldMeetIngestPerformanceStandardsAcrossManyPartitions.report.log
```

## Performance benchmarks

These figures are the average values from the system tests described above. Note that these averages are only based on
a small number of tests and are therefore not definitive results. There is also significant variability in AWS which
means that it is hard to produce reproducible figures. In future work we hope to improve the tests so that they produce
more accurate results. Nevertheless, these tests have caught several significant performance regressions that would
otherwise not have been noticed.

| Version number | Test date  | Compaction rate (records/s) | Ingest S3 write rate (records/s) |
|----------------|------------|-----------------------------|----------------------------------|
| 0.11.0         | 13/06/2022 | 366000                      | 160000                           |
| 0.12.0         | 18/10/2022 | 378000                      | 146600                           |
| 0.13.0         | 06/01/2023 | 326000                      | 144000                           |
| 0.14.0         | 20/01/2023 | 349000                      | 153000                           |
| 0.15.0         | 30/03/2023 | 336000                      | 136000                           |
| 0.16.0         | 28/04/2023 | 325000                      | 137000                           |
| 0.17.0         | 09/06/2023 | 308000                      | 163000                           |
| 0.18.0         | 09/08/2023 | 326000                      | 147000                           |
