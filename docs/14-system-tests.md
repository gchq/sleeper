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

This test module contains several JUnit test suites:
- QuickSystemTestSuite (the default)
- NightlyFunctionalSystemTestSuite
- NightlyPerformanceSystemTestSuite

Tests that are tagged as Slow or Expensive will not be included in the quick suite. The quick suite is intended to run
in around 40 minutes. The nightly functional suite includes tests tagged as Slow, and will take a bit longer. The
nightly performance suite includes all tests, including ones tagged as Expensive. The performance tests work with a
larger bulk of data. They take time to run and can be costly to run frequently.

### Running tests

This command will build the system and run the default, quick system test suite:

```bash
./scripts/test/maven/buildDeployTest.sh <short-id> <vpc> <subnets>
```

The short ID will be used to generate the instance IDs of Sleeper instances deployed for the tests. The quick test suite
will run on one Sleeper instance, but other test suites will deploy more. A separate system test CDK stack will also
be deployed with `SystemTestStandaloneApp`, for resources shared between tests. The system test stack will use the short
ID as its stack name.

You can avoid rebuilding the whole system by using `deployTest.sh` instead of `buildDeployTest.sh`. This is useful if
you've only changed test code and just want to re-run tests against the same instance.

There's also `performanceTest.sh` which acts like `deployTest.sh` but takes an argument for which tests you want to run,
and it enables resources required for performance tests. These have no additional cost but do take some time to deploy.

You can run specific tests like this:

```bash
./scripts/build/buildForTest.sh # This is not necessary if you used buildDeployTest.sh or have already built the system
./scripts/test/maven/performanceTest.sh <short-id> <vpc> <subnets> CompactionPerformanceST,IngestPerformanceST
```

You can run a specific test suite like this:

```bash
./scripts/build/buildForTest.sh # This is not necessary if you used buildDeployTest.sh or have already built the system
./scripts/test/maven/performanceTest.sh <short-id> <vpc> <subnets> NightlyPerformanceSystemTestSuite
```

This can also be used with NightlyFunctionalSystemTestSuite.

Note that some system tests are designed to run on at least 3 subnets in different availability zones. There is no
guarantee that these tests will pass if you use fewer subnets than this, as there might not be enough capacity in
the availaiblity zones you have provided.

### Reusing resources

After the tests, the instances will remain deployed. If you run again with the same short ID, the same instances will
be used. Usually they will not be redeployed, but if configuration has changed which requires it, they will be updated
automatically. If you've rebuilt the system, this will not automatically cause redeployment.

You can force redeployment by setting the Maven property `sleeper.system.test.force.redeploy` to redeploy the system
test stack, and `sleeper.system.test.instances.force.redeploy` to redeploy Sleeper instances used in the tests you run.
You can add Maven arguments to a system test script like this:

```bash
./scripts/test/maven/buildDeployTest.sh <short-id> <vpc> <subnets> \
  -Dsleeper.system.test.force.redeploy=true \
  -Dsleeper.system.test.instances.force.redeploy=true
```

### Tear down

You can tear down all resources associated with a short ID like this:

```bash
./scripts/test/maven/tearDown.sh <short-id> <instance-ids>
```

This requires you to know which instance IDs are deployed. These will be in the form `<short-id>-<short-instance-name>`.
The easiest way to find these is by checking the CloudFormation stacks in the AWS console. An instance uses its instance
ID as the stack name.

You can also find the different short instance names used in the SystemTestInstance class. When a test runs, it deploys
an instance with the associated instance ID if one does not exist.

### Performance tests

Performance tests use an ECS cluster to generate data, which we call the system test cluster. This is deployed in the
system test CDK stack with `SystemTestStandaloneApp`. This is a CloudFormation stack with the short ID as its name. For
non-performance tests, the system test stack is still deployed, but the system test cluster is not.

When you use `performanceTest.sh` this will enable the system test cluster. Note that this does not come with any
additional costs, as data generation is done in AWS Fargate in tasks started for the specific test. You can also enable
the system test cluster by adding a Maven argument to one of the other scripts like
`-Dsleeper.system.test.cluster.enabled=true`.

If you enable the system test cluster when you previously deployed with the same short ID without the cluster, this
will also redeploy all Sleeper instances that you test against, to give the system test cluster access to them.

### Output

You'll get a lot of output and logs from Maven. Performance tests will also output reports of jobs and tasks that were
run, and statistics about their performance. Other tests may also output similar reports if they fail.

By default you'll get results on the command line, but there might be too much output to find what you're interested in.
You could redirect the output to a file to make it a bit easier to navigate, or you can set an output directory where
the test suite will write reports in separate files. Here are some examples:

```bash
# Set an output directory for reports
./scripts/test/maven/performanceTest.sh <short-id> <vpc> <subnets> IngestPerformanceST \
  -Dsleeper.system.test.output.dir=/tmp/sleeper/output
less /tmp/sleeper/output/IngestPerformanceST.shouldMeetIngestPerformanceStandardsAcrossManyPartitions.report.log

# Redirect output to a file
./scripts/test/maven/buildDeployTest.sh <short-id> <vpc> <subnets> &> test.log &
less -R test.log # The -R option presents colours correctly. When it opens, use shift+F to follow output as it's written to the file.
```

### Nightly test scripts

We run the acceptance test suite nightly. The scripts and crontab we use for this are available
in `scripts/test/nightly`. See the documentation for this [here](../scripts/test/nightly/README.md).

This uploads the output to an S3 bucket, including an HTML site with Maven Failsafe reports
on which tests were run, including information about failures. This will deploy fresh instances, and tear them down
afterwards.

You can use the results from these tests to update the performance results table below.

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
| 0.19.0         | 19/09/2023 | 326700                      | 143500                           |
| 0.20.0         | 20/11/2023 | 318902                      | 137402                           |
| 0.21.0         | 08/02/2024 | 330460                      | 145683                           |
| 0.22.0         | 22/03/2024 | 350177                      | 155302                           |
| 0.23.0         | 22/05/2024 | 273585                      | 154574                           |
| 0.24.0         | 17/07/2024 | 242175                      | 151578                           |
