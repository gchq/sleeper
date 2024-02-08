Nightly system tests
====================

These are the scripts and crontab we use to run the nightly acceptance test suite. This uploads the output to an S3
bucket, including an HTML site with Maven Failsafe reports on the tests that were run. This will deploy fresh instances,
and tear them down afterwards.

If you want to run this manually you can use the Sleeper CLI. You can bring the CLI up to date and check out the Git
repository like this:

```bash
sleeper cli upgrade main
sleeper builder
git clone https://github.com/gchq/sleeper.git
cd sleeper
```

You can then configure the nightly tests inside the builder. Note that any files you put in the builder under the
directory `/sleeper-builder` will be persisted between invocations of `sleeper builder`. Copy the configuration template
out of the Sleeper Git repository so that it will not be overwritten when updating the repository, and edit it to
configure the tests:

```bash
cp /sleeper-builder/sleeper/scripts/test/nightly/nightlyTestSettings.json /sleeper-builder
vim nightlyTestSettings.json
```

You'll need to set a VPC, subnets, results S3 bucket and a path to your fork in GitHub.

The GitHub App settings are just for automatically merging into main. If you don't enable merging into main, you can
leave them alone or delete them.

Next, run this from the host machine:

```bash
sleeper builder ./sleeper/scripts/test/nightly/updateAndRunTests.sh /sleeper-builder/nightlyTestSettings.json performance &> /tmp/sleeperTests.log
```

This will take 6 hours or so. You can check output in `/tmp/sleeperTests.log`, but once the suite starts that file will
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
<results-bucket>/<date>/performance/CompactionPerformanceIT.shouldMeetCompactionPerformanceStandards.report.log
<results-bucket>/<date>/performance/IngestPerformanceIT.shouldMeetIngestPerformanceStandardsAcrossManyPartitions.report.log
```

You can use these results to update the performance table
in [the documentation](../../../docs/13-system-tests.md#performance-benchmarks)
