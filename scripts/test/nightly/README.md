Nightly system tests
====================

These are the scripts and crontab we use to run the nightly acceptance test suite. This uploads the output to an S3
bucket, including an HTML site with Maven Failsafe reports on the tests that were run. This will deploy fresh instances,
and tear them down afterwards.

### Setup

If you want to run this manually you can use the Sleeper CLI. You can bring the CLI up to date and check out the Git
repository like this:

```bash
sleeper cli upgrade main
sleeper builder
git clone https://github.com/gchq/sleeper.git
cd sleeper
```

### Configuration

You can configure the nightly tests inside the builder. Note that any files you put in the builder under the
directory `/sleeper-builder` will be persisted between invocations of `sleeper builder`. Copy the configuration template
out of the Sleeper Git repository so that it will not be overwritten when updating the repository, and edit it to
configure the tests:

```bash
cp /sleeper-builder/sleeper/scripts/test/nightly/nightlyTestSettings.json /sleeper-builder
vim nightlyTestSettings.json
```

You'll need to set a VPC, subnets, results S3 bucket and a path to your fork in GitHub.

#### Automatic merge to main

There are settings for automatically merging into the main branch under "mergeToMainOnTestType". This must be set to
true or false for any test type you will use. If it's set to true, the settings under "gitHubApp" will be used to
authenticate with GitHub and merge the develop branch into main. If you don't need to merge into main, you can leave the
"gitHubApp" entry as it is, or delete it if you prefer.

### Running tests

There's an [example crontab](crontab.example) you can edit and use to regularly run nightly system tests.

To run the tests as they would be run by the cron job, use this command from the host machine:

```bash
sleeper builder ./sleeper/scripts/test/nightly/updateAndRunTests.sh /sleeper-builder/nightlyTestSettings.json performance &> /tmp/sleeperTests.log
```

This will take 6 hours or so. You can check the output in `/tmp/sleeperTests.log`, but once each suite starts it will
only update once the suite finishes. The output of the tests will be in a tmp folder in the Docker container, and will
later be uploaded to S3. You can connect to the Docker container to view the output as it's happening:

```bash
docker images # Find the image ID of the sleeper-builder image with the 'current' tag
docker ps # Find the container ID running the updateAndRunTests command with that image
docker exec -it <container ID> bash
cd /tmp/sleeper/performanceTests
ls # Find the directory named by the start time of the test suite
less -R <directory>/performance.log # Once this opens, use shift+F to follow the output of the test
```

#### Results in S3

Once the tests have finished, you can get the output and performance figures from the S3 bucket.

You can find a summary of which test suites passed or failed at this S3 key:

```
<results-bucket>/summary.txt
```

You can find Maven output and logs in a folder for the test like `<results-bucket>/<date>`.

The performance reports can be found here:

```
<results-bucket>/<date>/performance/CompactionPerformanceIT.shouldMeetCompactionPerformanceStandards.report.log
<results-bucket>/<date>/performance/IngestPerformanceIT.shouldMeetIngestPerformanceStandardsAcrossManyPartitions.report.log
```

You can use these results to update the performance table
in [the documentation](../../../docs/13-system-tests.md#performance-benchmarks)
