Developer Guide
===============

This is a brief guide to developing Sleeper.

## Get your environment setup
Before you do any dev work on Sleeper it is worth reading the "Get your 
environment setup" section in the [deployment guide](02-deployment-guide.md) 
as exactly the same will apply here, especially for running the system 
tests.

## Building
To build Sleeper from source you'll need Docker and maven installed.

Provided script (recommended) - this builds the code and copies the jars
into the scripts directory so that the scripts work.
```bash
./scripts/build/build.sh
```

maven (removing the '-Pquick' option will cause the unit and integration tests
to run):
```bash
cd java
mvn clean install -Pquick
```

## System Tests
Sleeper's system tests can be used to measure the performance of the standard
ingest and compaction components of Sleeper. This is useful to ensure that
performance degradations have not been introduced when we release new versions.
The system tests load some random data. This allows us to see the number of records
written per second using the standard ingest process. Once the data has been
ingested, some compaction jobs will happen. Looking at the logs for these shows
us the number of records per second that a compaction job processes.

To run the system tests use:
```bash
./scripts/test/buildDeployTest.sh <unique-identifier> <vpc-id> <subnet-id>
```
This will generate everything for you including:
* An S3 Bucket containing all the necessary jars
* ECR repositories for ingest, compaction and system test images
* The Sleeper properties file
Once generated, it deploys Sleeper using CDK. Note that currently this needs
to be run on an x86 machine.

The system tests use the schema in `./scripts/templates/schema.template`. You can
change the instance and table properties that are used to deploy the system test 
by editing the files in `./scripts/templates`. However, note that properties in
these files with the value "changeme" will be overwritten by the script.

You can also change any system test specific properties in the files
`scripts/test/system-test-instance.properties` and `scripts/test/buildDeployTest.sh`.

In the `deploy.sh` script the optional stacks property is set - you may want to
customise this to experiment with different stacks.

All resources are tagged with the tags defined in the file `deploy/templates/tags.properties.template`
file.

You can get a report of your instance by running:
```bash
./scripts/test/systemTestStatusReport.sh
```

Finally, when you are ready to tear down the instance, run:
```bash
./scripts/test/tearDown.sh
```
It is advisable to manually stop any tasks running in the compaction and system test
clusters.

Note you will still need the files in the /generated folder that are created
by the `deploy.sh` script for this tear down script to work correctly.
This will remove your deployment and then any ECR repos, S3 buckets and local
files that have been generated.

## Standalone deployment
See the [deployment guide](02-deployment-guide.md) for notes on how to deploy Sleeper.

## Release Process
1. Publish latest performance statistics.
   Run a system test and use the [performance test](./12-performance-test.md) to work out the average 
   compaction rate and ingest rate. Write these to the 
   [12-performance-test.md](./12-performance-test.md) file. Commit 
   this change locally on master.
   
2. Set the new version number using `./scripts/dev/updateVersionNumber.sh`, create a tag for that
version and then push to the repo.

If you are storing versions of the code in an AWS account then upload the jars and push the Docker
images. The following assumes that the environment variable SLEEPER_JARS contains the name of the
bucket for the jars, that VERSION is the version of the code to upload, and that REPO_PREFIX is the
prefix for the ECR repositories, e.g. 123456789.dkr.ecr.eu-west-2.amazonaws.com.

3. Push jars to the S3 jars bucket.
   Copy the jars to the S3 bucket that is used to contain the Sleeper jar files:
   ```bash
   git checkout v${VERSION}
   cd java
   mvn clean install -Pquick 
   cd ..
   ./scripts/deploy/uploadJars.sh ${SLEEPER_JARS} eu-west-2 ${VERSION} ./java/distribution/target/distribution-${VERSION}-bin/scripts/jars
   ```
   
4. Push the ingest Docker repo.
   ```bash
   aws ecr get-login-password --region eu-west-2 | docker login --username AWS --password-stdin ${REPO_PREFIX}
   cp java/ingest/target/ingest-*-utility.jar java/ingest/docker/ingest.jar
   docker build -t ${REPO_PREFIX}/sleeper-ingest:${VERSION} ./java/ingest/docker
   docker push ${REPO_PREFIX}/sleeper-ingest:${VERSION}
   ```

5. Push the compaction Docker repo.
   ```bash
   cp java/compaction-job-execution/target/compaction-job-execution-*-utility.jar java/compaction-job-execution/docker/compaction-job-execution.jar
   docker build -t ${REPO_PREFIX}/sleeper-compaction:${VERSION} ./java/compaction-job-execution/docker
   docker push ${REPO_PREFIX}/sleeper-compaction:${VERSION}
   ```

6. Push the bulk import Docker repo (if you do not intend to use the experimental EKS-based bulk import then this can
be ignored).
   ```bash
   cp java/bulk-import/bulk-import-runner/target/bulk-import-runner-*-utility.jar java/bulk-import/bulk-import-runner/docker/bulk-import-runner.jar
   docker build -t ${REPO_PREFIX}/sleeper-bulk-import:${VERSION} ./java/bulk-import/bulk-import-runner/docker
   docker push ${REPO_PREFIX}/sleeper-bulk-import:${VERSION}
   ```

7. Build and copy zip archive to bucket.
   ```bash
   git checkout v${VERSION}
   cd java
   mvn clean install -Pquick -Ddistribution.format=zip
   aws s3 cp ./distribution/target/distribution-${VERSION}-bin.zip s3://${SLEEPER_JARS}
   ```
