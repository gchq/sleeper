Releases
=========

This page documents the releases of Sleeper. Performance figures for each release
are available [here](docs/12-performance-test.md)

## Version 0.13.0

This contains the following improvements:

General code improvements:
- The various compaction related modules are now all submodules of one parent compaction module.
- Simplified the names of Cloudwatch log groups.

Standard ingest:
- Refactored standard ingest code to simplify it and make it easier to use.
- Observability of ingest jobs: it is now possible to see the status of ingest jobs (i.e. whether they are queued,
in progress, finished) and how long they took to run.
- Fixed bug where standard ingest would fail if it needed to upload a file greater than 5GB to S3. This was done
by replacing the use of put object with transfer manager.
- The minimum part size to be used for uploads is now configurable and defaults to 128MB.
- Changed the default value of `sleeper.ingest.arrow.max.local.store.bytes` from 16GB to 2GB to reduce the latency
before data is uploaded to S3.
- Various integration tests were converted to unit tests to speed up the build process.

Bulk import:
- Added new Dataframe based approach to bulk import that uses a custom partitioner so that Spark partitions the
data according to Sleeper's leaf partitions. The data is then sorted within those partitions. This avoids the
global sort required by the other Dataframe based approach, and means there is one fewer pass through the data
to be loaded. This reduced the time of a test bulk import job from 24 to 14 minutes.
- EBS storage can be configured for EMR clusters created for bulk import jobs.
- Bumped default EMR version to 6.8.0 and Spark version to 3.3.0.

Compactions:
- Compactions can now be run on Graviton Fargate containers.

Scripts:
- The script to report information about the partitions now reports more detailed information about the number of
elements in a partition and whether it needs splitting.
- System test script reports elapsed time.

Build:
- Various improvments to github actions reliability.
- Created a Docker image that can be used to deploy Sleeper. This avoids the user needing to install multiple tools
locally.

## Version 0.12.0

This contains the following improvements:

General code improvements:
- Added spotbugs.
- Added checkstyle.
- GitHub actions are now used for CI.
- Builders added to the Java BulkImportJob, CompactionJob, FileInfo, Partition, Query and Schema classes.
- Use AssertJ for test assertions, and enforce that this is used instead of Junit assertions.

Core:
- Improved testing of RangeCanonicaliser.
- Schema now checks that field names are unique.
- Top level pom now specifies source encoding.
- A UUID is now always used for the name of a partition.

Queries:
- SqsQueryProcessorLambda now allows the number of threads in the executor service to be configured.

Compactions:
- Recording of the status of compaction jobs in DynamoDB, with scripts to enable querying of the status of a job or all jobs.
- Recording of the status of compaction ECS tasks in DynamoDB, with scripts to enable querying of the status of a task or all tasks.
- Various integration tests converted to unit tests to speed up build process.
- Fixed error in `DeleteMessageAction` log message.

Bulk imports:
- Persistent EMR bulk import stack now uses a lambda to submit an EMR step to the cluster to run the bulk import job. The job is written
to a file in S3 in the bulk import logs bucket.
- The default class to use for bulk imports can now be configured as an instance property.
- `spark.sql.shuffle.partitions` option is now set on Spark bulk import jobs.
- The Spark properties used across the EMR and persistent EMR stacks are now consistent.
- EMR clusters used in the (non-persistent) EMR stack now have the security group provided by the user added to allow ssh into the master.
- Bulk import stacks now always create a logs bucket.

State store:
- DynamoDB and S3 state stores refactored into smaller classes.

Ingest:
- Default value of `sleeper.ingest.arrow.working.buffer.bytes` has been increased to 256MB.
- Improved logging in `ArrowRecordBatchBase`.
- The documentation for the `sleeper.ingest.arrow.max.single.write.to.file.bytes` parameter in the example instance properties
file was incorrectly described.
- The Arrow-based record batching approach now supports schemas with maps and lists.
- The `IngestCoordinator` class no longer makes an unnecessary request to list the partitions.

CDK:
- Added CDK stack to create an EC2 instance that can be used to deploy Sleeper from.
- Added optional instance property that allows a tag name to be provided that will be used to tag the different stacks with the tag name.
- The topic stack now checks that the email address provided is not the empty string.

Scripts:
- Added a script to regenerate the `generated` directory used in scripts.
- Files of split points added to system-test scripts directory to allow testing with different numbers of partitions.
- Fixed incorrect message in scripts that upload Docker images.
- Files status report script prints out number of records in a human readable form.
- Fixed issue with use of `declare -A` in scripts that only worked on bash version 4 and later.
- Fixed issue with use of `sed -i` that causes deploy scripts to fail on Macs.

Python API:
- Bulk import class name can now be configured when submitting a bulk import job from Python.
- Fixed bug in `create_batch_writer` method.

Documentation:
- Added note about CloudWatch metrics stack.
- Fixed various minor formatting issues.
- Improved documentation of bulk import process.
- Clarified required versions of command-line dependencies such as Maven, Java, etc.
- Corrected error in bulk import key pair property name in docs and `example/full/instance.properties`.
- Clarified documentation on `cdk bootstrap` process.

## Version 0.11.0

First public release.
