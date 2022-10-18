Releases
=========

This page documents the releases of Sleeper. Performance figures for each release
are available [here](docs/12-performance-test.md)

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
