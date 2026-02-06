Releases
=========

This page documents the releases of Sleeper. Performance figures for each release
are available [here](docs/development/system-tests.md#performance-benchmarks). A roadmap of current and future work is
available [here](docs/development/roadmap.md).


## Version 0.35.0

### 6th February, 2026

This includes automation of pre-splitting table partitions in bulk import, and a high throughput committer for the state
store.

Bulk import:
- When a bulk import job is submitted to a Sleeper table with too few partitions, they are pre-split automatically.
  - This assumes the data in the bulk import job is representative of the table as a whole.
  - Increased the default value of [`sleeper.default.table.bulk.import.min.leaf.partitions`](docs/usage/properties/instance/user/table_property_defaults.md) to 256.

State store:
- An experimental high throughput version of the state store committer is now available.
  - This is a single persistent EC2 instance that listens for all messages from the commit queue.
  - Can handle many Sleeper tables at much higher throughput than the default lambda version.
  - Can be chosen with the instance property [`sleeper.statestore.committer.platform`](docs/usage/properties/instance/user/table_state.md).

Configuration:
- Instance properties that are default values for table properties were renamed to start with `sleeper.default.table`.
- Added [`sleeper.table.parquet.rowgroup.rows.max`](docs/usage/properties/table/data_storage.md) to set the maximum Parquet row group size for the DataFusion data engine.

Deployment:
- ECS tasks and services now use their own security group, rather than the default security group for the VPC.
- Restricted permissions on default security group in environment deployed by Sleeper CLI.
- Added lifecycle rules to ECR repositories so that Docker images older than a year are deleted.

Bugfixes:
- DataFusion data engine failed when aggregations were specified that did not match the order of the table schema.
- Tags configured for an instance are now correctly added to the instance by the CDK.
- Removed confusing logs about constructing a partition tree when reading table configuration, e.g. when the CDK starts.
- A bug in the CDK meant changing the minimum capacity of a bulk import persistent EMR cluster caused deployment to fail.
- When reinitialising a table that contained data, the underlying data files were not deleted.
- Reasons that an ingest or compaction job failed were not always shown in reports.
- EMR Serverless is now shown in counts of jobs on queues in ingest job reports.
- The admin client crashed when invalid tags were configured for the Sleeper instance.


## Version 0.34.1

### 9th December, 2025

This is a bug fix release that corrects deployment of Sleeper with the CDK.

Deployment:
- The examples and templates for instance and table properties no longer set all properties to their default values.

Bugfixes:
- The CDK deployment of a Sleeper instance now correctly references artefacts by the current Sleeper version number.
  - In the previous release we could not deploy an instance as it looked for artefacts with a null version.
  - This was caused by a fix for a test that failed when the version number changed.
  - We've updated our process for when tests fail during release, to re-run tests in AWS after any fix.


## Version 0.34.0

### 2nd December, 2025

This contains a process to publish artefacts for deployment of a version of Sleeper, improvements for deploying Sleeper
directly with the CDK, and queries with DataFusion.

Query:
- Queries can be run much faster with DataFusion
  - This is experimental initially, and is only used for tables set to the experimental data engine

Compaction:
- Added an option to disable readahead for DataFusion compactions
  - This reduces the number of GETs to S3, reducing cost, and was previously always enabled
  - There's a known issue with this causing occasional failures in large compactions (see https://github.com/gchq/sleeper/issues/5777)
  - Configured in table property `sleeper.table.datafusion.s3.readahead.enabled`, which defaults to true

Clients:
- Added support for web socket queries in Java `SleeperClient`

Deployment:
- Added scripts to retrieve and use published artefacts for deployment
- Added a script for declarative deployment
- Enabled deletion of a Sleeper instance directly with the CDK, instead of the tear down script
  - ECS tasks are now stopped automatically
  - EMR instances are now stopped automatically
  - Deployment artefacts held in AWS are managed by the CDK
- Enabled inclusion of Sleeper instances in another CDK app
- Added an option to delete log groups when a Sleeper instance is deleted
- Stopped deploying Athena integration by default, as it is experimental

Logging:
- Added ability to configure logging for Rust code
- Clarified logs in task scaling lambdas
- Reduced noisy logging in compaction creation
- Added further logging in bulk export processor lambda

Upgrades:
- Upgraded to AWS EMR 7.12.0
- Upgraded to Apache Spark 3.5.6
- Upgraded to Apache DataFusion 51.0.0

Build:
- Added scripts to publish Docker images and Maven artifacts to remote repositories

System tests:
- Nightly system tests now run in parallel, in a number of smaller test suites

Bugfixes:
- Ingest batcher can now accept files held in the table data bucket
- Docker-based CLI no longer leaves behind old Docker images during upgrade
- Avoided hitting API Gateway size limits during web socket queries
- DataFusion compactions no longer change the type of fields from int to long when applying aggregation
- Stopped scaling compaction cluster to zero during redeployment, removed instance property `sleeper.compaction.ec2.pool.desired`
- Disabled provenance on Docker images for AWS Lambda
- Each call to run a web socket query in the Python `SleeperClient` now produces a single query for Sleeper
- Resolved problems running integration tests w/Testcontainers on latest Docker version


## Version 0.33.0

### 26th September, 2025

This contains configuration to apply aggregation and filtering in Java or DataFusion.

Compaction:
- DataFusion compactions are now no longer experimental
- Aggregation and filtering can now be set in table properties
  - This can be applied by Java or DataFusion
  - This can be combined with custom iterators in Java
  - Aggregation supports sum, min, max, and map operators, grouping by all row and sort keys
  - Filtering supports age off
- Removed experimental "AGGREGATORS" value for iterator class name
- Replaced table property `sleeper.table.compaction.method` with `sleeper.table.data.engine`

Query:
- Added experimental DataFusion query command line client
- Added queries via a web socket to the Python client API

Ingest:
- Files submitted for ingest or bulk export must now end with `.parquet`

Build:
- Removed cross-rs from the project, multiplatform Rust builds are now done in custom Docker-based builders
- Experimental option to pre-build and publish fat jars to a Maven repository

Documentation:
- Documented which Docker images are uploaded in deployment guide

Bugfixes:
- A number of instance properties now trigger redeployment with the CDK when they did not before
- A DataFusion compaction that results in no output data no longer corrupts the state store
- If a table is deleted when files are held for it in the ingest batcher, this no longer prevents the batcher from creating jobs
- It's no longer possible to split a partition and leave the parent partition marked as a leaf


## Version 0.32.0

### 29th July, 2025

*Note: this release contains breaking changes. It is not possible to upgrade from a previous version of Sleeper
to version 0.32.0*

This contains fixes to compaction with DataFusion, and preparation for deployment from published artefacts.

Ingest:
- All ingest systems (standard, bulk import, batcher) now allow filenames that start with the scheme s3a:// or s3://
  - Previously the scheme had to be omitted

Iterators:
- Experimental aggregation configuration is now applied to queries, standard ingest and compactions in Java
  - This is set as an iterator, but previously it was only allowed in compactions with DataFusion
  - This can be configured as in issue https://github.com/gchq/sleeper/issues/4344
  - This will be redesigned in issue https://github.com/gchq/sleeper/issues/5102

Query:
- The web socket query client in Java now parses returned rows for further processing, where previously they were returned as strings
- Added experimental example code to query via a web socket in Python

Reporting:
- Included reasons for failures in reports on unfinished jobs

Build:
- Added `scripts/deploy/deployToDockerRepository.sh` to publish all Docker images to a repository

Upgrades:
- Updated to latest version of Athena Query Federation SDK
- Updated to Trino 435
- Removed all use of AWS SDK v1

Misc:
- Renamed the concept of "records" to use "rows" instead
- Removed the concept of "active files" in favour of referenced/unreferenced files

Documentation:
- Documented which Docker images are needed when manually deploying the system
- Improved design documentation of DataFusion compaction and aggregation
- Improved documentation of test strategy

Bugfixes:
- Compactions with DataFusion now produce correctly sorted data at large scales
- Fixed `scripts/utility/runCompactionTasks.sh` to invoke the correct Java class


## Version 0.31.1

### 1st July, 2025

This is a bug fix release for the Python API.

Compaction:
- Running compaction jobs are now included when limiting the number of jobs that should be created

Iterators:
- Added Java `AggregationFilteringIterator` equivalent to experimental DataFusion aggregation support
  - This can be used in compaction and/or queries, but will be replaced with generic support for filtering and aggregation
- Converted experimental age off filter to use milliseconds rather than seconds

Build:
- Improved Checkstyle linting of Java code

Bugfixes:
- The Python API can now read an instance properties file with a % sign in a property value
  - This was preventing the Python `SleeperClient` from being instantiated
  - This was a conflict with Python's configparser variable interpolation, which we no longer use
  - This occurred when explicitly setting the default values for properties that include a % in their defaults
  - Default values are normally only set explicitly when deploying in a couple of ways:
    - From templates, like `scripts/deploy/deployNew.sh ${ID} ${VPC} ${SUBNETS}` with no explicit properties file
    - Setting a properties file based on the full example at [`example/full/instance.properties`](example/full/instance.properties)
- The Python API can now reliably read SQS queue URLs
  - This was a new bug introduced in the previous release, as before that it looked up queues by their name
  - Some queue URLs are saved in the instance properties file with an escaped colon character, like `http\://...`
  - This prevented the Python API from sending messages to those queues


## Version 0.31.0

### 24th June, 2025

This includes the first version of bulk export, upgrading from AWS SDK v1 to AWS SDK v2, and upgrading EMR, Spark and
Hadoop.

Bulk export:
- Added an optional `BulkExportStack` to export a whole Sleeper table
  - Will write one Parquet file per Sleeper partition

Compaction:
- Better logging messages for DataFusion compactions
- Added experimental support for aggregation and filtering in DataFusion compactions

Bulk import:
- Bulk import job definitions are now deleted from the bulk import bucket after they are read by Spark

Clients:
- Improved usability of Java `SleeperClient` with multiple instances of Sleeper
- Can submit bulk export queries with `SleeperClient` in Java or Python
- Can submit files to the ingest batcher with Python `SleeperClient`, which was previously only in Java
- Added a limit for the number of records to read when estimating table split points
- Admin client now lists tables when asking which table you want to use
- Added a timeout to the web socket query client

Documentation:
- Restructured introductory documentation:
  - Simplified readme
  - Streamlined getting started guide
  - Restructured deployment & usage guides
- Improved ingest & tables documentation
  - Added getting started section to ingest guide
  - Improved guidance for choosing an ingest system
  - Explained pre-splitting a Sleeper table
- Separated user defined from CDK defined properties in documentation
- Improved Javadoc for ingest with `SleeperClient`
- Added design diagrams for:
  - Ingest
  - Bulk import
  - Ingest batcher
  - Garbage collection
- Added coding conventions and test strategy under developer guide

Upgrades:
- Upgraded all AWS clients to Java SDK v2
  - S3 SDK v1 is still used by the experimental Athena integration
- Upgraded AWS EMR to 7.9.0
- Upgraded Apache Spark to 3.5.5
- Upgraded Hadoop to 3.4.1

Build:
- Enforced dependency version convergence in Maven build
- Enforced no duplicate Maven dependencies in the same pom
- Removed use of Hadoop in most modules
- Added tests and linting for Python API

System tests:
- When draining an SQS queue for assertions, added retries if no messages are found, to capture full contents of queue
- Support for future test parallelisation
  - Multiple tests can run different data generation tasks in ECS at the same time
  - Multiple tests can wait on deployment of data generation ECS cluster at the same time

Bugfixes:
- DataFusion can now write the output of a compaction to a different bucket than the input files
- DataFusion can now read compaction input files from different buckets
- Ingest batcher can no longer discard submitted files if there's a network failure
- Compaction jobs report no longer crashes when a job run has no started status update
- Scripts now work when multiple versions of Sleeper are present in the jars directory

## Version 0.30.1

### 28th April, 2025

*Note: this may be a breaking change if you have deployed with your own custom-named ECR repositories, see below under Deployment.*

This includes fixes for the new Java client API to directly interact with a Sleeper instance.

Clients:
- Added ability to check if a Sleeper table exists in `SleeperClient`
- Added ability to submit files to the ingest batcher in `SleeperClient`

Deployment:
- Separate instance properties have been removed for the names of ECR repositories holding Docker images to be deployed.
  These names are now derived from `sleeper.ecr.repository.prefix`.

Bugfixes:
- `SleeperClient` now has the table properties store set correctly by default
- `SleeperClient` now extends `AutoCloseable`, and will shut down thread pools and AWS clients as necessary when closed
- Files containing batches of compaction jobs are now deleted from the data bucket once the batch has been sent
- Files containing bulk import jobs read by Spark are now deleted after they have been read
- Compaction on DataFusion can now handle files where the first column is not a row key


## Version 0.30.0

### 14th April, 2025

This includes improvements to garbage collection to keep up with larger numbers of compaction jobs.

Garbage collection:
- Increased throughput and batch size when deleting files which are no longer needed after compaction
- Added instance property for maximum number of files to delete per garbage collection invocation

Bulk import:
- Handling for cases where many bulk import jobs are submitted at once
  - Increased retries with backoff when rate limited submitting Spark jobs
  - Requeued jobs with a delay when persistent EMR cluster is full

Compaction:
- Applied Parquet statistics truncate length in compactions with DataFusion

Clients:
- Added Java API to directly interact with a Sleeper instance (see usage guide)

Documentation:
- Added descriptions of every instance and table property
- Added design risks and mitigations document
- Added potential deployment improvements document
- Improved overview of ingest, added explanation of ingest job tracker
- Added diagrams and details of transaction log state store design
- Updated release process
- Updated design diagrams with 2025 AWS icons

Build:
- Switched from GCC to Clang for C/C++ code used in Rust
- Stopped building some unused jars

Bugfixes:
- Metadata of state store snapshots is now deleted when a Sleeper table is deleted
- Granted permission for compaction dispatcher to send unretryable compaction batches to dead letter queue
- Restored ability to run compactions in a LocalStack deployment
- Avoided query executor cache invalidation when a Sleeper table is renamed


## Version 0.29.0

### 12th March, 2025

State store:
- Removed DynamoDB & S3 state store implementations
- Introduced a transaction log follower lambda following a DynamoDB stream
- Moved job tracker updates derived from state store commits from state store committer lambda to follower lambda
- Split table state snapshots into Arrow record batches
- Replaced method signatures to apply updates to the state store
  - Ensured all updates are defined as transactions
  - Clarified differences between synchronous and asynchronous commit

Documentation:
- Added design diagrams of how compaction jobs are created and run

Build:
- Use LocalStack for all DynamoDB tests, avoiding intermittent test failures due to lack of transactional integrity in DynamoDB local
- Fixed intermittent failures where built jars had Hadoop file system implementations set incorrectly
- Suppressed Rust build output in Maven quiet mode builds

System tests:
- System test for throughput of compaction commits
- Parallelised emptying and draining SQS queues in system tests
- Tools for debugging table state after a system test has run, in `CheckTransactionLogs`

Bugfixes:
- Prevented state store transactions being uploaded to S3 multiple times

## Version 0.28.0

### 31st January, 2025

*Note: this release contains breaking changes. It is not possible to upgrade from a previous version of Sleeper
to version 0.28.0*

This includes further batching to allow for much larger numbers of compaction jobs.

Compaction:
- Added a step to combine commits of finished compactions into one transaction in the state store
  - This is done in a new lambda and SQS queue, and enabled by default
- Added an option to disable updating the job tracker when a compaction is committed to the state store asynchronously
- Reduced number of S3 GET requests made during DataFusion compaction
- Added further metrics in logging during DataFusion compaction

State store:
- Improved throughput in the state store committer by avoiding writes to S3
  - Transactions are now created before being sent to the committer, rather than derived from separate requests
  - Large transactions are uploaded to S3 before being sent

Bulk import:
- Switched bulk import on EMR to run on Graviton by default

Deployment:
- Aligned configuration of GC lambda timeout to use seconds instead of minutes, similar to other lambdas
- Added descriptions to some schedules and alarms which did not have one, this is visible in the AWS console

Reporting:
- The stores used to generate reports are now referred to as job trackers and task trackers rather than status stores
- Job started times are now only reported once, rather than duplicated in finished and failed status updates

Documentation:
- Reorganised documentation into folders
- Added usage guide
- Updated out of date information under common problems and their solutions
- Added an explanation of record batch types in standard ingest
- Added further information about environment deployment with `sleeper environment` CLI commands

Build:
- Upgraded LocalStack to latest version
- Moved example iterator classes to their own module and created an example user jar that includes them

Misc:
- Efficiency improvements to library configuration for working with Parquet files in DataFusion and Java
- Split configuration property for queue visibility timeout into two, for ingest and query results

System tests:
- Scheduled rules for background processes are now enabled during system tests
  - Some system tests handle this by taking tables offline or online
- Some improvements to test isolation and preparation for concurrent execution
- Added system tests for custom iterators defined in a user jar

Bugfixes:
- Prevented an intermittent failure during teardown where the CDK tried to delete managed policies before the roles that use them
- Dev container no longer requires AWS, Maven or SSH configuration folders to exist before it starts

## Version 0.27.0

### 13th December, 2024

*Note: this release contains breaking changes. It is not possible to upgrade from a previous version of Sleeper
to version 0.27.0*

This includes batching to allow for much larger numbers of compaction jobs.

Upgrades:
- Upgraded Apache DataFusion to 43.0.0

Compaction:
- Parallelised sending compaction jobs in batches across instances of a new lambda
  - Creation of batches is now separate from creation of individual jobs to be run on tasks
  - Can now create over 100,000 compaction jobs per scheduled invocation
- Reduced duplication of unnecessary compaction runs by pre-validating jobs before sending
- Added configuration of a limit per Sleeper table for the number of compaction jobs created per scheduled invocation
- Improved estimates when scaling EC2 instances to run compaction tasks
  - Determines available CPU and memory based on EC2 instance type
  - Added a configurable overhead to avoid slowdown due to overprovisioning

Deployment:
- Improved handling of cases where AWS account concurrency limit may be approached
  - Defaulted most lambdas to a maximum concurrency of 10 instances
  - Defaulted reserved concurrency for state store committer lambda to 10 instances
- Increased default memory requirement for lambdas that work with Sleeper table state

Reporting:
- Compaction jobs only show as created once they've been sent from a batch

Documentation:
- Reorganised documentation of scripts & clients
- Documented issue with compaction on LocalStack

Build:
- Automated checking Maven dependencies are included in the NOTICES file

System tests:
- Removed DynamoDB state store implementation from nightly system test suite
- Some improvements to test isolation and preparation for concurrent execution
- Enabled some scheduled rules in system tests that are normally enabled in a real system

Bugfixes:
- Fixed connecting to an existing deployment with `sleeper environment add`, removed use of old CDK output format
- Fixed deployment with reporting status stores disabled
- Fixed compaction with DataFusion generating invalid sketches for integer and long type row keys
- Fixed bulk import on EKS incorrectly using Netty Arrow implementation


## Version 0.26.0

### 12th November, 2024

This includes upgrades to Java, EMR, Spark and the AWS SDK, and improvements to the deployment process.

Upgrades:
- Fully upgraded from Java 11 to 17
- Upgraded AWS EMR to 7.2.0
- Upgraded Apache Spark to 3.5.1
- Upgraded a number of AWS clients to Java SDK v2
  - Kept some back to avoid breaking size limit of lambdas including the v1 SDK from Hadoop
  - We use the Hadoop S3 integration to read/write Parquet files
  - Prepared for further upgrades when EMR moves to a version of Hadoop using SDK v2

Compaction:
- Made it optional to wait for input files to be assigned to a compaction before beginning a job

Bulk import:
- Added logging via CloudWatch in bulk import on EKS

State store:
- Added ability to configure timeout of snapshot creation lambda
- Changed configuration of snapshot creation frequency from minutes to seconds
- Improved speed of reading transactions for files with many references

Deployment:
- Added ability to remove an optional stack and then redeploy it
  - Log groups are retained and kept under management of the CDK separately
- Added an option to deploy all lambdas in Docker containers
- Added ability to deploy a Sleeper instance with no tables
- Added ability to deploy a Sleeper instance with no optional stacks enabled
- Adjustments for further control of log retention & permissions settings
  - Replaced built-in CDK S3 object autodelete with a custom lambda
- Added encryption of EMR EBS volumes
- Added instance properties to set encryption keys in Athena and EMR
- Automation in `sleeper environment` deployment
  - Automated setup of nightly system tests
  - Automatic start and shutdown of EC2 instances

Reporting:
- Added information to partitions status report
  - Counts of files assigned to jobs
  - Estimates of records on a partition if all files were compacted down the tree
- Added percentiles to statistics in compaction jobs status report

Documentation:
- Added an overview of dependency conflicts
- Added missing Javadoc to the configuration module
- Further design for PostgreSQL state store
- Documented supported operations on Sleeper tables

CLI:
- Improved output when CLI fails due to missing Docker images

System tests:
- Added a template to override properties during JUnit-based system tests
- Added system tests for:
  - Transaction log snapshot creation
  - Compaction performance with Apache DataFusion
- Allowed running deployAll system test without initially writing any data

Bugfixes:
- Fixed deleting jars bucket during tear down
- Fixed memory overhead default value to run expected number of executors in bulk import jobs
- Prevented a case where compaction on EC2 occasionally did not deploy as a needed permission was deployed after the autoscaling group
- Avoided retrying invalid compaction jobs where the file references have been reassigned or deleted
- Avoided retrying compaction jobs for a Sleeper table which no longer exists
- Fixed deploying an instance without checking for endpoints in the VPC
- Fixed garbage collection deleting too many files to fit in an SQS message


## Version 0.25.0

### 19th September, 2024

This includes improvements to the transaction log state store implementation, and stabilisation of accelerated
compactions with Apache DataFusion.

State store:
- Asynchronous commit enabled by default for transaction log state store
  - Routes state store updates to a single lambda instance per Sleeper table
  - Avoids throughput reduction due to contention
- Asynchronous commit can be enabled/disabled per table, per update type
- Added asynchronous commit for remaining state store update types
  - Assigning input files to a compaction job
  - Partition splitting
  - Garbage collection
- Asynchronous commit of state store updates too big to fit in an SQS message
  - Stored in S3
- Removed the need to check transaction log DynamoDB table between every update in the committer lambda
- Converted state store commit queue to high throughput FIFO

Compaction:
- Added a limit to the number of compaction jobs created per lambda invocation
- Improved speed of compaction job creation, the rate at which jobs can be created per Sleeper table
- Increased the default delay of retrying a failed compaction job

Reporting:
- Added retry counts in summaries of ingest & compaction job reports
- Added input file assignment to compaction jobs reports
  - Status updates & times
  - Statistics of delay between creation and file assignment

Monitoring:
- Renamed metrics for files in a Sleeper table to distinguish files from references

Deployment:
- Added optional limits for concurrency of lambdas that scale per Sleeper table
- Added optional limits for concurrency of partition splitting
- Added property to set timeout for garbage collection
- Added validation to property setting optional deployment stacks

Docker CLI:
- Removed versions linked to releases of Sleeper
  - Only latest images are published to GitHub Container Registry
  - Based on develop branch
- Converted to use a non-root user in Docker images
  - Permissions are now preserved in host directories mounted into container

Documentation:
- Updated design of jobs vs tasks, state store vs status store
- Explained system test use of subnets & availability zones

Testing:
- Automated tests of Apache DataFusion accelerated compactions
- Automated tests of throughput of asynchronous commit to state store through a lambda per Sleeper table
- Converted performance tests to run on AWS EC2 rather than Fargate
- Linting & dependency checks for Rust code
- Automated checks for unused Maven dependencies

Bugfixes:
- Closed input files at end of standard ingest
- Added retries during throttling failures when a DynamoDB table is scaling up
  - When waiting for compaction job files to be assigned
  - When committing state store updates
- Fixed cases where failures were not reported to status store when committing a compaction to the state store
- Stopped deploying default optional stacks when property is set to an empty string
- Stopped reporting failed compaction jobs when Sleeper table was deleted

## Version 0.24.0

### 17th July, 2024

*Note: this release contains breaking changes. It is not possible to upgrade from a previous version of Sleeper
to version 0.24.0*

This includes improvements to the transaction log state store implementation, and experimental support for accelerated
compactions.

State store:
- Added a lambda to delete old transactions and snapshots.
  - This is specific to the `DynamoDBTransactionLogStateStore` type.
  - This may reduce the load on the transaction log by reducing the amount of data held.
  - This limits retention of unneeded data.
- Converted state store implementations to store data in Arrow format.
  - This should mean faster loading of snapshots for the transaction log state store, and data for the S3 state store.

Ingest:
- Added an option to add files to the state store asynchronously.
  - Converted the previous handler for committing compaction jobs asynchronously, to also handle adding files.
  - All asynchronous state store updates for each Sleeper table are applied in a single lambda instance.
  - This allows for higher throughput by reducing contention, particularly when using a transaction log.
  - Can be turned on/off separately for ingest, bulk import and compaction.

Compaction:
- Added an option to run compactions using Apache DataFusion instead of Java.
  - This is currently experimental, but allows for much faster compaction on the same hardware.
- Added a property for the number of retries on the compaction job queue.

Reporting:
- Added reporting of state store updates for compaction and ingest.
  - Reports any delay between finishing and committing to the state store for compactions.
  - Reports when files are added partway through an ingest job.
- Added reporting of failures in compaction, bulk import and ingest jobs.

Scripts:
- Added a script to estimate partition split points for a Sleeper table based on a sample of data.

Deployment:
- Adjusted SQS queue names for consistency.

Docker CLI:
- Removed deployment Docker image for deploying a pre-built version of Sleeper (`sleeper deployment`).

Documentation:
- Added missing Javadoc to the state store module.
- Added missing Javadoc to the ingest-core module.

Bugfixes:
- Restored Trino plugin's ability to interact with Sleeper files in S3 via the Hadoop file system.
- Explicitly grants permission for the bulk import starter to add tags to a non-persistent EMR cluster.
- Prevented truncating resource names deployed as part of Sleeper.
- Fixed cases where an ingest or compaction has finished but is still displayed as in progress in the jobs report.
- Prevented cases where compactions could run to completion but be unable to apply in the state store.
  - Compaction tasks now wait for input files to be assigned in the state store before they start a compaction job.

## Version 0.23.0

### 23rd May, 2024

This contains the following improvements:

Tables:

- Updated queues used by scheduled operations to be FIFO queues. This is to ensure only one scheduled action is running at a time for each Sleeper table.

State store:

- Added a new state store type - `DynamoDBTransactionLogStateStore`.
  - This state store intends to improve on update times by only dealing with changes that have happened since the last state. This is an improvement over the `S3StateStore`, which always loads the entire state of the state store, regardless of how little has changed since the last revision.
  - This state store has been made the default state store.
- Data files in S3 are now stored in `data/partition_<id>/` rather than just `partition_<id>`.

Compactions:

- Added a new lambda to commit the results of compaction jobs asynchronously.
- Added a retry to compaction jobs committing in the case where the input files have not yet been assigned to the job.

Deployment

- Added a way to keep query lambdas warm using the optional stack `KeepLambdaWarmStack`.
- Added dashboard error metrics for all dead letter queues.
- Added ability to deploy Sleeper instances without using the `AdministratorAccess` policy.

Documentation
- Added a step in GitHub Actions to fail if Javadoc is missing.
  - Some modules have been suppressed as there is too much to update at once.
- Added missing javadoc in the core module.

System tests:

- Added system tests for compactions running on EC2.
- Added system tests for queries running against a WebSocket.
- Added ability to run multiple ingests in a single system test task.


Bugfixes:

- Fixed an issue where the WebSocket query script failed to print records to the console.
- Fixed an issue where the WebSocket query script failed to run 2 consecutive queries.
- Fixed an issue where CDK would fail to update if there were too many tables in an instance.
- Fixed an issue where the file status report would fail when a file belongs in a partition that no longer exists.
- Fixed an issue where the system tests did not set the Hadoop configuration correctly when loading records in queries.

## Version 0.22.1

### 25th March, 2024

This is a bug-fix release. It contains a fix of the following bug:

- Fixed bug in compaction task starter logic that was introduced in 0.22.0.

## Version 0.22.0

### 22nd March, 2024

*Note: this release contains breaking changes. It is not possible to upgrade from a previous version of Sleeper
to version 0.22.0*

This contains the following improvements:

Tables:

- Increased scalability of creating and running compaction jobs when using multiple tables.
- Increased scalability of partition splitting when using multiple tables.
- Increased scalability of garbage collection when using multiple tables.
- Increased scalability of table metrics when using multiple tables.
- Added ability to delete a table using the script `scripts/deploy/deleteTable.sh`.
- Added ability to rename a table using the script `scripts/deploy/renameTable.sh`.
- Added ability to take a table offline and put a table online using the scripts `scripts/utility/takeTableOffline.sh`
and `scripts/utility/putTableOnline.sh` respectively. Partition splitting and compactions will not be performed on
offline tables, but you will still be able to perform ingests and queries against them.
See the documentation [here](docs/design.md#tables) for more information.

Compaction:

- Added new properties to configure how long compaction tasks will wait for compaction jobs before they terminate.
- Added script to allow you to create compaction tasks manually, even if there are no compaction jobs on the queue
using the script `scripts/utility/startCompactionTasks.sh`.
- Added ability to create compaction jobs for a specific table using the script `scripts/utility/compactAllFiles.sh`.

Ingest:

- Added a new property `INGEST_FILE_WRITING_STRATEGY` to specify how files are created when performing an ingest.

Query:

- Moved the WebSocket API into a new optional stack called `WebSocketQueryStack`.

System tests:

- Added tests for running compactions in parallel, in preparation for the next milestone.

## Version 0.21.0

### 8th February, 2024

*Note: this release contains breaking changes. It is not possible to upgrade from a previous version of Sleeper
to version 0.21.0*

This contains the following improvements:

Compactions:

- The concept of splitting compactions has been removed. More information about how splits are now done can be
  found [here](docs/design.md#compactions).
- Compactions now use references and the partitions they exist in to only read and compact data within that partition.
- File splitting now happens in the compaction job creation lambda, before compaction jobs are created.
- Updated javadoc for `CompactionStrategy` and `LeafStrategy` classes.
- Added script to force creation of compaction jobs, ignoring batch size and strategy - `scripts/utility/compactAllFiles.sh`.

State store:

- Files are now split by storing references to them in the state store. More information about how file references are
  stored can be found [here](docs/design.md#state-store).
- `FileInfo` has been renamed to `FileReference`.
- Renamed several state store methods to reflect new file reference changes.
- Improved logging in the `S3StateStore` update process.
- Improved detail of `FilesStatusReport` record counts.
- New exceptions have been created to reflect specific failures in the state store.

Query:

- The query execution lambda has been split into 2. One lambda is for running the base query, the other handles
  sub-queries.
- Updated `SqsQueryProcessor` to periodically refresh the cache of `QueryExecutor`s.

Tests:

- Realigned `FileReferenceStoreIT` tests with different implementations.
- Improve speed of `DynamoDBStateStoreIT` tests.
- Tidy file assertions in `IngestRecords*IT` tests.

System tests:

- Separated functional system test suite into slow tests to be run nightly, and faster tests to be run any time.
- Added system test for EKS bulk import.
- Added system test for compactions.
- Improved logging in `SQSQueryDriver`.
- Improved file assertion output in partition splitting tests.

Bugfixes:

- Fixed an issue where `QueryCommandLineClient` set the wrong minimum if the user required no minimum.
- Fixed an issue where if the state store update failed after finishing a compaction, files will never be compacted.
- Fixed an issue where the persistent EMR bulk import system test failed because the instance properties were not fully
  reloaded.
- Fixed an issue where system tests could not tear down an empty instance with `instance.properties` file deployed.
- Fixed an issue where system tests would sometimes see old DynamoDB states after clearing table data.
- Fixed an issue where `ManagedPoliciesStack` failed to deploy if no ingest source buckets were set.
- Fixed an issue where `AddTable` would sometimes fail if credentials provider was not set.
- Fixed an issue where compaction jobs could be picked up by a task and finished before the state store assigns the
  input files to the job.
- Fixed an issue where `GarbageCollector` would stop processing files if an` IOException` is thrown when trying to
  delete a file.
- Fixed an issue where the `TableDataStack` failed to deploy if no ingest source buckets were set.
- Fixed an issue where the EMR bulk import performance test failed because it was expecting a running ingest task.
- Fixed an issue where the `S3StateStore` did not throw an exception if all update attempts fail.
- Fixed an issue where running compaction jobs with large numbers of input files could time out.
- Fixed an issue where running the `deployAll` system test would throw an `AccessDeniedException` when trying to access
  the system test bucket.
- Fixed an issue where running the `deployAll` system test would throw an `AccessDeniedException` when trying to access
  the ingest source bucket.

## Version 0.20.0

### 20th November, 2023

*Note: this release contains breaking changes. It is not possible to upgrade from a previous version of Sleeper
to version 0.20.0*

This contains the following improvements:

Tables:

- Tables are now internally referenced by a unique ID assigned upon creation. This is in preparation for
  adding the ability to rename tables in the future.
- Improved support for lots of tables in compaction and ingest status stores by updating the hash key of
  the DynamoDB tables.
- Table related infrastructure is now shared between all tables. The following resources are now only deployed once:
    - Table data bucket.
    - Table metrics lambda.
    - State store.
- Table initialisation is no longer performed by CDK.
    - A new client class `AddTable` is now responsible for initialising tables.
- Added configurable timeout property for `TablePropertiesProvider`.

State store:

- The default state store has been updated to the `S3StateStore`.
- The `minRowKey`, `maxRowKey`, and `rowKeyTypes` fields have been removed from the `FileInfo` class.

Ingest:

- Added instance property to allow setting the S3 upload block size.

Bulk import:

- Added support for overriding spark configuration and platform specification in EMR serverless jobs.
- Added support for setting the initial capacity of the EMR serverless application.
- Added support for enabling EMR Studio by using the optional stack `EmrStudioStack`.

Clients:

- The admin client now respects the `EDITOR` environment variable when updating properties.
- Adding an optional stack in the admin client now uploads docker images if the new stack requires one.

Query:

- Validation failures for queries are now recorded in the `DynamoDBQueryTracker`.
- Added client to view status of query tracker in `scripts/utility/queryTrackerReport.sh`.
- Removed inheritance relationship between `Query` and `LeafPartitionQuery`.

Tests:

- Added system tests for using the `S3StateStore`.
- System tests now purge relevant SQS queues if a test fails.
- Improved performance of `ingest-runner` module tests.
- Added system tests with many tables in one instance.

Bugfixes:

- Fixed an issue where the python API would not generate unique IDs for each query.
- Fixed an issue where the instance ID length was not being validated correctly.
- Fixed an issue where trying to bulk import using EMR serverless to a table using `S3StateStore` would
  throw a `NullPointerException`.
- Fixed an issue where sending an ingest job with a null file would not report the job as invalid.
- Fixed an issue where the role assumed by tasks in the system test data generation cluster exceeded the maximum size.
- Fixed an issue where the CDK deployment would fail if an ingest source bucket was not set.
- Fixed a conflict between temporary directory paths used by the CLI.

## Version 0.19.0

### 20th September, 2023

This contains the following improvements:

Bulk Import:

- Use official Spark Docker image for bulk import in an EKS Kubernetes cluster
- Added support for EMR Serverless in the ingest batcher

Deployment:

- Added ability to deploy to Docker with LocalStack

Tests:

- Converted all system tests to run through JUnit-based DSL
- Added performance test for ingest
- Added functional system tests for
    - Ingest
    - Queries, SQS and direct
    - Bulk import via persistent EMR cluster
    - Python API
- Added Maven site reports to nightly system test output
- Improved output of Sleeper reports in nightly system tests
- Allow running either functional or performance test suite in nightly system tests
- Improved integration test coverage of ingest and partition splitting

Documentation:

- Documented system tests
- Updated deployment guide
- Updated getting started guide
- Updated release process

Misc:

- Added a utility script to show Parquet file page indexes
- Upgraded to use EMR version 6.13.0 and Spark 3.4.1
- Logging improvements in ingest, compaction, ingest batcher
- Use Python conventions for non-release version numbers in Python code

Bugfixes:

- Ingest batcher can now include any number of files in a batch
- Setting a property to an empty string will now behave the same as not setting it at all
- Admin client can now load properties which were set to an invalid value, and fix them
- Made reports output correctly based on query type when the query type was prompted
- Improved teardown of EMR Serverless to properly stop jobs and application before invoking CDK
- Scripts no longer fail when CDPATH variable is set
- Stopped building unused fat jars
- Stopped tear down and admin client failing when generated directory does not exist
- Prune Docker system in nightly tests to avoid disk filling up with images

## Version 0.18.0

### 10th August, 2023

This contains the following improvements:

Bulk Import:

- Support use of EMR serverless.
- Support use of Graviton instances in bulk import EMR.
- Report on validation status for standard ingest jobs and bulk import jobs.
- Added option to query rejected jobs in the `IngestJobStatusReport`.
- Support use of instance fleets in bulk import EMR.
- Updated default x86 instance types to use m6i equivalents.

Build:

- Converted GitHub Actions to run on pull requests from forks.
- Use Maven site to generate HTML reports on tests and linting failures.

Environment:

- Allow multiple users to access cdk-environment EC2.
- Allow users to load the configuration for an existing deployed environment without redeploying.

Deployment:

- Added ability to deploy to multiple subnets.
- Split properties templating from the deployment process, allowing you to specify your own configuration file
  while still defaulting to the templates if a configuration file is not provided.
- Added retry and wait for running ECS tasks when capacity is unavailable.
- Updated performance test documentation.

Tests:

- Remove irrelevant properties from system test configurations.
- Created a JUnit test framework for system tests.
- Converted ingest batcher system test to use new JUnit test framework.

Documentation:

- Added high-level Sleeper design diagram.
- Added details on how to contribute and sign the CLA [here](CONTRIBUTING.md).

Misc:

- Split user defined instance property declarations by property groups.
- Added issue and pull request templates.
- Added VSCode configuration files.
- Update and manage several dependencies to resolve CVEs found by dependency check.
- Added a way to visualise internal dependencies between Maven modules.
- Added a property to force reload the configuration whenever a Lambda is executed.

Bugfixes:

- Fixed an issue where files under directories were not counted correctly in `IngestJobStatusReport`.
- Raised timeout for system tests when waiting for lambdas to run.
- Fixed an issue where the jars bucket failed to tear down because it was not empty during the tearDown process.
- Raised timeout for Lambda starting bulk import jobs
- Stopped Lambda starting bulk import jobs processing multiple jobs at once.
- Fixed an issue where submitting a bulk import job twice with the same ID twice would overwrite the first one.
- Fixed issues where passing one CDK parameter into the environment CLI commands would ignore the parameter.
- Fixed an issue where if a stack failed to delete during the tear down process, it would keep waiting for the
  state to update until it timed out.
- Stopped Docker CLI wrapping terminal commands onto the same line.

## Version 0.17.0

### 9th June, 2023

This contains the following improvements:

Ingest batcher:

- Added a new system for batching files into ingest jobs. See [docs/usage/ingest.md](./docs/usage/ingest.md)
  and [docs/design.md](./docs/design.md) for more information.

Bulk Import:

- Upgrade EMR version to 6.10.0.
- Upgrade Spark version to 3.3.1.

Development:

- Added devcontainers support.
- Added a script to regenerate properties templates from property definitions.
- Added OWASP Dependency-Check Maven plugin.

Tests:

- Added a way to automatically run system tests and upload the results to an S3 bucket.
- Increase rate at which Fargate tasks are started in system tests.

Misc:

- Upgrade parquet-mr version to 1.13.0.
- Rename `LINES` to `RECORDS` in reports and throughout the project.
- Update properties templates.

Bugfixes:

- Fixed an issue where ingest tasks reported an ingest rate of NaN when exiting immediately.
- Fixed an issue where the default value for a table property did not display when confirming changes in the admin
  client if the property was unset.
- Fixed an issue where tearing down an instance would fail if the config bucket was empty.

## Version 0.16.0

### 2nd May, 2023

This contains the following improvements:

Trino:

- Added the ability to query Sleeper tables using Trino, see the documentation [here](docs/usage/trino.md). This is an
  experimental feature.

Bulk Import:

- Improve observability of bulk import jobs by including them in ingest job status reports.
- Added table property for minimum leaf partition count. If the minimum is not met, bulk import jobs will not be run.

Scripts:

- Added logging output to `DownloadConfig` class.
- Added ability to define splitpoints file in `deploy.sh`.
- Added runnable class to remove log groups left over from old instances (`CleanUpLogGroups`).

CDK:

- Added the flag `deployPaused` to deploy the system in a paused state.
- Add the tag `InstanceId` to all AWS resources when they are deployed.
- Pre-authenticate the environment EC2 instance with AWS.

Clients:

- Added count of input files to compaction job report.
- For persistent EMR bulk import, report on steps that have not started yet in the ingest status report.
- Avoid loading properties unnecessarily in the admin client.
- Refactor compaction and ingest reports to remove unnecessary wrapping of arguments.

Tests:

- Simplify `compactionPerformance` system test to only perform merge compactions.
- Assert output of `compactionPerformance` system test to detect failures
- Create `partitionSplitting` system test, which do not perform merge compactions and only perform splitting
  compactions.
- Create `bulkImportPerformance` system test, which performs a bulk import and does no merge/splitting compactions.
- Reduce code duplication in Arrow ingest test helpers.
- Introduce test fakes for querying properties and status stores in the admin client and reports.

Bugfixes:

- Fixed issue where the queue estimates sometimes did not update before invoking the compaction task lambda in
  the `compactionPerformance` system test.
- Fixed issue where the `tearDown` script failed if non-persistent EMR clusters were still running.
- Fixed issue where `WaitForGenerateData` was excluding 1 task from checks, causing it to not wait if the number of
  tasks was 1.

## Version 0.15.0

### 30th March, 2023

This contains the following improvements:

Standard ingest:

- Added ability to define multiple source buckets.

Tables:

- Added ability to export partition information to a file.

Scripts:

- Added a script to add a new table to an existing instance of Sleeper (`scripts/deploy/addTable.sh`).
- Added a script to bring an existing instance of Sleeper up to date (`scripts/deploy/deployExisting.sh`).
- Replace the deployment scripts with Java.

Docker CLI:

- Added a builder docker image, to be used inside EC2 to run scripts and Sleeper CLI commands (`sleeper builder`).
- Added deployment docker image to Sleeper CLI for deploying a pre-built version of Sleeper (`sleeper deployment`).
- Added a command to bring the Sleeper CLI up to date (`sleeper cli upgrade`).
- Added support for Apple M1 and other ARM-based processors.

CDK:

- Added a way to run `cdk deploy` from Java.
- Added a way to run `cdk destroy` from Java.
- Add validation for Sleeper version on `cdk deploy` by default.
- Add the Sleeper CLI to the cdk-environment EC2.
- Add versioning for Lambdas, update when code is changed on `cdk deploy`.

Clients:

- Added a "shopping basket" view to the admin client.
    - Viewing and editing properties now brings you to a text editor where you can make changes and save them.
    - Upon saving and leaving the editor, you will be presented with a summary of your changes, and have the
      option to save these changes to S3, return to the editor, or discard the changes.
    - Any validation issues will appear in the summary screen, and prevent you from saving until they are resolved.
- Properties that require a `cdk deploy` are now flagged, and the `cdk deploy` is performed after changing
  any of these properties.
- Properties are now grouped based on their context.
    - You can also filter properties by group in the admin client.
- Descriptions are now displayed above properties in the editor.
- Properties that cannot be changed (either they are system defined or they require redeploying the instance)
  are included in the validation checks when making changes in the editor.
- Added the following status reports to the admin client main menu:
    - Partitions.
    - Files.
    - Compaction jobs & tasks.
    - Ingest jobs & tasks.

Tests:

- Upgrade LocalStack and DynamoDB in Testcontainers tests.

Bugfixes:

- You can now deploy an EC2 environment into an existing VPC.
- The deploy script no longer fails to find a bucket to upload jars to after creating it.
- All records are now loaded for the compaction and ingest reports - some records were missing when there were
  too many records.
- The compaction performance test can now run with more than 100 data generation ECS tasks.
- Added transitive dependency declarations to built Maven artifacts.
- The partitions status report now displays the correct field name for the split field.
- CDK now references bulk import bucket correctly. Previously you could encounter deployment failures when
  switching bulk import stacks.
- Running the `connectToTable.sh` script no longer clear the generated directory if you encounter an AWS auth
  failure (this has been moved to  `scripts/utility/downloadConfig.sh`).
- The compaction performance test no longer fails if a job started before it was reported as created.
- The compaction performance test no longer fails if the partition splitting job queue size updates too slowly.

## Version 0.14.0

### 30th January, 2023

This contains the following improvements:

General code improvements:

- All Java modules have been upgraded to Java 11 and the project can now be built with Java 11 or 17. All
  code executed in lambdas, containers and on EMR now uses Amazon Corretto 11.
- The SleeperProperty enums have been converted into static constants. This will allow additional properties
  to be added to them more easily.
- Upgraded from JUnit 4 to 5.
- Updated versions of many dependencies, including EMR, Spark, Hadoop.

Compactions:

- The lambda that creates more ECS tasks to run compactions previously scaled up too quickly as the calculation
  of the number of tasks currently running ignored pending tasks. This meant that occasionally tasks would start
  up and find they had no work to do.

Standard ingest:

- Fixed bug where the asynchronous file uploader could fail if a file was greater than 5GB.
- Split ingest modules into submodules so that code needed for lambda to start jobs is smaller.

System tests:

- Made system tests more deterministic by explicitly triggering compactions.

Clients:

- Reports now report durations in human-readable format.

Docker CLI:

- Added a CLI for setting up an environment to deploy Sleeper
- Uploaded an image for deploying a fixed, built version of Sleeper to be used in the future

## Version 0.13.0

### 6th January, 2023

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

### 20th October, 2022

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

- Recording of the status of compaction jobs in DynamoDB, with scripts to enable querying of the status of a job or all
  jobs.
- Recording of the status of compaction ECS tasks in DynamoDB, with scripts to enable querying of the status of a task
  or all tasks.
- Various integration tests converted to unit tests to speed up build process.
- Fixed error in `DeleteMessageAction` log message.

Bulk imports:

- Persistent EMR bulk import stack now uses a lambda to submit an EMR step to the cluster to run the bulk import job.
  The job is written
  to a file in S3 in the bulk import logs bucket.
- The default class to use for bulk imports can now be configured as an instance property.
- `spark.sql.shuffle.partitions` option is now set on Spark bulk import jobs.
- The Spark properties used across the EMR and persistent EMR stacks are now consistent.
- EMR clusters used in the (non-persistent) EMR stack now have the security group provided by the user added to allow
  ssh into the master.
- Bulk import stacks now always create a logs bucket.

State store:

- DynamoDB and S3 state stores refactored into smaller classes.

Ingest:

- Default value of `sleeper.ingest.arrow.working.buffer.bytes` has been increased to 256MB.
- Improved logging in `ArrowRecordBatchBase`.
- The documentation for the `sleeper.ingest.arrow.max.single.write.to.file.bytes` parameter in the example instance
  properties
  file was incorrectly described.
- The Arrow-based record batching approach now supports schemas with maps and lists.
- The `IngestCoordinator` class no longer makes an unnecessary request to list the partitions.

CDK:

- Added CDK stack to create an EC2 instance that can be used to deploy Sleeper from.
- Added optional instance property that allows a tag name to be provided that will be used to tag the different stacks
  with the tag name.
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

### 20th June, 2022

First public release.
