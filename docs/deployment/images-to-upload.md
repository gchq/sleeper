## Docker Deployment Images
These are the Docker deployment images.<br>
A build of Sleeper outputs several directories under scripts/docker. Each is the directory to build a Docker image, with a Dockerfile.
Some of these are used for parts of Sleeper that are always deployed from Docker images, and those are listed here.<br>
* Deployment name - This is both the name of its directory under scripts/docker, and the name of the image when it's built and the repository it's uploaded to.<br>
* Optional Stack - They're each associated with an optional stack, and will only be used when that optional stack is deployed in an instance of Sleeper.<br>
* Multiplatform - Compaction job execution is built as a multiplatform image, so it can be deployed in both x86 and ARM architectures.


| Deployment Name            | Optional Stack     | Multiplatform |
|----------------------------|--------------------|---------------|
| ingest                     | IngestStack        | false         |
| bulk-import-runner         | EksBulkImportStack | false         |
| compaction-job-execution   | CompactionStack    | true          |
| bulk-export-task-execution | BulkExportStack    | false         |

## Lambda Deployment Images
These are the Lambda deployment images.<br>
These are all used with the Docker build directory that's output during a build of Sleeper at scripts/docker/lambda.
Most lambdas are usually deployed from a jar in the jars bucket, but some need to be deployed as a Docker container, and we have the option to deploy all lambdas as Docker containers as well.
To build a Docker image for a lambda, we copy its jar file from scripts/jars to scripts/docker/lambda/lambda.jar, and then run the Docker build for that directory.
This results in a separate Docker image for each lambda jar.<br>
* Filename - This is the name of the jar file that's output by the build in scripts/jars.
It includes the version number you've built, which we've included as a placeholder here.<br>
* Image name - This is the name of the Docker image that's built, and the name of the repository it's uploaded to.<br>
* Always Docker deploy - This means that that lambda will always be deployed with Docker, usually because the jar is too large to deploy directly.


| Filename                                            | Image Name                        | Always Docker deploy |
|-----------------------------------------------------|-----------------------------------|----------------------|
| athena-`<version-number>`.jar                       | athena-lambda                     | true                 |
| bulk-import-starter-`<version-number>`.jar          | bulk-import-starter-lambda        | false                |
| bulk-export-planner-`<version-number>`.jar          | bulk-export-planner               | false                |
| bulk-export-task-creator-`<version-number>`.jar     | bulk-export-task-creator          | false                |
| ingest-taskrunner-`<version-number>`.jar            | ingest-task-creator-lambda        | false                |
| ingest-batcher-submitter-`<version-number>`.jar     | ingest-batcher-submitter-lambda   | false                |
| ingest-batcher-job-creator-`<version-number>`.jar   | ingest-batcher-job-creator-lambda | false                |
| lambda-garbagecollector-`<version-number>`.jar      | garbage-collector-lambda          | false                |
| lambda-jobSpecCreationLambda-`<version-number>`.jar | compaction-job-creator-lambda     | false                |
| runningjobs-`<version-number>`.jar                  | compaction-task-creator-lambda    | false                |
| lambda-splitter-`<version-number>`.jar              | partition-splitter-lambda         | false                |
| query-`<version-number>`.jar                        | query-lambda                      | true                 |
| cdk-custom-resources-`<version-number>`.jar         | custom-resources-lambda           | false                |
| metrics-`<version-number>`.jar                      | metrics-lambda                    | false                |
| statestore-lambda-`<version-number>`.jar            | statestore-lambda                 | false                |

