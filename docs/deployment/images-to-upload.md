## Docker Deployment Images
These are the docker deployment Images

| Property Name              | Optional Stack     | Multiplatform |
|----------------------------|--------------------|---------------|
| ingest                     | IngestStack        | false         |
| bulk-import-runner         | EksBulkImportStack | false         |
| compaction-job-execution   | CompactionStack    | true          |
| bulk-export-task-execution | BulkExportStack    | false         |

## Lambda Deployment Images
These are the Lambda deployment Images

| Filename                             | Image Name                        | Always docker deploy |
|--------------------------------------|-----------------------------------|----------------------|
| athena-XXX.jar                       | athena-lambda                     | true                 |
| bulk-import-starter-XXX.jar          | bulk-import-starter-lambda        | false                |
| bulk-export-planner-XXX.jar          | bulk-export-planner               | false                |
| bulk-export-task-creator-XXX.jar     | bulk-export-task-creator          | false                |
| ingest-taskrunner-XXX.jar            | ingest-task-creator-lambda        | false                |
| ingest-batcher-submitter-XXX.jar     | ingest-batcher-submitter-lambda   | false                |
| ingest-batcher-job-creator-XXX.jar   | ingest-batcher-job-creator-lambda | false                |
| lambda-garbagecollector-XXX.jar      | garbage-collector-lambda          | false                |
| lambda-jobSpecCreationLambda-XXX.jar | compaction-job-creator-lambda     | false                |
| runningjobs-XXX.jar                  | compaction-task-creator-lambda    | false                |
| lambda-splitter-XXX.jar              | partition-splitter-lambda         | false                |
| query-XXX.jar                        | query-lambda                      | true                 |
| cdk-custom-resources-XXX.jar         | custom-resources-lambda           | false                |
| metrics-XXX.jar                      | metrics-lambda                    | false                |
| statestore-lambda-XXX.jar            | statestore-lambda                 | false                |

