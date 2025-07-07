## Deployment Jars

These are the docker deployment Jars

| Property Name              | Optional Stack     | Multiplatform |
|----------------------------|--------------------|---------------|
| ingest                     | IngestStack        | false         |
| bulk-import-runner         | EksBulkImportStack | false         |
| compaction-job-execution   | CompactionStack    | true          |
| bulk-export-task-execution | BulkExportStack    | false         |
## Lambda Jars

These are the Lambda deploy jars

| File Name                                        | Image Name                        | Always docker deploy |
|--------------------------------------------------|-----------------------------------|----------------------|
| athena-0.32.0-SNAPSHOT.jar                       | athena-lambda                     | true                 |
| bulk-import-starter-0.32.0-SNAPSHOT.jar          | bulk-import-starter-lambda        | false                |
| bulk-export-planner-0.32.0-SNAPSHOT.jar          | bulk-export-planner               | false                |
| bulk-export-task-creator-0.32.0-SNAPSHOT.jar     | bulk-export-task-creator          | false                |
| ingest-taskrunner-0.32.0-SNAPSHOT.jar            | ingest-task-creator-lambda        | false                |
| ingest-batcher-submitter-0.32.0-SNAPSHOT.jar     | ingest-batcher-submitter-lambda   | false                |
| ingest-batcher-job-creator-0.32.0-SNAPSHOT.jar   | ingest-batcher-job-creator-lambda | false                |
| lambda-garbagecollector-0.32.0-SNAPSHOT.jar      | garbage-collector-lambda          | false                |
| lambda-jobSpecCreationLambda-0.32.0-SNAPSHOT.jar | compaction-job-creator-lambda     | false                |
| runningjobs-0.32.0-SNAPSHOT.jar                  | compaction-task-creator-lambda    | false                |
| lambda-splitter-0.32.0-SNAPSHOT.jar              | partition-splitter-lambda         | false                |
| query-0.32.0-SNAPSHOT.jar                        | query-lambda                      | true                 |
| cdk-custom-resources-0.32.0-SNAPSHOT.jar         | custom-resources-lambda           | false                |
| metrics-0.32.0-SNAPSHOT.jar                      | metrics-lambda                    | false                |
| statestore-lambda-0.32.0-SNAPSHOT.jar            | statestore-lambda                 | false                |
