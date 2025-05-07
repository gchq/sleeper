Exporting Data
==============

## Introduction

The bulk export feature allows you to export data from a Sleeper table to an S3 bucket in a number of Parquet files.

## Prerequisites
Before performing a bulk export, ensure that the `BulkExportStack` has been deployed. This stack is optional and is not included in a vanilla installation. When deployed CDK will create the following properties. These do not need to be set:
- `sleeper.bulk.export.cluster`: The name of the cluster used for bulk export.
- `sleeper.bulk.export.fargate.task.definition`: The name of the family of Fargate task definitions used for bulk export.
- `sleeper.bulk.export.lambda.role.arn`: The ARN of the role for the bulk export lambda.
- `sleeper.bulk.export.leaf.partition.queue.arn`: The ARN of the SQS queue that triggers the bulk export for a leaf partition.
- `sleeper.bulk.export.leaf.partition.queue.dlq.arn`: The ARN of the SQS dead letter queue that is used by the bulk export for a leaf partition.
- `sleeper.bulk.export.leaf.partition.queue.dlq.url`: The URL of the SQS dead letter queue that is used by the bulk export for a leaf partition.
- `sleeper.bulk.export.leaf.partition.queue.url`: The URL of the SQS queue that triggers the bulk export for a leaf partition.
- `sleeper.bulk.export.queue.arn`: The ARN of the SQS queue that triggers the bulk export lambda.
- `sleeper.bulk.export.queue.dlq.arn`: The ARN of the SQS dead letter queue that is used by the bulk export lambda.
- `sleeper.bulk.export.queue.dlq.url`: The URL of the SQS dead letter queue that is used by the bulk export lambda.
- `sleeper.bulk.export.queue.url`: The URL of the SQS queue that triggers the bulk export lambda.
- `sleeper.bulk.export.s3.bucket`: The name of the S3 bucket where the bulk export files are stored.
- `sleeper.bulk.export.task.creation.lambda.function`: The function name of the bulk export task creation lambda.
- `sleeper.bulk.export.task.creation.rule`: The name of the CloudWatch rule that periodically triggers the bulk export task creation lambda.

## How to perform a bulk export

### Step 1: Submit a bulk export query

To initiate a bulk export, submit a **bulk export query** to the SQS queue specified in the `sleeper.bulk.export.queue.url` property. The query should specify the Sleeper table name or table id to be exported. If wanted an `exportId` can also be provided, if one isn't then one is generated.

The system will automatically split the query into smaller export tasks (leaf partition bulk export queries) and process them.

#### Example query message:
```json
{
  "tableName": "example-table",
  "exportId": "export-123"
}
```

### Step 2: Monitor the export process

Once the export query is submitted, the system will:
1. Automatically split the bulk export query into smaller export queries for each leaf partition.
2. Add the leaf partition bulk export queries to the SQS queue `sleeper.bulk.export.leaf.partition.queue.url`.
3. The ECS Bulk Export Task Runner will:
   - Retrieve the leaf partition export queries from the SQS queue.
   - Process each export query using the existing compaction code. This will use the compaction code defined in the table properties. Either Java or Datafusion.
   - Save the results to the configured S3 bucket.

Logs for the export process can be monitored in the ECS bulk export cluster task logs or CloudWatch.

## Where the results are saved

The exported data is saved in the S3 bucket in the `sleeper.bulk.export.s3.bucket` property. The output file path is constructed as follows:

```
s3://<BUCKET>/<tableId>/<exportId>/<subExportId>.parquet
```

### Example output path:
For the example query above, if `sleeper.bulk.export.s3.bucket` is set to `my-export-bucket`, the output files will be saved at:
```
s3://my-export-bucket/example-table/export-456/<subExportId>.parquet
```

## Additional notes

- The exported files are saved in Parquet format, which is optimised for analytical workloads.
- Users should avoid submitting leaf partition queries directly unless they fully understand the internal process.
- The export process uses compaction to ensure efficient storage and retrieval of data.
