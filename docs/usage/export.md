Exporting Data
==============

## Introduction

The Bulk Export feature allows you to export data from a Sleeper table to an S3 bucket in a number of Parquet files.

## Prerequisites
Before performing a Bulk Export, ensure that the `BulkExportStack` has been deployed. This stack is optional and is not included in a vanilla installation.

## How to Perform a Bulk Export

### Step 1: Submit a Bulk Export Query

To initiate a Bulk Export, submit a **Bulk Export Query** to the SQS queue specified in the `BULK_EXPORT_QUEUE_URL` property. The query should specify the Sleeper table name or table id to be exported. If wanted an `exportId` can also be provided, if one isn't then one is generated.

The system will automatically split the query into smaller export tasks (Leaf Partition Bulk Export Queries) and process them.

#### Example Query Message:
```json
{
  "tableName": "example-table",
  "exportId": "export-123"
}
```

### Step 2: Monitor the Export Process

Once the export query is submitted, the system will:
1. Automatically split the Bulk Export Query into smaller export queries for each Leaf Partition.
2. Add the Leaf Partition Bulk Export Queries to the SQS queue `LEAF_PARTITION_BULK_EXPORT_QUEUE_URL`.
3. The ECS Bulk Export Task Runner will:
   - Retrieve the Leaf Partition Export Queries from the SQS queue.
   - Process each export query using the existing compaction code. This will use the compaction code defined in the table properties. Either Java or Datafusion.
   - Save the results to the configured S3 bucket.

Logs for the export process can be monitored in the ECS Bulk Export Cluster task logs or CloudWatch.

## Where the Results Are Saved

The exported data is saved in the S3 bucket specified in the `BULK_EXPORT_S3_BUCKET` property. The output file path is constructed as follows:

```
s3://<BULK_EXPORT_S3_BUCKET>/<tableId>/<exportId>/<subExportId>.parquet
```

### Example Output Path:
For the example query above, if `BULK_EXPORT_S3_BUCKET` is set to `my-export-bucket`, the output files will be saved at:
```
s3://my-export-bucket/example-table/export-456/<subExportId>.parquet
```

## Additional Notes

- The exported files are saved in Parquet format, which is optimised for analytical workloads.
- Users should avoid submitting leaf partition queries directly unless they fully understand the internal process.
- The export process uses compaction to ensure efficient storage and retrieval of data.
