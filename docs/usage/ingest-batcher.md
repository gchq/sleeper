Ingest Batcher
==============

An alternative to creating ingest jobs directly is to use the ingest batcher. This lets you submit a list of
files or directories, and Sleeper will group them into jobs for you.

This is deployed when `IngestBatcherStack` is in the list of optional stacks in the instance property
`sleeper.optional.stacks`.

By default this will use bulk import on EMR Serverless. Note that bulk import requires a minimum number of partitions,
and by default a table starts with just one. The minimum is set in the table
property `sleeper.table.bulk.import.min.leaf.partitions`, documented [here](properties/table/bulk_import.md). If too few
partitions are present, then when a bulk import job is submitted the partitions will be split automatically, based on
the data in the bulk import job. This will assume that the job's data is a representative sample for the table. If
multiple bulk import jobs are submitted simultaneously, they will attempt to pre-split separately, which can waste
compute resources. You can take control over this by pre-splitting the table as
described [here](../usage/tables.md#pre-split-partitions).

Files to be ingested must be accessible to the ingest system you will use, and to the ingest batcher as well in order to
expand directories and check the size of files. See the [ingest guide](ingest.md#prepare-files) for how to prepare your
files for access.


Files can be submitted as messages to the batcher submission SQS queue. A script is available to do this:

```bash
./scripts/utility/sendToIngestBatcher.sh <instance-id> <table-name> <parquet-paths-as-separate-args>
```

Paths to the files must be in an S3 bucket, specified with the bucket name and object key like
this: `bucket-name/folder-prefix/file.parquet`. If you provide a directory in S3 instead of a file, the batcher
will look in all subdirectories and track any files found in them.

You can also submit requests to the queue manually as described [below](#manually-sending-files-to-the-batcher-queue).

### Job creation

The batcher will track all submitted files and group them into jobs periodically, based on its configuration. The
configuration specifies minimum and maximum size of a batch, and a maximum age for files.

The minimum batch size determines whether any jobs will be created. The maximum batch size splits the tracked files
into multiple jobs. The maximum file age overrides the minimum batch size, so that when any file exceeds that age, a job
will be created with all currently tracked files.

If you submit requests to ingest files with the same path into the same table, this will overwrite the previous request
for that file, unless it has already been added to a job. When a file has been added to a job, further requests for a
file at that path will be treated as a new file.

For details of the batcher configuration, see the property descriptions in the example
[table.properties](../../example/full/table.properties) and
[instance.properties](../../example/full/instance.properties) files. The relevant table properties are under
`sleeper.table.ingest.batcher`. The relevant instance properties are under `sleeper.ingest.batcher` and
`sleeper.default.ingest.batcher`.

You can query the files being processed by the ingest batcher by using the following utility script:

```shell
./scripts/utility/ingestBatcherReport.sh <instance-id> <report-type-standard-or-json> <optional-query-type>
```

The query type can be one of the following options:

- `-a` shows all files, whether waiting to be batched, or already in jobs
- `-p` shows pending files, which have not yet been added to a job

If you do not provide a query type as a parameter to the script you will be prompted to select one.

### Manually sending files to the batcher queue

You can find the URL of the ingest batcher submission queue in the system-defined
property `sleeper.ingest.batcher.submit.queue.url`, documented [here](properties/instance/cdk/ingest.md).

An example message is shown below:

```json
{
  "tableName": "target-table",
  "files": [
    "source-bucket-name/folder-prefix/file.parquet"
  ]
}
```

Each message is a request to ingest a collection of files into a Sleeper table. If you provide a directory in S3
instead of a file, the batcher will look in all subdirectories and track any files found in them.
