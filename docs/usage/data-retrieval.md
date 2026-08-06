Retrieving data
================

There are several ways to retrieve data from a Sleeper table. Remember that Sleeper is optimised for returning rows
where the row key takes a particular value or range of values. If there are several row key fields, then the query
should either specify all of the row key fields, or the first one or more fields, e.g. if there are three row key
fields, key1, key2 and key3, then a query should specify either ranges for key1, key2 and key3, or ranges for key1 and
key2, or ranges for key1.

The methods below describe how queries can be executed using scripts. See the docs on the [Python API](python-api.md)
for details of how to execute them from Python.

These instructions will assume you start in the project root directory and Sleeper has been built
(see [the developer guide](../developer-guide.md) for how to set that up).

Also see the [data processing document](data-processing.md) for how to apply data processing during queries.

## Running queries directly using the Java client

The simplest way to retrieve data is to use the `query.sh` script. This simply calls the Java
class `sleeper.clients.query.QueryClient`. This class retrieves data directly from S3 to this machine. It can be run using:

```bash
INSTANCE_ID=myInstanceId
./scripts/utility/query.sh ${INSTANCE_ID}
```

This will give you the option of running either an "exact" query which allows you to type in either the exact key that
you wish to find rows for, or a "range" query which allows you to specify the range of keys you wish to find rows
for. The results are printed to standard out.

Note that as this approach to running queries retrieves the relevant rows from S3 to this Java process. Therefore if
you specify a large range, the query may take a long time to run and may transfer a large amount of data from S3 to your
machine.

### SQL Query Filtering (Experimental)

When using `query.sh`, you have the option to apply optional SQL query filtering to the results. This capability is
**experimental** and currently only supported with the **DataFusion query engine**.

After specifying your query range or exact key, the script will prompt you to enter an optional SQL statement:

```
Enter an optional SQL statement to execute on Sleeper query results. Table name is "query_results".
Enter SQL statement (blank for none):
```

The SQL query is executed on the query results, allowing you to:
- Filter rows based on column values
- Select specific columns
- Perform transformations on the data
- Aggregate results

#### Prerequisites for SQL Query Filtering

1. **DataFusion Query Engine**: The table must be configured to use DataFusion. Set the table property:
   ```properties
   sleeper.table.query.data.engine=datafusion
   ```

2. **Source Table Name**: Regardless of the actual Sleeper table name, the SQL source table is always named `query_results`.

#### Example SQL Queries

Filter rows by value:
```sql
SELECT * FROM query_results WHERE timestamp > 1234567890000
```

Select specific columns:
```sql
SELECT id, name, timestamp FROM query_results
```

Aggregate data:
```sql
SELECT id, COUNT(*) as count FROM query_results GROUP BY id
```

## Submitting queries to be executed via lambda

This is similar to the `QueryClient` class except that the query is sent to an SQS queue and then executed using AWS
lambda. If the query spans multiple leaf partitions then it will be executed in parallel by multiple lambda invocations.
By default the results are written to files in an S3 bucket. Alternatively, the results can be sent to an SQS queue.
Note that if you specify SQS as the output and query for a large range, a very significant amount of data could be sent
to SQS which could cost a lot of money.

```bash
INSTANCE_ID=myInstanceId
./scripts/utility/lambdaQuery.sh ${INSTANCE_ID}
```

This will first ask you to choose whether you want the results to be sent to an S3 bucket or an SQS queue. If you
specify S3 then the results are written to the S3 bucket named `sleeper-<instance-id>-query-results`. (The instance
property `sleeper.query.results.bucket` will be set to this value. Note that this is a system defined property, so it is
set to a bucket that is created during the CDK deployment process.)

This will again ask you to choose between an exact query and a range query. It will ask for the key or keys, and then
send the appropriate message to the query SQS queue.

If you chose the S3 option, then the results will be written to Parquet files in a directory called `query-<query-id>`
in the bucket `sleeper-<instance-id>-query-results`. They can then manually be retrieved from there. If you specified
SQS then the results are written to the SQS queue named `<instance-id>-QueryResultsQ`. To poll the results queue and
print the results when they are available use:

```bash
java -cp scripts/jars/clients-*-utility.jar sleeper.clients.query.QueryResultsSQSQueuePoller ${INSTANCE_ID}
```

This will print the results to standard out as they appear on the queue.

## Using a WebSocket to submit queries to be executed via lambda

If you have the `WebSocketQueryStack` optional stack deployed, you can also submit queries to be executed using a
WebSocket. This uses the Java class `QueryWebSocketClient`. These queries will then be
executed in a lambda and the results returned directly through the WebSocket. This can be done using:

```bash
INSTANCE_ID=myInstanceId
./scripts/utility/webSocketQuery.sh ${INSTANCE_ID}
```

This will print the results to standard out.

You can also use `QueryWebSocketClient` directly, although there's an issue which will change the method signature
on this class:

[Receive queried rows through CloseableIterator as they come from web socket](https://github.com/gchq/sleeper/issues/6463)

If you use `QueryWebSocketClient` directly you can set extra processing configuration on the query. This includes
a `resultsPublisherConfig` map, which lets you set the following additional options:

| Name                                  | Description                                                                                                   |
|---------------------------------------|---------------------------------------------------------------------------------------------------------------|
| webSocketPublishMaxAttempts           | The maximum number of times the lambda will attempt to publish to the web socket before giving up.            |
| webSocketThrottlingRetryBaseDelaySecs | The minimum time in seconds the lambda will wait before retrying publishing to the web socket when throttled. |
| webSocketThrottlingRetryMaxDelaySecs  | The maximum time in seconds the lambda will wait before retrying publishing to the web socket when throttled. |

The usual case where publishing to a web socket is throttled is when results are produced faster than either the client,
or API Gateway, can consume them. These options let you adjust the AWS SDK retry settings for throttling, including
exponential backoff with half jitter. By default, they're set to wait for at least 10 seconds for the data to be
consumed. In general a web socket may not be appropriate for longer running queries.

## Send messages via SQS

You can execute queries by sending messages directly to the SQS query queue. This will require you to specify the query
in JSON.

To execute a range query, use the following query:

```JSON
{
  "queryId": "a_unique_id",
  "tableName": "my-table",
  "type": "Query",
  "regions": [
    {
      "key1": {
        "min": "goodbye",
        "minInclusive": true,
        "max": "hello",
        "maxInclusive": false
      },
      "stringsBase64Encoded": false
    }
  ]
}
```

This will return all rows in the table "myTable" where key1 is in the range 'goodbye' to 'hello'. If there are
rows in the table where key1 is 'goodbye' then these will be included in the results; rows where key1 is 'hello'
will not be included. This is clearly quite verbose, but it is not intended that users will construct queries directly
in JSON. This query should be sent to the SQS queue with a URL given by the `sleeper.query.queue.url` property in the
instance properties file. The results will appear in the S3 query results bucket, as described above.

If you want to find all rows where the key is a certain value, construct your query in the following form:

```JSON
{
  "queryId": "a_unique_id",
  "tableName": "my-table",
  "type": "Query",
  "regions": [
    {
      "key1": {
        "min": "goodbye",
        "minInclusive": true,
        "max": "goodbye",
        "maxInclusive": true
      },
      "stringsBase64Encoded": false
    },
    {
      "key1": {
        "min": "hello",
        "minInclusive": true,
        "max": "hello",
        "maxInclusive": true
      },
      "stringsBase64Encoded": false
    }
  ]
}
```

This is a query for all rows in table "my-table" where key1 takes the value "goodbye" or the value "hello" (this
assumes that the first row key field in the schema has a name of "key1" and is a string).

It is possible to configure the lambda-based query executor to send the results of the query to different places. For
example, to send the results to the S3 bucket myBucket, use the following:

```JSON
{
  "queryId": "a_unique_id",
  "tableName": "my-table",
  "type": "Query",
  "regions": [
    {
      "key1": {
        "min": "goodbye",
        "minInclusive": true,
        "max": "hello",
        "maxInclusive": false
      },
      "stringsBase64Encoded": false
    }
  ],
  "resultsPublisherConfig": {
    "destination": "S3",
    "bucket": "myBucket",
    "compressionCodec": "zstd"
  }
}
```

To send the results to a particular SQS queue use:

```JSON
{
  "queryId": "a_unique_id",
  "tableName": "my-table",
  "type": "Query",
  "regions": [
    {
      "key1": {
        "min": "goodbye",
        "minInclusive": true,
        "max": "hello",
        "maxInclusive": false
      },
      "stringsBase64Encoded": false
    }
  ],
  "resultsPublisherConfig": {
    "destination": "SQS",
    "sqsResultsUrl": "someUrl",
    "batchSize": "100"
  }
}
```

You will need to give Sleeper's writing data IAM role (given by the CloudFormation
export `<instance-id>-QueryLambdaRoleArn`) permission to write to the above S3 bucket or SQS queue.

### SQL Query Filtering (Experimental)

SQL query filtering is also supported when submitting queries via SQS. See the [SQL Query Filtering](#sql-query-filtering-experimental)
section under "Running queries directly using the Java client" for details, prerequisites, and example SQL queries.

To apply SQL filtering to an SQS query, add the `processingConfig` field with the `sqlQuery` property:

```JSON
{
  "queryId": "a_unique_id",
  "tableName": "my-table",
  "type": "Query",
  "regions": [
    {
      "key1": {
        "min": "goodbye",
        "minInclusive": true,
        "max": "hello",
        "maxInclusive": false
      },
      "stringsBase64Encoded": false
    }
  ],
  "processingConfig": {
    "sqlQuery": "SELECT * FROM query_results WHERE value > 100"
  }
}
```

## Keep Lambda Warm Optional Stack

Lambdas inherently have a startup time usually refer to as cold start. This can add a significant delay thus increasing
a queries execution time.

To address this issue the KeepLambdaWarmStack can be enabled. This will create an Event Rule running every 5 minutes which
triggers the query lambdas thus ensuring its in a warm state. Enabling this will incur extra charges since the Lambdas are running every 5 minutes.

This can be enabled by adding `KeepLambdaWarmStack` to the optional stacks. It is not enabled by default.

## Use the Java API directly

You can also retrieve data using the Java class `QueryExecutor`.

## Use Athena to perform SQL Analytics and Queries

Sleeper allows you to query tables using Amazon Athena. This functionality is experimental. To do this, ensure you have
the `AthenaStack` enabled in the `sleeper.optional.stacks` instance property. This stack is not included by default.

Two different connectors to Athena are available: `sleeper.athena.composite.DataFusionCompositeHandler` and
`sleeper.athena.composite.IteratorApplyingCompositeHandler`. The first of these is recommended as it uses DataFusion to
read the data from the Parquet files in the Sleeper table which results in better performance than the other,
Java-based one.

Visit the Amazon console and choose Athena from the list of services. Click "Query your data in Athena console" and then
"Launch query editor". You should be able to find your Connector in the data source list. If your instance id is
"abc123" then the connector will be called "abc123DataFusionSleeperConnector". When you select your connector
the tables list should be populated. If you select the three dots next to the table name there is a "preview table" option.
If you select this, it will populate the SQL input with an example query which will run a 'SELECT * FROM ... LIMIT 10'
query.

The integration of Athena with Sleeper inspects the query and uses that to restrict the partitions, and hence files,
that are read. For example if you have a Sleeper table with 100 leaf partitions, and a schema with a row-key field which
has type string and a name of 'key' and your Athena query is of the form `SELECT * FROM "abc123"."table1" WHERE key='a'`
then it will be able to skip 99 partitions, and only read a small number of files, and within those files it will only
read a small amount of data (rather than the whole file).

## Use SQL with Trino

See the [Trino plugin documentation](trino.md) for how to interact with Sleeper via Trino. This functionality is
experimental.
