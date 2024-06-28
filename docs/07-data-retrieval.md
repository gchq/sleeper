Retrieving data
================

There are several ways to retrieve data from a Sleeper table. Remember that Sleeper is optimised for returning records
where the row key takes a particular value or range of values. If there are several row key fields, then the query
should either specify all of the row key fields, or the first one or more fields, e.g. if there are three row key
fields, key1, key2 and key3, then a query should specify either ranges for key1, key2 and key3, or ranges for key1 and
key2, or ranges for key1.

The methods below describe how queries can be executed using scripts. See the docs on the [Python API](08-python-api.md)
for details of how to execute them from Python.

These instructions will assume you start in the project root directory and you have the required dependencies
(see [Installing prerequisite software](11-dev-guide.md#install-prerequisite-software) for how to set that up).

## Running queries directly using the Java client

The simplest way to retrieve data is to use the `query.sh` script. This simply calls the Java
class `sleeper.clients.QueryClient`. This class retrieves data directly from S3 to this machine. It can be run using:

```bash
INSTANCE_ID=myInstanceId
./scripts/utility/query.sh ${INSTANCE_ID}
```

This will give you the option of running either an "exact" query which allows you to type in either the exact key that
you wish to find records for, or a "range" query which allows you to specify the range of keys you wish to find records
for. The results are printed to standard out.

Note that as this approach to running queries retrieves the relevant records from S3 to this Java process. Therefore if
you specify a large range, the query may take a long time to run and may transfer a large amount of data from S3 to your
machine.

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
java -cp jars/clients-*-utility.jar sleeper.clients.QueryResultsSQSQueuePoller ${INSTANCE_ID}
```

This will print the results to standard out as they appear on the queue.

## Using websockets to submit queries to be executed via lambda

If you have the `WebSocketQueryStack` optional stack deployed, you can also submit queries to be executed using websockets.
These queries will then be executed using lambda and the results returned directly to the websocket client.
This can be done using:

```bash
INSTANCE_ID=myInstanceId
./scripts/utility/webSocketQuery.sh ${INSTANCE_ID}
```

The results will be returned directly to the client.

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

This will return all records in the table "myTable" where key1 is in the range 'goodbye' to 'hello'. If there are
records in the table where key1 is 'goodbye' then these will be included in the results; records where key1 is 'hello'
will not be included. This is clearly quite verbose, but it is not intended that users will construct queries directly
in JSON. This query should be sent to the SQS queue with a URL given by the `sleeper.query.queue.url` property in the
instance properties file. The results will appear in the S3 query results bucket, as described above.

If you want to find all records where the key is a certain value, construct your query in the following form:

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

This is a query for all records in table "my-table" where key1 takes the value "goodbye" or the value "hello" (this
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

Visit the Amazon console and choose Athena from the list of services. You should be able to find your Connector in the
data source list. If your instance id is "abc123" then the connector will be called "abc123SimpleSleeperConnector".
Click "Query Editor" and then select your connector under "Data Source". When you select it, the tables list should be
populated. You will need to select a query results location.

If you select the three dots next to the table name there is a "preview table" option. If you select this, it will
populate the SQL input with an example query which will select all columns and limit to the first 10 results.

To make queries in Athena efficient, filter primitive columns where you can (especially the row keys). These predicates
will be pushed down to S3 and mean that you scan less data and incur a smaller fee as a result.

We provide support for all Sleeper data types apart from the map type. This is just because Athena has not yet added
support for maps. When Athena does add this support, we will be able to add support for it.

### The two different connectors

You might notice that you have a choice of two connectors by default:

* The simple connector
* The iterator applying connector

You can choose at runtime which one you want to use:

```sql
SELECT *
FROM "MyInstanceSimpleSleeperConnector"."MyInstance"."myTable"
```

or

```sql
SELECT *
FROM "MyInstanceIteratorApplyingSleeperConnector"."MyInstance"."myTable"
```

The simple connector creates one Athena handler for each file. This means that any iterators are not applied. The
iterator applying connector performs a query time compaction so that each Sleeper leaf partition is processed by a
single Athena handler. The handler receives the data in sorted order and applies any iterators.

#### Improving query performance with the iterator applying connector

If you want to use the Sleeper iterators, it means we have to read a whole Sleeper partition in one Athena handler. If
you've got multiple files in a leaf partition, that means reading all the files in one lambda, rather than federating
out the reads to multiple handlers.

#### Changing the handlers

To alter the handlers that are deployed with your instance, you can change the `sleeper.athena.handler.classes` instance
property:

```properties
# Remove the default Simple handler
sleeper.athena.handler.classes=sleeper.athena.composite.IteratorApplyingCompositeHandler
```

When you add or remove a handler, an Athena data catalogue will be deployed for you.
