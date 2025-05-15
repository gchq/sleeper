## QUERY

Below is a table containing all the details for the property group: Query

| Property Name                          | Description                                                                           |
|----------------------------------------|---------------------------------------------------------------------------------------|
| sleeper.query.role.arn                 | The ARN for the role with required permissions to query sleeper.                      |
| sleeper.query.websocket.api.url        | The URL of the WebSocket API for querying sleeper.                                    |
| sleeper.query.queue.url                | The URL of the queue responsible for sending a query to sleeper.                      |
| sleeper.query.queue.arn                | The ARN of the queue responsible for sending a query to sleeper.                      |
| sleeper.query.dlq.url                  | The URL of the dead letter queue used when querying sleeper.                          |
| sleeper.query.dlq.arn                  | The ARN of the dead letter queue used when querying sleeper.                          |
| sleeper.query.results.queue.url        | The URL of the queue responsible for retrieving results from a query sent to sleeper. |
| sleeper.query.results.queue.arn        | The ARN of the queue responsible for retrieving results from a query sent to sleeper. |
| sleeper.query.results.bucket           | The S3 Bucket name of the query results bucket.                                       |
| sleeper.query.tracker.table.name       | The name of the table responsible for tracking query progress.                        |
| sleeper.query.warm.lambda.rule         | The name of the CloudWatch rule to trigger the query lambda to keep it warm.          |
| sleeper.query.leaf.partition.queue.url | The URL of the queue responsible for sending a leaf partition query to sleeper.       |
| sleeper.query.leaf.partition.queue.arn | The ARN of the queue responsible for sending a leaf partition query to sleeper.       |
| sleeper.query.leaf.partition.dlq.url   | The URL of the dead letter queue used when leaf partition querying sleeper.           |
| sleeper.query.leaf.partition.dlq.arn   | The ARN of the dead letter queue used when leaf partition querying sleeper.           |
