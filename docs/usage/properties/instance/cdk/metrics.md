## Instance Properties - Metrics - CDK Defined

The following instance properties relate to metrics.

| Property Name                         | Description                                                                     |
|---------------------------------------|---------------------------------------------------------------------------------|
| sleeper.lambda.table.metrics.function | The name of the Lambda function that triggers generation of metrics for tables. |
| sleeper.sqs.table.metrics.queue.url   | The URL of the queue for table metrics calculation requests.                    |
| sleeper.sqs.table.metrics.queue.arn   | The ARN of the queue for table metrics calculation requests.                    |
| sleeper.sqs.table.metrics.dlq.url     | The URL of the dead letter queue for table metrics calculation requests.        |
| sleeper.sqs.table.metrics.dlq.arn     | The ARN of the dead letter queue for table metrics calculation requests.        |
| sleeper.schedule.table.metrics.rule   | The name of the schedule rule that triggers generation of metrics for tables.   |
