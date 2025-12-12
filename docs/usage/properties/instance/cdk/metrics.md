## Instance Properties - Metrics - CDK Defined

The following instance properties relate to metrics.

| Property Name                   | Description                                                                     |
|---------------------------------|---------------------------------------------------------------------------------|
| sleeper.metrics.lambda.function | The name of the Lambda function that triggers generation of metrics for tables. |
| sleeper.metrics.queue.url       | The URL of the queue for table metrics calculation requests.                    |
| sleeper.metrics.queue.arn       | The ARN of the queue for table metrics calculation requests.                    |
| sleeper.metrics.dlq.url         | The URL of the dead letter queue for table metrics calculation requests.        |
| sleeper.metrics.dlq.arn         | The ARN of the dead letter queue for table metrics calculation requests.        |
| sleeper.metrics.rule            | The name of the CloudWatch rule that triggers generation of metrics for tables. |
