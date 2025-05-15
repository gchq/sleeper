## Garbage Collector - CDK Defined

The following instance properties relate to garbage collection.

| Property Name              | Description                                                                              |
|----------------------------|------------------------------------------------------------------------------------------|
| sleeper.gc.rule            | The name of the CloudWatch rule that periodically triggers the garbage collector lambda. |
| sleeper.gc.lambda.function | The function name of the garbage collector lambda.                                       |
| sleeper.gc.queue.url       | The URL of the queue for sending batches of garbage collection requests.                 |
| sleeper.gc.queue.arn       | The ARN of the queue for sending batches of garbage collection requests.                 |
| sleeper.gc.dlq.url         | The URL of the dead letter queue for sending batches of garbage collection requests.     |
| sleeper.gc.dlq.arn         | The ARN of the dead letter queue for sending batches of garbage collection requests.     |
