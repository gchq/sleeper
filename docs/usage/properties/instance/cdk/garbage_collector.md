## GARBAGE COLLECTOR

Below is a table containing all the details for the property group: Garbage Collector

| Property Name              | Description                                                                              | Default Value | Run CdkDeploy When Changed |
|----------------------------|------------------------------------------------------------------------------------------|---------------|----------------------------|
| sleeper.gc.rule            | The name of the CloudWatch rule that periodically triggers the garbage collector lambda. |               | true                       |
| sleeper.gc.lambda.function | The function name of the garbage collector lambda.                                       |               | true                       |
| sleeper.gc.queue.url       | The URL of the queue for sending batches of garbage collection requests.                 |               | true                       |
| sleeper.gc.queue.arn       | The ARN of the queue for sending batches of garbage collection requests.                 |               | true                       |
| sleeper.gc.dlq.url         | The URL of the dead letter queue for sending batches of garbage collection requests.     |               | true                       |
| sleeper.gc.dlq.arn         | The ARN of the dead letter queue for sending batches of garbage collection requests.     |               | true                       |
