## Instance Properties - Partition Splitting - CDK Defined

The following instance properties relate to the splitting of partitions.

| Property Name                                       | Description                                                                                                  |
|-----------------------------------------------------|--------------------------------------------------------------------------------------------------------------|
| sleeper.partition.splitting.finder.queue.url        | The URL of the queue for requests to find partitions to split.                                               |
| sleeper.partition.splitting.finder.queue.arn        | The ARN of the queue for requests to find partitions to split.                                               |
| sleeper.partition.splitting.finder.dlq.url          | The URL of the dead letter queue for requests to find partitions to split.                                   |
| sleeper.partition.splitting.finder.dlq.arn          | The ARN of the dead letter queue for requests to find partitions to split.                                   |
| sleeper.partition.splitting.job.queue.url           | The URL of the queue for partition splitting jobs.                                                           |
| sleeper.partition.splitting.job.queue.arn           | The ARN of the queue for partition splitting jobs.                                                           |
| sleeper.partition.splitting.job.dlq.url             | The URL of the dead letter queue for partition splitting jobs.                                               |
| sleeper.partition.splitting.job.dlq.arn             | The ARN of the dead letter queue for partition splitting jobs.                                               |
| sleeper.partition.splitting.trigger.lambda.function | The function name of the lambda that finds partitions to split and sends jobs to the split partition lambda. |
| sleeper.partition.splitting.rule                    | The name of the CloudWatch rule that periodically triggers the partition splitting lambda.                   |
