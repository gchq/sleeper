## PARTITION SPLITTING

Below is a table containing all the details for the property group: Partition Splitting

| Property Name                                       | Description                                                                                                  | Default Value | Run CdkDeploy When Changed |
|-----------------------------------------------------|--------------------------------------------------------------------------------------------------------------|---------------|----------------------------|
| sleeper.partition.splitting.finder.queue.url        | The URL of the queue for requests to find partitions to split.                                               |               | true                       |
| sleeper.partition.splitting.finder.queue.arn        | The ARN of the queue for requests to find partitions to split.                                               |               | true                       |
| sleeper.partition.splitting.finder.dlq.url          | The URL of the dead letter queue for requests to find partitions to split.                                   |               | true                       |
| sleeper.partition.splitting.finder.dlq.arn          | The ARN of the dead letter queue for requests to find partitions to split.                                   |               | true                       |
| sleeper.partition.splitting.job.queue.url           | The URL of the queue for partition splitting jobs.                                                           |               | true                       |
| sleeper.partition.splitting.job.queue.arn           | The ARN of the queue for partition splitting jobs.                                                           |               | true                       |
| sleeper.partition.splitting.job.dlq.url             | The URL of the dead letter queue for partition splitting jobs.                                               |               | true                       |
| sleeper.partition.splitting.job.dlq.arn             | The ARN of the dead letter queue for partition splitting jobs.                                               |               | true                       |
| sleeper.partition.splitting.trigger.lambda.function | The function name of the lambda that finds partitions to split and sends jobs to the split partition lambda. |               | true                       |
| sleeper.partition.splitting.rule                    | The name of the CloudWatch rule that periodically triggers the partition splitting lambda.                   |               | true                       |
