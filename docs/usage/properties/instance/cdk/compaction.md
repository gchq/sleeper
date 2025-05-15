## Compaction - CDK Defined

The following instance properties relate to compactions.

| Property Name                                           | Description                                                                                     |
|---------------------------------------------------------|-------------------------------------------------------------------------------------------------|
| sleeper.compaction.cluster                              | The name of the cluster used for compactions.                                                   |
| sleeper.compaction.ec2.task.definition                  | The name of the family of EC2 task definitions used for compactions.                            |
| sleeper.compaction.fargate.task.definition              | The name of the family of Fargate task definitions used for compactions.                        |
| sleeper.compaction.job.creation.trigger.lambda.function | The function name of the lambda to trigger compaction job creation for all tables.              |
| sleeper.compaction.job.creation.rule                    | The name of the CloudWatch rule that periodically triggers the compaction job creation lambda.  |
| sleeper.compaction.job.creation.queue.url               | The URL of the queue for tables requiring compaction job creation.                              |
| sleeper.compaction.job.creation.queue.arn               | The ARN of the queue for tables requiring compaction job creation.                              |
| sleeper.compaction.job.creation.dlq.url                 | The URL of the dead letter queue for tables that failed compaction job creation.                |
| sleeper.compaction.job.creation.dlq.arn                 | The ARN of the dead letter queue for tables that failed compaction job creation.                |
| sleeper.compaction.job.queue.url                        | The URL of the queue for compaction jobs.                                                       |
| sleeper.compaction.job.queue.arn                        | The ARN of the queue for compaction jobs.                                                       |
| sleeper.compaction.job.dlq.url                          | The URL of the dead letter queue for compaction jobs.                                           |
| sleeper.compaction.job.dlq.arn                          | The ARN of the dead letter queue for compaction jobs.                                           |
| sleeper.compaction.pending.queue.url                    | The URL of the queue for pending compaction job batches.                                        |
| sleeper.compaction.pending.queue.arn                    | The ARN of the queue for pending compaction job batches.                                        |
| sleeper.compaction.pending.dlq.url                      | The URL of the dead letter queue for pending compaction job batches.                            |
| sleeper.compaction.pending.dlq.arn                      | The ARN of the dead letter queue for pending compaction job batches.                            |
| sleeper.compaction.commit.queue.url                     | The URL of the queue for compaction jobs ready to commit to the state store.                    |
| sleeper.compaction.commit.queue.arn                     | The ARN of the queue for compaction jobs ready to commit to the state store.                    |
| sleeper.compaction.commit.dlq.url                       | The URL of the dead letter queue for compaction jobs ready to commit to the state store.        |
| sleeper.compaction.commit.dlq.arn                       | The ARN of the dead letter queue for compaction jobs ready to commit to the state store.        |
| sleeper.compaction.task.creation.lambda.function        | The function name of the compaction task creation lambda.                                       |
| sleeper.compaction.task.creation.rule                   | The name of the CloudWatch rule that periodically triggers the compaction task creation lambda. |
| sleeper.compaction.scaling.group                        | The name of the compaction EC2 auto scaling group.                                              |
