## BULK EXPORT

Below is a table containing all the details for the property group: Bulk Export

| Property Name                                     | Description                                                                                      |
|---------------------------------------------------|--------------------------------------------------------------------------------------------------|
| sleeper.bulk.export.queue.url                     | The URL of the SQS queue that triggers the bulk export lambda.                                   |
| sleeper.bulk.export.queue.arn                     | The ARN of the SQS queue that triggers the bulk export lambda.                                   |
| sleeper.bulk.export.queue.dlq.url                 | The URL of the SQS dead letter queue that is used by the bulk export lambda.                     |
| sleeper.bulk.export.queue.dlq.arn                 | The ARN of the SQS dead letter queue that is used by the bulk export lambda.                     |
| sleeper.bulk.export.lambda.role.arn               | The ARN of the role for the bulk export lambda.                                                  |
| sleeper.bulk.export.fargate.task.definition       | The name of the family of Fargate task definitions used for bulk export.                         |
| sleeper.bulk.export.cluster                       | The name of the cluster used for bulk export.                                                    |
| sleeper.bulk.export.task.creation.lambda.function | The function name of the bulk export task creation lambda.                                       |
| sleeper.bulk.export.task.creation.rule            | The name of the CloudWatch rule that periodically triggers the bulk export task creation lambda. |
| sleeper.bulk.export.leaf.partition.queue.url      | The URL of the SQS queue that triggers the bulk export for a leaf partition.                     |
| sleeper.bulk.export.leaf.partition.queue.arn      | The ARN of the SQS queue that triggers the bulk export for a leaf partition.                     |
| sleeper.bulk.export.leaf.partition.queue.dlq.url  | The URL of the SQS dead letter queue that is used by the bulk export for a leaf partition.       |
| sleeper.bulk.export.leaf.partition.queue.dlq.arn  | The ARN of the SQS dead letter queue that is used by the bulk export for a leaf partition.       |
| sleeper.bulk.export.s3.bucket                     | The name of the S3 bucket where the bulk export files are stored.                                |
