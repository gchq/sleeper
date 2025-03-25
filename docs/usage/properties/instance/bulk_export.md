## BULK EXPORT

Below is a table containing all the details for the property group: Bulk Export

| Property Name                                    | Description                                                                                | Default Value | Run CdkDeploy When Changed |
|--------------------------------------------------|--------------------------------------------------------------------------------------------|---------------|----------------------------|
| sleeper.bulk.export.queue.url                    | The URL of the SQS queue that triggers the bulk export lambda.                             |               | true                       |
| sleeper.bulk.export.queue.arn                    | The ARN of the SQS queue that triggers the bulk export lambda.                             |               | true                       |
| sleeper.bulk.export.queue.dlq.url                | The URL of the SQS dead letter queue that is used by the bulk export lambda.               |               | true                       |
| sleeper.bulk.export.queue.dlq.arn                | The ARN of the SQS dead letter queue that is used by the bulk export lambda.               |               | true                       |
| sleeper.bulk.export.lambda.role.arn              | The ARN of the role for the bulk export lambda.                                            |               | true                       |
| sleeper.bulk.export.leaf.partition.queue.url     | The URL of the SQS queue that triggers the bulk export for a leaf partition.               |               | true                       |
| sleeper.bulk.export.leaf.partition.queue.arn     | The ARN of the SQS queue that triggers the bulk export for a leaf partition.               |               | true                       |
| sleeper.bulk.export.leaf.partition.queue.dlq.url | The URL of the SQS dead letter queue that is used by the bulk export for a leaf partition. |               | true                       |
| sleeper.bulk.export.leaf.partition.queue.dlq.arn | The ARN of the SQS dead letter queue that is used by the bulk export for a leaf partition. |               | true                       |
