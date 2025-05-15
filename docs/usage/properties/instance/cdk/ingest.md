## INGEST

Below is a table containing all the details for the property group: Ingest

| Property Name                              | Description                                                                                      | Default Value | Run CdkDeploy When Changed |
|--------------------------------------------|--------------------------------------------------------------------------------------------------|---------------|----------------------------|
| sleeper.ingest.lambda.function             | The function name of the ingest task creator lambda.                                             |               | true                       |
| sleeper.ingest.rule                        | The name of the CloudWatch rule that periodically triggers the ingest task creator lambda.       |               | true                       |
| sleeper.ingest.job.queue.url               | The URL of the queue for ingest jobs.                                                            |               | true                       |
| sleeper.ingest.job.queue.arn               | The ARN of the queue for ingest jobs.                                                            |               | true                       |
| sleeper.ingest.job.dlq.url                 | The URL of the dead letter queue for ingest jobs.                                                |               | true                       |
| sleeper.ingest.job.dlq.arn                 | The ARN of the dead letter queue for ingest jobs.                                                |               | true                       |
| sleeper.ingest.batcher.submit.queue.url    | The URL of the queue for ingest batcher file submission.                                         |               | true                       |
| sleeper.ingest.batcher.submit.queue.arn    | The ARN of the queue for ingest batcher file submission.                                         |               | true                       |
| sleeper.ingest.batcher.submit.dlq.url      | The URL of the dead letter queue for ingest batcher file submission.                             |               | true                       |
| sleeper.ingest.batcher.submit.dlq.arn      | The ARN of the dead letter queue for ingest batcher file submission.                             |               | true                       |
| sleeper.ingest.batcher.job.creation.rule   | The name of the CloudWatch rule to trigger the batcher to create jobs from file ingest requests. |               | true                       |
| sleeper.ingest.batcher.job.creation.lambda | The name of the ingest batcher Lambda to create jobs from file ingest requests.                  |               | true                       |
| sleeper.ingest.batcher.submit.lambda       | The name of the ingest batcher Lambda to submit file ingest requests.                            |               | true                       |
| sleeper.ingest.task.definition.family      | The name of the family of task definitions used for ingest tasks.                                |               | true                       |
| sleeper.ingest.cluster                     | The name of the cluster used for ingest.                                                         |               | true                       |
| sleeper.ingest.by.queue.role.arn           | The ARN of a role that has permissions to perform an ingest by queue for the instance.           |               | true                       |
| sleeper.ingest.direct.role.arn             | The ARN of a role that has permissions to perform a direct ingest for the instance.              |               | true                       |
