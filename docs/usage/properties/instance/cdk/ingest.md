## Ingest

The following properties relate to standard ingest.

| Property Name                              | Description                                                                                      |
|--------------------------------------------|--------------------------------------------------------------------------------------------------|
| sleeper.ingest.lambda.function             | The function name of the ingest task creator lambda.                                             |
| sleeper.ingest.rule                        | The name of the CloudWatch rule that periodically triggers the ingest task creator lambda.       |
| sleeper.ingest.job.queue.url               | The URL of the queue for ingest jobs.                                                            |
| sleeper.ingest.job.queue.arn               | The ARN of the queue for ingest jobs.                                                            |
| sleeper.ingest.job.dlq.url                 | The URL of the dead letter queue for ingest jobs.                                                |
| sleeper.ingest.job.dlq.arn                 | The ARN of the dead letter queue for ingest jobs.                                                |
| sleeper.ingest.batcher.submit.queue.url    | The URL of the queue for ingest batcher file submission.                                         |
| sleeper.ingest.batcher.submit.queue.arn    | The ARN of the queue for ingest batcher file submission.                                         |
| sleeper.ingest.batcher.submit.dlq.url      | The URL of the dead letter queue for ingest batcher file submission.                             |
| sleeper.ingest.batcher.submit.dlq.arn      | The ARN of the dead letter queue for ingest batcher file submission.                             |
| sleeper.ingest.batcher.job.creation.rule   | The name of the CloudWatch rule to trigger the batcher to create jobs from file ingest requests. |
| sleeper.ingest.batcher.job.creation.lambda | The name of the ingest batcher Lambda to create jobs from file ingest requests.                  |
| sleeper.ingest.batcher.submit.lambda       | The name of the ingest batcher Lambda to submit file ingest requests.                            |
| sleeper.ingest.task.definition.family      | The name of the family of task definitions used for ingest tasks.                                |
| sleeper.ingest.cluster                     | The name of the cluster used for ingest.                                                         |
| sleeper.ingest.by.queue.role.arn           | The ARN of a role that has permissions to perform an ingest by queue for the instance.           |
| sleeper.ingest.direct.role.arn             | The ARN of a role that has permissions to perform a direct ingest for the instance.              |
