## BULK IMPORT

Below is a table containing all the details for the property group: Bulk Import

| Property Name                                     | Description                                                                            | Default Value | Run CdkDeploy When Changed |
|---------------------------------------------------|----------------------------------------------------------------------------------------|---------------|----------------------------|
| sleeper.bulk.import.bucket                        | The S3 Bucket name of the bulk import bucket.                                          |               | true                       |
| sleeper.bulk.import.emr.ec2.role.name             | The name of the job flow role for bulk import using EMR.                               |               | true                       |
| sleeper.bulk.import.emr.role.name                 | The name of the role assumed by the bulk import clusters using EMR.                    |               | true                       |
| sleeper.bulk.import.emr.security.conf.name        | The name of the security configuration used by bulk import using EMR.                  |               | true                       |
| sleeper.bulk.import.emr.job.queue.url             | The URL of the queue for bulk import jobs using EMR.                                   |               | true                       |
| sleeper.bulk.import.emr.job.queue.arn             | The ARN of the queue for bulk import jobs using EMR.                                   |               | true                       |
| sleeper.bulk.import.emr.serverless.cluster.name   | The name of the cluster used for EMR Serverless bulk import jobs.                      |               | true                       |
| sleeper.bulk.import.emr.serverless.application.id | The id of the application used for EMR Serverless bulk import jobs.                    |               | true                       |
| sleeper.bulk.import.emr.serverless.role.arn       | The ARN of the role assumed by the bulk import clusters using EMR Serverless.          |               | true                       |
| sleeper.bulk.import.emr.serverless.job.queue.url  | The URL of the queue for bulk import jobs using EMR Serverless.                        |               | true                       |
| sleeper.bulk.import.emr.serverless.job.queue.arn  | The ARN of the queue for bulk import jobs using EMR Serverless.                        |               | true                       |
| sleeper.bulk.import.emr.serverless.studio.url     | The url for EMR Studio used to access EMR Serverless.                                  |               | true                       |
| sleeper.bulk.import.persistent.emr.job.queue.url  | The URL of the queue for bulk import jobs using persistent EMR.                        |               | true                       |
| sleeper.bulk.import.persistent.emr.job.queue.arn  | The ARN of the queue for bulk import jobs using persistent EMR.                        |               | true                       |
| sleeper.bulk.import.persistent.emr.cluster.name   | The name of the cluster used for persistent EMR bulk import jobs.                      |               | true                       |
| sleeper.bulk.import.persistent.emr.master         | The public DNS name of the cluster's master node for bulk import using persistent EMR. |               | true                       |
| sleeper.bulk.import.eks.job.queue.url             | The URL of the queue for bulk import jobs using EKS.                                   |               | true                       |
| sleeper.bulk.import.eks.job.queue.arn             | The ARN of the queue for bulk import jobs using EKS.                                   |               | true                       |
| sleeper.bulk.import.eks.statemachine.arn          | The ARN of the state machine for bulk import jobs using EKS.                           |               | true                       |
| sleeper.bulk.import.eks.k8s.namespace             | The namespace ID of the bulk import cluster using EKS.                                 |               | true                       |
| sleeper.bulk.import.eks.k8s.endpoint              | The endpoint of the bulk import cluster using EKS.                                     |               | true                       |
