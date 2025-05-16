## Instance Properties - Bulk Import - CDK Defined

The following instance properties relate to bulk import, i.e. ingesting data using Spark jobs running on EMR or EKS.

| Property Name                                     | Description                                                                            |
|---------------------------------------------------|----------------------------------------------------------------------------------------|
| sleeper.bulk.import.bucket                        | The S3 Bucket name of the bulk import bucket.                                          |
| sleeper.bulk.import.emr.ec2.role.name             | The name of the job flow role for bulk import using EMR.                               |
| sleeper.bulk.import.emr.role.name                 | The name of the role assumed by the bulk import clusters using EMR.                    |
| sleeper.bulk.import.emr.security.conf.name        | The name of the security configuration used by bulk import using EMR.                  |
| sleeper.bulk.import.emr.job.queue.url             | The URL of the queue for bulk import jobs using EMR.                                   |
| sleeper.bulk.import.emr.job.queue.arn             | The ARN of the queue for bulk import jobs using EMR.                                   |
| sleeper.bulk.import.emr.serverless.cluster.name   | The name of the cluster used for EMR Serverless bulk import jobs.                      |
| sleeper.bulk.import.emr.serverless.application.id | The id of the application used for EMR Serverless bulk import jobs.                    |
| sleeper.bulk.import.emr.serverless.role.arn       | The ARN of the role assumed by the bulk import clusters using EMR Serverless.          |
| sleeper.bulk.import.emr.serverless.job.queue.url  | The URL of the queue for bulk import jobs using EMR Serverless.                        |
| sleeper.bulk.import.emr.serverless.job.queue.arn  | The ARN of the queue for bulk import jobs using EMR Serverless.                        |
| sleeper.bulk.import.emr.serverless.studio.url     | The url for EMR Studio used to access EMR Serverless.                                  |
| sleeper.bulk.import.persistent.emr.job.queue.url  | The URL of the queue for bulk import jobs using persistent EMR.                        |
| sleeper.bulk.import.persistent.emr.job.queue.arn  | The ARN of the queue for bulk import jobs using persistent EMR.                        |
| sleeper.bulk.import.persistent.emr.cluster.name   | The name of the cluster used for persistent EMR bulk import jobs.                      |
| sleeper.bulk.import.persistent.emr.master         | The public DNS name of the cluster's master node for bulk import using persistent EMR. |
| sleeper.bulk.import.eks.job.queue.url             | The URL of the queue for bulk import jobs using EKS.                                   |
| sleeper.bulk.import.eks.job.queue.arn             | The ARN of the queue for bulk import jobs using EKS.                                   |
| sleeper.bulk.import.eks.statemachine.arn          | The ARN of the state machine for bulk import jobs using EKS.                           |
| sleeper.bulk.import.eks.k8s.namespace             | The namespace ID of the bulk import cluster using EKS.                                 |
| sleeper.bulk.import.eks.k8s.endpoint              | The endpoint of the bulk import cluster using EKS.                                     |
