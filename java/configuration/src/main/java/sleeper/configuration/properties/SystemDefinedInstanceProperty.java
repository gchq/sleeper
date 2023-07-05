/*
 * Copyright 2022-2023 Crown Copyright
 *
 * Licensed under the Apache License, Version 2.0 (the "License");
 * you may not use this file except in compliance with the License.
 * You may obtain a copy of the License at
 *
 *     http://www.apache.org/licenses/LICENSE-2.0
 *
 * Unless required by applicable law or agreed to in writing, software
 * distributed under the License is distributed on an "AS IS" BASIS,
 * WITHOUT WARRANTIES OR CONDITIONS OF ANY KIND, either express or implied.
 * See the License for the specific language governing permissions and
 * limitations under the License.
 */
package sleeper.configuration.properties;

import edu.umd.cs.findbugs.annotations.SuppressFBWarnings;

import java.util.List;

/**
 * Properties set internally by sleeper and not by the user. These are set by the system itself at deployment time
 * so require no default or validation. Even if you set these in your properties file, they will be overwritten.
 */
// Suppress as this class will always be referenced before impl class, so initialization behaviour will be deterministic
@SuppressFBWarnings("IC_SUPERCLASS_USES_SUBCLASS_DURING_INITIALIZATION")
public interface SystemDefinedInstanceProperty extends InstanceProperty {
    SystemDefinedInstanceProperty VERSION = Index.propertyBuilder("sleeper.version")
            .description("The version of Sleeper that is being used. This property is used to identify the correct " +
                    "jars in the S3 jars bucket and to select the correct tag in the ECR repositories.")
            .propertyGroup(InstancePropertyGroup.COMMON).build();

    // Configuration
    SystemDefinedInstanceProperty CONFIG_BUCKET = Index.propertyBuilder("sleeper.config.bucket")
            .description("The S3 bucket name used to store configuration files.")
            .propertyGroup(InstancePropertyGroup.COMMON)
            .build();

    // Table metrics
    SystemDefinedInstanceProperty TABLE_METRICS_RULES = Index.propertyBuilder("sleeper.table.metrics.rulenames")
            .description("The names of the CloudWatch rules that trigger generation of metrics for tables.")
            .propertyGroup(InstancePropertyGroup.COMMON)
            .build();

    // Query
    SystemDefinedInstanceProperty QUERY_LAMBDA_ROLE = Index.propertyBuilder("sleeper.query.role.arn")
            .description("The ARN for the role with required permissions to query sleeper.")
            .propertyGroup(InstancePropertyGroup.QUERY)
            .build();
    SystemDefinedInstanceProperty QUERY_WEBSOCKET_API_URL = Index.propertyBuilder("sleeper.query.websocket.api.url")
            .description("The URL of the WebSocket API for querying sleeper.")
            .propertyGroup(InstancePropertyGroup.QUERY)
            .build();
    SystemDefinedInstanceProperty QUERY_QUEUE_URL = Index.propertyBuilder("sleeper.query.queue.url")
            .description("The URL of the queue responsible for sending a query to sleeper.")
            .propertyGroup(InstancePropertyGroup.QUERY)
            .build();
    SystemDefinedInstanceProperty QUERY_DLQ_URL = Index.propertyBuilder("sleeper.query.dlq.url")
            .description("The URL of the dead letter queue used when querying sleeper.")
            .propertyGroup(InstancePropertyGroup.QUERY)
            .build();
    SystemDefinedInstanceProperty QUERY_RESULTS_QUEUE_URL = Index.propertyBuilder("sleeper.query.results.queue.url")
            .description("The URL of the queue responsible for retrieving results from a query sent to sleeper.")
            .propertyGroup(InstancePropertyGroup.QUERY)
            .build();
    SystemDefinedInstanceProperty QUERY_RESULTS_BUCKET = Index.propertyBuilder("sleeper.query.results.bucket")
            .description("The S3 Bucket name of the query results bucket.")
            .propertyGroup(InstancePropertyGroup.QUERY)
            .build();
    SystemDefinedInstanceProperty QUERY_TRACKER_TABLE_NAME = Index.propertyBuilder("sleeper.query.tracker.table.name")
            .description("The name of the table responsible for tracking query progress.")
            .propertyGroup(InstancePropertyGroup.QUERY)
            .build();

    // Compactions
    SystemDefinedInstanceProperty COMPACTION_CLUSTER = Index.propertyBuilder("sleeper.compaction.cluster")
            .description("The name of the cluster used for compactions.")
            .propertyGroup(InstancePropertyGroup.COMPACTION)
            .build();
    SystemDefinedInstanceProperty SPLITTING_COMPACTION_CLUSTER = Index.propertyBuilder("sleeper.compaction.splitting.cluster")
            .description("The name of the cluster used for splitting compactions.")
            .propertyGroup(InstancePropertyGroup.COMPACTION)
            .build();
    SystemDefinedInstanceProperty COMPACTION_TASK_EC2_DEFINITION_FAMILY = Index.propertyBuilder("sleeper.compaction.ec2.task.definition")
            .description("The name of the family of EC2 task definitions used for compactions.")
            .propertyGroup(InstancePropertyGroup.COMPACTION)
            .build();
    SystemDefinedInstanceProperty COMPACTION_TASK_FARGATE_DEFINITION_FAMILY = Index.propertyBuilder("sleeper.compaction.fargate.task.definition")
            .description("The name of the family of Fargate task definitions used for compactions.")
            .propertyGroup(InstancePropertyGroup.COMPACTION)
            .build();
    SystemDefinedInstanceProperty COMPACTION_JOB_CREATION_LAMBDA_FUNCTION = Index.propertyBuilder("sleeper.compaction.job.creation.lambda.function")
            .description("The function name of the compaction job creation lambda.")
            .propertyGroup(InstancePropertyGroup.COMPACTION)
            .build();
    SystemDefinedInstanceProperty COMPACTION_JOB_CREATION_CLOUDWATCH_RULE = Index.propertyBuilder("sleeper.compaction.job.creation.rule")
            .description("The name of the CloudWatch rule that periodically triggers the compaction job creation lambda.")
            .propertyGroup(InstancePropertyGroup.COMPACTION)
            .build();
    SystemDefinedInstanceProperty COMPACTION_JOB_QUEUE_URL = Index.propertyBuilder("sleeper.compaction.job.queue.url")
            .description("The URL of the queue for compaction jobs.")
            .propertyGroup(InstancePropertyGroup.COMPACTION)
            .build();
    SystemDefinedInstanceProperty COMPACTION_JOB_DLQ_URL = Index.propertyBuilder("sleeper.compaction.job.dlq.url")
            .description("The URL of the dead letter queue for compaction jobs.")
            .propertyGroup(InstancePropertyGroup.COMPACTION)
            .build();
    SystemDefinedInstanceProperty COMPACTION_TASK_CREATION_LAMBDA_FUNCTION = Index.propertyBuilder("sleeper.compaction.task.creation.lambda.function")
            .description("The function name of the compaction task creation lambda.")
            .propertyGroup(InstancePropertyGroup.COMPACTION)
            .build();
    SystemDefinedInstanceProperty COMPACTION_TASK_CREATION_CLOUDWATCH_RULE = Index.propertyBuilder("sleeper.compaction.task.creation.rule")
            .description("The name of the CloudWatch rule that periodically triggers the compaction task creation lambda.")
            .propertyGroup(InstancePropertyGroup.COMPACTION)
            .build();
    SystemDefinedInstanceProperty COMPACTION_AUTO_SCALING_GROUP = Index.propertyBuilder("sleeper.compaction.scaling.group")
            .description("The name of the compaction EC2 auto scaling group.")
            .propertyGroup(InstancePropertyGroup.COMPACTION)
            .build();
    SystemDefinedInstanceProperty SPLITTING_COMPACTION_TASK_EC2_DEFINITION_FAMILY = Index.propertyBuilder("sleeper.compaction.splitting.ec2.task.definition")
            .description("The name of the family of EC2 task definitions used for splitting compactions.")
            .propertyGroup(InstancePropertyGroup.COMPACTION)
            .build();
    SystemDefinedInstanceProperty SPLITTING_COMPACTION_TASK_FARGATE_DEFINITION_FAMILY = Index.propertyBuilder("sleeper.compaction.splitting.fargate.task.definition")
            .description("The name of the family of Fargate task definitions used for splitting compactions.")
            .propertyGroup(InstancePropertyGroup.COMPACTION)
            .build();

    SystemDefinedInstanceProperty SPLITTING_COMPACTION_JOB_QUEUE_URL = Index.propertyBuilder("sleeper.compaction.splitting.job.queue.url")
            .description("The URL of the queue for splitting compaction jobs.")
            .propertyGroup(InstancePropertyGroup.COMPACTION)
            .build();
    SystemDefinedInstanceProperty SPLITTING_COMPACTION_JOB_DLQ_URL = Index.propertyBuilder("sleeper.compaction.splitting.job.dlq.url")
            .description("The URL of the dead letter queue for splitting compaction jobs.")
            .propertyGroup(InstancePropertyGroup.COMPACTION)
            .build();
    SystemDefinedInstanceProperty SPLITTING_COMPACTION_TASK_CREATION_LAMBDA_FUNCTION = Index.propertyBuilder("sleeper.compaction.splitting.task.creation.lambda.function")
            .description("The function name of the splitting compaction task creation lambda.")
            .propertyGroup(InstancePropertyGroup.COMPACTION)
            .build();
    SystemDefinedInstanceProperty SPLITTING_COMPACTION_TASK_CREATION_CLOUDWATCH_RULE = Index.propertyBuilder("sleeper.compaction.splitting.task.creation.rule")
            .description("The name of the CloudWatch rule that periodically triggers the splitting compaction task creation lambda.")
            .propertyGroup(InstancePropertyGroup.COMPACTION)
            .build();
    SystemDefinedInstanceProperty SPLITTING_COMPACTION_AUTO_SCALING_GROUP = Index.propertyBuilder("sleeper.compaction.splitting.scaling.group")
            .description("The name of the splitting compaction EC2 auto scaling group.")
            .propertyGroup(InstancePropertyGroup.COMPACTION)
            .build();

    // Partition splitting
    SystemDefinedInstanceProperty PARTITION_SPLITTING_QUEUE_URL = Index.propertyBuilder("sleeper.partition.splitting.queue.url")
            .description("The URL of the queue for partition splitting.")
            .propertyGroup(InstancePropertyGroup.PARTITION_SPLITTING)
            .build();
    SystemDefinedInstanceProperty PARTITION_SPLITTING_DLQ_URL = Index.propertyBuilder("sleeper.partition.splitting.dlq.url")
            .description("The URL of the dead letter queue for partition splitting.")
            .propertyGroup(InstancePropertyGroup.PARTITION_SPLITTING)
            .build();
    SystemDefinedInstanceProperty PARTITION_SPLITTING_LAMBDA_FUNCTION = Index.propertyBuilder("sleeper.partition.splitting.lambda.function")
            .description("The function name of the partition splitting lambda.")
            .propertyGroup(InstancePropertyGroup.PARTITION_SPLITTING)
            .build();
    SystemDefinedInstanceProperty PARTITION_SPLITTING_CLOUDWATCH_RULE = Index.propertyBuilder("sleeper.partition.splitting.rule")
            .description("The name of the CloudWatch rule that periodically triggers the partition splitting lambda.")
            .propertyGroup(InstancePropertyGroup.PARTITION_SPLITTING)
            .build();

    // Garbage collection
    SystemDefinedInstanceProperty GARBAGE_COLLECTOR_CLOUDWATCH_RULE = Index.propertyBuilder("sleeper.gc.rule")
            .description("The name of the CloudWatch rule that periodically triggers the garbage collector lambda.")
            .propertyGroup(InstancePropertyGroup.GARBAGE_COLLECTOR)
            .build();

    // Ingest
    SystemDefinedInstanceProperty INGEST_LAMBDA_FUNCTION = Index.propertyBuilder("sleeper.ingest.lambda.function")
            .description("The function name of the ingest task creator lambda.")
            .propertyGroup(InstancePropertyGroup.INGEST)
            .build();
    SystemDefinedInstanceProperty INGEST_CLOUDWATCH_RULE = Index.propertyBuilder("sleeper.ingest.rule")
            .description("The name of the CloudWatch rule that periodically triggers the ingest task creator lambda.")
            .propertyGroup(InstancePropertyGroup.INGEST)
            .build();
    SystemDefinedInstanceProperty INGEST_JOB_QUEUE_URL = Index.propertyBuilder("sleeper.ingest.job.queue.url")
            .description("The URL of the queue for ingest jobs.")
            .propertyGroup(InstancePropertyGroup.INGEST)
            .build();
    SystemDefinedInstanceProperty INGEST_JOB_DLQ_URL = Index.propertyBuilder("sleeper.ingest.job.dlq.url")
            .description("The URL of the dead letter queue for ingest jobs.")
            .propertyGroup(InstancePropertyGroup.INGEST)
            .build();
    SystemDefinedInstanceProperty INGEST_BATCHER_SUBMIT_QUEUE_URL = Index.propertyBuilder("sleeper.ingest.batcher.submit.queue.url")
            .description("The URL of the queue for ingest batcher file submission.")
            .propertyGroup(InstancePropertyGroup.INGEST)
            .build();
    SystemDefinedInstanceProperty INGEST_BATCHER_SUBMIT_DLQ_URL = Index.propertyBuilder("sleeper.ingest.batcher.submit.dlq.url")
            .description("The URL of the dead letter queue for ingest batcher file submission.")
            .propertyGroup(InstancePropertyGroup.INGEST)
            .build();
    SystemDefinedInstanceProperty INGEST_BATCHER_JOB_CREATION_CLOUDWATCH_RULE = Index.propertyBuilder("sleeper.ingest.batcher.job.creation.rule")
            .description("The name of the CloudWatch rule to trigger the batcher to create jobs from file ingest requests.")
            .propertyGroup(InstancePropertyGroup.INGEST)
            .build();
    SystemDefinedInstanceProperty INGEST_BATCHER_JOB_CREATION_FUNCTION = Index.propertyBuilder("sleeper.ingest.batcher.job.creation.lambda")
            .description("The name of the ingest batcher Lambda to create jobs from file ingest requests.")
            .propertyGroup(InstancePropertyGroup.INGEST)
            .build();
    SystemDefinedInstanceProperty INGEST_BATCHER_SUBMIT_REQUEST_FUNCTION = Index.propertyBuilder("sleeper.ingest.batcher.submit.lambda")
            .description("The name of the ingest batcher Lambda to submit file ingest requests.")
            .propertyGroup(InstancePropertyGroup.INGEST)
            .build();
    SystemDefinedInstanceProperty INGEST_TASK_DEFINITION_FAMILY = Index.propertyBuilder("sleeper.ingest.task.definition.family")
            .description("The name of the family of task definitions used for ingest tasks.")
            .propertyGroup(InstancePropertyGroup.INGEST)
            .build();
    SystemDefinedInstanceProperty INGEST_CLUSTER = Index.propertyBuilder("sleeper.ingest.cluster")
            .description("The name of the cluster used for ingest.")
            .propertyGroup(InstancePropertyGroup.INGEST)
            .build();

    // Bulk import
    SystemDefinedInstanceProperty BULK_IMPORT_BUCKET = Index.propertyBuilder("sleeper.bulk.import.bucket")
            .description("The S3 Bucket name of the bulk import bucket.")
            .propertyGroup(InstancePropertyGroup.BULK_IMPORT)
            .build();

    // Bulk import using EMR - these properties are used by both the persistent
    // and non-persistent EMR stacks
    SystemDefinedInstanceProperty BULK_IMPORT_EMR_EC2_ROLE_NAME = Index.propertyBuilder("sleeper.bulk.import.emr.ec2.role.name")
            .description("The name of the job flow role for bulk import using EMR.")
            .propertyGroup(InstancePropertyGroup.BULK_IMPORT)
            .build();
    SystemDefinedInstanceProperty BULK_IMPORT_EMR_CLUSTER_ROLE_NAME = Index.propertyBuilder("sleeper.bulk.import.emr.role.name")
            .description("The name of the role assumed by the bulk import clusters using EMR.")
            .propertyGroup(InstancePropertyGroup.BULK_IMPORT)
            .build();
    SystemDefinedInstanceProperty BULK_IMPORT_EMR_SECURITY_CONF_NAME = Index.propertyBuilder("sleeper.bulk.import.emr.security.conf.name")
            .description("The name of the security configuration used by bulk import using EMR.")
            .propertyGroup(InstancePropertyGroup.BULK_IMPORT)
            .build();

    // Bulk import using EMR
    SystemDefinedInstanceProperty BULK_IMPORT_EMR_JOB_QUEUE_URL = Index.propertyBuilder("sleeper.bulk.import.emr.job.queue.url")
            .description("The URL of the queue for bulk import jobs using EMR.")
            .propertyGroup(InstancePropertyGroup.BULK_IMPORT)
            .build();

    // Bulk import using EMR Serverless
    SystemDefinedInstanceProperty BULK_IMPORT_EMR_SERVERLESS_CLUSTER_NAME = Index.propertyBuilder("sleeper.bulk.import.emr.serverless.cluster.name")
            .description("The name of the cluster used for EMR Severless bulk import jobs.")
            .propertyGroup(InstancePropertyGroup.BULK_IMPORT)
            .build();

    SystemDefinedInstanceProperty BULK_IMPORT_EMR_SERVERLESS_APPLICATION_ID = Index.propertyBuilder("sleeper.bulk.import.emr.serverless.application.id")
            .description("The id of the application used for EMR Severless bulk import jobs.")
            .propertyGroup(InstancePropertyGroup.BULK_IMPORT)
            .build();
    // Bulk import using persistent EMR
    SystemDefinedInstanceProperty BULK_IMPORT_PERSISTENT_EMR_JOB_QUEUE_URL = Index.propertyBuilder("sleeper.bulk.import.persistent.emr.job.queue.url")
            .description("The URL of the queue for bulk import jobs using persistent EMR.")
            .propertyGroup(InstancePropertyGroup.BULK_IMPORT)
            .build();
    SystemDefinedInstanceProperty BULK_IMPORT_PERSISTENT_EMR_CLUSTER_NAME = Index.propertyBuilder("sleeper.bulk.import.persistent.emr.cluster.name")
            .description("The name of the cluster used for persistent EMR bulk import jobs.")
            .propertyGroup(InstancePropertyGroup.BULK_IMPORT)
            .build();
    SystemDefinedInstanceProperty BULK_IMPORT_PERSISTENT_EMR_MASTER_DNS = Index.propertyBuilder("sleeper.bulk.import.persistent.emr.master")
            .description("The public DNS name of the cluster's master node for bulk import using persistent EMR.")
            .propertyGroup(InstancePropertyGroup.BULK_IMPORT)
            .build();
    // Bulk import using EKS
    SystemDefinedInstanceProperty BULK_IMPORT_EKS_JOB_QUEUE_URL = Index.propertyBuilder("sleeper.bulk.import.eks.job.queue.url")
            .description("The URL of the queue for bulk import jobs using EKS.")
            .propertyGroup(InstancePropertyGroup.BULK_IMPORT)
            .build();
    SystemDefinedInstanceProperty BULK_IMPORT_EKS_STATE_MACHINE_ARN = Index.propertyBuilder("sleeper.bulk.import.eks.statemachine.arn")
            .description("The ARN of the state machine for bulk import jobs using EKS.")
            .propertyGroup(InstancePropertyGroup.BULK_IMPORT)
            .build();
    SystemDefinedInstanceProperty BULK_IMPORT_EKS_NAMESPACE = Index.propertyBuilder("sleeper.bulk.import.eks.k8s.namespace")
            .description("The namespace ID of the bulk import cluster using EKS.")
            .propertyGroup(InstancePropertyGroup.BULK_IMPORT)
            .build();
    SystemDefinedInstanceProperty BULK_IMPORT_EKS_CLUSTER_ENDPOINT = Index.propertyBuilder("sleeper.bulk.import.eks.k8s.endpoint")
            .description("The endpoint of the bulk import cluster using EKS.")
            .propertyGroup(InstancePropertyGroup.BULK_IMPORT)
            .build();

    static List<SystemDefinedInstanceProperty> getAll() {
        return Index.INSTANCE.getAll();
    }

    static boolean has(String propertyName) {
        return Index.INSTANCE.getByName(propertyName).isPresent();
    }

    @Override
    default boolean isSystemDefined() {
        return true;
    }


    class Index {

        private Index() {
        }

        private static final SleeperPropertyIndex<SystemDefinedInstanceProperty> INSTANCE = new SleeperPropertyIndex<>();

        private static SystemDefinedInstancePropertyImpl.Builder propertyBuilder(String propertyName) {
            return SystemDefinedInstancePropertyImpl.named(propertyName)
                    .addToIndex(INSTANCE::add);
        }
    }
}
