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

import static sleeper.configuration.properties.SystemDefinedInstancePropertyImpl.named;

/**
 * Properties set internally by sleeper and not by the user. These are set by the system itself at deployment time
 * so require no default or validation. Even if you set these in your properties file, they will be overwritten.
 */
// Suppress as this class will always be referenced before impl class, so initialization behaviour will be deterministic
@SuppressFBWarnings("IC_SUPERCLASS_USES_SUBCLASS_DURING_INITIALIZATION")
public interface SystemDefinedInstanceProperty extends InstanceProperty {
    // Configuration
    SystemDefinedInstanceProperty CONFIG_BUCKET = named("sleeper.config.bucket")
            .description("The S3 bucket name used to store configuration files.")
            .group(PropertyGroup.COMMON)
            .build();

    // Table management
    SystemDefinedInstanceProperty TABLE_REQUEST_FUNCTION_NAME = named("sleeper.table.requests.function.name")
            .description("Unknown - not used.")
            .group(PropertyGroup.UNKNOWN)
            .build();

    // Table metrics
    SystemDefinedInstanceProperty TABLE_METRICS_RULES = named("sleeper.table.metrics.rulenames")
            .description("The names of the CloudWatch rules that trigger generation of metrics for tables.")
            .group(PropertyGroup.COMMON)
            .build();

    // Query
    SystemDefinedInstanceProperty QUERY_LAMBDA_ROLE = named("sleeper.query.role.arn")
            .description("The ARN for the role with required permissions to query sleeper.")
            .group(PropertyGroup.QUERY)
            .build();
    SystemDefinedInstanceProperty QUERY_WEBSOCKET_API_URL = named("sleeper.query.websocket.api.url")
            .description("The URL of the WebSocket API for querying sleeper.")
            .group(PropertyGroup.QUERY)
            .build();
    SystemDefinedInstanceProperty QUERY_QUEUE_URL = named("sleeper.query.queue.url")
            .description("The URL of the queue responsible for sending a query to sleeper.")
            .group(PropertyGroup.QUERY)
            .build();
    SystemDefinedInstanceProperty QUERY_DLQ_URL = named("sleeper.query.dlq.url")
            .description("The URL of the dead letter queue used when querying sleeper.")
            .group(PropertyGroup.QUERY)
            .build();
    SystemDefinedInstanceProperty QUERY_RESULTS_QUEUE_URL = named("sleeper.query.results.queue.url")
            .description("The URL of the queue responsible for retrieving results from a query sent to sleeper.")
            .group(PropertyGroup.QUERY)
            .build();
    SystemDefinedInstanceProperty QUERY_RESULTS_BUCKET = named("sleeper.query.results.bucket")
            .description("The S3 Bucket name of the query results bucket.")
            .group(PropertyGroup.QUERY)
            .build();
    SystemDefinedInstanceProperty QUERY_TRACKER_TABLE_NAME = named("sleeper.query.tracker.table.name")
            .description("The name of the table responsible for tracking query progress.")
            .group(PropertyGroup.QUERY)
            .build();

    // Compactions
    SystemDefinedInstanceProperty COMPACTION_CONTAINER = named("sleeper.compaction.container")
            .description("Unknown - not used.")
            .group(PropertyGroup.UNKNOWN)
            .build();
    SystemDefinedInstanceProperty COMPACTION_CLUSTER = named("sleeper.compaction.cluster")
            .description("The name of the cluster used for compactions.")
            .group(PropertyGroup.COMPACTION)
            .build();
    SystemDefinedInstanceProperty SPLITTING_COMPACTION_CLUSTER = named("sleeper.compaction.splitting.cluster")
            .description("The name of the cluster used for splitting compactions.")
            .group(PropertyGroup.COMPACTION)
            .build();
    SystemDefinedInstanceProperty COMPACTION_TASK_DEFINITION_FAMILY = named("sleeper.compaction.task.definition")
            .description("The name of the family of task definitions used for compactions.")
            .group(PropertyGroup.COMPACTION)
            .build();
    SystemDefinedInstanceProperty COMPACTION_JOB_CREATION_LAMBDA_FUNCTION = named("sleeper.compaction.job.creation.lambda.function")
            .description("The function name of the compaction job creation lambda.")
            .group(PropertyGroup.COMPACTION)
            .build();
    SystemDefinedInstanceProperty COMPACTION_JOB_CREATION_CLOUDWATCH_RULE = named("sleeper.compaction.job.creation.rule")
            .description("The name of the CloudWatch rule that periodically triggers the compaction job creation lambda.")
            .group(PropertyGroup.COMPACTION)
            .build();
    SystemDefinedInstanceProperty COMPACTION_JOB_QUEUE_URL = named("sleeper.compaction.job.queue.url")
            .description("The URL of the queue for compaction jobs.")
            .group(PropertyGroup.COMPACTION)
            .build();
    SystemDefinedInstanceProperty COMPACTION_JOB_DLQ_URL = named("sleeper.compaction.job.dlq.url")
            .description("The URL of the dead letter queue for compaction jobs.")
            .group(PropertyGroup.COMPACTION)
            .build();
    SystemDefinedInstanceProperty COMPACTION_TASK_CREATION_LAMBDA_FUNCTION = named("sleeper.compaction.task.creation.lambda.function")
            .description("The function name of the compaction task creation lambda.")
            .group(PropertyGroup.COMPACTION)
            .build();
    SystemDefinedInstanceProperty COMPACTION_TASK_CREATION_CLOUDWATCH_RULE = named("sleeper.compaction.task.creation.rule")
            .description("The name of the CloudWatch rule that periodically triggers the compaction task creation lambda.")
            .group(PropertyGroup.COMPACTION)
            .build();
    SystemDefinedInstanceProperty SPLITTING_COMPACTION_TASK_DEFINITION_FAMILY = named("sleeper.compaction.splitting.task.definition")
            .description("The name of the family of task definitions used for splitting compactions.")
            .group(PropertyGroup.COMPACTION)
            .build();

    SystemDefinedInstanceProperty SPLITTING_COMPACTION_JOB_QUEUE_URL = named("sleeper.compaction.splitting.job.queue.url")
            .description("The URL of the queue for splitting compaction jobs.")
            .group(PropertyGroup.COMPACTION)
            .build();
    SystemDefinedInstanceProperty SPLITTING_COMPACTION_JOB_DLQ_URL = named("sleeper.compaction.splitting.job.dlq.url")
            .description("The URL of the dead letter queue for splitting compaction jobs.")
            .group(PropertyGroup.COMPACTION)
            .build();
    SystemDefinedInstanceProperty SPLITTING_COMPACTION_TASK_CREATION_LAMBDA_FUNCTION = named("sleeper.compaction.splitting.task.creation.lambda.function")
            .description("The function name of the splitting compaction task creation lambda.")
            .group(PropertyGroup.COMPACTION)
            .build();
    SystemDefinedInstanceProperty SPLITTING_COMPACTION_TASK_CREATION_CLOUDWATCH_RULE = named("sleeper.compaction.splitting.task.creation.rule")
            .description("The name of the CloudWatch rule that periodically triggers the splitting compaction task creation lambda.")
            .group(PropertyGroup.COMPACTION)
            .build();

    // Partition splitting
    SystemDefinedInstanceProperty PARTITION_SPLITTING_QUEUE_URL = named("sleeper.partition.splitting.queue.url")
            .description("The URL of the queue for partition splitting.")
            .group(PropertyGroup.PARTITION_SPLITTING)
            .build();
    SystemDefinedInstanceProperty PARTITION_SPLITTING_DLQ_URL = named("sleeper.partition.splitting.dlq.url")
            .description("The URL of the dead letter queue for partition splitting.")
            .group(PropertyGroup.PARTITION_SPLITTING)
            .build();
    SystemDefinedInstanceProperty PARTITION_SPLITTING_LAMBDA_FUNCTION = named("sleeper.partition.splitting.lambda.function")
            .description("The function name of the partition splitting lambda.")
            .group(PropertyGroup.PARTITION_SPLITTING)
            .build();
    SystemDefinedInstanceProperty PARTITION_SPLITTING_CLOUDWATCH_RULE = named("sleeper.partition.splitting.rule")
            .description("The name of the CloudWatch rule that periodically triggers the partition splitting lambda.")
            .group(PropertyGroup.PARTITION_SPLITTING)
            .build();

    // Garbage collection
    SystemDefinedInstanceProperty GARBAGE_COLLECTOR_CLOUDWATCH_RULE = named("sleeper.gc.rule")
            .description("The name of the CloudWatch rule that periodically triggers the garbage collector lambda.")
            .group(PropertyGroup.GARBAGE_COLLECTOR)
            .build();

    // Ingest
    SystemDefinedInstanceProperty INGEST_LAMBDA_FUNCTION = named("sleeper.ingest.lambda.function")
            .description("The function name of the ingest task creator lambda.")
            .group(PropertyGroup.INGEST)
            .build();
    SystemDefinedInstanceProperty INGEST_CLOUDWATCH_RULE = named("sleeper.ingest.rule")
            .description("The name of the CloudWatch rule that periodically triggers the ingest task creator lambda.")
            .group(PropertyGroup.INGEST)
            .build();
    SystemDefinedInstanceProperty INGEST_JOB_QUEUE_URL = named("sleeper.ingest.job.queue.url")
            .description("The URL of the queue for ingest jobs.")
            .group(PropertyGroup.INGEST)
            .build();
    SystemDefinedInstanceProperty INGEST_JOB_DLQ_URL = named("sleeper.ingest.job.dlq.url")
            .description("The URL of the dead letter queue for ingest jobs.")
            .group(PropertyGroup.INGEST)
            .build();
    SystemDefinedInstanceProperty INGEST_TASK_DEFINITION_FAMILY = named("sleeper.ingest.task.definition.family")
            .description("The name of the family of task definitions used for ingest tasks.")
            .group(PropertyGroup.INGEST)
            .build();
    SystemDefinedInstanceProperty INGEST_CLUSTER = named("sleeper.ingest.cluster")
            .description("The name of the cluster used for ingest.")
            .group(PropertyGroup.INGEST)
            .build();

    // Bulk import
    SystemDefinedInstanceProperty BULK_IMPORT_BUCKET = named("sleeper.bulk.import.bucket")
            .description("The S3 Bucket name of the bulk import bucket.")
            .group(PropertyGroup.BULK_IMPORT)
            .build();

    // Bulk import using EMR - these properties are used by both the persistent
    // and non-persistent EMR stacks
    SystemDefinedInstanceProperty BULK_IMPORT_EMR_EC2_ROLE_NAME = named("sleeper.bulk.import.emr.ec2.role.name")
            .description("The name of the job flow role for bulk import using EMR.")
            .group(PropertyGroup.BULK_IMPORT)
            .build();
    SystemDefinedInstanceProperty BULK_IMPORT_EMR_CLUSTER_ROLE_NAME = named("sleeper.bulk.import.emr.role.name")
            .description("The name of the role assumed by the bulk import clusters using EMR.")
            .group(PropertyGroup.BULK_IMPORT)
            .build();
    SystemDefinedInstanceProperty BULK_IMPORT_EMR_SECURITY_CONF_NAME = named("sleeper.bulk.import.emr.security.conf.name")
            .description("The name of the security configuration used by bulk import using EMR.")
            .group(PropertyGroup.BULK_IMPORT)
            .build();

    // Bulk import using EMR
    SystemDefinedInstanceProperty BULK_IMPORT_EMR_JOB_QUEUE_URL = named("sleeper.bulk.import.emr.job.queue.url")
            .description("The URL of the queue for bulk import jobs using EMR")
            .group(PropertyGroup.BULK_IMPORT)
            .build();

    // Bulk import using persistent EMR
    SystemDefinedInstanceProperty BULK_IMPORT_PERSISTENT_EMR_JOB_QUEUE_URL = named("sleeper.bulk.import.persistent.emr.job.queue.url")
            .description("The URL of the queue for bulk import jobs using persistent EMR.")
            .group(PropertyGroup.BULK_IMPORT)
            .build();
    SystemDefinedInstanceProperty BULK_IMPORT_PERSISTENT_EMR_MASTER_DNS = named("sleeper.bulk.import.persistent.emr.master")
            .description("The public DNS name of the cluster's master node for bulk import using persistent EMR.")
            .group(PropertyGroup.BULK_IMPORT)
            .build();

    // Bulk import using EKS
    SystemDefinedInstanceProperty BULK_IMPORT_EKS_JOB_QUEUE_URL = named("sleeper.bulk.import.eks.job.queue.url")
            .description("The URL of the queue for bulk import jobs using EKS.")
            .group(PropertyGroup.BULK_IMPORT)
            .build();
    SystemDefinedInstanceProperty BULK_IMPORT_EKS_STATE_MACHINE_ARN = named("sleeper.bulk.import.eks.statemachine.arn")
            .description("The ARN of the state machine for bulk import jobs using EKS.")
            .group(PropertyGroup.BULK_IMPORT)
            .build();
    SystemDefinedInstanceProperty BULK_IMPORT_EKS_NAMESPACE = named("sleeper.bulk.import.eks.k8s.namespace")
            .description("The namespace ID of the bulk import cluster using EKS.")
            .group(PropertyGroup.BULK_IMPORT)
            .build();
    SystemDefinedInstanceProperty BULK_IMPORT_EKS_CLUSTER_ENDPOINT = named("sleeper.bulk.import.eks.k8s.endpoint")
            .description("The endpoint of the bulk import cluster using EKS.")
            .group(PropertyGroup.BULK_IMPORT)
            .build();

    static SystemDefinedInstanceProperty[] values() {
        return SystemDefinedInstancePropertyImpl.all().toArray(new SystemDefinedInstanceProperty[0]);
    }

    static SystemDefinedInstanceProperty from(String propertyName) {
        return SystemDefinedInstancePropertyImpl.get(propertyName);
    }

    static boolean has(String propertyName) {
        return SystemDefinedInstancePropertyImpl.get(propertyName) != null;
    }
}
