/*
 * Copyright 2022 Crown Copyright
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

/**
 * Properties set internally by sleeper and not by the user. These are set by the system itself at deployment time
 * so require no default or validation. Even if you set these in your properties file, they will be overwritten.
 */
public enum SystemDefinedInstanceProperty implements InstanceProperty {
    // Configuration
    CONFIG_BUCKET("sleeper.config.bucket"),

    // Table management
    TABLE_REQUEST_FUNCTION_NAME("sleeper.table.requests.function.name"),

    // Table metrics
    TABLE_METRICS_RULES("sleeper.table.metrics.rulenames"),

    // Query
    QUERY_LAMBDA_ROLE("sleeper.query.role.arn"),
    QUERY_WEBSOCKET_API_URL("sleeper.query.websocket.api.url"),
    QUERY_QUEUE_URL("sleeper.query.queue.url"),
    QUERY_DLQ_URL("sleeper.query.dlq.url"),
    QUERY_RESULTS_QUEUE_URL("sleeper.query.results.queue.url"),
    QUERY_RESULTS_BUCKET("sleeper.query.results.bucket"),
    QUERY_TRACKER_TABLE_NAME("sleeper.query.tracker.table.name"),

    // Compactions
    COMPACTION_CONTAINER("sleeper.compaction.container"),
    COMPACTION_CLUSTER("sleeper.compaction.cluster"),
    SPLITTING_COMPACTION_CLUSTER("sleeper.compaction.splitting.cluster"),
    COMPACTION_TASK_EC2_DEFINITION_FAMILY("sleeper.compaction.ec2.task.definition"),
    COMPACTION_TASK_FARGATE_DEFINITION_FAMILY("sleeper.compaction.fargate.task.definition"),
    COMPACTION_JOB_CREATION_CLOUDWATCH_RULE("sleeper.compaction.job.creation.rule"),
    COMPACTION_JOB_QUEUE_URL("sleeper.compaction.job.queue.url"),
    COMPACTION_JOB_DLQ_URL("sleeper.compaction.job.dlq.url"),
    COMPACTION_TASK_CREATION_CLOUDWATCH_RULE("sleeper.compaction.task.creation.rule"),
    SPLITTING_COMPACTION_TASK_EC2_DEFINITION_FAMILY("sleeper.compaction.splitting.ec2.task.definition"),
    SPLITTING_COMPACTION_TASK_FARGATE_DEFINITION_FAMILY("sleeper.compaction.splitting.fargate.task.definition"),
    SPLITTING_COMPACTION_JOB_QUEUE_URL("sleeper.compaction.splitting.job.queue.url"),
    SPLITTING_COMPACTION_JOB_DLQ_URL("sleeper.compaction.splitting.job.dlq.url"),
    SPLITTING_COMPACTION_TASK_CREATION_CLOUDWATCH_RULE("sleeper.compaction.splitting.task.creation.rule"),

    // Partition splitting
    PARTITION_SPLITTING_QUEUE_URL("sleeper.partition.splitting.queue.url"),
    PARTITION_SPLITTING_DLQ_URL("sleeper.partition.splitting.dlq.url"),
    PARTITION_SPLITTING_CLOUDWATCH_RULE("sleeper.partition.splitting.rule"),

    // Garbage collection
    GARBAGE_COLLECTOR_CLOUDWATCH_RULE("sleeper.gc.rule"),

    // Ingest
    INGEST_CLOUDWATCH_RULE("sleeper.ingest.rule"),
    INGEST_JOB_QUEUE_URL("sleeper.ingest.job.queue.url"),
    INGEST_JOB_DLQ_URL("sleeper.ingest.job.dlq.url"),
    INGEST_TASK_DEFINITION_FAMILY("sleeper.ingest.task.definition.family"),
    INGEST_CLUSTER("sleeper.ingest.cluster"),

    // Bulk import
    BULK_IMPORT_BUCKET("sleeper.bulk.import.bucket"),

    // Bulk import using EMR - these properties are used by both the persistent
    // and non-persistent EMR stacks
    BULK_IMPORT_EMR_EC2_ROLE_NAME("sleeper.bulk.import.emr.ec2.role.name"),
    BULK_IMPORT_EMR_CLUSTER_ROLE_NAME("sleeper.bulk.import.emr.role.name"),
    BULK_IMPORT_EMR_SECURITY_CONF_NAME("sleeper.bulk.import.emr.security.conf.name"),

    // Bulk import using EMR
    BULK_IMPORT_EMR_JOB_QUEUE_URL("sleeper.bulk.import.emr.job.queue.url"),

    // Bulk import using persistent EMR
    BULK_IMPORT_PERSISTENT_EMR_JOB_QUEUE_URL("sleeper.bulk.import.persistent.emr.job.queue.url"),
    BULK_IMPORT_PERSISTENT_EMR_MASTER_DNS("sleeper.bulk.import.persistent.emr.master"),

    // Bulk import using EKS
    BULK_IMPORT_EKS_JOB_QUEUE_URL("sleeper.bulk.import.eks.job.queue.url"),
    BULK_IMPORT_EKS_STATE_MACHINE_ARN("sleeper.bulk.import.eks.statemachine.arn"),
    BULK_IMPORT_EKS_NAMESPACE("sleeper.bulk.import.eks.k8s.namespace"),
    BULK_IMPORT_EKS_CLUSTER_ENDPOINT("sleeper.bulk.import.eks.k8s.endpoint");

    private final String propertyName;

    SystemDefinedInstanceProperty(String propertyName) {
        this.propertyName = propertyName;
    }

    @Override
    public String getDefaultValue() {
        return null;
    }

    @Override
    public String getPropertyName() {
        return propertyName;
    }
}
