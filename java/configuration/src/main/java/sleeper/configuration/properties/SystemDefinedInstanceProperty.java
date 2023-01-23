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
 * Properties set internally by sleeper and not by the user. These are set by the system itself at
 * deployment time so require no default or validation. Even if you set these in your properties
 * file, they will be overwritten.
 */
// Suppress as this class will always be referenced before impl class, so initialization behaviour will be deterministic
@SuppressFBWarnings("IC_SUPERCLASS_USES_SUBCLASS_DURING_INITIALIZATION")
public interface SystemDefinedInstanceProperty extends InstanceProperty {
    // Configuration
    SystemDefinedInstanceProperty CONFIG_BUCKET = named("sleeper.config.bucket");

    // Table management
    SystemDefinedInstanceProperty TABLE_REQUEST_FUNCTION_NAME = named("sleeper.table.requests.function.name");

    // Table metrics
    SystemDefinedInstanceProperty TABLE_METRICS_RULES = named("sleeper.table.metrics.rulenames");

    // Query
    SystemDefinedInstanceProperty QUERY_LAMBDA_ROLE = named("sleeper.query.role.arn");
    SystemDefinedInstanceProperty QUERY_WEBSOCKET_API_URL = named("sleeper.query.websocket.api.url");
    SystemDefinedInstanceProperty QUERY_QUEUE_URL = named("sleeper.query.queue.url");
    SystemDefinedInstanceProperty QUERY_DLQ_URL = named("sleeper.query.dlq.url");
    SystemDefinedInstanceProperty QUERY_RESULTS_QUEUE_URL = named("sleeper.query.results.queue.url");
    SystemDefinedInstanceProperty QUERY_RESULTS_BUCKET = named("sleeper.query.results.bucket");
    SystemDefinedInstanceProperty QUERY_TRACKER_TABLE_NAME = named("sleeper.query.tracker.table.name");

    // Compactions
    SystemDefinedInstanceProperty COMPACTION_CONTAINER = named("sleeper.compaction.container");
    SystemDefinedInstanceProperty COMPACTION_CLUSTER = named("sleeper.compaction.cluster");
    SystemDefinedInstanceProperty SPLITTING_COMPACTION_CLUSTER = named("sleeper.compaction.splitting.cluster");
    SystemDefinedInstanceProperty COMPACTION_TASK_EC2_DEFINITION_FAMILY = named("sleeper.compaction.ec2.task.definition");
    SystemDefinedInstanceProperty COMPACTION_TASK_FARGATE_DEFINITION_FAMILY = named("sleeper.compaction.fargate.task.definition");
    SystemDefinedInstanceProperty COMPACTION_JOB_CREATION_LAMBDA_FUNCTION = named("sleeper.compaction.job.creation.lambda.function");
    SystemDefinedInstanceProperty COMPACTION_JOB_CREATION_CLOUDWATCH_RULE = named("sleeper.compaction.job.creation.rule");
    SystemDefinedInstanceProperty COMPACTION_JOB_QUEUE_URL = named("sleeper.compaction.job.queue.url");
    SystemDefinedInstanceProperty COMPACTION_JOB_DLQ_URL = named("sleeper.compaction.job.dlq.url");
    SystemDefinedInstanceProperty COMPACTION_TASK_CREATION_LAMBDA_FUNCTION = named("sleeper.compaction.task.creation.lambda.function");
    SystemDefinedInstanceProperty COMPACTION_TASK_CREATION_CLOUDWATCH_RULE = named("sleeper.compaction.task.creation.rule");
    SystemDefinedInstanceProperty COMPACTION_AUTO_SCALING_GROUP = named("sleeper.compaction.scaling.group");
    SystemDefinedInstanceProperty SPLITTING_COMPACTION_TASK_EC2_DEFINITION_FAMILY = named("sleeper.compaction.splitting.ec2.task.definition");
    SystemDefinedInstanceProperty SPLITTING_COMPACTION_TASK_FARGATE_DEFINITION_FAMILY = named("sleeper.compaction.splitting.fargate.task.definition");
    SystemDefinedInstanceProperty SPLITTING_COMPACTION_JOB_QUEUE_URL = named("sleeper.compaction.splitting.job.queue.url");
    SystemDefinedInstanceProperty SPLITTING_COMPACTION_JOB_DLQ_URL = named("sleeper.compaction.splitting.job.dlq.url");
    SystemDefinedInstanceProperty SPLITTING_COMPACTION_TASK_CREATION_LAMBDA_FUNCTION = named("sleeper.compaction.splitting.task.creation.lambda.function");
    SystemDefinedInstanceProperty SPLITTING_COMPACTION_TASK_CREATION_CLOUDWATCH_RULE = named("sleeper.compaction.splitting.task.creation.rule");
    SystemDefinedInstanceProperty SPLITTING_COMPACTION_AUTO_SCALING_GROUP = named("sleeper.compaction.splitting.scaling.group");

    // Partition splitting
    SystemDefinedInstanceProperty PARTITION_SPLITTING_QUEUE_URL = named("sleeper.partition.splitting.queue.url");
    SystemDefinedInstanceProperty PARTITION_SPLITTING_DLQ_URL = named("sleeper.partition.splitting.dlq.url");
    SystemDefinedInstanceProperty PARTITION_SPLITTING_LAMBDA_FUNCTION = named("sleeper.partition.splitting.lambda.function");
    SystemDefinedInstanceProperty PARTITION_SPLITTING_CLOUDWATCH_RULE = named("sleeper.partition.splitting.rule");

    // Garbage collection
    SystemDefinedInstanceProperty GARBAGE_COLLECTOR_CLOUDWATCH_RULE = named("sleeper.gc.rule");

    // Ingest
    SystemDefinedInstanceProperty INGEST_LAMBDA_FUNCTION = named("sleeper.ingest.lambda.function");
    SystemDefinedInstanceProperty INGEST_CLOUDWATCH_RULE = named("sleeper.ingest.rule");
    SystemDefinedInstanceProperty INGEST_JOB_QUEUE_URL = named("sleeper.ingest.job.queue.url");
    SystemDefinedInstanceProperty INGEST_JOB_DLQ_URL = named("sleeper.ingest.job.dlq.url");
    SystemDefinedInstanceProperty INGEST_TASK_DEFINITION_FAMILY = named("sleeper.ingest.task.definition.family");
    SystemDefinedInstanceProperty INGEST_CLUSTER = named("sleeper.ingest.cluster");

    // Bulk import
    SystemDefinedInstanceProperty BULK_IMPORT_BUCKET = named("sleeper.bulk.import.bucket");

    // Bulk import using EMR - these properties are used by both the persistent
    // and non-persistent EMR stacks
    SystemDefinedInstanceProperty BULK_IMPORT_EMR_EC2_ROLE_NAME = named("sleeper.bulk.import.emr.ec2.role.name");
    SystemDefinedInstanceProperty BULK_IMPORT_EMR_CLUSTER_ROLE_NAME = named("sleeper.bulk.import.emr.role.name");
    SystemDefinedInstanceProperty BULK_IMPORT_EMR_SECURITY_CONF_NAME = named("sleeper.bulk.import.emr.security.conf.name");

    // Bulk import using EMR
    SystemDefinedInstanceProperty BULK_IMPORT_EMR_JOB_QUEUE_URL = named("sleeper.bulk.import.emr.job.queue.url");

    // Bulk import using persistent EMR
    SystemDefinedInstanceProperty BULK_IMPORT_PERSISTENT_EMR_JOB_QUEUE_URL = named("sleeper.bulk.import.persistent.emr.job.queue.url");
    SystemDefinedInstanceProperty BULK_IMPORT_PERSISTENT_EMR_MASTER_DNS = named("sleeper.bulk.import.persistent.emr.master");

    // Bulk import using EKS
    SystemDefinedInstanceProperty BULK_IMPORT_EKS_JOB_QUEUE_URL = named("sleeper.bulk.import.eks.job.queue.url");
    SystemDefinedInstanceProperty BULK_IMPORT_EKS_STATE_MACHINE_ARN = named("sleeper.bulk.import.eks.statemachine.arn");
    SystemDefinedInstanceProperty BULK_IMPORT_EKS_NAMESPACE = named("sleeper.bulk.import.eks.k8s.namespace");
    SystemDefinedInstanceProperty BULK_IMPORT_EKS_CLUSTER_ENDPOINT = named("sleeper.bulk.import.eks.k8s.endpoint");

    static SystemDefinedInstanceProperty[] values() {
        return SystemDefinedInstancePropertyImpl.all().toArray(new SystemDefinedInstanceProperty[0]);
    }
}
