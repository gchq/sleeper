/*
 * Copyright 2022-2025 Crown Copyright
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
package sleeper.core.properties.instance;

import edu.umd.cs.findbugs.annotations.SuppressFBWarnings;

import sleeper.core.properties.PropertyGroup;
import sleeper.core.properties.SleeperPropertyIndex;

import java.util.List;

/**
 * Definitions of properties set internally by the Sleeper CDK app and not by
 * the user. These are set by the system
 * itself at deployment time so require no default or validation. If you set
 * these in your properties file, they will be
 * overwritten by the CDK.
 */
// Suppress as this class will always be referenced before impl class, so
// initialization behaviour will be deterministic
@SuppressFBWarnings("IC_SUPERCLASS_USES_SUBCLASS_DURING_INITIALIZATION")
public interface CdkDefinedInstanceProperty extends InstanceProperty {

    static List<CdkDefinedInstanceProperty> getAll() {
        return Index.INSTANCE.getAll();
    }

    /**
     * Retrieves a list of CDK defined instance properties in the given group.
     *
     * @param  group the group
     * @return       the properties
     */
    static List<CdkDefinedInstanceProperty> getAllInGroup(PropertyGroup group) {
        return Index.INSTANCE.getAllInGroup(group);
    }

    CdkDefinedInstanceProperty VERSION = Index.propertyBuilder("sleeper.version")
            .description("The version of Sleeper that is being used. This property is used to identify the correct " +
                    "jars in the S3 jars bucket and to select the correct tag in the ECR repositories.")
            .propertyGroup(InstancePropertyGroup.COMMON).build();
    CdkDefinedInstanceProperty ADMIN_ROLE_ARN = Index.propertyBuilder("sleeper.admin.role.arn")
            .description("The ARN of a role that has permissions to administer the instance.")
            .propertyGroup(InstancePropertyGroup.COMMON)
            .build();

    // Configuration
    CdkDefinedInstanceProperty CONFIG_BUCKET = Index.propertyBuilder("sleeper.config.bucket")
            .description("The S3 bucket name used to store configuration files.")
            .propertyGroup(InstancePropertyGroup.COMMON)
            .build();

    //AWS Config
    CdkDefinedInstanceProperty ACCOUNT = Index.propertyBuilder("sleeper.account")
            .description("The AWS account number. This is the AWS account that the instance is deployed in.")
            .propertyGroup(InstancePropertyGroup.COMMON)
            .build();
    CdkDefinedInstanceProperty REGION = Index.propertyBuilder("sleeper.region")
            .description("The AWS region the instance is deployed in.")
            .propertyGroup(InstancePropertyGroup.COMMON)
            .build();

    // Security Groups
    CdkDefinedInstanceProperty ECS_SECURITY_GROUP = Index.propertyBuilder("sleeper.ecs.security.group.id")
            .description("The security group ID to be used for ECS tasks and services.")
            .propertyGroup(InstancePropertyGroup.COMMON)
            .build();

    // Data
    CdkDefinedInstanceProperty DATA_BUCKET = Index.propertyBuilder("sleeper.data.bucket")
            .description("The S3 bucket name used to store table data.")
            .propertyGroup(InstancePropertyGroup.COMMON)
            .build();

    // Tables
    CdkDefinedInstanceProperty TABLE_NAME_INDEX_DYNAMO_TABLENAME = Index
            .propertyBuilder("sleeper.tables.name.index.dynamo.table")
            .description("The name of the DynamoDB table indexing tables by their name.")
            .propertyGroup(InstancePropertyGroup.COMMON)
            .build();
    CdkDefinedInstanceProperty TABLE_ID_INDEX_DYNAMO_TABLENAME = Index
            .propertyBuilder("sleeper.tables.id.index.dynamo.table")
            .description("The name of the DynamoDB table indexing tables by their ID.")
            .propertyGroup(InstancePropertyGroup.COMMON)
            .build();
    CdkDefinedInstanceProperty TABLE_ONLINE_INDEX_DYNAMO_TABLENAME = Index
            .propertyBuilder("sleeper.tables.online.index.dynamo.table")
            .description(
                    "The name of the DynamoDB table indexing tables by whether they are online or not, sorted by table name.")
            .propertyGroup(InstancePropertyGroup.COMMON)
            .build();

    // TransactionLogStateStore
    CdkDefinedInstanceProperty TRANSACTION_LOG_FILES_TABLENAME = Index
            .propertyBuilder("sleeper.statestore.transactionlog.dynamo.file.log.table")
            .description("The name of the DynamoDB table holding the state store file transaction log.")
            .propertyGroup(InstancePropertyGroup.COMMON)
            .build();
    CdkDefinedInstanceProperty TRANSACTION_LOG_PARTITIONS_TABLENAME = Index
            .propertyBuilder("sleeper.statestore.transactionlog.dynamo.partition.log.table")
            .description("The name of the DynamoDB table holding the state store partition transaction log.")
            .propertyGroup(InstancePropertyGroup.COMMON)
            .build();
    CdkDefinedInstanceProperty TRANSACTION_LOG_ALL_SNAPSHOTS_TABLENAME = Index
            .propertyBuilder("sleeper.statestore.transactionlog.dynamo.all.snapshots.table")
            .description("The name of the DynamoDB table holding information about all transaction log snapshots.")
            .propertyGroup(InstancePropertyGroup.COMMON)
            .build();
    CdkDefinedInstanceProperty TRANSACTION_LOG_LATEST_SNAPSHOTS_TABLENAME = Index
            .propertyBuilder("sleeper.statestore.transactionlog.dynamo.latest.snapshots.table")
            .description("The name of the DynamoDB table holding information about latest transaction log snapshots.")
            .propertyGroup(InstancePropertyGroup.COMMON)
            .build();
    CdkDefinedInstanceProperty TRANSACTION_LOG_SNAPSHOT_CREATION_QUEUE_URL = Index
            .propertyBuilder("sleeper.statestore.transactionlog.snapshots.creation.queue.url")
            .description("URL of the queue for transaction log snapshot creation requests.")
            .propertyGroup(InstancePropertyGroup.COMMON)
            .build();
    CdkDefinedInstanceProperty TRANSACTION_LOG_SNAPSHOT_CREATION_QUEUE_ARN = Index
            .propertyBuilder("sleeper.statestore.transactionlog.snapshots.creation.queue.arn")
            .description("The ARN of the queue for transaction log snapshot creation requests.")
            .propertyGroup(InstancePropertyGroup.COMMON)
            .build();
    CdkDefinedInstanceProperty TRANSACTION_LOG_SNAPSHOT_CREATION_DLQ_URL = Index
            .propertyBuilder("sleeper.statestore.transactionlog.snapshots.creation.dlq.url")
            .description("The URL of the dead letter queue for transaction log snapshot creation requests.")
            .propertyGroup(InstancePropertyGroup.COMMON)
            .build();
    CdkDefinedInstanceProperty TRANSACTION_LOG_SNAPSHOT_CREATION_DLQ_ARN = Index
            .propertyBuilder("sleeper.statestore.transactionlog.snapshots.creation.dlq.arn")
            .description("The ARN of the dead letter queue for transaction log snapshot creation requests.")
            .propertyGroup(InstancePropertyGroup.COMMON)
            .build();
    CdkDefinedInstanceProperty TRANSACTION_LOG_SNAPSHOT_CREATION_RULE = Index
            .propertyBuilder("sleeper.statestore.transactionlog.snapshots.creation.rule")
            .description("The name of the CloudWatch rule that triggers creation of transaction log snapshots.")
            .propertyGroup(InstancePropertyGroup.COMMON)
            .build();
    CdkDefinedInstanceProperty TRANSACTION_LOG_SNAPSHOT_DELETION_QUEUE_URL = Index
            .propertyBuilder("sleeper.statestore.transactionlog.snapshots.deletion.queue.url")
            .description("URL of the queue for transaction log snapshot deletion requests.")
            .propertyGroup(InstancePropertyGroup.COMMON)
            .build();
    CdkDefinedInstanceProperty TRANSACTION_LOG_SNAPSHOT_DELETION_QUEUE_ARN = Index
            .propertyBuilder("sleeper.statestore.transactionlog.snapshots.deletion.queue.arn")
            .description("The ARN of the queue for transaction log snapshot deletion requests.")
            .propertyGroup(InstancePropertyGroup.COMMON)
            .build();
    CdkDefinedInstanceProperty TRANSACTION_LOG_SNAPSHOT_DELETION_DLQ_URL = Index
            .propertyBuilder("sleeper.statestore.transactionlog.snapshots.deletion.dlq.url")
            .description("The URL of the dead letter queue for transaction log snapshot deletion requests.")
            .propertyGroup(InstancePropertyGroup.COMMON)
            .build();
    CdkDefinedInstanceProperty TRANSACTION_LOG_SNAPSHOT_DELETION_DLQ_ARN = Index
            .propertyBuilder("sleeper.statestore.transactionlog.snapshots.deletion.dlq.arn")
            .description("The ARN of the dead letter queue for transaction log snapshot deletion requests.")
            .propertyGroup(InstancePropertyGroup.COMMON)
            .build();
    CdkDefinedInstanceProperty TRANSACTION_LOG_SNAPSHOT_DELETION_RULE = Index
            .propertyBuilder("sleeper.statestore.transactionlog.snapshots.deletion.rule")
            .description("The name of the CloudWatch rule that triggers deletion of transaction log snapshots.")
            .propertyGroup(InstancePropertyGroup.COMMON)
            .build();
    CdkDefinedInstanceProperty TRANSACTION_LOG_TRANSACTION_DELETION_QUEUE_URL = Index
            .propertyBuilder("sleeper.statestore.transactionlog.transaction.deletion.queue.url")
            .description("URL of the queue for transaction log transaction deletion requests.")
            .propertyGroup(InstancePropertyGroup.COMMON)
            .build();
    CdkDefinedInstanceProperty TRANSACTION_LOG_TRANSACTION_DELETION_QUEUE_ARN = Index
            .propertyBuilder("sleeper.statestore.transactionlog.transaction.deletion.queue.arn")
            .description("The ARN of the queue for transaction log transaction deletion requests.")
            .propertyGroup(InstancePropertyGroup.COMMON)
            .build();
    CdkDefinedInstanceProperty TRANSACTION_LOG_TRANSACTION_DELETION_DLQ_URL = Index
            .propertyBuilder("sleeper.statestore.transactionlog.transaction.deletion.dlq.url")
            .description("The URL of the dead letter queue for transaction log snapshot deletion requests.")
            .propertyGroup(InstancePropertyGroup.COMMON)
            .build();
    CdkDefinedInstanceProperty TRANSACTION_LOG_TRANSACTION_DELETION_DLQ_ARN = Index
            .propertyBuilder("sleeper.statestore.transactionlog.transaction.deletion.dlq.arn")
            .description("The ARN of the dead letter queue for transaction log transaction deletion requests.")
            .propertyGroup(InstancePropertyGroup.COMMON)
            .build();
    CdkDefinedInstanceProperty TRANSACTION_LOG_TRANSACTION_DELETION_RULE = Index
            .propertyBuilder("sleeper.statestore.transactionlog.transaction.deletion.rule")
            .description("The name of the CloudWatch rule that triggers deletion of transaction log transactions.")
            .propertyGroup(InstancePropertyGroup.COMMON)
            .build();
    // State store commits
    CdkDefinedInstanceProperty STATESTORE_COMMITTER_QUEUE_URL = Index
            .propertyBuilder("sleeper.statestore.committer.queue.url")
            .description("The URL of the queue for statestore commit requests.")
            .propertyGroup(InstancePropertyGroup.COMMON)
            .build();
    CdkDefinedInstanceProperty STATESTORE_COMMITTER_QUEUE_ARN = Index
            .propertyBuilder("sleeper.statestore.committer.queue.arn")
            .description("The ARN of the queue for statestore commit requests.")
            .propertyGroup(InstancePropertyGroup.COMMON)
            .build();
    CdkDefinedInstanceProperty STATESTORE_COMMITTER_DLQ_URL = Index
            .propertyBuilder("sleeper.statestore.committer.dlq.url")
            .description("The URL of the dead letter queue for statestore commit requests.")
            .propertyGroup(InstancePropertyGroup.COMMON)
            .build();
    CdkDefinedInstanceProperty STATESTORE_COMMITTER_DLQ_ARN = Index
            .propertyBuilder("sleeper.statestore.committer.dlq.arn")
            .description("The ARN of the dead letter queue for statestore commit requests.")
            .propertyGroup(InstancePropertyGroup.COMMON)
            .build();
    CdkDefinedInstanceProperty STATESTORE_COMMITTER_LOG_GROUP = Index
            .propertyBuilder("sleeper.statestore.committer.log.group")
            .description("The name of the log group for the state store committer.")
            .propertyGroup(InstancePropertyGroup.COMMON)
            .build();
    CdkDefinedInstanceProperty STATESTORE_COMMITTER_EVENT_SOURCE_ID = Index
            .propertyBuilder("sleeper.statestore.committer.event.source.id")
            .description("The UUID of the event source mapping for the state store committer.")
            .propertyGroup(InstancePropertyGroup.COMMON)
            .build();

    // Table metrics
    CdkDefinedInstanceProperty TABLE_METRICS_LAMBDA_FUNCTION = Index
            .propertyBuilder("sleeper.lambda.table.metrics.function")
            .description("The name of the Lambda function that triggers generation of metrics for tables.")
            .propertyGroup(InstancePropertyGroup.METRICS)
            .build();
    CdkDefinedInstanceProperty TABLE_METRICS_QUEUE_URL = Index.propertyBuilder("sleeper.sqs.table.metrics.queue.url")
            .description("The URL of the queue for table metrics calculation requests.")
            .propertyGroup(InstancePropertyGroup.METRICS)
            .build();
    CdkDefinedInstanceProperty TABLE_METRICS_QUEUE_ARN = Index.propertyBuilder("sleeper.sqs.table.metrics.queue.arn")
            .description("The ARN of the queue for table metrics calculation requests.")
            .propertyGroup(InstancePropertyGroup.METRICS)
            .build();
    CdkDefinedInstanceProperty TABLE_METRICS_DLQ_URL = Index.propertyBuilder("sleeper.sqs.table.metrics.dlq.url")
            .description("The URL of the dead letter queue for table metrics calculation requests.")
            .propertyGroup(InstancePropertyGroup.METRICS)
            .build();
    CdkDefinedInstanceProperty TABLE_METRICS_DLQ_ARN = Index.propertyBuilder("sleeper.sqs.table.metrics.dlq.arn")
            .description("The ARN of the dead letter queue for table metrics calculation requests.")
            .propertyGroup(InstancePropertyGroup.METRICS)
            .build();
    CdkDefinedInstanceProperty TABLE_METRICS_RULE = Index.propertyBuilder("sleeper.schedule.table.metrics.rule")
            .description("The name of the schedule rule that triggers generation of metrics for tables.")
            .propertyGroup(InstancePropertyGroup.METRICS)
            .build();

    // Query
    CdkDefinedInstanceProperty QUERY_LAMBDA_ROLE = Index.propertyBuilder("sleeper.query.role.arn")
            .description("The ARN for the role with required permissions to query sleeper.")
            .propertyGroup(InstancePropertyGroup.QUERY)
            .build();
    CdkDefinedInstanceProperty QUERY_WEBSOCKET_API_URL = Index.propertyBuilder("sleeper.query.websocket.api.url")
            .description("The URL of the WebSocket API for querying sleeper.")
            .propertyGroup(InstancePropertyGroup.QUERY)
            .build();
    CdkDefinedInstanceProperty QUERY_QUEUE_URL = Index.propertyBuilder("sleeper.query.queue.url")
            .description("The URL of the queue responsible for sending a query to sleeper.")
            .propertyGroup(InstancePropertyGroup.QUERY)
            .build();
    CdkDefinedInstanceProperty QUERY_QUEUE_ARN = Index.propertyBuilder("sleeper.query.queue.arn")
            .description("The ARN of the queue responsible for sending a query to sleeper.")
            .propertyGroup(InstancePropertyGroup.QUERY)
            .build();
    CdkDefinedInstanceProperty QUERY_DLQ_URL = Index.propertyBuilder("sleeper.query.dlq.url")
            .description("The URL of the dead letter queue used when querying sleeper.")
            .propertyGroup(InstancePropertyGroup.QUERY)
            .build();
    CdkDefinedInstanceProperty QUERY_DLQ_ARN = Index.propertyBuilder("sleeper.query.dlq.arn")
            .description("The ARN of the dead letter queue used when querying sleeper.")
            .propertyGroup(InstancePropertyGroup.QUERY)
            .build();
    CdkDefinedInstanceProperty QUERY_RESULTS_QUEUE_URL = Index.propertyBuilder("sleeper.query.results.queue.url")
            .description("The URL of the queue responsible for retrieving results from a query sent to sleeper.")
            .propertyGroup(InstancePropertyGroup.QUERY)
            .build();
    CdkDefinedInstanceProperty QUERY_RESULTS_QUEUE_ARN = Index.propertyBuilder("sleeper.query.results.queue.arn")
            .description("The ARN of the queue responsible for retrieving results from a query sent to sleeper.")
            .propertyGroup(InstancePropertyGroup.QUERY)
            .build();
    CdkDefinedInstanceProperty QUERY_RESULTS_BUCKET = Index.propertyBuilder("sleeper.query.results.bucket")
            .description("The S3 Bucket name of the query results bucket.")
            .propertyGroup(InstancePropertyGroup.QUERY)
            .build();
    CdkDefinedInstanceProperty QUERY_TRACKER_TABLE_NAME = Index.propertyBuilder("sleeper.query.tracker.table.name")
            .description("The name of the table responsible for tracking query progress.")
            .propertyGroup(InstancePropertyGroup.QUERY)
            .build();
    CdkDefinedInstanceProperty QUERY_WARM_LAMBDA_CLOUDWATCH_RULE = Index
            .propertyBuilder("sleeper.query.warm.lambda.rule")
            .description("The name of the CloudWatch rule to trigger the query lambda to keep it warm.")
            .propertyGroup(InstancePropertyGroup.QUERY)
            .build();
    CdkDefinedInstanceProperty LEAF_PARTITION_QUERY_QUEUE_URL = Index
            .propertyBuilder("sleeper.query.leaf.partition.queue.url")
            .description("The URL of the queue responsible for sending a leaf partition query to sleeper.")
            .propertyGroup(InstancePropertyGroup.QUERY)
            .build();
    CdkDefinedInstanceProperty LEAF_PARTITION_QUERY_QUEUE_ARN = Index
            .propertyBuilder("sleeper.query.leaf.partition.queue.arn")
            .description("The ARN of the queue responsible for sending a leaf partition query to sleeper.")
            .propertyGroup(InstancePropertyGroup.QUERY)
            .build();
    CdkDefinedInstanceProperty LEAF_PARTITION_QUERY_QUEUE_DLQ_URL = Index
            .propertyBuilder("sleeper.query.leaf.partition.dlq.url")
            .description("The URL of the dead letter queue used when leaf partition querying sleeper.")
            .propertyGroup(InstancePropertyGroup.QUERY)
            .build();
    CdkDefinedInstanceProperty LEAF_PARTITION_QUERY_QUEUE_DLQ_ARN = Index
            .propertyBuilder("sleeper.query.leaf.partition.dlq.arn")
            .description("The ARN of the dead letter queue used when leaf partition querying sleeper.")
            .propertyGroup(InstancePropertyGroup.QUERY)
            .build();
    // Bulk Export
    CdkDefinedInstanceProperty BULK_EXPORT_QUEUE_URL = Index
            .propertyBuilder("sleeper.bulk.export.queue.url")
            .description("The URL of the SQS queue that triggers the bulk export lambda.")
            .propertyGroup(InstancePropertyGroup.BULK_EXPORT)
            .build();
    CdkDefinedInstanceProperty BULK_EXPORT_QUEUE_ARN = Index
            .propertyBuilder("sleeper.bulk.export.queue.arn")
            .description("The ARN of the SQS queue that triggers the bulk export lambda.")
            .propertyGroup(InstancePropertyGroup.BULK_EXPORT)
            .build();
    CdkDefinedInstanceProperty BULK_EXPORT_QUEUE_DLQ_URL = Index
            .propertyBuilder("sleeper.bulk.export.queue.dlq.url")
            .description("The URL of the SQS dead letter queue that is used by the bulk export lambda.")
            .propertyGroup(InstancePropertyGroup.BULK_EXPORT)
            .build();
    CdkDefinedInstanceProperty BULK_EXPORT_QUEUE_DLQ_ARN = Index
            .propertyBuilder("sleeper.bulk.export.queue.dlq.arn")
            .description("The ARN of the SQS dead letter queue that is used by the bulk export lambda.")
            .propertyGroup(InstancePropertyGroup.BULK_EXPORT)
            .build();
    CdkDefinedInstanceProperty BULK_EXPORT_LAMBDA_ROLE_ARN = Index
            .propertyBuilder("sleeper.bulk.export.lambda.role.arn")
            .description("The ARN of the role for the bulk export lambda.")
            .propertyGroup(InstancePropertyGroup.BULK_EXPORT)
            .build();
    CdkDefinedInstanceProperty BULK_EXPORT_TASK_FARGATE_DEFINITION_FAMILY = Index
            .propertyBuilder("sleeper.bulk.export.fargate.task.definition")
            .description("The name of the family of Fargate task definitions used for bulk export.")
            .propertyGroup(InstancePropertyGroup.BULK_EXPORT)
            .build();
    CdkDefinedInstanceProperty BULK_EXPORT_CLUSTER = Index.propertyBuilder("sleeper.bulk.export.cluster")
            .description("The name of the cluster used for bulk export.")
            .propertyGroup(InstancePropertyGroup.BULK_EXPORT)
            .build();
    CdkDefinedInstanceProperty BULK_EXPORT_TASK_CREATION_LAMBDA_FUNCTION = Index
            .propertyBuilder("sleeper.bulk.export.task.creation.lambda.function")
            .description("The function name of the bulk export task creation lambda.")
            .propertyGroup(InstancePropertyGroup.BULK_EXPORT)
            .build();
    CdkDefinedInstanceProperty BULK_EXPORT_TASK_CREATION_CLOUDWATCH_RULE = Index
            .propertyBuilder("sleeper.bulk.export.task.creation.rule")
            .description(
                    "The name of the CloudWatch rule that periodically triggers the bulk export task creation lambda.")
            .propertyGroup(InstancePropertyGroup.BULK_EXPORT)
            .build();
    CdkDefinedInstanceProperty LEAF_PARTITION_BULK_EXPORT_QUEUE_URL = Index
            .propertyBuilder("sleeper.bulk.export.leaf.partition.queue.url")
            .description("The URL of the SQS queue that triggers the bulk export for a leaf partition.")
            .propertyGroup(InstancePropertyGroup.BULK_EXPORT)
            .build();
    CdkDefinedInstanceProperty LEAF_PARTITION_BULK_EXPORT_QUEUE_ARN = Index
            .propertyBuilder("sleeper.bulk.export.leaf.partition.queue.arn")
            .description("The ARN of the SQS queue that triggers the bulk export for a leaf partition.")
            .propertyGroup(InstancePropertyGroup.BULK_EXPORT)
            .build();
    CdkDefinedInstanceProperty LEAF_PARTITION_BULK_EXPORT_QUEUE_DLQ_URL = Index
            .propertyBuilder("sleeper.bulk.export.leaf.partition.queue.dlq.url")
            .description("The URL of the SQS dead letter queue that is used by the bulk export for a leaf partition.")
            .propertyGroup(InstancePropertyGroup.BULK_EXPORT)
            .build();
    CdkDefinedInstanceProperty LEAF_PARTITION_BULK_EXPORT_QUEUE_DLQ_ARN = Index
            .propertyBuilder("sleeper.bulk.export.leaf.partition.queue.dlq.arn")
            .description("The ARN of the SQS dead letter queue that is used by the bulk export for a leaf partition.")
            .propertyGroup(InstancePropertyGroup.BULK_EXPORT)
            .build();
    CdkDefinedInstanceProperty BULK_EXPORT_S3_BUCKET = Index
            .propertyBuilder("sleeper.bulk.export.s3.bucket")
            .description("The name of the S3 bucket where the bulk export files are stored.")
            .propertyGroup(InstancePropertyGroup.BULK_EXPORT)
            .build();
    // Compactions
    CdkDefinedInstanceProperty COMPACTION_CLUSTER = Index.propertyBuilder("sleeper.compaction.cluster")
            .description("The name of the cluster used for compactions.")
            .propertyGroup(InstancePropertyGroup.COMPACTION)
            .build();
    CdkDefinedInstanceProperty COMPACTION_TASK_EC2_DEFINITION_FAMILY = Index
            .propertyBuilder("sleeper.compaction.ec2.task.definition")
            .description("The name of the family of EC2 task definitions used for compactions.")
            .propertyGroup(InstancePropertyGroup.COMPACTION)
            .build();
    CdkDefinedInstanceProperty COMPACTION_TASK_FARGATE_DEFINITION_FAMILY = Index
            .propertyBuilder("sleeper.compaction.fargate.task.definition")
            .description("The name of the family of Fargate task definitions used for compactions.")
            .propertyGroup(InstancePropertyGroup.COMPACTION)
            .build();
    CdkDefinedInstanceProperty COMPACTION_JOB_CREATION_TRIGGER_LAMBDA_FUNCTION = Index
            .propertyBuilder("sleeper.compaction.job.creation.trigger.lambda.function")
            .description("The function name of the lambda to trigger compaction job creation for all tables.")
            .propertyGroup(InstancePropertyGroup.COMPACTION)
            .build();
    CdkDefinedInstanceProperty COMPACTION_JOB_CREATION_CLOUDWATCH_RULE = Index
            .propertyBuilder("sleeper.compaction.job.creation.rule")
            .description(
                    "The name of the CloudWatch rule that periodically triggers the compaction job creation lambda.")
            .propertyGroup(InstancePropertyGroup.COMPACTION)
            .build();
    CdkDefinedInstanceProperty COMPACTION_JOB_CREATION_QUEUE_URL = Index
            .propertyBuilder("sleeper.compaction.job.creation.queue.url")
            .description("The URL of the queue for tables requiring compaction job creation.")
            .propertyGroup(InstancePropertyGroup.COMPACTION)
            .build();
    CdkDefinedInstanceProperty COMPACTION_JOB_CREATION_QUEUE_ARN = Index
            .propertyBuilder("sleeper.compaction.job.creation.queue.arn")
            .description("The ARN of the queue for tables requiring compaction job creation.")
            .propertyGroup(InstancePropertyGroup.COMPACTION)
            .build();
    CdkDefinedInstanceProperty COMPACTION_JOB_CREATION_DLQ_URL = Index
            .propertyBuilder("sleeper.compaction.job.creation.dlq.url")
            .description("The URL of the dead letter queue for tables that failed compaction job creation.")
            .propertyGroup(InstancePropertyGroup.COMPACTION)
            .build();
    CdkDefinedInstanceProperty COMPACTION_JOB_CREATION_DLQ_ARN = Index
            .propertyBuilder("sleeper.compaction.job.creation.dlq.arn")
            .description("The ARN of the dead letter queue for tables that failed compaction job creation.")
            .propertyGroup(InstancePropertyGroup.COMPACTION)
            .build();
    CdkDefinedInstanceProperty COMPACTION_JOB_QUEUE_URL = Index.propertyBuilder("sleeper.compaction.job.queue.url")
            .description("The URL of the queue for compaction jobs.")
            .propertyGroup(InstancePropertyGroup.COMPACTION)
            .build();
    CdkDefinedInstanceProperty COMPACTION_JOB_QUEUE_ARN = Index.propertyBuilder("sleeper.compaction.job.queue.arn")
            .description("The ARN of the queue for compaction jobs.")
            .propertyGroup(InstancePropertyGroup.COMPACTION)
            .build();
    CdkDefinedInstanceProperty COMPACTION_JOB_DLQ_URL = Index.propertyBuilder("sleeper.compaction.job.dlq.url")
            .description("The URL of the dead letter queue for compaction jobs.")
            .propertyGroup(InstancePropertyGroup.COMPACTION)
            .build();
    CdkDefinedInstanceProperty COMPACTION_JOB_DLQ_ARN = Index.propertyBuilder("sleeper.compaction.job.dlq.arn")
            .description("The ARN of the dead letter queue for compaction jobs.")
            .propertyGroup(InstancePropertyGroup.COMPACTION)
            .build();
    CdkDefinedInstanceProperty COMPACTION_PENDING_QUEUE_URL = Index
            .propertyBuilder("sleeper.compaction.pending.queue.url")
            .description("The URL of the queue for pending compaction job batches.")
            .propertyGroup(InstancePropertyGroup.COMPACTION)
            .build();
    CdkDefinedInstanceProperty COMPACTION_PENDING_QUEUE_ARN = Index
            .propertyBuilder("sleeper.compaction.pending.queue.arn")
            .description("The ARN of the queue for pending compaction job batches.")
            .propertyGroup(InstancePropertyGroup.COMPACTION)
            .build();
    CdkDefinedInstanceProperty COMPACTION_PENDING_DLQ_URL = Index.propertyBuilder("sleeper.compaction.pending.dlq.url")
            .description("The URL of the dead letter queue for pending compaction job batches.")
            .propertyGroup(InstancePropertyGroup.COMPACTION)
            .build();
    CdkDefinedInstanceProperty COMPACTION_PENDING_DLQ_ARN = Index.propertyBuilder("sleeper.compaction.pending.dlq.arn")
            .description("The ARN of the dead letter queue for pending compaction job batches.")
            .propertyGroup(InstancePropertyGroup.COMPACTION)
            .build();
    CdkDefinedInstanceProperty COMPACTION_COMMIT_QUEUE_URL = Index
            .propertyBuilder("sleeper.compaction.commit.queue.url")
            .description("The URL of the queue for compaction jobs ready to commit to the state store.")
            .propertyGroup(InstancePropertyGroup.COMPACTION)
            .build();
    CdkDefinedInstanceProperty COMPACTION_COMMIT_QUEUE_ARN = Index
            .propertyBuilder("sleeper.compaction.commit.queue.arn")
            .description("The ARN of the queue for compaction jobs ready to commit to the state store.")
            .propertyGroup(InstancePropertyGroup.COMPACTION)
            .build();
    CdkDefinedInstanceProperty COMPACTION_COMMIT_DLQ_URL = Index.propertyBuilder("sleeper.compaction.commit.dlq.url")
            .description("The URL of the dead letter queue for compaction jobs ready to commit to the state store.")
            .propertyGroup(InstancePropertyGroup.COMPACTION)
            .build();
    CdkDefinedInstanceProperty COMPACTION_COMMIT_DLQ_ARN = Index.propertyBuilder("sleeper.compaction.commit.dlq.arn")
            .description("The ARN of the dead letter queue for compaction jobs ready to commit to the state store.")
            .propertyGroup(InstancePropertyGroup.COMPACTION)
            .build();

    CdkDefinedInstanceProperty COMPACTION_TASK_CREATION_LAMBDA_FUNCTION = Index
            .propertyBuilder("sleeper.compaction.task.creation.lambda.function")
            .description("The function name of the compaction task creation lambda.")
            .propertyGroup(InstancePropertyGroup.COMPACTION)
            .build();
    CdkDefinedInstanceProperty COMPACTION_TASK_CREATION_CLOUDWATCH_RULE = Index
            .propertyBuilder("sleeper.compaction.task.creation.rule")
            .description(
                    "The name of the CloudWatch rule that periodically triggers the compaction task creation lambda.")
            .propertyGroup(InstancePropertyGroup.COMPACTION)
            .build();
    CdkDefinedInstanceProperty COMPACTION_AUTO_SCALING_GROUP = Index.propertyBuilder("sleeper.compaction.scaling.group")
            .description("The name of the compaction EC2 auto scaling group.")
            .propertyGroup(InstancePropertyGroup.COMPACTION)
            .build();

    // Partition splitting
    CdkDefinedInstanceProperty FIND_PARTITIONS_TO_SPLIT_QUEUE_URL = Index
            .propertyBuilder("sleeper.partition.splitting.finder.queue.url")
            .description("The URL of the queue for requests to find partitions to split.")
            .propertyGroup(InstancePropertyGroup.PARTITION_SPLITTING)
            .build();
    CdkDefinedInstanceProperty FIND_PARTITIONS_TO_SPLIT_QUEUE_ARN = Index
            .propertyBuilder("sleeper.partition.splitting.finder.queue.arn")
            .description("The ARN of the queue for requests to find partitions to split.")
            .propertyGroup(InstancePropertyGroup.PARTITION_SPLITTING)
            .build();
    CdkDefinedInstanceProperty FIND_PARTITIONS_TO_SPLIT_DLQ_URL = Index
            .propertyBuilder("sleeper.partition.splitting.finder.dlq.url")
            .description("The URL of the dead letter queue for requests to find partitions to split.")
            .propertyGroup(InstancePropertyGroup.PARTITION_SPLITTING)
            .build();
    CdkDefinedInstanceProperty FIND_PARTITIONS_TO_SPLIT_DLQ_ARN = Index
            .propertyBuilder("sleeper.partition.splitting.finder.dlq.arn")
            .description("The ARN of the dead letter queue for requests to find partitions to split.")
            .propertyGroup(InstancePropertyGroup.PARTITION_SPLITTING)
            .build();
    CdkDefinedInstanceProperty PARTITION_SPLITTING_JOB_QUEUE_URL = Index
            .propertyBuilder("sleeper.partition.splitting.job.queue.url")
            .description("The URL of the queue for partition splitting jobs.")
            .propertyGroup(InstancePropertyGroup.PARTITION_SPLITTING)
            .build();
    CdkDefinedInstanceProperty PARTITION_SPLITTING_JOB_QUEUE_ARN = Index
            .propertyBuilder("sleeper.partition.splitting.job.queue.arn")
            .description("The ARN of the queue for partition splitting jobs.")
            .propertyGroup(InstancePropertyGroup.PARTITION_SPLITTING)
            .build();
    CdkDefinedInstanceProperty PARTITION_SPLITTING_JOB_DLQ_URL = Index
            .propertyBuilder("sleeper.partition.splitting.job.dlq.url")
            .description("The URL of the dead letter queue for partition splitting jobs.")
            .propertyGroup(InstancePropertyGroup.PARTITION_SPLITTING)
            .build();
    CdkDefinedInstanceProperty PARTITION_SPLITTING_JOB_DLQ_ARN = Index
            .propertyBuilder("sleeper.partition.splitting.job.dlq.arn")
            .description("The ARN of the dead letter queue for partition splitting jobs.")
            .propertyGroup(InstancePropertyGroup.PARTITION_SPLITTING)
            .build();
    CdkDefinedInstanceProperty PARTITION_SPLITTING_TRIGGER_LAMBDA_FUNCTION = Index
            .propertyBuilder("sleeper.partition.splitting.trigger.lambda.function")
            .description(
                    "The function name of the lambda that finds partitions to split and sends jobs to the split partition lambda.")
            .propertyGroup(InstancePropertyGroup.PARTITION_SPLITTING)
            .build();
    CdkDefinedInstanceProperty PARTITION_SPLITTING_CLOUDWATCH_RULE = Index
            .propertyBuilder("sleeper.partition.splitting.rule")
            .description("The name of the CloudWatch rule that periodically triggers the partition splitting lambda.")
            .propertyGroup(InstancePropertyGroup.PARTITION_SPLITTING)
            .build();

    // Garbage collection
    CdkDefinedInstanceProperty GARBAGE_COLLECTOR_CLOUDWATCH_RULE = Index.propertyBuilder("sleeper.gc.rule")
            .description("The name of the CloudWatch rule that periodically triggers the garbage collector lambda.")
            .propertyGroup(InstancePropertyGroup.GARBAGE_COLLECTOR)
            .build();
    CdkDefinedInstanceProperty GARBAGE_COLLECTOR_LAMBDA_FUNCTION = Index.propertyBuilder("sleeper.gc.lambda.function")
            .description("The function name of the garbage collector lambda.")
            .propertyGroup(InstancePropertyGroup.GARBAGE_COLLECTOR)
            .build();
    CdkDefinedInstanceProperty GARBAGE_COLLECTOR_QUEUE_URL = Index.propertyBuilder("sleeper.gc.queue.url")
            .description("The URL of the queue for sending batches of garbage collection requests.")
            .propertyGroup(InstancePropertyGroup.GARBAGE_COLLECTOR)
            .build();
    CdkDefinedInstanceProperty GARBAGE_COLLECTOR_QUEUE_ARN = Index.propertyBuilder("sleeper.gc.queue.arn")
            .description("The ARN of the queue for sending batches of garbage collection requests.")
            .propertyGroup(InstancePropertyGroup.GARBAGE_COLLECTOR)
            .build();
    CdkDefinedInstanceProperty GARBAGE_COLLECTOR_DLQ_URL = Index.propertyBuilder("sleeper.gc.dlq.url")
            .description("The URL of the dead letter queue for sending batches of garbage collection requests.")
            .propertyGroup(InstancePropertyGroup.GARBAGE_COLLECTOR)
            .build();
    CdkDefinedInstanceProperty GARBAGE_COLLECTOR_DLQ_ARN = Index.propertyBuilder("sleeper.gc.dlq.arn")
            .description("The ARN of the dead letter queue for sending batches of garbage collection requests.")
            .propertyGroup(InstancePropertyGroup.GARBAGE_COLLECTOR)
            .build();

    // Ingest
    CdkDefinedInstanceProperty INGEST_LAMBDA_FUNCTION = Index.propertyBuilder("sleeper.ingest.lambda.function")
            .description("The function name of the ingest task creator lambda.")
            .propertyGroup(InstancePropertyGroup.INGEST)
            .build();
    CdkDefinedInstanceProperty INGEST_CLOUDWATCH_RULE = Index.propertyBuilder("sleeper.ingest.rule")
            .description("The name of the CloudWatch rule that periodically triggers the ingest task creator lambda.")
            .propertyGroup(InstancePropertyGroup.INGEST)
            .build();
    CdkDefinedInstanceProperty INGEST_JOB_QUEUE_URL = Index.propertyBuilder("sleeper.ingest.job.queue.url")
            .description("The URL of the queue for ingest jobs.")
            .propertyGroup(InstancePropertyGroup.INGEST)
            .build();
    CdkDefinedInstanceProperty INGEST_JOB_QUEUE_ARN = Index.propertyBuilder("sleeper.ingest.job.queue.arn")
            .description("The ARN of the queue for ingest jobs.")
            .propertyGroup(InstancePropertyGroup.INGEST)
            .build();
    CdkDefinedInstanceProperty INGEST_JOB_DLQ_URL = Index.propertyBuilder("sleeper.ingest.job.dlq.url")
            .description("The URL of the dead letter queue for ingest jobs.")
            .propertyGroup(InstancePropertyGroup.INGEST)
            .build();
    CdkDefinedInstanceProperty INGEST_JOB_DLQ_ARN = Index.propertyBuilder("sleeper.ingest.job.dlq.arn")
            .description("The ARN of the dead letter queue for ingest jobs.")
            .propertyGroup(InstancePropertyGroup.INGEST)
            .build();
    CdkDefinedInstanceProperty INGEST_BATCHER_SUBMIT_QUEUE_URL = Index
            .propertyBuilder("sleeper.ingest.batcher.submit.queue.url")
            .description("The URL of the queue for ingest batcher file submission.")
            .propertyGroup(InstancePropertyGroup.INGEST)
            .build();
    CdkDefinedInstanceProperty INGEST_BATCHER_SUBMIT_QUEUE_ARN = Index
            .propertyBuilder("sleeper.ingest.batcher.submit.queue.arn")
            .description("The ARN of the queue for ingest batcher file submission.")
            .propertyGroup(InstancePropertyGroup.INGEST)
            .build();
    CdkDefinedInstanceProperty INGEST_BATCHER_SUBMIT_DLQ_URL = Index
            .propertyBuilder("sleeper.ingest.batcher.submit.dlq.url")
            .description("The URL of the dead letter queue for ingest batcher file submission.")
            .propertyGroup(InstancePropertyGroup.INGEST)
            .build();
    CdkDefinedInstanceProperty INGEST_BATCHER_SUBMIT_DLQ_ARN = Index
            .propertyBuilder("sleeper.ingest.batcher.submit.dlq.arn")
            .description("The ARN of the dead letter queue for ingest batcher file submission.")
            .propertyGroup(InstancePropertyGroup.INGEST)
            .build();
    CdkDefinedInstanceProperty INGEST_BATCHER_JOB_CREATION_CLOUDWATCH_RULE = Index
            .propertyBuilder("sleeper.ingest.batcher.job.creation.rule")
            .description(
                    "The name of the CloudWatch rule to trigger the batcher to create jobs from file ingest requests.")
            .propertyGroup(InstancePropertyGroup.INGEST)
            .build();
    CdkDefinedInstanceProperty INGEST_BATCHER_JOB_CREATION_FUNCTION = Index
            .propertyBuilder("sleeper.ingest.batcher.job.creation.lambda")
            .description("The name of the ingest batcher Lambda to create jobs from file ingest requests.")
            .propertyGroup(InstancePropertyGroup.INGEST)
            .build();
    CdkDefinedInstanceProperty INGEST_BATCHER_SUBMIT_REQUEST_FUNCTION = Index
            .propertyBuilder("sleeper.ingest.batcher.submit.lambda")
            .description("The name of the ingest batcher Lambda to submit file ingest requests.")
            .propertyGroup(InstancePropertyGroup.INGEST)
            .build();
    CdkDefinedInstanceProperty INGEST_TASK_DEFINITION_FAMILY = Index
            .propertyBuilder("sleeper.ingest.task.definition.family")
            .description("The name of the family of task definitions used for ingest tasks.")
            .propertyGroup(InstancePropertyGroup.INGEST)
            .build();
    CdkDefinedInstanceProperty INGEST_CLUSTER = Index.propertyBuilder("sleeper.ingest.cluster")
            .description("The name of the cluster used for ingest.")
            .propertyGroup(InstancePropertyGroup.INGEST)
            .build();
    CdkDefinedInstanceProperty INGEST_BY_QUEUE_ROLE_ARN = Index.propertyBuilder("sleeper.ingest.by.queue.role.arn")
            .description("The ARN of a role that has permissions to perform an ingest by queue for the instance.")
            .propertyGroup(InstancePropertyGroup.INGEST)
            .build();
    CdkDefinedInstanceProperty INGEST_DIRECT_ROLE_ARN = Index.propertyBuilder("sleeper.ingest.direct.role.arn")
            .description("The ARN of a role that has permissions to perform a direct ingest for the instance.")
            .propertyGroup(InstancePropertyGroup.INGEST)
            .build();

    // Bulk import
    CdkDefinedInstanceProperty BULK_IMPORT_BUCKET = Index.propertyBuilder("sleeper.bulk.import.bucket")
            .description("The S3 Bucket name of the bulk import bucket.")
            .propertyGroup(InstancePropertyGroup.BULK_IMPORT)
            .build();

    // Bulk import using EMR - these properties are used by both the persistent
    // and non-persistent EMR stacks
    CdkDefinedInstanceProperty BULK_IMPORT_EMR_EC2_ROLE_NAME = Index
            .propertyBuilder("sleeper.bulk.import.emr.ec2.role.name")
            .description("The name of the job flow role for bulk import using EMR.")
            .propertyGroup(InstancePropertyGroup.BULK_IMPORT)
            .build();
    CdkDefinedInstanceProperty BULK_IMPORT_EMR_CLUSTER_ROLE_NAME = Index
            .propertyBuilder("sleeper.bulk.import.emr.role.name")
            .description("The name of the role assumed by the bulk import clusters using EMR.")
            .propertyGroup(InstancePropertyGroup.BULK_IMPORT)
            .build();
    CdkDefinedInstanceProperty BULK_IMPORT_EMR_SECURITY_CONF_NAME = Index
            .propertyBuilder("sleeper.bulk.import.emr.security.conf.name")
            .description("The name of the security configuration used by bulk import using EMR.")
            .propertyGroup(InstancePropertyGroup.BULK_IMPORT)
            .build();
    // Bulk import using EMR
    CdkDefinedInstanceProperty BULK_IMPORT_EMR_JOB_QUEUE_URL = Index
            .propertyBuilder("sleeper.bulk.import.emr.job.queue.url")
            .description("The URL of the queue for bulk import jobs using EMR.")
            .propertyGroup(InstancePropertyGroup.BULK_IMPORT)
            .build();
    CdkDefinedInstanceProperty BULK_IMPORT_EMR_JOB_QUEUE_ARN = Index
            .propertyBuilder("sleeper.bulk.import.emr.job.queue.arn")
            .description("The ARN of the queue for bulk import jobs using EMR.")
            .propertyGroup(InstancePropertyGroup.BULK_IMPORT)
            .build();
    // Bulk import using EMR Serverless
    CdkDefinedInstanceProperty BULK_IMPORT_EMR_SERVERLESS_CLUSTER_NAME = Index
            .propertyBuilder("sleeper.bulk.import.emr.serverless.cluster.name")
            .description("The name of the cluster used for EMR Serverless bulk import jobs.")
            .propertyGroup(InstancePropertyGroup.BULK_IMPORT)
            .build();
    CdkDefinedInstanceProperty BULK_IMPORT_EMR_SERVERLESS_APPLICATION_ID = Index
            .propertyBuilder("sleeper.bulk.import.emr.serverless.application.id")
            .description("The id of the application used for EMR Serverless bulk import jobs.")
            .propertyGroup(InstancePropertyGroup.BULK_IMPORT)
            .build();
    CdkDefinedInstanceProperty BULK_IMPORT_EMR_SERVERLESS_CLUSTER_ROLE_ARN = Index
            .propertyBuilder("sleeper.bulk.import.emr.serverless.role.arn")
            .description("The ARN of the role assumed by the bulk import clusters using EMR Serverless.")
            .propertyGroup(InstancePropertyGroup.BULK_IMPORT)
            .build();
    CdkDefinedInstanceProperty BULK_IMPORT_EMR_SERVERLESS_JOB_QUEUE_URL = Index
            .propertyBuilder("sleeper.bulk.import.emr.serverless.job.queue.url")
            .description("The URL of the queue for bulk import jobs using EMR Serverless.")
            .propertyGroup(InstancePropertyGroup.BULK_IMPORT)
            .build();
    CdkDefinedInstanceProperty BULK_IMPORT_EMR_SERVERLESS_JOB_QUEUE_ARN = Index
            .propertyBuilder("sleeper.bulk.import.emr.serverless.job.queue.arn")
            .description("The ARN of the queue for bulk import jobs using EMR Serverless.")
            .propertyGroup(InstancePropertyGroup.BULK_IMPORT)
            .build();
    CdkDefinedInstanceProperty BULK_IMPORT_EMR_SERVERLESS_STUDIO_URL = Index
            .propertyBuilder("sleeper.bulk.import.emr.serverless.studio.url")
            .description("The url for EMR Studio used to access EMR Serverless.")
            .propertyGroup(InstancePropertyGroup.BULK_IMPORT)
            .build();
    // Bulk import using persistent EMR
    CdkDefinedInstanceProperty BULK_IMPORT_PERSISTENT_EMR_JOB_QUEUE_URL = Index
            .propertyBuilder("sleeper.bulk.import.persistent.emr.job.queue.url")
            .description("The URL of the queue for bulk import jobs using persistent EMR.")
            .propertyGroup(InstancePropertyGroup.BULK_IMPORT)
            .build();
    CdkDefinedInstanceProperty BULK_IMPORT_PERSISTENT_EMR_JOB_QUEUE_ARN = Index
            .propertyBuilder("sleeper.bulk.import.persistent.emr.job.queue.arn")
            .description("The ARN of the queue for bulk import jobs using persistent EMR.")
            .propertyGroup(InstancePropertyGroup.BULK_IMPORT)
            .build();
    CdkDefinedInstanceProperty BULK_IMPORT_PERSISTENT_EMR_CLUSTER_NAME = Index
            .propertyBuilder("sleeper.bulk.import.persistent.emr.cluster.name")
            .description("The name of the cluster used for persistent EMR bulk import jobs.")
            .propertyGroup(InstancePropertyGroup.BULK_IMPORT)
            .build();
    CdkDefinedInstanceProperty BULK_IMPORT_PERSISTENT_EMR_MASTER_DNS = Index
            .propertyBuilder("sleeper.bulk.import.persistent.emr.master")
            .description("The public DNS name of the cluster's master node for bulk import using persistent EMR.")
            .propertyGroup(InstancePropertyGroup.BULK_IMPORT)
            .build();
    // Bulk import using EKS
    CdkDefinedInstanceProperty BULK_IMPORT_EKS_JOB_QUEUE_URL = Index
            .propertyBuilder("sleeper.bulk.import.eks.job.queue.url")
            .description("The URL of the queue for bulk import jobs using EKS.")
            .propertyGroup(InstancePropertyGroup.BULK_IMPORT)
            .build();
    CdkDefinedInstanceProperty BULK_IMPORT_EKS_JOB_QUEUE_ARN = Index
            .propertyBuilder("sleeper.bulk.import.eks.job.queue.arn")
            .description("The ARN of the queue for bulk import jobs using EKS.")
            .propertyGroup(InstancePropertyGroup.BULK_IMPORT)
            .build();
    CdkDefinedInstanceProperty BULK_IMPORT_EKS_STATE_MACHINE_ARN = Index
            .propertyBuilder("sleeper.bulk.import.eks.statemachine.arn")
            .description("The ARN of the state machine for bulk import jobs using EKS.")
            .propertyGroup(InstancePropertyGroup.BULK_IMPORT)
            .build();
    CdkDefinedInstanceProperty BULK_IMPORT_EKS_NAMESPACE = Index
            .propertyBuilder("sleeper.bulk.import.eks.k8s.namespace")
            .description("The namespace ID of the bulk import cluster using EKS.")
            .propertyGroup(InstancePropertyGroup.BULK_IMPORT)
            .build();
    CdkDefinedInstanceProperty BULK_IMPORT_EKS_CLUSTER_ENDPOINT = Index
            .propertyBuilder("sleeper.bulk.import.eks.k8s.endpoint")
            .description("The endpoint of the bulk import cluster using EKS.")
            .propertyGroup(InstancePropertyGroup.BULK_IMPORT)
            .build();

    @Override
    default boolean isSetByCdk() {
        return true;
    }

    /**
     * An index of property definitions in this file.
     */
    class Index {

        private Index() {
        }

        private static final SleeperPropertyIndex<CdkDefinedInstanceProperty> INSTANCE = new SleeperPropertyIndex<>();

        private static CdkDefinedInstancePropertyImpl.Builder propertyBuilder(String propertyName) {
            return CdkDefinedInstancePropertyImpl.named(propertyName)
                    .addToIndex(INSTANCE::add);
        }
    }
}
