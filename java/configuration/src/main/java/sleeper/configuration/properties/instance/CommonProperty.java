/*
 * Copyright 2022-2024 Crown Copyright
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

package sleeper.configuration.properties.instance;

import sleeper.configuration.Utils;
import sleeper.configuration.properties.SleeperPropertyIndex;

import java.util.List;
import java.util.Objects;

public interface CommonProperty {
    int ID_MAX_LENGTH = 20;
    UserDefinedInstanceProperty ID = Index.propertyBuilder("sleeper.id")
            .description("A string to uniquely identify this deployment. This should be no longer than 20 chars. " +
                    "It should be globally unique as it will be used to name AWS resources such as S3 buckets.")
            .validationPredicate(value -> Utils.isNonNullNonEmptyStringWithMaxLength(value, ID_MAX_LENGTH))
            .propertyGroup(InstancePropertyGroup.COMMON)
            .editable(false).build();
    UserDefinedInstanceProperty JARS_BUCKET = Index.propertyBuilder("sleeper.jars.bucket")
            .description("The S3 bucket containing the jar files of the Sleeper components.")
            .validationPredicate(Objects::nonNull)
            .propertyGroup(InstancePropertyGroup.COMMON)
            .runCdkDeployWhenChanged(true).build();
    UserDefinedInstanceProperty USER_JARS = Index.propertyBuilder("sleeper.userjars")
            .description("A comma-separated list of the jars containing application specific iterator code. " +
                    "These jars are assumed to be in the bucket given by sleeper.jars.bucket, e.g. if that " +
                    "bucket contains two iterator jars called iterator1.jar and iterator2.jar then the " +
                    "property should be 'sleeper.userjars=iterator1.jar,iterator2.jar'.")
            .propertyGroup(InstancePropertyGroup.COMMON)
            .includedInBasicTemplate(true).build();
    UserDefinedInstanceProperty TAGS = Index.propertyBuilder("sleeper.tags")
            .description("A list of tags for the project.")
            .propertyGroup(InstancePropertyGroup.COMMON)
            .runCdkDeployWhenChanged(true)
            .includedInTemplate(false).build();
    UserDefinedInstanceProperty STACK_TAG_NAME = Index.propertyBuilder("sleeper.stack.tag.name")
            .description("A name for a tag to identify the stack that deployed a resource. This will be set for all AWS resources, to the ID of " +
                    "the CDK stack that they are deployed under. This can be used to organise the cost explorer for billing.")
            .defaultValue("DeploymentStack")
            .propertyGroup(InstancePropertyGroup.COMMON)
            .runCdkDeployWhenChanged(true).build();
    UserDefinedInstanceProperty RETAIN_INFRA_AFTER_DESTROY = Index.propertyBuilder("sleeper.retain.infra.after.destroy")
            .description("Whether to keep the sleeper table bucket, Dynamo tables, query results bucket, etc., " +
                    "when the instance is destroyed.")
            .defaultValue("true")
            .validationPredicate(Utils::isTrueOrFalse)
            .propertyGroup(InstancePropertyGroup.COMMON)
            .runCdkDeployWhenChanged(true)
            .includedInBasicTemplate(true).build();
    UserDefinedInstanceProperty OPTIONAL_STACKS = Index.propertyBuilder("sleeper.optional.stacks")
            .description("The optional stacks to deploy.")
            .defaultValue("CompactionStack,GarbageCollectorStack,IngestStack,IngestBatcherStack," +
                    "PartitionSplittingStack,QueryStack,AthenaStack,EmrServerlessBulkImportStack,EmrStudioStack," +
                    "DashboardStack,TableMetricsStack")
            .propertyGroup(InstancePropertyGroup.COMMON)
            .runCdkDeployWhenChanged(true)
            .includedInBasicTemplate(true).build();
    UserDefinedInstanceProperty ACCOUNT = Index.propertyBuilder("sleeper.account")
            .description("The AWS account number. This is the AWS account that the instance will be deployed to.")
            .validationPredicate(Objects::nonNull)
            .propertyGroup(InstancePropertyGroup.COMMON)
            .editable(false).build();
    UserDefinedInstanceProperty REGION = Index.propertyBuilder("sleeper.region")
            .description("The AWS region to deploy to.")
            .validationPredicate(Objects::nonNull)
            .propertyGroup(InstancePropertyGroup.COMMON)
            .editable(false).build();
    UserDefinedInstanceProperty VPC_ID = Index.propertyBuilder("sleeper.vpc")
            .description("The id of the VPC to deploy to.")
            .validationPredicate(Objects::nonNull)
            .propertyGroup(InstancePropertyGroup.COMMON)
            .editable(false).build();
    UserDefinedInstanceProperty VPC_ENDPOINT_CHECK = Index.propertyBuilder("sleeper.vpc.endpoint.check")
            .description("Whether to check that the VPC that the instance is deployed to has an S3 endpoint. " +
                    "If there is no S3 endpoint then the NAT costs can be very significant.")
            .defaultValue("true")
            .propertyGroup(InstancePropertyGroup.COMMON).build();
    UserDefinedInstanceProperty SUBNETS = Index.propertyBuilder("sleeper.subnets")
            .description("A comma separated list of subnets to deploy to. ECS tasks will be run across multiple " +
                    "subnets. EMR clusters will be deployed in a subnet chosen when the cluster is created.")
            .validationPredicate(Objects::nonNull)
            .propertyGroup(InstancePropertyGroup.COMMON)
            .editable(false).build();
    UserDefinedInstanceProperty FILE_SYSTEM = Index.propertyBuilder("sleeper.filesystem")
            .description("The Hadoop filesystem used to connect to S3.")
            .defaultValue("s3a://")
            .propertyGroup(InstancePropertyGroup.COMMON).build();
    UserDefinedInstanceProperty EMAIL_ADDRESS_FOR_ERROR_NOTIFICATION = Index.propertyBuilder("sleeper.errors.email")
            .description("An email address used by the TopicStack to publish SNS notifications of errors.")
            .propertyGroup(InstancePropertyGroup.COMMON)
            .runCdkDeployWhenChanged(true)
            .includedInBasicTemplate(true).build();
    UserDefinedInstanceProperty QUEUE_VISIBILITY_TIMEOUT_IN_SECONDS = Index.propertyBuilder("sleeper.queue.visibility.timeout.seconds")
            .description("The visibility timeout on the queues used in ingest, query, etc.")
            .defaultValue("900")
            .validationPredicate(Utils::isValidLambdaTimeout)
            .propertyGroup(InstancePropertyGroup.COMMON)
            .runCdkDeployWhenChanged(true).build();
    UserDefinedInstanceProperty LOG_RETENTION_IN_DAYS = Index.propertyBuilder("sleeper.log.retention.days")
            .description("The length of time in days that CloudWatch logs from lambda functions, ECS containers, etc., are retained.\n" +
                    "See https://docs.aws.amazon.com/AWSCloudFormation/latest/UserGuide/aws-resource-logs-loggroup.html for valid options.\n" +
                    "Use -1 to indicate infinite retention.")
            .defaultValue("30")
            .validationPredicate(Utils::isValidLogRetention)
            .propertyGroup(InstancePropertyGroup.COMMON)
            .runCdkDeployWhenChanged(true).build();
    UserDefinedInstanceProperty MAXIMUM_CONNECTIONS_TO_S3 = Index.propertyBuilder("sleeper.fs.s3a.max-connections")
            .description("Used to set the value of fs.s3a.connection.maximum on the Hadoop configuration. This controls the " +
                    "maximum number of http connections to S3.\n" +
                    "See https://hadoop.apache.org/docs/stable/hadoop-aws/tools/hadoop-aws/performance.html")
            .defaultValue("100")
            .validationPredicate(Utils::isPositiveInteger)
            .propertyGroup(InstancePropertyGroup.COMMON).build();
    UserDefinedInstanceProperty S3_UPLOAD_BLOCK_SIZE = Index.propertyBuilder("sleeper.fs.s3a.upload.block.size")
            .description("Used to set the value of fs.s3a.block.size on the Hadoop configuration. Uploads to S3 " +
                    "happen in blocks, and this sets the size of blocks. If a larger value is used, then more data " +
                    "is buffered before the upload begins.\n" +
                    "See https://hadoop.apache.org/docs/stable/hadoop-aws/tools/hadoop-aws/performance.html")
            .defaultValue("32M")
            .validationPredicate(Utils::isValidHadoopLongBytes)
            .propertyGroup(InstancePropertyGroup.COMMON).build();
    UserDefinedInstanceProperty FARGATE_VERSION = Index.propertyBuilder("sleeper.fargate.version")
            .description("The version of Fargate to use.")
            .defaultValue("1.4.0")
            .propertyGroup(InstancePropertyGroup.COMMON).build();
    UserDefinedInstanceProperty TASK_RUNNER_LAMBDA_MEMORY_IN_MB = Index.propertyBuilder("sleeper.task.runner.memory")
            .description("The amount of memory in MB for the lambda that creates ECS tasks to execute compaction and ingest jobs.")
            .defaultValue("1024")
            .propertyGroup(InstancePropertyGroup.COMMON)
            .runCdkDeployWhenChanged(true).build();
    UserDefinedInstanceProperty TASK_RUNNER_LAMBDA_TIMEOUT_IN_SECONDS = Index.propertyBuilder("sleeper.task.runner.timeout.seconds")
            .description("The timeout in seconds for the lambda that creates ECS tasks to execute compaction jobs and ingest jobs.\n" +
                    "This must be >0 and <= 900.")
            .defaultValue("900")
            .validationPredicate(Utils::isValidLambdaTimeout)
            .propertyGroup(InstancePropertyGroup.COMMON)
            .runCdkDeployWhenChanged(true).build();
    UserDefinedInstanceProperty METRICS_NAMESPACE = Index.propertyBuilder("sleeper.metrics.namespace")
            .description("The namespaces for the metrics used in the metrics stack.")
            .defaultValue("Sleeper")
            .validationPredicate(Utils::isNonNullNonEmptyString)
            .propertyGroup(InstancePropertyGroup.COMMON)
            .runCdkDeployWhenChanged(true).build();
    UserDefinedInstanceProperty METRICS_TABLE_BATCH_SIZE = Index.propertyBuilder("sleeper.metrics.batch.size")
            .description("The number of tables to calculate metrics for in a single invocation. " +
                    "This will be the batch size for a lambda as an SQS FIFO event source. This can be a maximum of 10.")
            .defaultValue("1")
            .validationPredicate(Utils::isPositiveIntegerLtEq10)
            .propertyGroup(InstancePropertyGroup.COMMON).build();
    UserDefinedInstanceProperty METRICS_FOR_OFFLINE_TABLES = Index.propertyBuilder("sleeper.metrics.offline.enabled")
            .description("Whether to calculate table metrics for offline tables.")
            .defaultValue("false")
            .validationPredicate(Utils::isTrueOrFalse)
            .propertyGroup(InstancePropertyGroup.COMMON).build();
    UserDefinedInstanceProperty FORCE_RELOAD_PROPERTIES = Index.propertyBuilder("sleeper.properties.force.reload")
            .description("If true, properties will be reloaded every time a long running job is started or a lambda is run. " +
                    "This will mainly be used in test scenarios to ensure properties are up to date.")
            .defaultValue("false")
            .validationPredicate(Utils::isTrueOrFalse)
            .propertyGroup(InstancePropertyGroup.COMMON)
            .build();
    UserDefinedInstanceProperty ECR_REPOSITORY_PREFIX = Index.propertyBuilder("sleeper.ecr.repository.prefix")
            .description("If set, this property will be used as a prefix for the names of ECR repositories. " +
                    "If unset, then the instance ID will be used to determine the names instead.\n" +
                    "Note: This is only used by the deployment scripts to upload Docker images, not the CDK. " +
                    "We may add the ability to use this in the CDK in the future.")
            .propertyGroup(InstancePropertyGroup.COMMON)
            .editable(false).build();
    UserDefinedInstanceProperty STATESTORE_PROVIDER_CACHE_SIZE = Index.propertyBuilder("sleeper.statestore.statestore.provider.cache.size")
            .description("The maximum size of state store providers. If a state store is needed and the cache is full, the oldest state store in the cache will be removed to make space.")
            .defaultValue("10")
            .validationPredicate(Utils::isPositiveInteger)
            .propertyGroup(InstancePropertyGroup.COMMON)
            .build();
    UserDefinedInstanceProperty DYNAMO_STATE_STORE_POINT_IN_TIME_RECOVERY = Index.propertyBuilder("sleeper.statestore.dynamo.pointintimerecovery")
            .description("This specifies whether point in time recovery is enabled for the DynamoDB state store. " +
                    "This is set on the DynamoDB tables.")
            .defaultValue("false")
            .validationPredicate(Utils::isTrueOrFalse)
            .propertyGroup(InstancePropertyGroup.COMMON)
            .runCdkDeployWhenChanged(true).build();
    UserDefinedInstanceProperty S3_STATE_STORE_DYNAMO_POINT_IN_TIME_RECOVERY = Index.propertyBuilder("sleeper.statestore.s3.dynamo.pointintimerecovery")
            .description("This specifies whether point in time recovery is enabled for the S3 state store. " +
                    "This is set on the revision DynamoDB table.")
            .defaultValue("false")
            .validationPredicate(Utils::isTrueOrFalse)
            .propertyGroup(InstancePropertyGroup.COMMON)
            .runCdkDeployWhenChanged(true).build();
    UserDefinedInstanceProperty TRANSACTION_LOG_SNAPSHOT_CREATION_BATCH_SIZE = Index.propertyBuilder("sleeper.statestore.transactionlog.snapshot.creation.batch.size")
            .description("The number of tables to create transaction log snapshots for in a single invocation. This will be the batch size" +
                    " for a lambda as an SQS FIFO event source. This can be a maximum of 10.")
            .defaultValue("1")
            .validationPredicate(Utils::isPositiveIntegerLtEq10)
            .propertyGroup(InstancePropertyGroup.COMMON)
            .build();
    UserDefinedInstanceProperty TRANSACTION_LOG_SNAPSHOT_CREATION_LAMBDA_PERIOD_IN_MINUTES = Index.propertyBuilder("sleeper.statestore.transactionlog.snapshot.creation.lambda.period.minutes")
            .description("The frequency in minutes with which the transaction log snapshot creation lambda is run.")
            .defaultValue("5")
            .validationPredicate(Utils::isPositiveInteger)
            .propertyGroup(InstancePropertyGroup.COMMON)
            .build();
    UserDefinedInstanceProperty TRANSACTION_LOG_SNAPSHOT_DELETION_BATCH_SIZE = Index.propertyBuilder("sleeper.statestore.transactionlog.snapshot.deletion.batch.size")
            .description("The number of tables to delete old transaction log snapshots for in a single invocation. This will be the batch size" +
                    " for a lambda as an SQS FIFO event source. This can be a maximum of 10.")
            .defaultValue("1")
            .validationPredicate(Utils::isPositiveIntegerLtEq10)
            .propertyGroup(InstancePropertyGroup.COMMON)
            .build();
    UserDefinedInstanceProperty TRANSACTION_LOG_SNAPSHOT_DELETION_LAMBDA_PERIOD_IN_MINUTES = Index.propertyBuilder("sleeper.statestore.transactionlog.snapshot.deletion.lambda.period.minutes")
            .description("The frequency in minutes with which the transaction log snapshot deletion lambda is run.")
            .defaultValue("60")
            .validationPredicate(Utils::isPositiveInteger)
            .propertyGroup(InstancePropertyGroup.COMMON)
            .build();
    UserDefinedInstanceProperty TRANSACTION_LOG_TRANSACTION_DELETION_BATCH_SIZE = Index.propertyBuilder("sleeper.statestore.transactionlog.transaction.deletion.batch.size")
            .description("The number of tables to delete old transaction log transactions for in a single invocation. This will be the batch size" +
                    " for a lambda as an SQS FIFO event source. This can be a maximum of 10.")
            .defaultValue("1")
            .validationPredicate(Utils::isPositiveIntegerLtEq10)
            .propertyGroup(InstancePropertyGroup.COMMON)
            .build();
    UserDefinedInstanceProperty TRANSACTION_LOG_TRANSACTION_DELETION_LAMBDA_PERIOD_IN_MINUTES = Index.propertyBuilder("sleeper.statestore.transactionlog.transaction.deletion.lambda.period.minutes")
            .description("The frequency in minutes with which the transaction log transaction deletion lambda is run.")
            .defaultValue("60")
            .validationPredicate(Utils::isPositiveInteger)
            .propertyGroup(InstancePropertyGroup.COMMON)
            .build();
    UserDefinedInstanceProperty TABLE_INDEX_DYNAMO_POINT_IN_TIME_RECOVERY = Index.propertyBuilder("sleeper.tables.index.dynamo.pointintimerecovery")
            .description("This specifies whether point in time recovery is enabled for the Sleeper table index. " +
                    "This is set on the DynamoDB tables.")
            .defaultValue("false")
            .validationPredicate(Utils::isTrueOrFalse)
            .propertyGroup(InstancePropertyGroup.COMMON)
            .runCdkDeployWhenChanged(true).build();
    UserDefinedInstanceProperty TABLE_INDEX_DYNAMO_STRONGLY_CONSISTENT_READS = Index.propertyBuilder("sleeper.tables.index.dynamo.consistent.reads")
            .description("This specifies whether queries and scans against the table index DynamoDB tables " +
                    "are strongly consistent.")
            .defaultValue("true")
            .validationPredicate(Utils::isTrueOrFalse)
            .propertyGroup(InstancePropertyGroup.COMMON)
            .build();
    UserDefinedInstanceProperty TABLE_PROPERTIES_PROVIDER_TIMEOUT_IN_MINS = Index.propertyBuilder("sleeper.cache.table.properties.provider.timeout.minutes")
            .description("The timeout in minutes for when the table properties provider cache should be cleared, " +
                    "forcing table properties to be reloaded from S3.")
            .defaultValue("60")
            .validationPredicate(Utils::isPositiveInteger)
            .propertyGroup(InstancePropertyGroup.COMMON)
            .build();
    UserDefinedInstanceProperty TABLE_BATCHING_LAMBDAS_MEMORY_IN_MB = Index.propertyBuilder("sleeper.batch.table.lambdas.memory")
            .description("The amount of memory in MB for lambdas that create batches of tables to run some operation against, " +
                    "eg. create compaction jobs, run garbage collection, perform partition splitting.")
            .defaultValue("1024")
            .validationPredicate(Utils::isPositiveInteger)
            .propertyGroup(InstancePropertyGroup.COMMON)
            .build();
    UserDefinedInstanceProperty TABLE_BATCHING_LAMBDAS_TIMEOUT_IN_SECONDS = Index.propertyBuilder("sleeper.batch.table.lambdas.timeout.seconds")
            .description("The timeout in seconds for lambdas that create batches of tables to run some operation against, " +
                    "eg. create compaction jobs, run garbage collection, perform partition splitting.")
            .defaultValue("60")
            .validationPredicate(Utils::isPositiveInteger)
            .propertyGroup(InstancePropertyGroup.COMMON)
            .build();
    UserDefinedInstanceProperty STATESTORE_COMMITTER_LAMBDA_MEMORY_IN_MB = Index.propertyBuilder("sleeper.statestore.committer.lambda.memory")
            .description("The amount of memory in MB for the lambda that commits state store updates.")
            .defaultValue("1024")
            .propertyGroup(InstancePropertyGroup.COMMON)
            .runCdkDeployWhenChanged(true).build();
    UserDefinedInstanceProperty STATESTORE_COMMITTER_LAMBDA_TIMEOUT_IN_SECONDS = Index.propertyBuilder("sleeper.statestore.committer.lambda.timeout.seconds")
            .description("The timeout for the lambda that commits state store updates in seconds.")
            .defaultValue("900")
            .validationPredicate(Utils::isValidLambdaTimeout)
            .propertyGroup(InstancePropertyGroup.COMMON)
            .runCdkDeployWhenChanged(true).build();
    UserDefinedInstanceProperty STATESTORE_COMMITTER_BATCH_SIZE = Index.propertyBuilder("sleeper.statestore.committer.batch.size")
            .description("The number of state store updates to be sent to the state store committer lambda in one invocation. " +
                    "This will be the batch size for a lambda as an SQS FIFO event source. This can be a maximum of 10.")
            .defaultValue("10")
            .validationPredicate(Utils::isPositiveIntegerLtEq10)
            .propertyGroup(InstancePropertyGroup.COMMON).build();
    UserDefinedInstanceProperty ECS_SECURITY_GROUPS = Index.propertyBuilder("sleeper.ecs.security.groups")
            .description("A comma-separated list of security group to be used when running ECS tasks.")
            .defaultValue("")
            .propertyGroup(InstancePropertyGroup.COMMON)
            .build();

    static List<UserDefinedInstanceProperty> getAll() {
        return Index.INSTANCE.getAll();
    }

    static boolean has(String propertyName) {
        return Index.INSTANCE.getByName(propertyName).isPresent();
    }

    class Index {
        private Index() {
        }

        private static final SleeperPropertyIndex<UserDefinedInstanceProperty> INSTANCE = new SleeperPropertyIndex<>();

        static UserDefinedInstancePropertyImpl.Builder propertyBuilder(String propertyName) {
            return UserDefinedInstancePropertyImpl.named(propertyName)
                    .addToIndex(INSTANCE::add);
        }
    }
}
