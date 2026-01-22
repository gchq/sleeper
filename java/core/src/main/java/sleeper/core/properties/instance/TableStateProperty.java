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

import sleeper.core.properties.SleeperPropertyIndex;
import sleeper.core.properties.model.SleeperPropertyValueUtils;
import sleeper.core.properties.model.StateStoreCommitterPlatform;

import java.util.List;

import static sleeper.core.properties.instance.CommonProperty.DEFAULT_LAMBDA_CONCURRENCY_MAXIMUM;
import static sleeper.core.properties.instance.CommonProperty.DEFAULT_LAMBDA_CONCURRENCY_RESERVED;
import static sleeper.core.properties.model.SleeperPropertyValueUtils.describeEnumValuesInLowerCase;

/**
 * Definitions of instance properties relating to handling the state of Sleeper tables.
 */
public interface TableStateProperty {

    static List<UserDefinedInstanceProperty> getAll() {
        return Index.INSTANCE.getAll();
    }

    /**
     * An index of property definitions in this file.
     */
    class Index {
        private Index() {
        }

        private static final SleeperPropertyIndex<UserDefinedInstanceProperty> INSTANCE = new SleeperPropertyIndex<>();

        private static UserDefinedInstancePropertyImpl.Builder propertyBuilder(String propertyName) {
            return UserDefinedInstancePropertyImpl.named(propertyName)
                    .addToIndex(INSTANCE::add);
        }
    }

    UserDefinedInstanceProperty DEFAULT_TABLE_STATE_LAMBDA_MEMORY = Index.propertyBuilder("sleeper.default.lambda.table.state.memory.mb")
            .description("Default value for amount of memory in MB for each lambda that holds the state of Sleeper " +
                    "tables in memory. These use a state store provider which caches a number of tables at " +
                    "once, set in `sleeper.statestore.provider.cache.size`. Not all lambdas are covered " +
                    "by this, e.g. see `sleeper.batch.table.lambdas.memory.mb`.")
            .validationPredicate(SleeperPropertyValueUtils::isPositiveInteger)
            .defaultValue("4096")
            .propertyGroup(InstancePropertyGroup.TABLE_STATE)
            .runCdkDeployWhenChanged(true)
            .build();
    UserDefinedInstanceProperty TABLE_BATCHING_LAMBDAS_MEMORY_IN_MB = Index.propertyBuilder("sleeper.batch.table.lambdas.memory.mb")
            .description("The amount of memory in MB for lambdas that create batches of tables to run some operation against, " +
                    "eg. create compaction jobs, run garbage collection, perform partition splitting.")
            .defaultValue("1024")
            .validationPredicate(SleeperPropertyValueUtils::isPositiveInteger)
            .propertyGroup(InstancePropertyGroup.TABLE_STATE)
            .runCdkDeployWhenChanged(true)
            .build();
    UserDefinedInstanceProperty TABLE_BATCHING_LAMBDAS_TIMEOUT_IN_SECONDS = Index.propertyBuilder("sleeper.batch.table.lambdas.timeout.seconds")
            .description("The timeout in seconds for lambdas that create batches of tables to run some operation against, " +
                    "eg. create compaction jobs, run garbage collection, perform partition splitting.")
            .defaultValue("60")
            .validationPredicate(SleeperPropertyValueUtils::isValidLambdaTimeout)
            .propertyGroup(InstancePropertyGroup.TABLE_STATE)
            .runCdkDeployWhenChanged(true)
            .build();
    UserDefinedInstanceProperty TABLE_PROPERTIES_PROVIDER_TIMEOUT_IN_MINS = Index.propertyBuilder("sleeper.cache.table.properties.provider.timeout.minutes")
            .description("The timeout in minutes for when the table properties provider cache should be cleared, " +
                    "forcing table properties to be reloaded from S3.")
            .defaultValue("60")
            .validationPredicate(SleeperPropertyValueUtils::isPositiveInteger)
            .propertyGroup(InstancePropertyGroup.TABLE_STATE)
            .build();
    UserDefinedInstanceProperty STATESTORE_PROVIDER_CACHE_SIZE = Index.propertyBuilder("sleeper.statestore.provider.cache.size")
            .description("The maximum size of state store providers. If a state store is needed and the cache is " +
                    "full, the oldest state store in the cache will be removed to make space. This can be disabled " +
                    "by setting a negative value, e.g. -1.")
            .defaultValue("10")
            .validationPredicate(SleeperPropertyValueUtils::isPositiveInteger)
            .propertyGroup(InstancePropertyGroup.TABLE_STATE)
            .build();
    UserDefinedInstanceProperty DYNAMO_STATE_STORE_POINT_IN_TIME_RECOVERY = Index.propertyBuilder("sleeper.statestore.dynamo.pointintimerecovery")
            .description("This specifies whether point in time recovery is enabled for the DynamoDB state store. " +
                    "This is set on the DynamoDB tables.")
            .defaultValue("false")
            .validationPredicate(SleeperPropertyValueUtils::isTrueOrFalse)
            .propertyGroup(InstancePropertyGroup.TABLE_STATE)
            .runCdkDeployWhenChanged(true).build();
    UserDefinedInstanceProperty S3_STATE_STORE_DYNAMO_POINT_IN_TIME_RECOVERY = Index.propertyBuilder("sleeper.statestore.s3.dynamo.pointintimerecovery")
            .description("This specifies whether point in time recovery is enabled for the S3 state store. " +
                    "This is set on the revision DynamoDB table.")
            .defaultValue("false")
            .validationPredicate(SleeperPropertyValueUtils::isTrueOrFalse)
            .propertyGroup(InstancePropertyGroup.TABLE_STATE)
            .runCdkDeployWhenChanged(true).build();
    UserDefinedInstanceProperty SNAPSHOT_CREATION_BATCH_SIZE = Index.propertyBuilder("sleeper.statestore.snapshot.creation.batch.size")
            .description("The number of tables to create transaction log snapshots for in a single invocation. This will be the batch size" +
                    " for a lambda as an SQS FIFO event source. This can be a maximum of 10.")
            .defaultValue("1")
            .validationPredicate(SleeperPropertyValueUtils::isPositiveIntegerLtEq10)
            .propertyGroup(InstancePropertyGroup.TABLE_STATE)
            .runCdkDeployWhenChanged(true)
            .build();
    UserDefinedInstanceProperty SNAPSHOT_CREATION_LAMBDA_PERIOD_IN_SECONDS = Index.propertyBuilder("sleeper.statestore.snapshot.creation.lambda.period.seconds")
            .description("The frequency in seconds with which the transaction log snapshot creation lambda is run.")
            .defaultValue("60")
            .validationPredicate(SleeperPropertyValueUtils::isPositiveInteger)
            .propertyGroup(InstancePropertyGroup.TABLE_STATE)
            .runCdkDeployWhenChanged(true)
            .build();
    UserDefinedInstanceProperty SNAPSHOT_CREATION_LAMBDA_TIMEOUT_IN_SECONDS = Index.propertyBuilder("sleeper.statestore.snapshot.creation.lambda.timeout.seconds")
            .description("The timeout in seconds after which to terminate the transaction log snapshot creation lambda.")
            .defaultValue("900")
            .validationPredicate(SleeperPropertyValueUtils::isValidLambdaTimeout)
            .propertyGroup(InstancePropertyGroup.TABLE_STATE)
            .runCdkDeployWhenChanged(true)
            .build();
    UserDefinedInstanceProperty SNAPSHOT_CREATION_LAMBDA_MEMORY = Index.propertyBuilder("sleeper.statestore.snapshot.creation.memory.mb")
            .description("The amount of memory in MB for the transaction log snapshot creation lambda.")
            .defaultProperty(DEFAULT_TABLE_STATE_LAMBDA_MEMORY)
            .propertyGroup(InstancePropertyGroup.TABLE_STATE)
            .runCdkDeployWhenChanged(true)
            .build();
    UserDefinedInstanceProperty SNAPSHOT_CREATION_LAMBDA_CONCURRENCY_RESERVED = Index.propertyBuilder("sleeper.statestore.snapshot.creation.concurrency.reserved")
            .defaultProperty(DEFAULT_LAMBDA_CONCURRENCY_RESERVED)
            .description("The reserved concurrency for the snapshot creation lambda.\n" +
                    "See reserved concurrency overview at: https://docs.aws.amazon.com/lambda/latest/dg/configuration-concurrency.html")
            .propertyGroup(InstancePropertyGroup.TABLE_STATE)
            .runCdkDeployWhenChanged(true).build();
    UserDefinedInstanceProperty SNAPSHOT_CREATION_LAMBDA_CONCURRENCY_MAXIMUM = Index.propertyBuilder("sleeper.statestore.snapshot.creation.concurrency.max")
            .defaultProperty(DEFAULT_LAMBDA_CONCURRENCY_MAXIMUM)
            .description("The maximum given concurrency allowed for the snapshot creation lambda.\n" +
                    "See maximum concurrency overview at: https://aws.amazon.com/blogs/compute/introducing-maximum-concurrency-of-aws-lambda-functions-when-using-amazon-sqs-as-an-event-source/")
            .propertyGroup(InstancePropertyGroup.TABLE_STATE)
            .runCdkDeployWhenChanged(true).build();
    UserDefinedInstanceProperty SNAPSHOT_DELETION_BATCH_SIZE = Index.propertyBuilder("sleeper.statestore.snapshot.deletion.batch.size")
            .description("The number of tables to delete old transaction log snapshots for in a single invocation. This will be the batch size" +
                    " for a lambda as an SQS FIFO event source. This can be a maximum of 10.")
            .defaultValue("1")
            .validationPredicate(SleeperPropertyValueUtils::isPositiveIntegerLtEq10)
            .propertyGroup(InstancePropertyGroup.TABLE_STATE)
            .runCdkDeployWhenChanged(true)
            .build();
    UserDefinedInstanceProperty SNAPSHOT_DELETION_LAMBDA_PERIOD_IN_MINUTES = Index.propertyBuilder("sleeper.statestore.snapshot.deletion.lambda.period.minutes")
            .description("The frequency in minutes with which the transaction log snapshot deletion lambda is run.")
            .defaultValue("60")
            .validationPredicate(SleeperPropertyValueUtils::isPositiveInteger)
            .propertyGroup(InstancePropertyGroup.TABLE_STATE)
            .runCdkDeployWhenChanged(true)
            .build();
    UserDefinedInstanceProperty SNAPSHOT_DELETION_LAMBDA_CONCURRENCY_RESERVED = Index.propertyBuilder("sleeper.statestore.snapshot.deletion.concurrency.reserved")
            .defaultProperty(DEFAULT_LAMBDA_CONCURRENCY_RESERVED)
            .description("The reserved concurrency for the snapshot deletion lambda.\n" +
                    "See reserved concurrency overview at: https://docs.aws.amazon.com/lambda/latest/dg/configuration-concurrency.html")
            .propertyGroup(InstancePropertyGroup.TABLE_STATE)
            .runCdkDeployWhenChanged(true).build();
    UserDefinedInstanceProperty SNAPSHOT_DELETION_LAMBDA_CONCURRENCY_MAXIMUM = Index.propertyBuilder("sleeper.statestore.snapshot.deletion.concurrency.max")
            .defaultProperty(DEFAULT_LAMBDA_CONCURRENCY_MAXIMUM)
            .description("The maximum given concurrency allowed for the snapshot deletion lambda.\n" +
                    "See maximum concurrency overview at: https://aws.amazon.com/blogs/compute/introducing-maximum-concurrency-of-aws-lambda-functions-when-using-amazon-sqs-as-an-event-source/")
            .propertyGroup(InstancePropertyGroup.TABLE_STATE)
            .runCdkDeployWhenChanged(true).build();
    UserDefinedInstanceProperty TRANSACTION_DELETION_BATCH_SIZE = Index.propertyBuilder("sleeper.statestore.transaction.deletion.batch.size")
            .description("The number of tables to delete old transaction log transactions for in a single invocation. This will be the batch size" +
                    " for a lambda as an SQS FIFO event source. This can be a maximum of 10.")
            .defaultValue("1")
            .validationPredicate(SleeperPropertyValueUtils::isPositiveIntegerLtEq10)
            .propertyGroup(InstancePropertyGroup.TABLE_STATE)
            .runCdkDeployWhenChanged(true)
            .build();
    UserDefinedInstanceProperty TRANSACTION_DELETION_LAMBDA_PERIOD_IN_MINUTES = Index.propertyBuilder("sleeper.statestore.transaction.deletion.lambda.period.minutes")
            .description("The frequency in minutes with which the transaction log transaction deletion lambda is run.")
            .defaultValue("60")
            .validationPredicate(SleeperPropertyValueUtils::isPositiveInteger)
            .propertyGroup(InstancePropertyGroup.TABLE_STATE)
            .runCdkDeployWhenChanged(true)
            .build();
    UserDefinedInstanceProperty TRANSACTION_DELETION_LAMBDA_CONCURRENCY_RESERVED = Index.propertyBuilder("sleeper.statestore.transaction.deletion.concurrency.reserved")
            .defaultProperty(DEFAULT_LAMBDA_CONCURRENCY_RESERVED)
            .description("The reserved concurrency for the transaction deletion lambda.\n" +
                    "See reserved concurrency overview at: https://docs.aws.amazon.com/lambda/latest/dg/configuration-concurrency.html")
            .propertyGroup(InstancePropertyGroup.TABLE_STATE)
            .runCdkDeployWhenChanged(true).build();
    UserDefinedInstanceProperty TRANSACTION_DELETION_LAMBDA_CONCURRENCY_MAXIMUM = Index.propertyBuilder("sleeper.statestore.transaction.deletion.concurrency.max")
            .defaultProperty(DEFAULT_LAMBDA_CONCURRENCY_MAXIMUM)
            .description("The maximum given concurrency allowed for the transaction deletion lambda.\n" +
                    "See maximum concurrency overview at: https://aws.amazon.com/blogs/compute/introducing-maximum-concurrency-of-aws-lambda-functions-when-using-amazon-sqs-as-an-event-source/")
            .propertyGroup(InstancePropertyGroup.TABLE_STATE)
            .runCdkDeployWhenChanged(true).build();
    UserDefinedInstanceProperty TRANSACTION_DELETION_LAMBDA_TIMEOUT_SECS = Index.propertyBuilder("sleeper.statestore.transaction.deletion.lambda.timeout.seconds")
            .description("The maximum timeout for the transaction deletion lambda in seconds.")
            .validationPredicate(SleeperPropertyValueUtils::isValidLambdaTimeout)
            .defaultValue("900")
            .propertyGroup(InstancePropertyGroup.TABLE_STATE)
            .runCdkDeployWhenChanged(true).build();
    UserDefinedInstanceProperty TRANSACTION_FOLLOWER_LAMBDA_CONCURRENCY_RESERVED = Index.propertyBuilder("sleeper.statestore.transaction.follower.concurrency.reserved")
            .defaultProperty(DEFAULT_LAMBDA_CONCURRENCY_RESERVED)
            .description("The reserved concurrency for the lambda that follows the state store transaction log to trigger updates.\n" +
                    "See reserved concurrency overview at: https://docs.aws.amazon.com/lambda/latest/dg/configuration-concurrency.html")
            .propertyGroup(InstancePropertyGroup.TABLE_STATE)
            .runCdkDeployWhenChanged(true).build();
    UserDefinedInstanceProperty TRANSACTION_FOLLOWER_LAMBDA_CONCURRENCY_MAXIMUM = Index.propertyBuilder("sleeper.statestore.transaction.follower.concurrency.max")
            .defaultProperty(DEFAULT_LAMBDA_CONCURRENCY_MAXIMUM)
            .description("The maximum given concurrency allowed for the lambda that follows the state store transaction log to trigger updates.\n" +
                    "See maximum concurrency overview at: https://aws.amazon.com/blogs/compute/introducing-maximum-concurrency-of-aws-lambda-functions-when-using-amazon-sqs-as-an-event-source/")
            .propertyGroup(InstancePropertyGroup.TABLE_STATE).build();
    UserDefinedInstanceProperty TRANSACTION_FOLLOWER_LAMBDA_TIMEOUT_SECS = Index.propertyBuilder("sleeper.statestore.transaction.follower.lambda.timeout.seconds")
            .description("The maximum timeout in seconds for the lambda that follows the state store transaction log to trigger updates.")
            .validationPredicate(SleeperPropertyValueUtils::isValidLambdaTimeout)
            .defaultValue("900")
            .propertyGroup(InstancePropertyGroup.TABLE_STATE)
            .runCdkDeployWhenChanged(true).build();
    UserDefinedInstanceProperty TRANSACTION_FOLLOWER_LAMBDA_MEMORY = Index.propertyBuilder("sleeper.statestore.transaction.follower.memory.mb")
            .description("The amount of memory in MB for the lambda that follows the state store transaction log to trigger updates.")
            .defaultProperty(DEFAULT_TABLE_STATE_LAMBDA_MEMORY)
            .propertyGroup(InstancePropertyGroup.TABLE_STATE)
            .runCdkDeployWhenChanged(true)
            .build();
    UserDefinedInstanceProperty TABLE_INDEX_DYNAMO_POINT_IN_TIME_RECOVERY = Index.propertyBuilder("sleeper.tables.index.dynamo.pointintimerecovery")
            .description("This specifies whether point in time recovery is enabled for the Sleeper table index. " +
                    "This is set on the DynamoDB tables.")
            .defaultValue("false")
            .validationPredicate(SleeperPropertyValueUtils::isTrueOrFalse)
            .propertyGroup(InstancePropertyGroup.TABLE_STATE)
            .runCdkDeployWhenChanged(true).build();
    UserDefinedInstanceProperty TABLE_INDEX_DYNAMO_STRONGLY_CONSISTENT_READS = Index.propertyBuilder("sleeper.tables.index.dynamo.consistent.reads")
            .description("This specifies whether queries and scans against the table index DynamoDB tables " +
                    "are strongly consistent.")
            .defaultValue("true")
            .validationPredicate(SleeperPropertyValueUtils::isTrueOrFalse)
            .propertyGroup(InstancePropertyGroup.TABLE_STATE)
            .build();
    UserDefinedInstanceProperty STATESTORE_COMMITTER_PLATFORM = Index.propertyBuilder("sleeper.statestore.committer.platform")
            .description("The platform that the state store committer will be deployed to for execution.\n" +
                    "Valid values are: " + describeEnumValuesInLowerCase(StateStoreCommitterPlatform.class) + "\n" +
                    "NB: The EC2 platform is currently considered experimental.")
            .defaultValue(StateStoreCommitterPlatform.LAMBDA.toString())
            .validationPredicate(StateStoreCommitterPlatform::isValid)
            .propertyGroup(InstancePropertyGroup.TABLE_STATE)
            .runCdkDeployWhenChanged(true)
            .build();
    UserDefinedInstanceProperty STATESTORE_COMMITTER_LAMBDA_MEMORY_IN_MB = Index.propertyBuilder("sleeper.statestore.committer.lambda.memory.mb")
            .description("The amount of memory in MB for the lambda that commits state store updates.")
            .defaultProperty(DEFAULT_TABLE_STATE_LAMBDA_MEMORY)
            .propertyGroup(InstancePropertyGroup.TABLE_STATE)
            .runCdkDeployWhenChanged(true).build();
    UserDefinedInstanceProperty STATESTORE_COMMITTER_LAMBDA_TIMEOUT_IN_SECONDS = Index.propertyBuilder("sleeper.statestore.committer.lambda.timeout.seconds")
            .description("The timeout for the lambda that commits state store updates in seconds.")
            .defaultValue("900")
            .validationPredicate(SleeperPropertyValueUtils::isValidLambdaTimeout)
            .propertyGroup(InstancePropertyGroup.TABLE_STATE)
            .runCdkDeployWhenChanged(true).build();
    UserDefinedInstanceProperty STATESTORE_COMMITTER_BATCH_SIZE = Index.propertyBuilder("sleeper.statestore.committer.batch.size")
            .description("The number of state store updates to be sent to the state store committer lambda in one invocation. " +
                    "This will be the batch size for a lambda as an SQS FIFO event source. This can be a maximum of 10.")
            .defaultValue("10")
            .validationPredicate(SleeperPropertyValueUtils::isPositiveIntegerLtEq10)
            .propertyGroup(InstancePropertyGroup.TABLE_STATE)
            .runCdkDeployWhenChanged(true).build();
    UserDefinedInstanceProperty STATESTORE_COMMITTER_LAMBDA_CONCURRENCY_RESERVED = Index.propertyBuilder("sleeper.statestore.committer.concurrency.reserved")
            .description("The reserved concurrency for the state store committer lambda.\n" +
                    "Presently this value defaults to 10 to align with expectations around table efficiency.\n" +
                    "This is to ensure that state store operations can still be applied to at least 10 tables, " +
                    "even when concurrency is used up in the account.\n" +
                    "See reserved concurrency overview at: https://docs.aws.amazon.com/lambda/latest/dg/configuration-concurrency.html")
            .defaultValue("10")
            .propertyGroup(InstancePropertyGroup.TABLE_STATE)
            .runCdkDeployWhenChanged(true).build();
    UserDefinedInstanceProperty STATESTORE_COMMITTER_LAMBDA_CONCURRENCY_MAXIMUM = Index.propertyBuilder("sleeper.statestore.committer.concurrency.max")
            .defaultProperty(DEFAULT_LAMBDA_CONCURRENCY_MAXIMUM)
            .description("The maximum given concurrency allowed for the state store committer lambda.\n" +
                    "See maximum concurrency overview at: https://aws.amazon.com/blogs/compute/introducing-maximum-concurrency-of-aws-lambda-functions-when-using-amazon-sqs-as-an-event-source/")
            .propertyGroup(InstancePropertyGroup.TABLE_STATE)
            .runCdkDeployWhenChanged(true).build();
    UserDefinedInstanceProperty STATESTORE_COMMITTER_EC2_INSTANCE_TYPE = Index.propertyBuilder("sleeper.statestore.committer.ec2.type")
            .description("The EC2 instance type that the multi-threaded state store committer should be deployed onto.")
            .defaultValue("m8g.xlarge")
            .runCdkDeployWhenChanged(true)
            .validationPredicate(SleeperPropertyValueUtils::isNonNullNonEmptyString)
            .propertyGroup(InstancePropertyGroup.TABLE_STATE)
            .build();
    UserDefinedInstanceProperty STATESTORE_COMMITTER_EC2_MIN_FREE_HEAP_TARGET_AMOUNT = Index.propertyBuilder("sleeper.statestore.committer.ec2.min.heap.target.amount")
            .description("The minimum amount of heap space that the committer will try to keep available. This affects how many state stores can be cached in memory.")
            .defaultValue("100M")
            .validationPredicate(SleeperPropertyValueUtils::isValidNumberOfBytes)
            .propertyGroup(InstancePropertyGroup.TABLE_STATE)
            .build();
    UserDefinedInstanceProperty STATESTORE_COMMITTER_EC2_MIN_FREE_HEAP_TARGET_PERCENTAGE = Index.propertyBuilder("sleeper.statestore.committer.ec2.min.heap.target.percentage")
            .description("The percentage of the total heap space that the committer should try to keep available, as a minimum. This affects how many state stores can be cached in memory.")
            .defaultValue("1")
            .validationPredicate(SleeperPropertyValueUtils::isPositiveLong)
            .propertyGroup(InstancePropertyGroup.TABLE_STATE)
            .build();
}
