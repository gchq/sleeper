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

import java.util.List;

import static sleeper.core.properties.instance.CommonProperty.DEFAULT_LAMBDA_CONCURRENCY_MAXIMUM;
import static sleeper.core.properties.instance.CommonProperty.DEFAULT_LAMBDA_CONCURRENCY_RESERVED;
import static sleeper.core.properties.instance.TableStateProperty.DEFAULT_TABLE_STATE_LAMBDA_MEMORY;

/**
 * Definitions of instance properties relating to partition splitting.
 */
public interface PartitionSplittingProperty {
    UserDefinedInstanceProperty PARTITION_SPLITTING_TRIGGER_PERIOD_IN_MINUTES = Index.propertyBuilder("sleeper.partition.splitting.period.minutes")
            .description("The frequency in minutes with which the lambda runs to find partitions that need splitting " +
                    "and send jobs to the splitting lambda.")
            .defaultValue("30")
            .propertyGroup(InstancePropertyGroup.PARTITION_SPLITTING)
            .runCdkDeployWhenChanged(true).build();
    UserDefinedInstanceProperty MAX_NUMBER_FILES_IN_PARTITION_SPLITTING_JOB = Index.propertyBuilder("sleeper.partition.splitting.files.maximum")
            .description("When a partition needs splitting, a partition splitting job is created. This reads in the sketch files " +
                    "associated to the files in the partition in order to identify the median. This parameter controls the " +
                    "maximum number of files that are read in.")
            .defaultValue("50")
            .propertyGroup(InstancePropertyGroup.PARTITION_SPLITTING).build();
    UserDefinedInstanceProperty FIND_PARTITIONS_TO_SPLIT_BATCH_SIZE = Index.propertyBuilder("sleeper.partition.splitting.finder.batch.size")
            .description("The number of tables to find partitions to split for in a single invocation. " +
                    "This will be the batch size for a lambda as an SQS FIFO event source. This can be a maximum of 10.")
            .defaultValue("1")
            .validationPredicate(SleeperPropertyValueUtils::isPositiveInteger)
            .propertyGroup(InstancePropertyGroup.PARTITION_SPLITTING)
            .runCdkDeployWhenChanged(true).build();
    UserDefinedInstanceProperty FIND_PARTITIONS_TO_SPLIT_LAMBDA_MEMORY_IN_MB = Index.propertyBuilder("sleeper.partition.splitting.finder.memory.mb")
            .description("The amount of memory in MB for the lambda function used to identify partitions that need to be split.")
            .defaultProperty(DEFAULT_TABLE_STATE_LAMBDA_MEMORY)
            .propertyGroup(InstancePropertyGroup.PARTITION_SPLITTING)
            .runCdkDeployWhenChanged(true).build();
    UserDefinedInstanceProperty FIND_PARTITIONS_TO_SPLIT_TIMEOUT_IN_SECONDS = Index.propertyBuilder("sleeper.partition.splitting.finder.timeout.seconds")
            .description("The timeout in seconds for the lambda function used to identify partitions that need to be split.")
            .defaultValue("900")
            .validationPredicate(SleeperPropertyValueUtils::isValidLambdaTimeout)
            .propertyGroup(InstancePropertyGroup.PARTITION_SPLITTING)
            .runCdkDeployWhenChanged(true).build();
    UserDefinedInstanceProperty FIND_PARTITIONS_TO_SPLIT_LAMBDA_CONCURRENCY_RESERVED = Index.propertyBuilder("sleeper.partition.splitting.finder.concurrency.reserved")
            .defaultProperty(DEFAULT_LAMBDA_CONCURRENCY_RESERVED)
            .description("The reserved concurrency for the find partitions to split lambda.\n" +
                    "See reserved concurrency overview at: https://docs.aws.amazon.com/lambda/latest/dg/configuration-concurrency.html")
            .propertyGroup(InstancePropertyGroup.PARTITION_SPLITTING)
            .runCdkDeployWhenChanged(true).build();
    UserDefinedInstanceProperty FIND_PARTITIONS_TO_SPLIT_LAMBDA_CONCURRENCY_MAXIMUM = Index.propertyBuilder("sleeper.partition.splitting.finder.concurrency.max")
            .defaultProperty(DEFAULT_LAMBDA_CONCURRENCY_MAXIMUM)
            .description("The maximum given concurrency allowed for the find partitions to split lambda.\n" +
                    "See maximum concurrency overview at: https://aws.amazon.com/blogs/compute/introducing-maximum-concurrency-of-aws-lambda-functions-when-using-amazon-sqs-as-an-event-source/")
            .propertyGroup(InstancePropertyGroup.PARTITION_SPLITTING)
            .runCdkDeployWhenChanged(true).build();
    UserDefinedInstanceProperty SPLIT_PARTITIONS_LAMBDA_MEMORY_IN_MB = Index.propertyBuilder("sleeper.partition.splitting.memory.mb")
            .description("The amount of memory in MB for the lambda function used to split partitions.")
            .defaultProperty(DEFAULT_TABLE_STATE_LAMBDA_MEMORY)
            .propertyGroup(InstancePropertyGroup.PARTITION_SPLITTING)
            .runCdkDeployWhenChanged(true).build();
    UserDefinedInstanceProperty SPLIT_PARTITIONS_TIMEOUT_IN_SECONDS = Index.propertyBuilder("sleeper.partition.splitting.timeout.seconds")
            .description("The timeout in seconds for the lambda function used to split partitions.")
            .defaultValue("900")
            .validationPredicate(SleeperPropertyValueUtils::isValidLambdaTimeout)
            .propertyGroup(InstancePropertyGroup.PARTITION_SPLITTING)
            .runCdkDeployWhenChanged(true).build();
    UserDefinedInstanceProperty SPLIT_PARTITIONS_RESERVED_CONCURRENCY = Index.propertyBuilder("sleeper.partition.splitting.reserved.concurrency")
            .description("The number of lambda instances to reserve from your AWS account's quota for splitting " +
                    "partitions. Note that this will not provision instances until they are needed. Each " +
                    "time partition splitting runs, a separate lambda invocation will be made for each partition " +
                    "that needs to be split. If the reserved concurrency is less than the number of partitions that " +
                    "need to be split across all Sleeper tables in the instance, these invocations may queue up.")
            .defaultValue("10")
            .propertyGroup(InstancePropertyGroup.PARTITION_SPLITTING)
            .runCdkDeployWhenChanged(true).build();
    UserDefinedInstanceProperty DEFAULT_PARTITION_SPLIT_THRESHOLD = Index.propertyBuilder("sleeper.default.table.partition.splitting.threshold")
            .description("This is the default value of the partition splitting threshold. Partitions with more than the following " +
                    "number of rows in will be split. This value can be overridden on a per-table basis.")
            .defaultValue("1000000000")
            .propertyGroup(InstancePropertyGroup.PARTITION_SPLITTING).build();
    UserDefinedInstanceProperty DEFAULT_PARTITION_SPLIT_MIN_ROWS = Index.propertyBuilder("sleeper.default.table.partition.splitting.min.rows")
            .description("When expanding the partition tree explicitly, this is the default for how many rows are " +
                    "required in the input data to be able to split a partition. This will be used when " +
                    "pre-splitting partitions.\n" +
                    "For example, during bulk import when there are too few leaf partitions, the partition tree will " +
                    "be extended based on the data in the bulk import job. The bulk import job must contain at least " +
                    "this much data per new split point.")
            .defaultValue("1000")
            .propertyGroup(InstancePropertyGroup.PARTITION_SPLITTING).build();
    UserDefinedInstanceProperty DEFAULT_PARTITION_SPLIT_MIN_DISTRIBUTION_PERCENT = Index.propertyBuilder("sleeper.default.table.partition.splitting.min.distribution.percent")
            .description("When expanding the partition tree explicitly, this is the default for a minimum percentage " +
                    "of the expected number of rows to split a partition assuming an even distribution of rows.\n" +
                    "For example, during bulk import when there are too few leaf partitions, the partition tree will " +
                    "be extended based on the data in the bulk import job. For each current leaf partition, we make " +
                    "a sketch of the data from the job that's in that partition. We divide the number of rows in the " +
                    "job's input data by the current number of leaf partitions, to get the expected rows per " +
                    "partition. If this propery is set to 10, then any partition with less than 10% of the expected " +
                    "rows per partition will be ignored when extending the partition tree.")
            .defaultValue("10")
            .propertyGroup(InstancePropertyGroup.PARTITION_SPLITTING).build();

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
}
