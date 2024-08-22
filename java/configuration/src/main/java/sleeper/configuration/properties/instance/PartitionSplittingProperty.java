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
            .validationPredicate(Utils::isPositiveInteger)
            .propertyGroup(InstancePropertyGroup.PARTITION_SPLITTING)
            .runCdkDeployWhenChanged(true).build();
    UserDefinedInstanceProperty FIND_PARTITIONS_TO_SPLIT_LAMBDA_MEMORY_IN_MB = Index.propertyBuilder("sleeper.partition.splitting.finder.memory")
            .description("The amount of memory in MB for the lambda function used to identify partitions that need to be split.")
            .defaultValue("2048")
            .propertyGroup(InstancePropertyGroup.PARTITION_SPLITTING)
            .runCdkDeployWhenChanged(true).build();
    UserDefinedInstanceProperty FIND_PARTITIONS_TO_SPLIT_TIMEOUT_IN_SECONDS = Index.propertyBuilder("sleeper.partition.splitting.finder.timeout.seconds")
            .description("The timeout in seconds for the lambda function used to identify partitions that need to be split.")
            .defaultValue("900")
            .propertyGroup(InstancePropertyGroup.PARTITION_SPLITTING)
            .runCdkDeployWhenChanged(true).build();
    UserDefinedInstanceProperty SPLIT_PARTITIONS_LAMBDA_MEMORY_IN_MB = Index.propertyBuilder("sleeper.partition.splitting.memory")
            .description("The amount of memory in MB for the lambda function used to split partitions.")
            .defaultValue("2048")
            .propertyGroup(InstancePropertyGroup.PARTITION_SPLITTING)
            .runCdkDeployWhenChanged(true).build();
    UserDefinedInstanceProperty SPLIT_PARTITIONS_TIMEOUT_IN_SECONDS = Index.propertyBuilder("sleeper.partition.splitting.timeout.seconds")
            .description("The timeout in seconds for the lambda function used to split partitions.")
            .defaultValue("900")
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
    UserDefinedInstanceProperty DEFAULT_PARTITION_SPLIT_THRESHOLD = Index.propertyBuilder("sleeper.default.partition.splitting.threshold")
            .description("This is the default value of the partition splitting threshold. Partitions with more than the following " +
                    "number of records in will be split. This value can be overridden on a per-table basis.")
            .defaultValue("1000000000")
            .propertyGroup(InstancePropertyGroup.PARTITION_SPLITTING).build();
    UserDefinedInstanceProperty DEFAULT_PARTITION_SPLIT_ASYNC_COMMIT = Index.propertyBuilder("sleeper.default.partition.splitting.commit.async")
            .description("If true, partition splits will be applied via asynchronous requests sent to the state " +
                    "store committer lambda. If false, the partition splitting lambda will apply splits synchronously.")
            .defaultValue("true")
            .propertyGroup(InstancePropertyGroup.PARTITION_SPLITTING).build();

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
