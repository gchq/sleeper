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
 * Definitions of instance properties relating to garbage collection.
 */
public interface GarbageCollectionProperty {
    UserDefinedInstanceProperty GARBAGE_COLLECTOR_PERIOD_IN_MINUTES = Index.propertyBuilder("sleeper.gc.period.minutes")
            .description("The frequency in minutes with which the garbage collector lambda is run.")
            .defaultValue("15")
            .propertyGroup(InstancePropertyGroup.GARBAGE_COLLECTOR)
            .runCdkDeployWhenChanged(true).build();
    UserDefinedInstanceProperty GARBAGE_COLLECTOR_LAMBDA_TIMEOUT_IN_SECONDS = Index.propertyBuilder("sleeper.gc.lambda.timeout.seconds")
            .description("The timeout in seconds for the garbage collector lambda.")
            .defaultValue("840")
            .validationPredicate(SleeperPropertyValueUtils::isValidLambdaTimeout)
            .propertyGroup(InstancePropertyGroup.GARBAGE_COLLECTOR)
            .runCdkDeployWhenChanged(true).build();
    UserDefinedInstanceProperty GARBAGE_COLLECTOR_LAMBDA_MEMORY_IN_MB = Index.propertyBuilder("sleeper.gc.memory.mb")
            .description("The amount of memory in MB for the lambda function used to perform garbage collection.")
            .defaultProperty(DEFAULT_TABLE_STATE_LAMBDA_MEMORY)
            .propertyGroup(InstancePropertyGroup.GARBAGE_COLLECTOR)
            .runCdkDeployWhenChanged(true).build();
    UserDefinedInstanceProperty GARBAGE_COLLECTOR_LAMBDA_CONCURRENCY_RESERVED = Index.propertyBuilder("sleeper.gc.concurrency.reserved")
            .defaultProperty(DEFAULT_LAMBDA_CONCURRENCY_RESERVED)
            .description("The reserved concurrency for the garbage collection lambda.\n" +
                    "See reserved concurrency overview at: https://docs.aws.amazon.com/lambda/latest/dg/configuration-concurrency.html")
            .propertyGroup(InstancePropertyGroup.GARBAGE_COLLECTOR)
            .runCdkDeployWhenChanged(true).build();
    UserDefinedInstanceProperty GARBAGE_COLLECTOR_LAMBDA_CONCURRENCY_MAXIMUM = Index.propertyBuilder("sleeper.gc.concurrency.max")
            .defaultProperty(DEFAULT_LAMBDA_CONCURRENCY_MAXIMUM)
            .description("The maximum given concurrency allowed for the garbage collection lambda.\n" +
                    "See maximum concurrency overview at: https://aws.amazon.com/blogs/compute/introducing-maximum-concurrency-of-aws-lambda-functions-when-using-amazon-sqs-as-an-event-source/")
            .propertyGroup(InstancePropertyGroup.GARBAGE_COLLECTOR)
            .runCdkDeployWhenChanged(true).build();
    UserDefinedInstanceProperty GARBAGE_COLLECTOR_TABLE_BATCH_SIZE = Index.propertyBuilder("sleeper.gc.table.batch.size")
            .description("The number of tables to perform garbage collection for in a single invocation. " +
                    "This will be the batch size for a lambda as an SQS FIFO event source. This can be a maximum of 10.")
            .defaultValue("1")
            .validationPredicate(SleeperPropertyValueUtils::isPositiveIntegerLtEq10)
            .propertyGroup(InstancePropertyGroup.GARBAGE_COLLECTOR)
            .runCdkDeployWhenChanged(true).build();
    UserDefinedInstanceProperty GARBAGE_COLLECT_OFFLINE_TABLES = Index.propertyBuilder("sleeper.run.gc.offline")
            .description("Whether to perform garbage collection for offline tables.")
            .defaultValue("false")
            .validationPredicate(SleeperPropertyValueUtils::isTrueOrFalse)
            .propertyGroup(InstancePropertyGroup.GARBAGE_COLLECTOR).build();
    UserDefinedInstanceProperty GARBAGE_COLLECTOR_BATCH_SIZE = Index.propertyBuilder("sleeper.gc.batch.size")
            .description("The number of deleted files recorded to the state store in a single commit.\n" +
                    "The garbage collector keeps deleting files as long as there are files to delete in the state " +
                    "store, and updates the state store whenever it has deleted this many files.")
            .defaultValue("10000")
            .propertyGroup(InstancePropertyGroup.GARBAGE_COLLECTOR).build();
    UserDefinedInstanceProperty GARBAGE_COLLECTOR_MAXIMUM_FILE_DELETION_PER_INVOCATION = Index.propertyBuilder("sleeper.gc.files.maximum")
            .description("The maximum number of files that can be deleted per invocation of the garbage collector.\n" +
                    "If a batch of files exceeds this limit, the whole batch will be deleted before terminating.\n" +
                    "This limit is applied separately for each Sleeper table.\n" +
                    "This restriction is placed to avoid reaching the lambda timeout for the garbage collector. " +
                    "If this timeout is met, it is most likely to happen whilst deleting a batch of files. " +
                    "This would result in files being deleted, but the state store not being updated. " +
                    "Any files caught in such a state will be found on the next garbage collector run and deleted again, " +
                    "updating the state store as expected.")
            .defaultValue("750000")
            .propertyGroup(InstancePropertyGroup.GARBAGE_COLLECTOR).build();
    UserDefinedInstanceProperty DEFAULT_GARBAGE_COLLECTOR_DELAY_BEFORE_DELETION = Index.propertyBuilder("sleeper.default.table.gc.delay.minutes")
            .description("A file will not be deleted until this number of minutes have passed after it has been marked as ready for " +
                    "garbage collection. The reason for not deleting files immediately after they have been marked as ready for " +
                    "garbage collection is that they may still be in use by queries. This property can be overridden on a per-table " +
                    "basis.")
            .defaultValue("15")
            .propertyGroup(InstancePropertyGroup.GARBAGE_COLLECTOR).build();

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
