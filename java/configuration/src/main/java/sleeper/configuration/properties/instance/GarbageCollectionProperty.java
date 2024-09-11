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

import static sleeper.configuration.properties.instance.DefaultProperty.DEFAULT_LAMBDA_CONCURRENCY_MAXIMUM;

public interface GarbageCollectionProperty {
    UserDefinedInstanceProperty GARBAGE_COLLECTOR_PERIOD_IN_MINUTES = Index.propertyBuilder("sleeper.gc.period.minutes")
            .description("The frequency in minutes with which the garbage collector lambda is run.")
            .defaultValue("15")
            .propertyGroup(InstancePropertyGroup.GARBAGE_COLLECTOR)
            .runCdkDeployWhenChanged(true).build();
    UserDefinedInstanceProperty GARBAGE_COLLECTOR_LAMBDA_TIMEOUT_IN_MINUTES = Index.propertyBuilder("sleeper.gc.lambda.timeout.minutes")
            .description("The configurable timeout wait in minutes for the garbage collector lambda.")
            .defaultValue("14")
            .propertyGroup(InstancePropertyGroup.GARBAGE_COLLECTOR)
            .validationPredicate(Utils::isPositiveIntegerLtEq15)
            .runCdkDeployWhenChanged(true).build();
    UserDefinedInstanceProperty GARBAGE_COLLECTOR_LAMBDA_MEMORY_IN_MB = Index.propertyBuilder("sleeper.gc.memory")
            .description("The amount of memory in MB for the lambda function used to perform garbage collection.")
            .defaultValue("1024")
            .propertyGroup(InstancePropertyGroup.GARBAGE_COLLECTOR)
            .runCdkDeployWhenChanged(true).build();
    UserDefinedInstanceProperty GARBAGE_COLLECTOR_LAMBDA_CONCURRENCY_RESERVED = Index.propertyBuilder("sleeper.gc.concurrency.reserved")
            .defafultProperty(DEFAULT_LAMBDA_CONCURRENCY_MAXIMUM)
            .description("The reserved concurrency for the garbage collection lambda.\n" +
                    "See reserved concurrency overview at: https://docs.aws.amazon.com/lambda/latest/dg/configuration-concurrency.html")
            .validationPredicate(Utils::isPositiveIntegerOrNull)
            .propertyGroup(InstancePropertyGroup.GARBAGE_COLLECTOR).build();
    UserDefinedInstanceProperty GARBAGE_COLLECTOR_LAMBDA_CONCURRENCY_MAXIMUM = Index.propertyBuilder("sleeper.gc.concurrency.max")
            .defafultProperty(DEFAULT_LAMBDA_CONCURRENCY_MAXIMUM)
            .description("The maximum given concurrency allowed for the garbage collection lambda.\n" +
                    "See maximum concurrency overview at: https://aws.amazon.com/blogs/compute/introducing-maximum-concurrency-of-aws-lambda-functions-when-using-amazon-sqs-as-an-event-source/")
            .validationPredicate(Utils::isPositiveIntegerOrNull)
            .propertyGroup(InstancePropertyGroup.GARBAGE_COLLECTOR).build();
    UserDefinedInstanceProperty GARBAGE_COLLECTOR_TABLE_BATCH_SIZE = Index.propertyBuilder("sleeper.gc.table.batch.size")
            .description("The number of tables to perform garbage collection for in a single invocation. " +
                    "This will be the batch size for a lambda as an SQS FIFO event source. This can be a maximum of 10.")
            .defaultValue("1")
            .validationPredicate(Utils::isPositiveIntegerLtEq10)
            .propertyGroup(InstancePropertyGroup.GARBAGE_COLLECTOR).build();
    UserDefinedInstanceProperty GARBAGE_COLLECT_OFFLINE_TABLES = Index.propertyBuilder("sleeper.gc.offline.enabled")
            .description("Whether to perform garbage collection for offline tables.")
            .defaultValue("false")
            .validationPredicate(Utils::isTrueOrFalse)
            .propertyGroup(InstancePropertyGroup.GARBAGE_COLLECTOR).build();
    UserDefinedInstanceProperty GARBAGE_COLLECTOR_BATCH_SIZE = Index.propertyBuilder("sleeper.gc.batch.size")
            .description("The size of the batch of files ready for garbage collection requested from the State Store.")
            .defaultValue("2000")
            .propertyGroup(InstancePropertyGroup.GARBAGE_COLLECTOR).build();
    UserDefinedInstanceProperty DEFAULT_GARBAGE_COLLECTOR_DELAY_BEFORE_DELETION = Index.propertyBuilder("sleeper.default.gc.delay.minutes")
            .description("A file will not be deleted until this number of minutes have passed after it has been marked as ready for " +
                    "garbage collection. The reason for not deleting files immediately after they have been marked as ready for " +
                    "garbage collection is that they may still be in use by queries. This property can be overridden on a per-table " +
                    "basis.")
            .defaultValue("15")
            .propertyGroup(InstancePropertyGroup.GARBAGE_COLLECTOR).build();

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
