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

import static sleeper.core.properties.instance.TableStateProperty.DEFAULT_TABLE_STATE_LAMBDA_MEMORY;

/**
 * Definitions of instance properties relating to queries.
 */
public interface QueryProperty {
    UserDefinedInstanceProperty MAXIMUM_CONNECTIONS_TO_S3_FOR_QUERIES = Index.propertyBuilder("sleeper.query.s3.max-connections")
            .description("The maximum number of simultaneous connections to S3 from a single query runner. This is separated " +
                    "from the main one as it's common for a query runner to need to open more files at once.")
            .defaultValue("1024")
            .validationPredicate(SleeperPropertyValueUtils::isPositiveInteger)
            .propertyGroup(InstancePropertyGroup.QUERY).build();
    UserDefinedInstanceProperty QUERY_PROCESSOR_LAMBDA_MEMORY_IN_MB = Index.propertyBuilder("sleeper.query.processor.memory.mb")
            .description("The amount of memory in MB for the lambda that executes queries.")
            .defaultProperty(DEFAULT_TABLE_STATE_LAMBDA_MEMORY)
            .propertyGroup(InstancePropertyGroup.QUERY)
            .runCdkDeployWhenChanged(true).build();
    UserDefinedInstanceProperty QUERY_PROCESSOR_LAMBDA_TIMEOUT_IN_SECONDS = Index.propertyBuilder("sleeper.query.processor.timeout.seconds")
            .description("The timeout for the lambda that executes queries in seconds.")
            .defaultValue("900")
            .validationPredicate(SleeperPropertyValueUtils::isValidLambdaTimeout)
            .propertyGroup(InstancePropertyGroup.QUERY)
            .runCdkDeployWhenChanged(true).build();
    UserDefinedInstanceProperty QUERY_PROCESSING_LAMBDA_STATE_REFRESHING_PERIOD_IN_SECONDS = Index.propertyBuilder("sleeper.query.processor.state.refresh.period.seconds")
            .description("The frequency with which the query processing lambda refreshes its knowledge of the system state " +
                    "(i.e. the partitions and the mapping from partition to files), in seconds.")
            .defaultValue("60")
            .propertyGroup(InstancePropertyGroup.QUERY).build();
    UserDefinedInstanceProperty QUERY_PROCESSING_LAMBDA_RESULTS_BATCH_SIZE = Index.propertyBuilder("sleeper.query.processor.results.batch.size")
            .description("The maximum number of rows to include in a batch of query results send to " +
                    "the results queue from the query processing lambda.")
            .defaultValue("2000")
            .propertyGroup(InstancePropertyGroup.QUERY).build();
    UserDefinedInstanceProperty QUERY_PROCESSOR_LAMBDA_ROW_RETRIEVAL_THREADS = Index.propertyBuilder("sleeper.query.processor.row.retrieval.threads")
            .description("The size of the thread pool for retrieving rows in a query processing lambda.")
            .defaultValue("10")
            .validationPredicate(SleeperPropertyValueUtils::isPositiveInteger)
            .propertyGroup(InstancePropertyGroup.QUERY).build();
    UserDefinedInstanceProperty DEFAULT_QUERY_PROCESSOR_CACHE_TIMEOUT = Index.propertyBuilder("sleeper.default.table.query.processor.cache.timeout.seconds")
            .description("The default amount of time in seconds the query executor's cache of partition and " +
                    "file reference information is valid for. After this it will time out and need refreshing.")
            .defaultValue("60")
            .validationPredicate(SleeperPropertyValueUtils::isPositiveInteger)
            .propertyGroup(InstancePropertyGroup.QUERY).build();
    UserDefinedInstanceProperty QUERY_TRACKER_ITEM_TTL_IN_DAYS = Index.propertyBuilder("sleeper.query.tracker.ttl.days")
            .description("This value is used to set the time-to-live on the tracking of the queries in the DynamoDB-based query tracker.")
            .defaultValue("1")
            .validationPredicate(SleeperPropertyValueUtils::isPositiveInteger)
            .propertyGroup(InstancePropertyGroup.QUERY).build();
    UserDefinedInstanceProperty QUERY_RESULTS_BUCKET_EXPIRY_IN_DAYS = Index.propertyBuilder("sleeper.query.results.bucket.expiry.days")
            .description("The length of time the results of queries remain in the query results bucket before being deleted.")
            .defaultValue("7")
            .validationPredicate(SleeperPropertyValueUtils::isPositiveInteger)
            .propertyGroup(InstancePropertyGroup.QUERY)
            .runCdkDeployWhenChanged(true).build();
    UserDefinedInstanceProperty QUERY_RESULTS_QUEUE_VISIBILITY_TIMEOUT_IN_SECONDS = Index.propertyBuilder("sleeper.query.results.queue.visibility.timeout.seconds")
            .description("The visibility timeout in seconds of the query results queue.")
            .defaultValue("900")
            .validationPredicate(SleeperPropertyValueUtils::isPositiveInteger)
            .propertyGroup(InstancePropertyGroup.QUERY)
            .runCdkDeployWhenChanged(true)
            .build();
    UserDefinedInstanceProperty DEFAULT_RESULTS_ROW_GROUP_SIZE = Index.propertyBuilder("sleeper.default.query.results.rowgroup.size")
            .description("The default value of the rowgroup size used when the results of queries are written to Parquet files. The " +
                    "value given below is 8MiB. This value can be overridden using the query config.")
            .defaultValue("" + (8 * 1024 * 1024)) // 8 MiB
            .propertyGroup(InstancePropertyGroup.QUERY).build();
    UserDefinedInstanceProperty DEFAULT_RESULTS_PAGE_SIZE = Index.propertyBuilder("sleeper.default.query.results.page.size")
            .description("The default value of the page size used when the results of queries are written to Parquet files. The " +
                    "value given below is 128KiB. This value can be overridden using the query config.")
            .defaultValue("" + (128 * 1024)) // 128 KiB
            .propertyGroup(InstancePropertyGroup.QUERY).build();
    UserDefinedInstanceProperty QUERY_WARM_LAMBDA_EXECUTION_PERIOD_IN_MINUTES = Index
            .propertyBuilder("sleeper.query.warm.lambda.period.minutes")
            .description("The rate at which the query lambda runs to keep it warm (in minutes, must be >=1). " +
                    " This only applies when the KeepLambdaWarmStack is enabled")
            .defaultValue("5")
            .validationPredicate(SleeperPropertyValueUtils::isPositiveInteger)
            .propertyGroup(InstancePropertyGroup.QUERY)
            .runCdkDeployWhenChanged(true).build();

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
