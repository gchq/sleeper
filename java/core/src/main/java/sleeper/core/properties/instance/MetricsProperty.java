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

/**
 * Definitions of instance properties relating to metrics.
 */
public interface MetricsProperty {
    UserDefinedInstanceProperty METRICS_NAMESPACE = Index.propertyBuilder("sleeper.cloudwatch.table.metrics.namespace")
            .description("The CloudWatch namespace to publish table metrics to.")
            .defaultValue("Sleeper")
            .validationPredicate(SleeperPropertyValueUtils::isNonNullNonEmptyString)
            .propertyGroup(InstancePropertyGroup.METRICS)
            .runCdkDeployWhenChanged(true).build();
    UserDefinedInstanceProperty METRICS_LAMBDA_CONCURRENCY_RESERVED = Index.propertyBuilder("sleeper.lambda.table.metrics.concurrency.reserved")
            .defaultProperty(DEFAULT_LAMBDA_CONCURRENCY_RESERVED)
            .description("The reserved concurrency for the table metrics lambda.\n" +
                    "See reserved concurrency overview at: https://docs.aws.amazon.com/lambda/latest/dg/configuration-concurrency.html")
            .propertyGroup(InstancePropertyGroup.METRICS)
            .runCdkDeployWhenChanged(true).build();
    UserDefinedInstanceProperty METRICS_LAMBDA_CONCURRENCY_MAXIMUM = Index.propertyBuilder("sleeper.lambda.table.metrics.concurrency.max")
            .defaultProperty(DEFAULT_LAMBDA_CONCURRENCY_MAXIMUM)
            .description("The maximum concurrency allowed for the table metrics lambda.\n" +
                    "See maximum concurrency overview at: https://aws.amazon.com/blogs/compute/introducing-maximum-concurrency-of-aws-lambda-functions-when-using-amazon-sqs-as-an-event-source/")
            .propertyGroup(InstancePropertyGroup.METRICS)
            .runCdkDeployWhenChanged(true).build();
    UserDefinedInstanceProperty METRICS_TABLE_BATCH_SIZE = Index.propertyBuilder("sleeper.run.table.metrics.batch.size")
            .description("The number of tables to calculate metrics for in a single invocation. " +
                    "This will be the batch size for a lambda as an SQS FIFO event source. This can be a maximum of 10.")
            .defaultValue("1")
            .validationPredicate(SleeperPropertyValueUtils::isPositiveIntegerLtEq10)
            .propertyGroup(InstancePropertyGroup.METRICS)
            .runCdkDeployWhenChanged(true).build();
    UserDefinedInstanceProperty METRICS_FOR_OFFLINE_TABLES = Index.propertyBuilder("sleeper.run.table.metrics.offline")
            .description("Whether to calculate table metrics for offline tables.")
            .defaultValue("false")
            .validationPredicate(SleeperPropertyValueUtils::isTrueOrFalse)
            .propertyGroup(InstancePropertyGroup.METRICS).build();
    UserDefinedInstanceProperty DASHBOARD_TIME_WINDOW_MINUTES = Index.propertyBuilder("sleeper.dashboard.time.window.minutes")
            .description("The period in minutes used in the dashboard.")
            .defaultValue("5")
            .validationPredicate(SleeperPropertyValueUtils::isPositiveInteger)
            .propertyGroup(InstancePropertyGroup.METRICS)
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
