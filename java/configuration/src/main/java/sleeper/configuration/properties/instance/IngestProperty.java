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

import sleeper.core.properties.SleeperPropertyIndex;
import sleeper.core.properties.instance.InstancePropertyGroup;
import sleeper.core.properties.validation.SleeperPropertyValueUtils;

import java.util.List;

/**
 * Definitions of instance properties relating to ingest.
 */
public interface IngestProperty {
    UserDefinedInstanceProperty ECR_INGEST_REPO = Index.propertyBuilder("sleeper.ingest.repo")
            .description("The name of the ECR repository for the ingest container. The Docker image from the ingest module should have been " +
                    "uploaded to an ECR repository of this name in this account.")
            .propertyGroup(InstancePropertyGroup.INGEST)
            .runCdkDeployWhenChanged(true).build();
    UserDefinedInstanceProperty MAXIMUM_CONCURRENT_INGEST_TASKS = Index.propertyBuilder("sleeper.ingest.max.concurrent.tasks")
            .description("The maximum number of concurrent ECS tasks to run.")
            .defaultValue("200")
            .propertyGroup(InstancePropertyGroup.INGEST).build();
    UserDefinedInstanceProperty INGEST_TASK_CREATION_PERIOD_IN_MINUTES = Index.propertyBuilder("sleeper.ingest.task.creation.period.minutes")
            .description("The frequency in minutes with which an EventBridge rule runs to trigger a lambda that, if necessary, runs more ECS " +
                    "tasks to perform ingest jobs.")
            .defaultValue("1")
            .validationPredicate(SleeperPropertyValueUtils::isPositiveInteger)
            .propertyGroup(InstancePropertyGroup.INGEST)
            .runCdkDeployWhenChanged(true).build();
    UserDefinedInstanceProperty INGEST_KEEP_ALIVE_PERIOD_IN_SECONDS = Index.propertyBuilder("sleeper.ingest.keepalive.period.seconds")
            .description("The frequency, in seconds, with which change message visibility requests are sent to extend the " +
                    "visibility of messages on the ingest queue so that they are not processed by other processes.\n" +
                    "This should be less than the value of sleeper.queue.visibility.timeout.seconds.")
            .defaultValue("300")
            .propertyGroup(InstancePropertyGroup.INGEST).build();
    UserDefinedInstanceProperty S3A_INPUT_FADVISE = Index.propertyBuilder("sleeper.ingest.fs.s3a.experimental.input.fadvise")
            .description("This sets the value of fs.s3a.experimental.input.fadvise on the Hadoop configuration used to read and write " +
                    "files to and from S3 in ingest jobs. Changing this value allows you to fine-tune how files are read. Possible " +
                    "values are \"normal\", \"sequential\" and \"random\". More information is available here:\n" +
                    "https://hadoop.apache.org/docs/current/hadoop-aws/tools/hadoop-aws/performance.html#fadvise.")
            .defaultValue("sequential")
            .validationPredicate(SleeperPropertyValueUtils::isValidFadvise)
            .propertyGroup(InstancePropertyGroup.INGEST).build();
    UserDefinedInstanceProperty INGEST_TASK_CPU = Index.propertyBuilder("sleeper.ingest.task.cpu")
            .description("The amount of CPU used by Fargate tasks that perform ingest jobs.\n" +
                    "Note that only certain combinations of CPU and memory are valid.\n" +
                    "See https://docs.aws.amazon.com/AmazonECS/latest/developerguide/task-cpu-memory-error.html for valid options.")
            .defaultValue("2048")
            .propertyGroup(InstancePropertyGroup.INGEST)
            .runCdkDeployWhenChanged(true).build();
    UserDefinedInstanceProperty INGEST_TASK_MEMORY = Index.propertyBuilder("sleeper.ingest.task.memory")
            .description("The amount of memory in MB used by Fargate tasks that perform ingest jobs.\n" +
                    "Note that only certain combinations of CPU and memory are valid.\n" +
                    "See https://docs.aws.amazon.com/AmazonECS/latest/developerguide/task-cpu-memory-error.html for valid options.")
            .defaultValue("4096")
            .propertyGroup(InstancePropertyGroup.INGEST)
            .runCdkDeployWhenChanged(true).build();
    UserDefinedInstanceProperty INGEST_PARTITION_REFRESH_PERIOD_IN_SECONDS = Index.propertyBuilder("sleeper.ingest.partition.refresh.period")
            .description("The frequency in seconds with which ingest tasks refresh their view of the partitions.\n" +
                    "(NB Refreshes only happen once a batch of data has been written so this is a lower bound " +
                    "on the refresh frequency.)")
            .defaultValue("120")
            .propertyGroup(InstancePropertyGroup.INGEST).build();
    UserDefinedInstanceProperty INGEST_SOURCE_BUCKET = Index.propertyBuilder("sleeper.ingest.source.bucket")
            .description("A comma-separated list of buckets that contain files to be ingested via ingest jobs. The buckets should already " +
                    "exist, i.e. they will not be created as part of the cdk deployment of this instance of Sleeper. The ingest " +
                    "and bulk import stacks will be given read access to these buckets so that they can consume data from them.")
            .propertyGroup(InstancePropertyGroup.INGEST)
            .runCdkDeployWhenChanged(true).build();

    UserDefinedInstanceProperty INGEST_STATUS_STORE_ENABLED = IngestProperty.Index.propertyBuilder("sleeper.ingest.status.store.enabled")
            .description("Flag to enable/disable storage of tracking information for ingest jobs and tasks.")
            .defaultValue("true")
            .propertyGroup(InstancePropertyGroup.INGEST)
            .runCdkDeployWhenChanged(true).build();
    UserDefinedInstanceProperty INGEST_JOB_STATUS_TTL_IN_SECONDS = IngestProperty.Index.propertyBuilder("sleeper.ingest.job.status.ttl")
            .description("The time to live in seconds for ingest job updates in the status store. Default is 1 week.\n" +
                    "The expiry time is fixed when an update is saved to the store, so changing this will only affect new data.")
            .defaultValue("604800") // Default is 1 week
            .validationPredicate(SleeperPropertyValueUtils::isPositiveInteger)
            .propertyGroup(InstancePropertyGroup.INGEST).build();
    UserDefinedInstanceProperty INGEST_TASK_STATUS_TTL_IN_SECONDS = IngestProperty.Index.propertyBuilder("sleeper.ingest.task.status.ttl")
            .description("The time to live in seconds for ingest task updates in the status store. Default is 1 week.\n" +
                    "The expiry time is fixed when an update is saved to the store, so changing this will only affect new data.")
            .defaultValue("604800") // Default is 1 week
            .validationPredicate(SleeperPropertyValueUtils::isPositiveInteger)
            .propertyGroup(InstancePropertyGroup.INGEST).build();
    UserDefinedInstanceProperty INGEST_JOB_QUEUE_WAIT_TIME = IngestProperty.Index.propertyBuilder("sleeper.ingest.job.queue.wait.time")
            .description("The time in seconds to wait for ingest jobs to appear on the queue before an ingest task terminates.\n" +
                    "Must be >= 0 and <= 20.\n" +
                    "See also https://docs.aws.amazon.com/AWSSimpleQueueService/latest/SQSDeveloperGuide/sqs-short-and-long-polling.html")
            .defaultValue("20")
            .validationPredicate(val -> SleeperPropertyValueUtils.isNonNegativeIntLtEqValue(val, 20))
            .propertyGroup(InstancePropertyGroup.INGEST).build();

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

        static UserDefinedInstancePropertyImpl.Builder propertyBuilder(String propertyName) {
            return UserDefinedInstancePropertyImpl.named(propertyName)
                    .addToIndex(INSTANCE::add);
        }
    }
}
