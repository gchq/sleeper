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
import sleeper.core.properties.validation.SleeperPropertyValueUtils;

import java.util.List;

import static sleeper.core.properties.instance.TableStateProperty.DEFAULT_TABLE_STATE_LAMBDA_MEMORY;

/**
 * Properties for bulk export processing.
 */
public interface BulkExportProperty {
    UserDefinedInstanceProperty BULK_EXPORT_LAMBDA_MEMORY_IN_MB = Index
            .propertyBuilder("sleeper.bulk.export.memory.mb")
            .description("The amount of memory in MB for lambda functions that start bulk export jobs.")
            .defaultProperty(DEFAULT_TABLE_STATE_LAMBDA_MEMORY)
            .propertyGroup(InstancePropertyGroup.BULK_EXPORT)
            .runCdkDeployWhenChanged(true).build();
    UserDefinedInstanceProperty BULK_EXPORT_LAMBDA_TIMEOUT_IN_SECONDS = Index
            .propertyBuilder("sleeper.bulk.export.timeout.seconds")
            .description("The default timeout in seconds for the bulk export lambda.")
            .defaultValue("900")
            .propertyGroup(InstancePropertyGroup.BULK_EXPORT)
            .runCdkDeployWhenChanged(true).build();
    UserDefinedInstanceProperty BULK_EXPORT_QUEUE_VISIBILITY_TIMEOUT_IN_SECONDS = Index
            .propertyBuilder("sleeper.bulk.export.queue.visibility.timeout.seconds")
            .description("The visibility timeout in seconds for the bulk export queue.")
            .defaultValue("900")
            .propertyGroup(InstancePropertyGroup.BULK_EXPORT)
            .runCdkDeployWhenChanged(true).build();
    UserDefinedInstanceProperty BULK_EXPORT_TASK_CPU_ARCHITECTURE = Index
            .propertyBuilder("sleeper.bulk.export.task.cpu.architecture")
            .description("The CPU architecture to run bulk export tasks on. Valid values are X86_64 and ARM64.\n" +
                    "See Task CPU architecture at https://docs.aws.amazon.com/AmazonECS/latest/developerguide/AWS_Fargate.html")
            .defaultValue("X86_64")
            .propertyGroup(InstancePropertyGroup.BULK_EXPORT)
            .runCdkDeployWhenChanged(true).build();
    UserDefinedInstanceProperty BULK_EXPORT_TASK_ARM_CPU = Index.propertyBuilder("sleeper.bulk.export.task.arm.cpu")
            .description("The CPU for a bulk. export task using an ARM64 architecture.\n" +
                    "See https://docs.aws.amazon.com/AmazonECS/latest/developerguide/task-cpu-memory-error.html for valid options.")
            .defaultValue("1024")
            .propertyGroup(InstancePropertyGroup.BULK_EXPORT)
            .runCdkDeployWhenChanged(true).build();
    UserDefinedInstanceProperty BULK_EXPORT_TASK_ARM_MEMORY = Index
            .propertyBuilder("sleeper.bulk.export.task.arm.memory.mb")
            .description("The amount of memory in MB for a bulk export task using an ARM64 architecture.\n" +
                    "See https://docs.aws.amazon.com/AmazonECS/latest/developerguide/task-cpu-memory-error.html for valid options.")
            .defaultValue("4096")
            .propertyGroup(InstancePropertyGroup.BULK_EXPORT)
            .runCdkDeployWhenChanged(true).build();
    UserDefinedInstanceProperty BULK_EXPORT_TASK_X86_CPU = Index.propertyBuilder("sleeper.bulk.export.task.x86.cpu")
            .description("The CPU for a bulk export task using an x86_64 architecture.\n" +
                    "See https://docs.aws.amazon.com/AmazonECS/latest/developerguide/task-cpu-memory-error.html for valid options.")
            .defaultValue("1024")
            .propertyGroup(InstancePropertyGroup.BULK_EXPORT)
            .runCdkDeployWhenChanged(true).build();
    UserDefinedInstanceProperty BULK_EXPORT_TASK_X86_MEMORY = Index
            .propertyBuilder("sleeper.bulk.export.task.x86.memory.mb")
            .description("The amount of memory in MB for a bulk export task using an x86_64 architecture.\n" +
                    "See https://docs.aws.amazon.com/AmazonECS/latest/developerguide/task-cpu-memory-error.html for valid options.")
            .defaultValue("4096")
            .propertyGroup(InstancePropertyGroup.BULK_EXPORT)
            .runCdkDeployWhenChanged(true).build();
    UserDefinedInstanceProperty BULK_EXPORT_ECR_REPO = Index.propertyBuilder("sleeper.bulk.export.ecr.repo")
            .description("The name of the repository for the bulk export container. The Docker image should have been" +
                    " uploaded to an ECR repository of this name in this account.")
            .propertyGroup(InstancePropertyGroup.BULK_EXPORT)
            .runCdkDeployWhenChanged(true).build();
    UserDefinedInstanceProperty BULK_EXPORT_TASK_CREATION_PERIOD_IN_MINUTES = Index
            .propertyBuilder("sleeper.bulk.export.task.creation.period.minutes")
            .description(
                    "The rate at which a check to see if bulk export ECS tasks need to be created is made (in minutes, must be >= 1).")
            .defaultValue("1")
            .validationPredicate(SleeperPropertyValueUtils::isPositiveInteger)
            .propertyGroup(InstancePropertyGroup.BULK_EXPORT)
            .runCdkDeployWhenChanged(true).build();
    UserDefinedInstanceProperty MAXIMUM_CONCURRENT_BULK_EXPORT_TASKS = Index
            .propertyBuilder("sleeper.bulk.export.max.concurrent.tasks")
            .description("The maximum number of concurrent bulk export tasks to run.")
            .defaultValue("300")
            .propertyGroup(InstancePropertyGroup.BULK_EXPORT).build();
    UserDefinedInstanceProperty BULK_EXPORT_RESULTS_BUCKET_EXPIRY_IN_DAYS = Index
            .propertyBuilder("sleeper.bulk.export.results.bucket.expiry.days")
            .description("The number of days the results of bulk export remain in the bulk" +
                    " export results bucket before being deleted.")
            .defaultValue("7")
            .validationPredicate(SleeperPropertyValueUtils::isPositiveInteger)
            .propertyGroup(InstancePropertyGroup.BULK_EXPORT)
            .runCdkDeployWhenChanged(true).build();
    UserDefinedInstanceProperty BULK_EXPORT_JOB_FAILED_VISIBILITY_TIMEOUT_IN_SECONDS = Index
            .propertyBuilder("sleeper.bulk.export.job.failed.visibility.timeout.seconds")
            .description("The delay in seconds until a failed bulk export job becomes visible on the bulk export " +
                    "queue and can be processed again.")
            .defaultValue("60")
            .propertyGroup(InstancePropertyGroup.BULK_EXPORT).build();
    UserDefinedInstanceProperty BULK_EXPORT_TASK_WAIT_TIME_IN_SECONDS = Index
            .propertyBuilder("sleeper.bulk.export.task.wait.time.seconds")
            .description("The time in seconds for a bulk export task to wait for a bulk export job to appear on the " +
                    "SQS queue (must be <= 20).\n" +
                    "When a bulk export task waits for bulk export jobs to appear on the SQS queue, if the task " +
                    "receives no messages in the time defined by this property, it will try to wait for a message " +
                    "again.")
            .defaultValue("20")
            .validationPredicate(val -> SleeperPropertyValueUtils.isNonNegativeIntLtEqValue(val, 20))
            .propertyGroup(InstancePropertyGroup.BULK_EXPORT).build();
    UserDefinedInstanceProperty BULK_EXPORT_KEEP_ALIVE_PERIOD_IN_SECONDS = Index
            .propertyBuilder("sleeper.bulk.export.keepalive.period.seconds")
            .description("The frequency, in seconds, with which change message visibility requests are sent to " +
                    "extend the visibility of messages on the bulk export job queue so that they are not processed by " +
                    "other processes.\n" +
                    "This should be less than the value of sleeper.bulk.export.queue.visibility.timeout.seconds.")
            .defaultValue("300")
            .propertyGroup(InstancePropertyGroup.BULK_EXPORT).build();

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
