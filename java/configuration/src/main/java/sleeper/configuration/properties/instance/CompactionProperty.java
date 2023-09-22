/*
 * Copyright 2022-2023 Crown Copyright
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

import java.util.Arrays;
import java.util.List;

public interface CompactionProperty {
    UserDefinedInstanceProperty ECR_COMPACTION_REPO = Index.propertyBuilder("sleeper.compaction.repo")
            .description("The name of the repository for the compaction container. The Docker image from the compaction-job-execution module " +
                    "should have been uploaded to an ECR repository of this name in this account.")
            .propertyGroup(InstancePropertyGroup.COMPACTION)
            .runCDKDeployWhenChanged(true).build();
    UserDefinedInstanceProperty COMPACTION_QUEUE_VISIBILITY_TIMEOUT_IN_SECONDS = Index.propertyBuilder("sleeper.compaction.queue.visibility.timeout.seconds")
            .description("The visibility timeout for the queue of compaction jobs.")
            .defaultValue("900")
            .propertyGroup(InstancePropertyGroup.COMPACTION)
            .runCDKDeployWhenChanged(true).build();
    UserDefinedInstanceProperty COMPACTION_KEEP_ALIVE_PERIOD_IN_SECONDS = Index.propertyBuilder("sleeper.compaction.keepalive.period.seconds")
            .description("The frequency, in seconds, with which change message visibility requests are sent to extend the " +
                    "visibility of messages on the compaction job queue so that they are not processed by other processes.\n" +
                    "This should be less than the value of sleeper.compaction.queue.visibility.timeout.seconds.")
            .defaultValue("300")
            .propertyGroup(InstancePropertyGroup.COMPACTION).build();
    UserDefinedInstanceProperty COMPACTION_JOB_CREATION_LAMBDA_PERIOD_IN_MINUTES = Index.propertyBuilder("sleeper.compaction.job.creation.period.minutes")
            .description("The rate at which the compaction job creation lambda runs (in minutes, must be >=1).")
            .defaultValue("1")
            .validationPredicate(Utils::isPositiveInteger)
            .propertyGroup(InstancePropertyGroup.COMPACTION)
            .runCDKDeployWhenChanged(true).build();
    UserDefinedInstanceProperty COMPACTION_JOB_CREATION_LAMBDA_MEMORY_IN_MB = Index.propertyBuilder("sleeper.compaction.job.creation.memory")
            .description("The amount of memory for the lambda that creates compaction jobs.")
            .defaultValue("1024")
            .propertyGroup(InstancePropertyGroup.COMPACTION)
            .runCDKDeployWhenChanged(true).build();
    UserDefinedInstanceProperty COMPACTION_JOB_CREATION_LAMBDA_TIMEOUT_IN_SECONDS = Index.propertyBuilder("sleeper.compaction.job.creation.timeout.seconds")
            .description("The timeout for the lambda that creates compaction jobs in seconds.")
            .defaultValue("900")
            .validationPredicate(Utils::isValidLambdaTimeout)
            .propertyGroup(InstancePropertyGroup.COMPACTION)
            .runCDKDeployWhenChanged(true).build();
    UserDefinedInstanceProperty MAXIMUM_CONCURRENT_COMPACTION_TASKS = Index.propertyBuilder("sleeper.compaction.max.concurrent.tasks")
            .description("The maximum number of concurrent compaction tasks to run.")
            .defaultValue("300")
            .propertyGroup(InstancePropertyGroup.COMPACTION).build();
    UserDefinedInstanceProperty COMPACTION_TASK_CREATION_PERIOD_IN_MINUTES = Index.propertyBuilder("sleeper.compaction.task.creation.period.minutes")
            .description("The rate at which a check to see if compaction ECS tasks need to be created is made (in minutes, must be >= 1).")
            .defaultValue("1")
            .validationPredicate(Utils::isPositiveInteger)
            .propertyGroup(InstancePropertyGroup.COMPACTION)
            .runCDKDeployWhenChanged(true).build();
    UserDefinedInstanceProperty COMPACTION_TASK_CPU_ARCHITECTURE = Index.propertyBuilder("sleeper.compaction.task.cpu.architecture")
            .description("The CPU architecture to run compaction tasks on. Valid values are X86_64 and ARM64.\n" +
                    "See Task CPU architecture at https://docs.aws.amazon.com/AmazonECS/latest/developerguide/AWS_Fargate.html")
            .defaultValue("X86_64")
            .propertyGroup(InstancePropertyGroup.COMPACTION)
            .runCDKDeployWhenChanged(true).build();
    UserDefinedInstanceProperty COMPACTION_TASK_ARM_CPU = Index.propertyBuilder("sleeper.compaction.task.arm.cpu")
            .description("The CPU for a compaction task using an ARM64 architecture.\n" +
                    "See https://docs.aws.amazon.com/AmazonECS/latest/developerguide/task-cpu-memory-error.html for valid options.")
            .defaultValue("1024")
            .propertyGroup(InstancePropertyGroup.COMPACTION)
            .runCDKDeployWhenChanged(true).build();
    UserDefinedInstanceProperty COMPACTION_TASK_ARM_MEMORY = Index.propertyBuilder("sleeper.compaction.task.arm.memory")
            .description("The memory for a compaction task using an ARM64 architecture.\n" +
                    "See https://docs.aws.amazon.com/AmazonECS/latest/developerguide/task-cpu-memory-error.html for valid options.")
            .defaultValue("4096")
            .propertyGroup(InstancePropertyGroup.COMPACTION)
            .runCDKDeployWhenChanged(true).build();
    UserDefinedInstanceProperty COMPACTION_TASK_X86_CPU = Index.propertyBuilder("sleeper.compaction.task.x86.cpu")
            .description("The CPU for a compaction task using an x86_64 architecture.\n" +
                    "See https://docs.aws.amazon.com/AmazonECS/latest/developerguide/task-cpu-memory-error.html for valid options.")
            .defaultValue("1024")
            .propertyGroup(InstancePropertyGroup.COMPACTION)
            .runCDKDeployWhenChanged(true).build();
    UserDefinedInstanceProperty COMPACTION_TASK_X86_MEMORY = Index.propertyBuilder("sleeper.compaction.task.x86.memory")
            .description("The memory for a compaction task using an x86_64 architecture.\n" +
                    "See https://docs.aws.amazon.com/AmazonECS/latest/developerguide/task-cpu-memory-error.html for valid options.")
            .defaultValue("4096")
            .propertyGroup(InstancePropertyGroup.COMPACTION)
            .runCDKDeployWhenChanged(true).build();
    UserDefinedInstanceProperty COMPACTION_ECS_LAUNCHTYPE = Index.propertyBuilder("sleeper.compaction.ecs.launch.type")
            .description("What launch type should compaction containers use? Valid options: FARGATE, EC2.")
            .defaultValue("FARGATE")
            .validationPredicate(Arrays.asList("EC2", "FARGATE")::contains)
            .propertyGroup(InstancePropertyGroup.COMPACTION)
            .build();
    UserDefinedInstanceProperty COMPACTION_EC2_TYPE = Index.propertyBuilder("sleeper.compaction.ec2.type")
            .description("The EC2 instance type to use for compaction tasks (when using EC2-based compactions).")
            .defaultValue("t3.xlarge")
            .validationPredicate(Utils::isNonNullNonEmptyString)
            .propertyGroup(InstancePropertyGroup.COMPACTION)
            .build();
    UserDefinedInstanceProperty COMPACTION_EC2_POOL_MINIMUM = Index.propertyBuilder("sleeper.compaction.ec2.pool.minimum")
            .description("The minimum number of instances for the EC2 cluster (when using EC2-based compactions).")
            .defaultValue("0")
            .validationPredicate(Utils::isNonNegativeInteger)
            .propertyGroup(InstancePropertyGroup.COMPACTION)
            .build();
    UserDefinedInstanceProperty COMPACTION_EC2_POOL_DESIRED = Index.propertyBuilder("sleeper.compaction.ec2.pool.desired")
            .description("The initial desired number of instances for the EC2 cluster (when using EC2-based compactions).\n" +
                    "Can be set by dividing initial maximum containers by number that should fit on instance type.")
            .defaultValue("0")
            .validationPredicate(Utils::isNonNegativeInteger)
            .propertyGroup(InstancePropertyGroup.COMPACTION)
            .build();
    UserDefinedInstanceProperty COMPACTION_EC2_POOL_MAXIMUM = Index.propertyBuilder("sleeper.compaction.ec2.pool.maximum")
            .description("The maximum number of instances for the EC2 cluster (when using EC2-based compactions).")
            .defaultValue("75")
            .validationPredicate(Utils::isNonNegativeInteger)
            .propertyGroup(InstancePropertyGroup.COMPACTION)
            .build();
    UserDefinedInstanceProperty COMPACTION_EC2_ROOT_SIZE = Index.propertyBuilder("sleeper.compaction.ec2.root.size")
            .description("The size in GiB of the root EBS volume attached to the EC2 instances (when using EC2-based compactions).")
            .defaultValue("50")
            .validationPredicate(Utils::isPositiveInteger)
            .propertyGroup(InstancePropertyGroup.COMPACTION)
            .build();
    UserDefinedInstanceProperty COMPACTION_STATUS_STORE_ENABLED = Index.propertyBuilder("sleeper.compaction.status.store.enabled")
            .description("Flag to enable/disable storage of tracking information for compaction jobs and tasks.")
            .defaultValue("true")
            .propertyGroup(InstancePropertyGroup.COMPACTION)
            .runCDKDeployWhenChanged(true).build();
    UserDefinedInstanceProperty COMPACTION_JOB_STATUS_TTL_IN_SECONDS = Index.propertyBuilder("sleeper.compaction.job.status.ttl")
            .description("The time to live in seconds for compaction job updates in the status store. Default is 1 week.\n" +
                    "The expiry time is fixed when an update is saved to the store, so changing this will only affect new data.")
            .defaultValue("604800") // Default is 1 week
            .validationPredicate(Utils::isPositiveInteger)
            .propertyGroup(InstancePropertyGroup.COMPACTION).build();
    UserDefinedInstanceProperty COMPACTION_TASK_STATUS_TTL_IN_SECONDS = Index.propertyBuilder("sleeper.compaction.task.status.ttl")
            .description("The time to live in seconds for compaction task updates in the status store. Default is 1 week.\n" +
                    "The expiry time is fixed when an update is saved to the store, so changing this will only affect new data.")
            .defaultValue("604800") // Default is 1 week
            .validationPredicate(Utils::isPositiveInteger)
            .propertyGroup(InstancePropertyGroup.COMPACTION).build();
    UserDefinedInstanceProperty DEFAULT_COMPACTION_STRATEGY_CLASS = Index.propertyBuilder("sleeper.default.compaction.strategy.class")
            .description("The name of the class that defines how compaction jobs should be created. " +
                    "This should implement sleeper.compaction.strategy.CompactionStrategy. The value of this property is the " +
                    "default value which can be overridden on a per-table basis.")
            .defaultValue("sleeper.compaction.strategy.impl.SizeRatioCompactionStrategy")
            .propertyGroup(InstancePropertyGroup.COMPACTION).build();
    UserDefinedInstanceProperty DEFAULT_COMPACTION_FILES_BATCH_SIZE = Index.propertyBuilder("sleeper.default.compaction.files.batch.size")
            .description("The minimum number of files to read in a compaction job. Note that the state store " +
                    "must support atomic updates for this many files. For the DynamoDBStateStore this " +
                    "is 11. It can be overridden on a per-table basis.\n" +
                    "(NB This does not apply to splitting jobs which will run even if there is only 1 file.)\n" +
                    "This is a default value and will be used if not specified in the table.properties file.")
            .defaultValue("11")
            .propertyGroup(InstancePropertyGroup.COMPACTION).build();
    UserDefinedInstanceProperty DEFAULT_SIZERATIO_COMPACTION_STRATEGY_RATIO = Index.propertyBuilder("sleeper.default.table.compaction.strategy.sizeratio.ratio")
            .description("Used by the SizeRatioCompactionStrategy to decide if a group of files should be compacted.\n" +
                    "If the file sizes are s_1, ..., s_n then the files are compacted if s_1 + ... + s_{n-1} >= ratio * s_n.\n" +
                    "It can be overridden on a per-table basis.")
            .defaultValue("3")
            .propertyGroup(InstancePropertyGroup.COMPACTION).build();
    UserDefinedInstanceProperty DEFAULT_SIZERATIO_COMPACTION_STRATEGY_MAX_CONCURRENT_JOBS_PER_PARTITION = Index.propertyBuilder("sleeper.default.table.compaction.strategy.sizeratio.max.concurrent.jobs.per.partition")
            .description("Used by the SizeRatioCompactionStrategy to control the maximum number of jobs that can be running " +
                    "concurrently per partition. It can be overridden on a per-table basis.")
            .defaultValue("" + Integer.MAX_VALUE)
            .propertyGroup(InstancePropertyGroup.COMPACTION).build();

    UserDefinedInstanceProperty DEFAULT_COMPACTION_METHOD = Index.propertyBuilder("sleeper.default.table.compaction.method")
            .description("Select what compation method to use on a table. Current options are JAVA and RUST. Rust compaction support is" +
                    "experimental.")
            .defaultValue("JAVA")
            .propertyGroup(InstancePropertyGroup.COMPACTION).build();

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
