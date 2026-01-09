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
import sleeper.core.properties.model.CompactionECSLaunchType;
import sleeper.core.properties.model.SleeperPropertyValueUtils;

import java.util.List;

import static sleeper.core.properties.instance.CommonProperty.DEFAULT_LAMBDA_CONCURRENCY_MAXIMUM;
import static sleeper.core.properties.instance.CommonProperty.DEFAULT_LAMBDA_CONCURRENCY_RESERVED;
import static sleeper.core.properties.instance.TableStateProperty.DEFAULT_TABLE_STATE_LAMBDA_MEMORY;

/**
 * Definitions of instance properties relating to compaction.
 */
public interface CompactionProperty {
    UserDefinedInstanceProperty COMPACTION_JOB_CREATION_BATCH_SIZE = Index.propertyBuilder("sleeper.compaction.job.creation.batch.size")
            .description("The number of tables to perform compaction job creation for in a single invocation. " +
                    "This will be the batch size for a lambda as an SQS FIFO event source. This can be a maximum of 10.")
            .defaultValue("1")
            .validationPredicate(SleeperPropertyValueUtils::isPositiveIntegerLtEq10)
            .propertyGroup(InstancePropertyGroup.COMPACTION)
            .runCdkDeployWhenChanged(true).build();
    UserDefinedInstanceProperty COMPACTION_COMMIT_BATCH_SIZE = Index.propertyBuilder("sleeper.compaction.job.commit.batch.size")
            .description("The number of finished compaction commits to gather in the batcher before committing to " +
                    "the state store. This will be the batch size for a lambda as an SQS event source.\n" +
                    "This can be a maximum of 10,000. In practice the effective maximum is limited by the number of " +
                    "messages that fit in a synchronous lambda invocation payload, see the AWS documentation:\n" +
                    "https://docs.aws.amazon.com/lambda/latest/dg/gettingstarted-limits.html")
            .defaultValue("1000")
            .validationPredicate(value -> SleeperPropertyValueUtils.isPositiveIntLtEqValue(value, 10_000))
            .propertyGroup(InstancePropertyGroup.COMPACTION)
            .runCdkDeployWhenChanged(true).build();
    UserDefinedInstanceProperty COMPACTION_COMMIT_BATCHING_WINDOW_IN_SECONDS = Index.propertyBuilder("sleeper.compaction.job.commit.batching.window.seconds")
            .description("The time in seconds that the batcher will wait for compaction commits to appear if the " +
                    "batch size is not filled. This will be set in the SQS event source for the lambda. This can be " +
                    "a maximum of 300, i.e. 5 minutes.")
            .defaultValue("30")
            .validationPredicate(value -> SleeperPropertyValueUtils.isPositiveIntLtEqValue(value, 300))
            .propertyGroup(InstancePropertyGroup.COMPACTION)
            .runCdkDeployWhenChanged(true).build();
    UserDefinedInstanceProperty COMPACTION_QUEUE_VISIBILITY_TIMEOUT_IN_SECONDS = Index.propertyBuilder("sleeper.compaction.queue.visibility.timeout.seconds")
            .description("The visibility timeout for the queue of compaction jobs.")
            .defaultValue("900")
            .propertyGroup(InstancePropertyGroup.COMPACTION)
            .runCdkDeployWhenChanged(true).build();
    UserDefinedInstanceProperty PENDING_COMPACTION_QUEUE_VISIBILITY_TIMEOUT_IN_SECONDS = Index.propertyBuilder("sleeper.compaction.pending.queue.visibility.timeout.seconds")
            .description("The visibility timeout for the queue of pending compaction job batches.")
            .defaultValue("900")
            .propertyGroup(InstancePropertyGroup.COMPACTION)
            .runCdkDeployWhenChanged(true).build();
    UserDefinedInstanceProperty COMPACTION_KEEP_ALIVE_PERIOD_IN_SECONDS = Index.propertyBuilder("sleeper.compaction.keepalive.period.seconds")
            .description("The frequency, in seconds, with which change message visibility requests are sent to " +
                    "extend the visibility of messages on the compaction job queue so that they are not processed by " +
                    "other processes.\n" +
                    "This should be less than the value of sleeper.compaction.queue.visibility.timeout.seconds.")
            .defaultValue("300")
            .propertyGroup(InstancePropertyGroup.COMPACTION).build();
    UserDefinedInstanceProperty COMPACTION_JOB_FAILED_VISIBILITY_TIMEOUT_IN_SECONDS = Index.propertyBuilder("sleeper.compaction.job.failed.visibility.timeout.seconds")
            .description("The delay in seconds until a failed compaction job becomes visible on the compaction job " +
                    "queue and can be processed again.")
            .defaultValue("60")
            .propertyGroup(InstancePropertyGroup.COMPACTION).build();
    UserDefinedInstanceProperty COMPACTION_TASK_WAIT_TIME_IN_SECONDS = Index.propertyBuilder("sleeper.compaction.task.wait.time.seconds")
            .description("The time in seconds for a compaction task to wait for a compaction job to appear on the " +
                    "SQS queue (must be <= 20).\n" +
                    "When a compaction task waits for compaction jobs to appear on the SQS queue, if the task " +
                    "receives no messages in the time defined by this property, it will try to wait for a message " +
                    "again.")
            .defaultValue("20")
            .validationPredicate(val -> SleeperPropertyValueUtils.isNonNegativeIntLtEqValue(val, 20))
            .propertyGroup(InstancePropertyGroup.COMPACTION).build();
    UserDefinedInstanceProperty COMPACTION_TASK_WAIT_FOR_INPUT_FILE_ASSIGNMENT = Index.propertyBuilder("sleeper.compaction.task.wait.for.input.file.assignment")
            .description("Set to true if compaction tasks should wait for input files to be assigned to a compaction " +
                    "job before starting it. The compaction task will poll the state store for whether the input " +
                    "files have been assigned to the job, and will only start once this has occurred.\n" +
                    "This prevents invalid compaction jobs from being run, particularly in the case where the " +
                    "compaction job creator runs again before the input files are assigned.\n" +
                    "This also causes compaction tasks to wait idle while input files are assigned, and puts extra " +
                    "load on the state store when there are many compaction tasks.\n" +
                    "If this is false, any created job will be executed, and will only be validated when committed " +
                    "to the state store.")
            .defaultValue("false")
            .validationPredicate(SleeperPropertyValueUtils::isTrueOrFalse)
            .propertyGroup(InstancePropertyGroup.COMPACTION).build();
    UserDefinedInstanceProperty COMPACTION_TASK_DELAY_BEFORE_RETRY_IN_SECONDS = Index.propertyBuilder("sleeper.compaction.task.delay.before.retry.seconds")
            .description("The time in seconds for a compaction task to wait after receiving no compaction jobs " +
                    "before attempting to receive a message again.\n" +
                    "When a compaction task waits for compaction jobs to appear on the SQS queue, if the task " +
                    "receives no messages in the time defined by the property " +
                    "\"sleeper.compaction.task.wait.time.seconds\", it will wait for a number of seconds defined by " +
                    "this property, then try to receive a message again.")
            .defaultValue("10")
            .validationPredicate(SleeperPropertyValueUtils::isNonNegativeInteger)
            .propertyGroup(InstancePropertyGroup.COMPACTION).build();
    UserDefinedInstanceProperty COMPACTION_TASK_MAX_IDLE_TIME_IN_SECONDS = Index.propertyBuilder("sleeper.compaction.task.max.idle.time.seconds")
            .description("The total time in seconds that a compaction task can be idle before it is terminated.\n" +
                    "When there are no compaction jobs available on the SQS queue, and SQS returns no jobs, the task " +
                    "will check whether this idle time has elapsed since the last time it finished a job. If so, the " +
                    "task will terminate.")
            .defaultValue("60")
            .validationPredicate(SleeperPropertyValueUtils::isNonNegativeInteger)
            .propertyGroup(InstancePropertyGroup.COMPACTION).build();
    UserDefinedInstanceProperty COMPACTION_TASK_MAX_CONSECUTIVE_FAILURES = Index.propertyBuilder("sleeper.compaction.task.max.consecutive.failures")
            .description("The maximum number of times that a compaction task can fail to process consecutive " +
                    "compaction jobs before it terminates.\n" +
                    "When the task starts or completes any job successfully, the count of consecutive failures is " +
                    "set to zero. Any time it fails to process a job, this count is incremented. If this maximum is " +
                    "reached, the task will terminate.")
            .defaultValue("3")
            .validationPredicate(SleeperPropertyValueUtils::isPositiveInteger)
            .propertyGroup(InstancePropertyGroup.COMPACTION).build();
    UserDefinedInstanceProperty COMPACTION_JOB_CREATION_LAMBDA_PERIOD_IN_MINUTES = Index.propertyBuilder("sleeper.compaction.job.creation.period.minutes")
            .description("The rate at which the compaction job creation lambda runs (in minutes, must be >=1).")
            .defaultValue("1")
            .validationPredicate(SleeperPropertyValueUtils::isPositiveInteger)
            .propertyGroup(InstancePropertyGroup.COMPACTION)
            .runCdkDeployWhenChanged(true).build();
    UserDefinedInstanceProperty COMPACTION_JOB_CREATION_LAMBDA_MEMORY_IN_MB = Index.propertyBuilder("sleeper.compaction.job.creation.memory.mb")
            .description("The amount of memory in MB for the lambda that creates compaction jobs.")
            .defaultProperty(DEFAULT_TABLE_STATE_LAMBDA_MEMORY)
            .propertyGroup(InstancePropertyGroup.COMPACTION)
            .runCdkDeployWhenChanged(true).build();
    UserDefinedInstanceProperty COMPACTION_JOB_CREATION_LAMBDA_TIMEOUT_IN_SECONDS = Index.propertyBuilder("sleeper.compaction.job.creation.timeout.seconds")
            .description("The timeout for the lambda that creates compaction jobs in seconds.")
            .defaultValue("900")
            .validationPredicate(SleeperPropertyValueUtils::isValidLambdaTimeout)
            .propertyGroup(InstancePropertyGroup.COMPACTION)
            .runCdkDeployWhenChanged(true).build();
    UserDefinedInstanceProperty COMPACTION_JOB_CREATION_LAMBDA_CONCURRENCY_RESERVED = Index.propertyBuilder("sleeper.compaction.job.creation.concurrency.reserved")
            .defaultProperty(DEFAULT_LAMBDA_CONCURRENCY_RESERVED)
            .description("The reserved concurrency for the lambda used to create compaction jobs.\n" +
                    "See reserved concurrency overview at: https://docs.aws.amazon.com/lambda/latest/dg/configuration-concurrency.html")
            .propertyGroup(InstancePropertyGroup.COMPACTION).build();
    UserDefinedInstanceProperty COMPACTION_JOB_CREATION_LAMBDA_CONCURRENCY_MAXIMUM = Index.propertyBuilder("sleeper.compaction.job.creation.concurrency.max")
            .defaultProperty(DEFAULT_LAMBDA_CONCURRENCY_MAXIMUM)
            .description("The maximum given concurrency allowed for the lambda used to create compaction jobs.\n" +
                    "See maximum concurrency overview at: https://aws.amazon.com/blogs/compute/introducing-maximum-concurrency-of-aws-lambda-functions-when-using-amazon-sqs-as-an-event-source/")
            .propertyGroup(InstancePropertyGroup.COMPACTION).build();
    UserDefinedInstanceProperty COMPACTION_JOB_DISPATCH_LAMBDA_MEMORY_IN_MB = Index.propertyBuilder("sleeper.compaction.job.dispatch.memory.mb")
            .description("The amount of memory in MB for the lambda that sends batches of compaction jobs.")
            .defaultProperty(DEFAULT_TABLE_STATE_LAMBDA_MEMORY)
            .propertyGroup(InstancePropertyGroup.COMPACTION)
            .runCdkDeployWhenChanged(true).build();
    UserDefinedInstanceProperty COMPACTION_JOB_DISPATCH_LAMBDA_TIMEOUT_IN_SECONDS = Index.propertyBuilder("sleeper.compaction.job.dispatch.timeout.seconds")
            .description("The timeout for the lambda that sends batches of compaction jobs in seconds.")
            .defaultValue("900")
            .validationPredicate(SleeperPropertyValueUtils::isValidLambdaTimeout)
            .propertyGroup(InstancePropertyGroup.COMPACTION)
            .runCdkDeployWhenChanged(true).build();
    UserDefinedInstanceProperty COMPACTION_JOB_DISPATCH_LAMBDA_CONCURRENCY_RESERVED = Index.propertyBuilder("sleeper.compaction.job.dispatch.concurrency.reserved")
            .defaultProperty(DEFAULT_LAMBDA_CONCURRENCY_RESERVED)
            .description("The reserved concurrency for the lambda that sends batches of compaction jobs.\n" +
                    "See reserved concurrency overview at: https://docs.aws.amazon.com/lambda/latest/dg/configuration-concurrency.html")
            .propertyGroup(InstancePropertyGroup.COMPACTION).build();
    UserDefinedInstanceProperty COMPACTION_JOB_DISPATCH_LAMBDA_CONCURRENCY_MAXIMUM = Index.propertyBuilder("sleeper.compaction.job.dispatch.concurrency.max")
            .defaultProperty(DEFAULT_LAMBDA_CONCURRENCY_MAXIMUM)
            .description("The maximum concurrency allowed for the lambda that sends batches of compaction jobs.\n" +
                    "See maximum concurrency overview at: https://aws.amazon.com/blogs/compute/introducing-maximum-concurrency-of-aws-lambda-functions-when-using-amazon-sqs-as-an-event-source/")
            .propertyGroup(InstancePropertyGroup.COMPACTION).build();
    UserDefinedInstanceProperty COMPACTION_COMMIT_BATCHER_LAMBDA_MEMORY_IN_MB = Index.propertyBuilder("sleeper.compaction.commit.batcher.memory.mb")
            .description("The amount of memory in MB for the lambda that batches up compaction commits.")
            .defaultProperty(DEFAULT_TABLE_STATE_LAMBDA_MEMORY)
            .propertyGroup(InstancePropertyGroup.COMPACTION)
            .runCdkDeployWhenChanged(true).build();
    UserDefinedInstanceProperty COMPACTION_COMMIT_BATCHER_LAMBDA_TIMEOUT_IN_SECONDS = Index.propertyBuilder("sleeper.compaction.commit.batcher.timeout.seconds")
            .description("The timeout for the lambda that batches up compaction commits in seconds.")
            .defaultValue("900")
            .validationPredicate(SleeperPropertyValueUtils::isValidLambdaTimeout)
            .propertyGroup(InstancePropertyGroup.COMPACTION)
            .runCdkDeployWhenChanged(true).build();
    UserDefinedInstanceProperty COMPACTION_COMMIT_BATCHER_LAMBDA_CONCURRENCY_RESERVED = Index.propertyBuilder("sleeper.compaction.commit.batcher.concurrency.reserved")
            .description("The reserved concurrency for the lambda that batches up compaction commits.\n" +
                    "See reserved concurrency overview at: https://docs.aws.amazon.com/lambda/latest/dg/configuration-concurrency.html")
            .defaultValue("2")
            .validationPredicate(SleeperPropertyValueUtils::isValidSqsLambdaMaximumConcurrency)
            .propertyGroup(InstancePropertyGroup.COMPACTION).build();
    UserDefinedInstanceProperty COMPACTION_COMMIT_BATCHER_LAMBDA_CONCURRENCY_MAXIMUM = Index.propertyBuilder("sleeper.compaction.commit.batcher.concurrency.max")
            .defaultValue("2")
            .description("The maximum concurrency allowed for the lambda that batches up compaction commits.\n" +
                    "See maximum concurrency overview at: https://aws.amazon.com/blogs/compute/introducing-maximum-concurrency-of-aws-lambda-functions-when-using-amazon-sqs-as-an-event-source/")
            .validationPredicate(SleeperPropertyValueUtils::isValidSqsLambdaMaximumConcurrency)
            .propertyGroup(InstancePropertyGroup.COMPACTION).build();
    UserDefinedInstanceProperty MAXIMUM_CONCURRENT_COMPACTION_TASKS = Index.propertyBuilder("sleeper.compaction.max.concurrent.tasks")
            .description("The maximum number of concurrent compaction tasks to run.")
            .defaultValue("300")
            .propertyGroup(InstancePropertyGroup.COMPACTION).build();
    UserDefinedInstanceProperty COMPACTION_TASK_CREATION_PERIOD_IN_MINUTES = Index.propertyBuilder("sleeper.compaction.task.creation.period.minutes")
            .description("The rate at which a check to see if compaction ECS tasks need to be created is made (in minutes, must be >= 1).")
            .defaultValue("1")
            .validationPredicate(SleeperPropertyValueUtils::isPositiveInteger)
            .propertyGroup(InstancePropertyGroup.COMPACTION)
            .runCdkDeployWhenChanged(true).build();
    UserDefinedInstanceProperty COMPACTION_JOB_MAX_RETRIES = Index.propertyBuilder("sleeper.compaction.job.max.retries")
            .description("The maximum number of times that a compaction job can be taken off the job definition queue " +
                    "before it is moved to the dead letter queue.\n" +
                    "This property is used to configure the maxReceiveCount of the compaction job definition queue.")
            .defaultValue("3")
            .validationPredicate(SleeperPropertyValueUtils::isPositiveInteger)
            .propertyGroup(InstancePropertyGroup.COMPACTION).build();
    UserDefinedInstanceProperty COMPACTION_DISPATCH_MAX_RETRIES = Index.propertyBuilder("sleeper.compaction.job.dispatch.max.retries")
            .description("The maximum number of times that a batch of compaction jobs can be taken off the pending queue " +
                    "before it is moved to the dead letter queue.\n" +
                    "This property is used to configure the maxReceiveCount of the pending compaction job batch queue.")
            .defaultValue("3")
            .validationPredicate(SleeperPropertyValueUtils::isPositiveInteger)
            .propertyGroup(InstancePropertyGroup.COMPACTION).build();
    UserDefinedInstanceProperty COMPACTION_COMMIT_MAX_RETRIES = Index.propertyBuilder("sleeper.compaction.job.commit.max.retries")
            .description("The maximum number of times that a compaction job can be taken off the batch committer queue " +
                    "before it is moved to the dead letter queue.\n" +
                    "This property is used to configure the maxReceiveCount of the compaction job committer queue.")
            .defaultValue("3")
            .validationPredicate(SleeperPropertyValueUtils::isPositiveInteger)
            .propertyGroup(InstancePropertyGroup.COMPACTION).build();
    UserDefinedInstanceProperty COMPACTION_TASK_CPU_ARCHITECTURE = Index.propertyBuilder("sleeper.compaction.task.cpu.architecture")
            .description("The CPU architecture to run compaction tasks on. Valid values are X86_64 and ARM64.\n" +
                    "See Task CPU architecture at https://docs.aws.amazon.com/AmazonECS/latest/developerguide/AWS_Fargate.html\n" +
                    "When `sleeper.compaction.ecs.launch.type` is set to EC2, this must match the architecture of the " +
                    "EC2 type set in `sleeper.compaction.ec2.type`.")
            .defaultValue("ARM64")
            .propertyGroup(InstancePropertyGroup.COMPACTION)
            .runCdkDeployWhenChanged(true).build();
    UserDefinedInstanceProperty COMPACTION_TASK_ARM_CPU = Index.propertyBuilder("sleeper.compaction.task.arm.cpu")
            .description("The CPU for a compaction task using an ARM64 architecture.\n" +
                    "See https://docs.aws.amazon.com/AmazonECS/latest/developerguide/task-cpu-memory-error.html for valid options.")
            .defaultValue("4096")
            .propertyGroup(InstancePropertyGroup.COMPACTION)
            .runCdkDeployWhenChanged(true).build();
    UserDefinedInstanceProperty COMPACTION_TASK_ARM_MEMORY = Index.propertyBuilder("sleeper.compaction.task.arm.memory.mb")
            .description("The amount of memory in MB for a compaction task using an ARM64 architecture.\n" +
                    "See https://docs.aws.amazon.com/AmazonECS/latest/developerguide/task-cpu-memory-error.html for valid options.")
            .defaultValue("8192")
            .propertyGroup(InstancePropertyGroup.COMPACTION)
            .runCdkDeployWhenChanged(true).build();
    UserDefinedInstanceProperty COMPACTION_TASK_X86_CPU = Index.propertyBuilder("sleeper.compaction.task.x86.cpu")
            .description("The CPU for a compaction task using an x86_64 architecture.\n" +
                    "See https://docs.aws.amazon.com/AmazonECS/latest/developerguide/task-cpu-memory-error.html for valid options.")
            .defaultValue("4096")
            .propertyGroup(InstancePropertyGroup.COMPACTION)
            .runCdkDeployWhenChanged(true).build();
    UserDefinedInstanceProperty COMPACTION_TASK_X86_MEMORY = Index.propertyBuilder("sleeper.compaction.task.x86.memory.mb")
            .description("The amount of memory in MB for a compaction task using an x86_64 architecture.\n" +
                    "See https://docs.aws.amazon.com/AmazonECS/latest/developerguide/task-cpu-memory-error.html for valid options.")
            .defaultValue("8192")
            .propertyGroup(InstancePropertyGroup.COMPACTION)
            .runCdkDeployWhenChanged(true).build();
    UserDefinedInstanceProperty COMPACTION_TASK_FIXED_OVERHEAD = Index.propertyBuilder("sleeper.compaction.task.scaling.overhead.fixed")
            .description("Used when scaling EC2 instances to support an expected number of compaction tasks. " +
                    "This is the amount of memory in MB that we expect ECS to reserve on an EC2 instance before " +
                    "making memory available for compaction tasks.\n" +
                    "If this is unset, it will be computed as a percentage of the memory on the EC2 instead, see " +
                    "`sleeper.compaction.task.scaling.overhead.percentage`.")
            .validationPredicate(SleeperPropertyValueUtils::isNonNegativeIntegerOrNull)
            .propertyGroup(InstancePropertyGroup.COMPACTION)
            .runCdkDeployWhenChanged(true).build();
    UserDefinedInstanceProperty COMPACTION_TASK_PERCENTAGE_OVERHEAD = Index.propertyBuilder("sleeper.compaction.task.scaling.overhead.percentage")
            .description("Used when scaling EC2 instances to support an expected number of compaction tasks. " +
                    "This is the percentage of memory in an EC2 instance that we expect ECS to reserve before " +
                    "making memory available for compaction tasks.\n" +
                    "Defaults to 10%, so we expect 90% of the memory on an EC2 instance to be used for compaction " +
                    "tasks.")
            .defaultValue("10")
            .validationPredicate(val -> SleeperPropertyValueUtils.isNonNegativeIntLtEqValue(val, 99))
            .propertyGroup(InstancePropertyGroup.COMPACTION)
            .runCdkDeployWhenChanged(true).build();
    UserDefinedInstanceProperty COMPACTION_ECS_LAUNCHTYPE = Index.propertyBuilder("sleeper.compaction.ecs.launch.type")
            .description("What launch type should compaction containers use? Valid options: FARGATE, EC2.")
            .defaultValue("FARGATE")
            .validationPredicate(CompactionECSLaunchType::isValid)
            .propertyGroup(InstancePropertyGroup.COMPACTION)
            .build();
    UserDefinedInstanceProperty COMPACTION_EC2_TYPE = Index.propertyBuilder("sleeper.compaction.ec2.type")
            .description("The EC2 instance type to use for compaction tasks (when using EC2-based compactions). " +
                    "Note that the architecture configured in `sleeper.compaction.task.cpu.architecture` must match.")
            .defaultValue("t4g.xlarge")
            .validationPredicate(SleeperPropertyValueUtils::isNonNullNonEmptyString)
            .propertyGroup(InstancePropertyGroup.COMPACTION)
            .build();
    UserDefinedInstanceProperty COMPACTION_EC2_POOL_MINIMUM = Index.propertyBuilder("sleeper.compaction.ec2.pool.minimum")
            .description("The minimum number of instances for the EC2 cluster (when using EC2-based compactions).")
            .defaultValue("0")
            .validationPredicate(SleeperPropertyValueUtils::isNonNegativeInteger)
            .propertyGroup(InstancePropertyGroup.COMPACTION)
            .build();
    UserDefinedInstanceProperty COMPACTION_EC2_POOL_MAXIMUM = Index.propertyBuilder("sleeper.compaction.ec2.pool.maximum")
            .description("The maximum number of instances for the EC2 cluster (when using EC2-based compactions).")
            .defaultValue("75")
            .validationPredicate(SleeperPropertyValueUtils::isNonNegativeInteger)
            .propertyGroup(InstancePropertyGroup.COMPACTION)
            .build();
    UserDefinedInstanceProperty COMPACTION_EC2_ROOT_SIZE = Index.propertyBuilder("sleeper.compaction.ec2.root.size")
            .description("The size in GiB of the root EBS volume attached to the EC2 instances (when using EC2-based compactions).")
            .defaultValue("50")
            .validationPredicate(SleeperPropertyValueUtils::isPositiveInteger)
            .propertyGroup(InstancePropertyGroup.COMPACTION)
            .build();
    UserDefinedInstanceProperty COMPACTION_TRACKER_ENABLED = Index.propertyBuilder("sleeper.compaction.tracker.enabled")
            .description("Flag to enable/disable storage of tracking information for compaction jobs and tasks.")
            .defaultValue("true")
            .propertyGroup(InstancePropertyGroup.COMPACTION)
            .validationPredicate(SleeperPropertyValueUtils::isTrueOrFalse)
            .runCdkDeployWhenChanged(true).build();
    UserDefinedInstanceProperty COMPACTION_TRACKER_ASYNC_COMMIT_UPDATES_ENABLED = Index.propertyBuilder("sleeper.compaction.tracker.async.commit.updates.enabled")
            .description("Flag to enable/disable storing an update to the tracker during async commits of compaction " +
                    "jobs. This may be disabled if there are enough compactions that the system is unable to apply " +
                    "all the updates to the tracker. This is mainly used for testing. Reports may show compactions " +
                    "as unfinished if this update is not present in the tracker.")
            .defaultValue("true")
            .propertyGroup(InstancePropertyGroup.COMPACTION)
            .validationPredicate(SleeperPropertyValueUtils::isTrueOrFalse)
            .build();
    UserDefinedInstanceProperty COMPACTION_JOB_STATUS_TTL_IN_SECONDS = Index.propertyBuilder("sleeper.compaction.job.status.ttl")
            .description("The time to live in seconds for compaction job updates in the job tracker. Default is 1 week.\n" +
                    "The expiry time is fixed when an update is saved to the store, so changing this will only affect new data.")
            .defaultValue("604800") // Default is 1 week
            .validationPredicate(SleeperPropertyValueUtils::isPositiveInteger)
            .propertyGroup(InstancePropertyGroup.COMPACTION).build();
    UserDefinedInstanceProperty COMPACTION_TASK_STATUS_TTL_IN_SECONDS = Index.propertyBuilder("sleeper.compaction.task.status.ttl")
            .description("The time to live in seconds for compaction task updates in the job tracker. Default is 1 week.\n" +
                    "The expiry time is fixed when an update is saved to the store, so changing this will only affect new data.")
            .defaultValue("604800") // Default is 1 week
            .validationPredicate(SleeperPropertyValueUtils::isPositiveInteger)
            .propertyGroup(InstancePropertyGroup.COMPACTION).build();
    UserDefinedInstanceProperty DEFAULT_COMPACTION_STRATEGY_CLASS = Index.propertyBuilder("sleeper.default.table.compaction.strategy.class")
            .description("The name of the class that defines how compaction jobs should be created. " +
                    "This should implement sleeper.compaction.core.job.creation.strategy.CompactionStrategy. The value of this property is the " +
                    "default value which can be overridden on a per-table basis.")
            .defaultValue("sleeper.compaction.core.job.creation.strategy.impl.SizeRatioCompactionStrategy")
            .propertyGroup(InstancePropertyGroup.COMPACTION).build();
    UserDefinedInstanceProperty DEFAULT_COMPACTION_FILES_BATCH_SIZE = Index.propertyBuilder("sleeper.default.table.compaction.files.batch.size")
            .description("The maximum number of files to read in a compaction job. Note that the state store must " +
                    "support atomic updates for this many files.\n" +
                    "Also note that this many files may need to be open simultaneously. The value of " +
                    "'sleeper.fs.s3a.max-connections' must be at least the value of this plus one. The extra one is for " +
                    "the output file.\n" +
                    "This is a default value and will be used if not specified in the table properties.")
            .defaultValue("12")
            .validationPredicate(SleeperPropertyValueUtils::isPositiveInteger)
            .propertyGroup(InstancePropertyGroup.COMPACTION).build();
    UserDefinedInstanceProperty DEFAULT_COMPACTION_JOB_SEND_BATCH_SIZE = Index.propertyBuilder("sleeper.default.table.compaction.job.send.batch.size")
            .description("The number of compaction jobs to send in a single batch.\n" +
                    "When compaction jobs are created, there is no limit on how many jobs can be created at once. " +
                    "A batch is a group of compaction jobs that will have their creation updates applied at the same time. " +
                    "For each batch, we send all compaction jobs to the SQS queue, then update the state store to " +
                    "assign job IDs to the input files.\n" +
                    "This can be overridden on a per-table basis.")
            .defaultValue("1000")
            .validationPredicate(SleeperPropertyValueUtils::isPositiveInteger)
            .propertyGroup(InstancePropertyGroup.COMPACTION).build();
    UserDefinedInstanceProperty DEFAULT_COMPACTION_JOB_SEND_TIMEOUT_SECS = Index.propertyBuilder("sleeper.default.table.compaction.job.send.timeout.seconds")
            .description("The amount of time in seconds a batch of compaction jobs may be pending before it should " +
                    "not be retried. If the input files have not been successfully assigned to the jobs, and this " +
                    "much time has passed, then the batch will fail to send.\n" +
                    "Once a pending batch fails the input files will never be compacted again without other " +
                    "intervention, so it's important to ensure file assignment will be done within this time. That " +
                    "depends on the throughput of state store commits.\n" +
                    "It's also necessary to ensure file assignment will be done before the next invocation of " +
                    "compaction job creation, otherwise invalid jobs will be created for the same input files. " +
                    "The rate of these invocations is set in `sleeper.compaction.job.creation.period.minutes`.")
            .defaultValue("90")
            .validationPredicate(SleeperPropertyValueUtils::isPositiveInteger)
            .propertyGroup(InstancePropertyGroup.COMPACTION).build();
    UserDefinedInstanceProperty DEFAULT_COMPACTION_JOB_SEND_RETRY_DELAY_SECS = Index.propertyBuilder("sleeper.default.table.compaction.job.send.retry.delay.seconds")
            .description("The amount of time in seconds to wait between attempts to send a batch of compaction jobs. " +
                    "The batch will be sent if all input files have been successfully assigned to the jobs, otherwise " +
                    "the batch will be retried after a delay.")
            .defaultValue("30")
            .validationPredicate(SleeperPropertyValueUtils::isNonNegativeInteger)
            .propertyGroup(InstancePropertyGroup.COMPACTION).build();
    UserDefinedInstanceProperty DEFAULT_COMPACTION_JOB_CREATION_LIMIT = Index.propertyBuilder("sleeper.default.table.compaction.job.creation.limit")
            .description("The default limit on the number of compactation jobs that can be created within a single invocation." +
                    "Exceeding this limit, results in the selection being randomised.")
            .defaultValue("100000")
            .validationPredicate(SleeperPropertyValueUtils::isNonNegativeInteger)
            .propertyGroup(InstancePropertyGroup.COMPACTION).build();
    UserDefinedInstanceProperty DEFAULT_SIZERATIO_COMPACTION_STRATEGY_RATIO = Index.propertyBuilder("sleeper.default.table.compaction.strategy.sizeratio.ratio")
            .description("Used by the SizeRatioCompactionStrategy to decide if a group of files should be compacted.\n" +
                    "If the file sizes are s_1, ..., s_n then the files are compacted if s_1 + ... + s_{n-1} >= ratio * s_n.\n" +
                    "It can be overridden on a per-table basis.")
            .defaultValue("3")
            .validationPredicate(SleeperPropertyValueUtils::isPositiveInteger)
            .propertyGroup(InstancePropertyGroup.COMPACTION).build();
    UserDefinedInstanceProperty DEFAULT_SIZERATIO_COMPACTION_STRATEGY_MAX_CONCURRENT_JOBS_PER_PARTITION = Index
            .propertyBuilder("sleeper.default.table.compaction.strategy.sizeratio.max.concurrent.jobs.per.partition")
            .description("Used by the SizeRatioCompactionStrategy to control the maximum number of jobs that can be running " +
                    "concurrently per partition. It can be overridden on a per-table basis.")
            .defaultValue("" + Integer.MAX_VALUE)
            .validationPredicate(SleeperPropertyValueUtils::isPositiveInteger)
            .propertyGroup(InstancePropertyGroup.COMPACTION).build();

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
