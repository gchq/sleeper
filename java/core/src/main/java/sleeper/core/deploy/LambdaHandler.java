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
package sleeper.core.deploy;

import sleeper.core.properties.model.LambdaDeployType;
import sleeper.core.properties.model.OptionalStack;

import java.util.ArrayList;
import java.util.Collection;
import java.util.Collections;
import java.util.HashMap;
import java.util.List;
import java.util.Map;
import java.util.Objects;

import static sleeper.core.properties.model.OptionalStack.BulkExportStack;

/**
 * Definitions of handler methods for lambda functions.
 */
public class LambdaHandler {

    private static final List<LambdaHandler> ALL = new ArrayList<>();
    private static final Map<String, LambdaHandler> ATHENA_HANDLER_BY_CLASSNAME = new HashMap<>();
    public static final LambdaHandler ATHENA_SIMPLE_COMPOSITE = builder()
            .jar(LambdaJar.ATHENA)
            .handler("sleeper.athena.composite.SimpleCompositeHandler")
            .optionalStack(OptionalStack.AthenaStack).add();
    public static final LambdaHandler ATHENA_ITERATORS_COMPOSITE = builder()
            .jar(LambdaJar.ATHENA)
            .handler("sleeper.athena.composite.IteratorApplyingCompositeHandler")
            .optionalStack(OptionalStack.AthenaStack).add();
    public static final LambdaHandler BULK_IMPORT_STARTER = builder()
            .jar(LambdaJar.BULK_IMPORT_STARTER)
            .handler("sleeper.bulkimport.starter.BulkImportStarterLambda")
            .optionalStacks(OptionalStack.BULK_IMPORT_STACKS).add();
    public static final LambdaHandler INGEST_TASK_CREATOR = builder()
            .jar(LambdaJar.INGEST_TASK_CREATOR)
            .handler("sleeper.ingest.taskrunner.RunIngestTasksLambda::eventHandler")
            .optionalStack(OptionalStack.IngestStack).add();
    public static final LambdaHandler INGEST_BATCHER_SUBMITTER = builder()
            .jar(LambdaJar.INGEST_BATCHER_SUBMITTER)
            .handler("sleeper.ingest.batcher.submitter.IngestBatcherSubmitterLambda::handleRequest")
            .optionalStack(OptionalStack.IngestBatcherStack).add();
    public static final LambdaHandler INGEST_BATCHER_JOB_CREATOR = builder()
            .jar(LambdaJar.INGEST_BATCHER_JOB_CREATOR)
            .handler("sleeper.ingest.batcher.job.creator.IngestBatcherJobCreatorLambda::eventHandler")
            .optionalStack(OptionalStack.IngestBatcherStack).add();
    public static final LambdaHandler GARBAGE_COLLECTOR_TRIGGER = builder()
            .jar(LambdaJar.GARBAGE_COLLECTOR)
            .handler("sleeper.garbagecollector.GarbageCollectorTriggerLambda::handleRequest")
            .optionalStack(OptionalStack.GarbageCollectorStack).add();
    public static final LambdaHandler GARBAGE_COLLECTOR = builder()
            .jar(LambdaJar.GARBAGE_COLLECTOR)
            .handler("sleeper.garbagecollector.GarbageCollectorLambda::handleRequest")
            .optionalStack(OptionalStack.GarbageCollectorStack).add();
    public static final LambdaHandler COMPACTION_JOB_CREATOR_TRIGGER = builder()
            .jar(LambdaJar.COMPACTION_JOB_CREATOR)
            .handler("sleeper.compaction.job.creation.lambda.CreateCompactionJobsTriggerLambda::handleRequest")
            .optionalStack(OptionalStack.CompactionStack).add();
    public static final LambdaHandler COMPACTION_JOB_CREATOR = builder()
            .jar(LambdaJar.COMPACTION_JOB_CREATOR)
            .handler("sleeper.compaction.job.creation.lambda.CreateCompactionJobsLambda::handleRequest")
            .optionalStack(OptionalStack.CompactionStack).add();
    public static final LambdaHandler COMPACTION_JOB_DISPATCHER = builder()
            .jar(LambdaJar.COMPACTION_JOB_CREATOR)
            .handler("sleeper.compaction.job.creation.lambda.CompactionJobDispatchLambda::handleRequest")
            .optionalStack(OptionalStack.CompactionStack).add();
    public static final LambdaHandler COMPACTION_TASK_CREATOR = builder()
            .jar(LambdaJar.COMPACTION_TASK_CREATOR)
            .handler("sleeper.compaction.task.creation.RunCompactionTasksLambda::eventHandler")
            .optionalStack(OptionalStack.CompactionStack).add();
    public static final LambdaHandler COMPACTION_TASK_TERMINATOR = builder()
            .jar(LambdaJar.COMPACTION_TASK_CREATOR)
            .handler("sleeper.compaction.task.creation.SafeTerminationLambda::handleRequest")
            .optionalStack(OptionalStack.CompactionStack).add();
    public static final LambdaHandler FIND_PARTITIONS_TO_SPLIT_TRIGGER = builder()
            .jar(LambdaJar.PARTITION_SPLITTER)
            .handler("sleeper.splitter.lambda.FindPartitionsToSplitTriggerLambda::handleRequest")
            .optionalStack(OptionalStack.PartitionSplittingStack).add();
    public static final LambdaHandler FIND_PARTITIONS_TO_SPLIT = builder()
            .jar(LambdaJar.PARTITION_SPLITTER)
            .handler("sleeper.splitter.lambda.FindPartitionsToSplitLambda::handleRequest")
            .optionalStack(OptionalStack.PartitionSplittingStack).add();
    public static final LambdaHandler SPLIT_PARTITION = builder()
            .jar(LambdaJar.PARTITION_SPLITTER)
            .handler("sleeper.splitter.lambda.SplitPartitionLambda::handleRequest")
            .optionalStack(OptionalStack.PartitionSplittingStack).add();
    public static final LambdaHandler KEEP_QUERY_WARM = builder()
            .jar(LambdaJar.QUERY)
            .handler("sleeper.query.lambda.WarmQueryExecutorLambda::handleRequest")
            .optionalStack(OptionalStack.KeepLambdaWarmStack).add();
    public static final LambdaHandler BULK_EXPORT_PLANNER = builder()
            .jar(LambdaJar.BULK_EXPORT_PLANNER)
            .handler("sleeper.bulkexport.planner.SqsBulkExportProcessorLambda::handleRequest")
            .optionalStack(BulkExportStack).add();
    public static final LambdaHandler BULK_EXPORT_TASK_CREATOR = builder()
            .jar(LambdaJar.BULK_EXPORT_TASK_CREATOR)
            .handler("sleeper.bulkexport.taskcreator.SqsTriggeredBulkExportTaskRunnerLambda::handleRequest")
            .optionalStack(BulkExportStack).add();
    public static final LambdaHandler QUERY_EXECUTOR = builder()
            .jar(LambdaJar.QUERY)
            .handler("sleeper.query.lambda.SqsQueryProcessorLambda::handleRequest")
            .optionalStack(OptionalStack.QueryStack).add();
    public static final LambdaHandler QUERY_LEAF_PARTITION = builder()
            .jar(LambdaJar.QUERY)
            .handler("sleeper.query.lambda.SqsLeafPartitionQueryLambda::handleRequest")
            .optionalStack(OptionalStack.QueryStack).add();
    public static final LambdaHandler WEB_SOCKET_QUERY = builder()
            .jar(LambdaJar.QUERY)
            .handler("sleeper.query.lambda.WebSocketQueryProcessorLambda::handleRequest")
            .optionalStack(OptionalStack.WebSocketQueryStack).add();
    public static final LambdaHandler AUTO_DELETE_S3_OBJECTS = builder()
            .jar(LambdaJar.CUSTOM_RESOURCES)
            .handler("sleeper.cdk.custom.AutoDeleteS3ObjectsLambda::handleEvent")
            .core().add();
    public static final LambdaHandler AUTO_STOP_ECS_CLUSTER_TASKS = builder()
            .jar(LambdaJar.CUSTOM_RESOURCES)
            .handler("sleeper.cdk.custom.AutoStopEcsClusterTasksLambda::handleEvent")
            .core().add();
    public static final LambdaHandler AUTO_STOP_EMR_SERVERLESS_APPLICATION = builder()
            .jar(LambdaJar.CUSTOM_RESOURCES)
            .handler("sleeper.cdk.custom.AutoStopEmrServerlessApplicationLambda::handleEvent")
            .core().add();
    public static final LambdaHandler INSTANCE_PROPERTIES_WRITER = builder()
            .jar(LambdaJar.CUSTOM_RESOURCES)
            .handler("sleeper.cdk.custom.InstancePropertiesWriterLambda::handleEvent")
            .core().add();
    public static final LambdaHandler TABLE_DEFINER = builder()
            .jar(LambdaJar.CUSTOM_RESOURCES)
            .handler("sleeper.cdk.custom.TableDefinerLambda::handleEvent")
            .core().add();
    public static final LambdaHandler VPC_CHECK = builder()
            .jar(LambdaJar.CUSTOM_RESOURCES)
            .handler("sleeper.cdk.custom.VpcCheckLambda::handleEvent")
            .core().add();
    public static final LambdaHandler METRICS_TRIGGER = builder()
            .jar(LambdaJar.METRICS)
            .handler("sleeper.metrics.TableMetricsTriggerLambda::handleRequest")
            .optionalStack(OptionalStack.TableMetricsStack).add();
    public static final LambdaHandler METRICS = builder()
            .jar(LambdaJar.METRICS)
            .handler("sleeper.metrics.TableMetricsLambda::handleRequest")
            .optionalStack(OptionalStack.TableMetricsStack).add();
    public static final LambdaHandler STATESTORE_COMMITTER = builder()
            .jar(LambdaJar.STATESTORE)
            .handler("sleeper.statestore.lambda.committer.StateStoreCommitterLambda::handleRequest")
            .core().add();
    public static final LambdaHandler COMPACTION_COMMIT_BATCHER = builder()
            .jar(LambdaJar.STATESTORE)
            .handler("sleeper.statestore.lambda.committer.CompactionCommitBatcherLambda::handleRequest")
            .core().add();
    public static final LambdaHandler SNAPSHOT_CREATION_TRIGGER = builder()
            .jar(LambdaJar.STATESTORE)
            .handler("sleeper.statestore.lambda.snapshot.TransactionLogSnapshotCreationTriggerLambda::handleRequest")
            .core().add();
    public static final LambdaHandler SNAPSHOT_CREATION = builder()
            .jar(LambdaJar.STATESTORE)
            .handler("sleeper.statestore.lambda.snapshot.TransactionLogSnapshotCreationLambda::handleRequest")
            .core().add();
    public static final LambdaHandler SNAPSHOT_DELETION_TRIGGER = builder()
            .jar(LambdaJar.STATESTORE)
            .handler("sleeper.statestore.lambda.snapshot.TransactionLogSnapshotDeletionTriggerLambda::handleRequest")
            .core().add();
    public static final LambdaHandler SNAPSHOT_DELETION = builder()
            .jar(LambdaJar.STATESTORE)
            .handler("sleeper.statestore.lambda.snapshot.TransactionLogSnapshotDeletionLambda::handleRequest")
            .core().add();
    public static final LambdaHandler TRANSACTION_DELETION_TRIGGER = builder()
            .jar(LambdaJar.STATESTORE)
            .handler("sleeper.statestore.lambda.transaction.TransactionLogTransactionDeletionTriggerLambda::handleRequest")
            .core().add();
    public static final LambdaHandler TRANSACTION_DELETION = builder()
            .jar(LambdaJar.STATESTORE)
            .handler("sleeper.statestore.lambda.transaction.TransactionLogTransactionDeletionLambda::handleRequest")
            .core().add();
    public static final LambdaHandler TRANSACTION_FOLLOWER = builder()
            .jar(LambdaJar.STATESTORE)
            .handler("sleeper.statestore.lambda.transaction.TransactionLogFollowerLambda::handleRequest")
            .core().add();

    private final LambdaJar jar;
    private final String handler;
    private final List<OptionalStack> optionalStacks;

    private LambdaHandler(Builder builder) {
        jar = Objects.requireNonNull(builder.jar, "jar must not be null");
        handler = Objects.requireNonNull(builder.handler, "handler must not be null");
        optionalStacks = Objects.requireNonNull(builder.optionalStacks, "optionalStacks must not be null");
    }

    /**
     * Returns all lambda handler definitions.
     *
     * @return the definitions
     */
    public static List<LambdaHandler> all() {
        return Collections.unmodifiableList(ALL);
    }

    public static Builder builder() {
        return new Builder();
    }

    public LambdaJar getJar() {
        return jar;
    }

    public String getHandler() {
        return handler;
    }

    public List<OptionalStack> getOptionalStacks() {
        return optionalStacks;
    }

    public boolean isAlwaysDockerDeploy() {
        return jar.isAlwaysDockerDeploy();
    }

    /**
     * Checks if this lambda is deployed given some configuration.
     *
     * @param  deployType the configuration for how lambdas are deployed
     * @param  stacks     the enabled optional stacks
     * @return            true if this lambda will be deployed
     */
    public boolean isDeployed(LambdaDeployType deployType, Collection<OptionalStack> stacks) {
        if (deployType == LambdaDeployType.JAR && !isAlwaysDockerDeploy()) {
            return false;
        }
        return optionalStacks.isEmpty() || optionalStacks.stream().anyMatch(stacks::contains);
    }

    @Override
    public int hashCode() {
        return Objects.hash(jar, handler, optionalStacks);
    }

    @Override
    public boolean equals(Object obj) {
        if (this == obj) {
            return true;
        }
        if (!(obj instanceof LambdaHandler)) {
            return false;
        }
        LambdaHandler other = (LambdaHandler) obj;
        return Objects.equals(jar, other.jar) && Objects.equals(handler, other.handler) && Objects.equals(optionalStacks, other.optionalStacks);
    }

    @Override
    public String toString() {
        return "LambdaHandler{jar=" + jar + ", handler=" + handler + ", optionalStacks=" + optionalStacks + "}";
    }

    /**
     * Builder to create a lambda handler definition.
     */
    public static class Builder {
        private LambdaJar jar;
        private String handler;
        private List<OptionalStack> optionalStacks;

        private Builder() {
        }

        /**
         * Sets the jar this handler is in.
         *
         * @param  jar the jar
         * @return     this builder
         */
        public Builder jar(LambdaJar jar) {
            this.jar = jar;
            return this;
        }

        /**
         * Sets the class/method reference for this handler.
         *
         * @param  handler the handler class/method reference
         * @return         this builder
         */
        public Builder handler(String handler) {
            this.handler = handler;
            return this;
        }

        /**
         * Sets the optional stacks that trigger deployment of this lambda.
         *
         * @param  optionalStacks the stacks
         * @return                this builder
         */
        public Builder optionalStacks(List<OptionalStack> optionalStacks) {
            this.optionalStacks = optionalStacks;
            return this;
        }

        /**
         * Sets the optional stack that triggers deployment of this lambda.
         *
         * @param  optionalStack the stack
         * @return               this builder
         */
        public Builder optionalStack(OptionalStack optionalStack) {
            return optionalStacks(List.of(optionalStack));
        }

        /**
         * Sets that this lambda is deployed regardless of which optional stacks are enabled.
         *
         * @return this builder
         */
        public Builder core() {
            return optionalStacks(List.of());
        }

        public LambdaHandler build() {
            return new LambdaHandler(this);
        }

        private LambdaHandler add() {
            LambdaHandler handler = build();
            LambdaJar jar = handler.getJar();
            ALL.add(handler);
            if (jar == LambdaJar.ATHENA) {
                ATHENA_HANDLER_BY_CLASSNAME.put(handler.getHandler(), handler);
            }
            return handler;
        }
    }
}
