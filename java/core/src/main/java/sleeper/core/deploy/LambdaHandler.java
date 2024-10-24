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
package sleeper.core.deploy;

import sleeper.core.properties.validation.OptionalStack;

import java.util.ArrayList;
import java.util.Collection;
import java.util.Collections;
import java.util.HashMap;
import java.util.List;
import java.util.Map;
import java.util.Objects;

/**
 * Definitions of handler methods for lambda functions.
 */
public class LambdaHandler {

    private static final List<LambdaHandler> ALL = new ArrayList<>();
    private static final Map<String, List<LambdaHandler>> HANDLERS_BY_JAR_FILENAME = new HashMap<>();
    public static final LambdaHandler ATHENA_SIMPLE_COMPOSITE = builder()
            .jar(LambdaJar.ATHENA)
            .handler("sleeper.athena.composite.SimpleCompositeHandler")
            .imageName("athena-simple-lambda")
            .optionalStack(OptionalStack.AthenaStack).add();
    public static final LambdaHandler ATHENA_ITERATORS_COMPOSITE = builder()
            .jar(LambdaJar.ATHENA)
            .handler("sleeper.athena.composite.IteratorApplyingCompositeHandler")
            .imageName("athena-iterators-lambda")
            .optionalStack(OptionalStack.AthenaStack).add();
    public static final LambdaHandler BULK_IMPORT_STARTER = builder()
            .jar(LambdaJar.BULK_IMPORT_STARTER)
            .handler("sleeper.bulkimport.starter.BulkImportStarterLambda")
            .imageName("bulk-import-starter-lambda")
            .optionalStacks(OptionalStack.BULK_IMPORT_STACKS).add();
    public static final LambdaHandler INGEST_TASK_CREATOR = builder()
            .jar(LambdaJar.INGEST_TASK_CREATOR)
            .handler("sleeper.ingest.starter.RunIngestTasksLambda::eventHandler")
            .imageName("ingest-task-creator-lambda")
            .optionalStack(OptionalStack.IngestStack).add();
    public static final LambdaHandler INGEST_BATCHER_SUBMITTER = builder()
            .jar(LambdaJar.INGEST_BATCHER_SUBMITTER)
            .handler("sleeper.ingest.batcher.submitter.IngestBatcherSubmitterLambda::handleRequest")
            .imageName("ingest-batcher-submitter-lambda")
            .optionalStack(OptionalStack.IngestBatcherStack).add();
    public static final LambdaHandler INGEST_BATCHER_JOB_CREATOR = builder()
            .jar(LambdaJar.INGEST_BATCHER_JOB_CREATOR)
            .handler("sleeper.ingest.batcher.job.creator.IngestBatcherJobCreatorLambda::eventHandler")
            .imageName("ingest-batcher-job-creator-lambda")
            .optionalStack(OptionalStack.IngestBatcherStack).add();
    public static final LambdaHandler GARBAGE_COLLECTOR_TRIGGER = builder()
            .jar(LambdaJar.GARBAGE_COLLECTOR)
            .handler("sleeper.garbagecollector.GarbageCollectorTriggerLambda::handleRequest")
            .imageName("garbage-collector-trigger-lambda")
            .optionalStack(OptionalStack.GarbageCollectorStack).add();
    public static final LambdaHandler GARBAGE_COLLECTOR = builder()
            .jar(LambdaJar.GARBAGE_COLLECTOR)
            .handler("sleeper.garbagecollector.GarbageCollectorLambda::handleRequest")
            .imageName("garbage-collector-lambda")
            .optionalStack(OptionalStack.GarbageCollectorStack).add();
    public static final LambdaHandler COMPACTION_JOB_CREATOR_TRIGGER = builder()
            .jar(LambdaJar.COMPACTION_JOB_CREATOR)
            .handler("sleeper.compaction.job.creation.lambda.CreateCompactionJobsTriggerLambda::handleRequest")
            .imageName("compaction-job-creator-trigger-lambda")
            .optionalStack(OptionalStack.CompactionStack).add();
    public static final LambdaHandler COMPACTION_JOB_CREATOR = builder()
            .jar(LambdaJar.COMPACTION_JOB_CREATOR)
            .handler("sleeper.compaction.job.creation.lambda.CreateCompactionJobsLambda::handleRequest")
            .imageName("compaction-job-creator-lambda")
            .optionalStack(OptionalStack.CompactionStack).add();
    public static final LambdaHandler COMPACTION_TASK_CREATOR = builder()
            .jar(LambdaJar.COMPACTION_TASK_CREATOR)
            .handler("sleeper.compaction.task.creation.RunCompactionTasksLambda::eventHandler")
            .imageName("compaction-task-creator-lambda")
            .optionalStack(OptionalStack.CompactionStack).add();
    public static final LambdaHandler COMPACTION_TASK_TERMINATOR = builder()
            .jar(LambdaJar.COMPACTION_TASK_CREATOR)
            .handler("sleeper.compaction.task.creation.SafeTerminationLambda::handleRequest")
            .imageName("compaction-task-terminator-lambda")
            .optionalStack(OptionalStack.CompactionStack).add();
    public static final LambdaHandler PARTITION_SPLITTER_TRIGGER = builder()
            .jar(LambdaJar.PARTITION_SPLITTER)
            .handler("sleeper.splitter.lambda.FindPartitionsToSplitTriggerLambda::handleRequest")
            .imageName("find-partitions-to-split-trigger-lambda")
            .optionalStack(OptionalStack.PartitionSplittingStack).add();
    public static final LambdaHandler FIND_PARTITIONS_TO_SPLIT = builder()
            .jar(LambdaJar.PARTITION_SPLITTER)
            .handler("sleeper.splitter.lambda.FindPartitionsToSplitLambda::handleRequest")
            .imageName("find-partitions-to-split-lambda")
            .optionalStack(OptionalStack.PartitionSplittingStack).add();
    public static final LambdaHandler SPLIT_PARTITION = builder()
            .jar(LambdaJar.PARTITION_SPLITTER)
            .handler("sleeper.splitter.lambda.SplitPartitionLambda::handleRequest")
            .imageName("split-partition-lambda")
            .optionalStack(OptionalStack.PartitionSplittingStack).add();
    public static final LambdaHandler KEEP_QUERY_WARM = builder()
            .jar(LambdaJar.QUERY)
            .handler("sleeper.query.lambda.WarmQueryExecutorLambda::handleRequest")
            .imageName("keep-query-warm-lambda")
            .optionalStack(OptionalStack.KeepLambdaWarmStack).add();
    public static final LambdaHandler QUERY_EXECUTOR = builder()
            .jar(LambdaJar.QUERY)
            .handler("sleeper.query.lambda.SqsQueryProcessorLambda::handleRequest")
            .imageName("query-executor-lambda")
            .optionalStack(OptionalStack.QueryStack).add();
    public static final LambdaHandler QUERY_LEAF_PARTITION = builder()
            .jar(LambdaJar.QUERY)
            .handler("sleeper.query.lambda.SqsLeafPartitionQueryLambda::handleRequest")
            .imageName("query-leaf-partition-lambda")
            .optionalStack(OptionalStack.QueryStack).add();
    public static final LambdaHandler WEB_SOCKET_QUERY = builder()
            .jar(LambdaJar.QUERY)
            .handler("sleeper.query.lambda.WebSocketQueryProcessorLambda::handleRequest")
            .imageName("web-socket-query-lambda")
            .optionalStack(OptionalStack.WebSocketQueryStack).add();
    public static final LambdaHandler AUTO_DELETE_S3_OBJECTS = builder()
            .jar(LambdaJar.CUSTOM_RESOURCES)
            .handler("sleeper.cdk.custom.AutoDeleteS3ObjectsLambda::handleEvent")
            .imageName("auto-delete-s3-objects-lambda")
            .core().add();
    public static final LambdaHandler PROPERTIES_WRITER = builder()
            .jar(LambdaJar.CUSTOM_RESOURCES)
            .handler("sleeper.cdk.custom.PropertiesWriterLambda::handleEvent")
            .imageName("properties-writer-lambda")
            .core().add();
    public static final LambdaHandler VPC_CHECK = builder()
            .jar(LambdaJar.CUSTOM_RESOURCES)
            .handler("sleeper.cdk.custom.VpcCheckLambda::handleEvent")
            .imageName("vpc-check-lambda")
            .core().add();
    public static final LambdaHandler METRICS_TRIGGER = builder()
            .jar(LambdaJar.METRICS)
            .handler("sleeper.metrics.TableMetricsTriggerLambda::handleRequest")
            .imageName("metrics-trigger-lambda")
            .optionalStack(OptionalStack.TableMetricsStack).add();
    public static final LambdaHandler METRICS = builder()
            .jar(LambdaJar.METRICS)
            .handler("sleeper.metrics.TableMetricsLambda::handleRequest")
            .imageName("metrics-lambda")
            .optionalStack(OptionalStack.TableMetricsStack).add();
    public static final LambdaHandler STATESTORE_COMMITTER = builder()
            .jar(LambdaJar.STATESTORE)
            .handler("sleeper.statestore.committer.lambda.StateStoreCommitterLambda::handleRequest")
            .imageName("statestore-committer-lambda")
            .core().add();
    public static final LambdaHandler SNAPSHOT_CREATION_TRIGGER = builder()
            .jar(LambdaJar.STATESTORE)
            .handler("sleeper.statestore.snapshot.TransactionLogSnapshotCreationTriggerLambda::handleRequest")
            .imageName("snapshot-creation-trigger-lambda")
            .core().add();
    public static final LambdaHandler SNAPSHOT_CREATION = builder()
            .jar(LambdaJar.STATESTORE)
            .handler("sleeper.statestore.snapshot.TransactionLogSnapshotCreationLambda::handleRequest")
            .imageName("snapshot-creation-lambda")
            .core().add();
    public static final LambdaHandler SNAPSHOT_DELETION_TRIGGER = builder()
            .jar(LambdaJar.STATESTORE)
            .handler("sleeper.statestore.snapshot.TransactionLogSnapshotDeletionTriggerLambda::handleRequest")
            .imageName("snapshot-deletion-trigger-lambda")
            .core().add();
    public static final LambdaHandler SNAPSHOT_DELETION = builder()
            .jar(LambdaJar.STATESTORE)
            .handler("sleeper.statestore.snapshot.TransactionLogSnapshotDeletionLambda::handleRequest")
            .imageName("snapshot-deletion-lambda")
            .core().add();
    public static final LambdaHandler TRANSACTION_DELETION_TRIGGER = builder()
            .jar(LambdaJar.STATESTORE)
            .handler("sleeper.statestore.transaction.TransactionLogTransactionDeletionTriggerLambda::handleRequest")
            .imageName("transaction-deletion-trigger-lambda")
            .core().add();
    public static final LambdaHandler TRANSACTION_DELETION = builder()
            .jar(LambdaJar.STATESTORE)
            .handler("sleeper.statestore.transaction.TransactionLogTransactionDeletionLambda::handleRequest")
            .imageName("transaction-deletion-lambda")
            .core().add();

    private final LambdaJar jar;
    private final String handler;
    private final String imageName;
    private final List<OptionalStack> optionalStacks;

    private LambdaHandler(Builder builder) {
        jar = Objects.requireNonNull(builder.jar, "jar must not be null");
        handler = Objects.requireNonNull(builder.handler, "handler must not be null");
        imageName = Objects.requireNonNull(builder.imageName, "imageName must not be null");
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

    /**
     * Returns all lambda handler definitions that are deployed with a given jar.
     *
     * @param  jar the jar
     * @return     the definitions
     */
    public static List<LambdaHandler> getHandlers(LambdaJar jar) {
        return Collections.unmodifiableList(
                HANDLERS_BY_JAR_FILENAME.getOrDefault(jar.getFilename(), List.of()));
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

    public String getImageName() {
        return imageName;
    }

    public List<OptionalStack> getOptionalStacks() {
        return optionalStacks;
    }

    /**
     * Checks if this lambda is deployed given the enabled optional stacks.
     *
     * @param  stacks the enabled optional stacks
     * @return        true if this lambda will be deployed
     */
    public boolean isDeployed(Collection<OptionalStack> stacks) {
        return optionalStacks.isEmpty() || isDeployedOptional(stacks);
    }

    /**
     * Checks if this lambda is deployed in an optional stack, given the enabled optional stacks.
     *
     * @param  stacks the enabled optional stacks
     * @return        true if this lambda will be deployed in an optional stack
     */
    public boolean isDeployedOptional(Collection<OptionalStack> stacks) {
        return optionalStacks.stream().anyMatch(stacks::contains);
    }

    /**
     * Builder to create a lambda handler definition.
     */
    public static class Builder {
        private LambdaJar jar;
        private String handler;
        private String imageName;
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
         * Sets the Docker image name for ECR.
         *
         * @param  imageName the image name
         * @return           this builder
         */
        public Builder imageName(String imageName) {
            this.imageName = imageName;
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
            ALL.add(handler);
            HANDLERS_BY_JAR_FILENAME.computeIfAbsent(jar.getFilename(), f -> new ArrayList<>()).add(handler);
            return handler;
        }
    }
}
