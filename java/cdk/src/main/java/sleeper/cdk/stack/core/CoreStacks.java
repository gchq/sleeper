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

package sleeper.cdk.stack.core;

import edu.umd.cs.findbugs.annotations.SuppressFBWarnings;
import software.amazon.awscdk.services.iam.IGrantable;
import software.amazon.awscdk.services.iam.IRole;
import software.amazon.awscdk.services.iam.ManagedPolicy;
import software.amazon.awscdk.services.lambda.IFunction;
import software.amazon.awscdk.services.logs.ILogGroup;
import software.amazon.awscdk.services.sqs.IQueue;

import sleeper.cdk.stack.compaction.CompactionTrackerResources;
import sleeper.cdk.stack.core.LoggingStack.LogGroupRef;
import sleeper.cdk.stack.ingest.IngestTrackerResources;

import javax.annotation.Nullable;

import java.util.Objects;

public class CoreStacks {

    private final LoggingStack loggingStack;
    private final ConfigBucketStack configBucketStack;
    private final TableIndexStack tableIndexStack;
    private final ManagedPoliciesStack policiesStack;
    private final StateStoreStacks stateStoreStacks;
    private final TableDataStack dataStack;
    private final StateStoreCommitterStack stateStoreCommitterStack;
    private final IngestTrackerResources ingestTracker;
    private final CompactionTrackerResources compactionTracker;

    public CoreStacks(LoggingStack loggingStack, ConfigBucketStack configBucketStack, TableIndexStack tableIndexStack,
            ManagedPoliciesStack policiesStack, StateStoreStacks stateStoreStacks, TableDataStack dataStack,
            StateStoreCommitterStack stateStoreCommitterStack,
            IngestTrackerResources ingestTracker,
            CompactionTrackerResources compactionTracker) {
        this.loggingStack = loggingStack;
        this.configBucketStack = configBucketStack;
        this.tableIndexStack = tableIndexStack;
        this.policiesStack = policiesStack;
        this.stateStoreStacks = stateStoreStacks;
        this.dataStack = dataStack;
        this.stateStoreCommitterStack = stateStoreCommitterStack;
        this.ingestTracker = ingestTracker;
        this.compactionTracker = compactionTracker;
    }

    public ILogGroup getLogGroup(LogGroupRef logGroupRef) {
        return loggingStack.getLogGroup(logGroupRef);
    }

    public void grantReadInstanceConfig(IGrantable grantee) {
        configBucketStack.grantRead(grantee);
    }

    public void grantWriteInstanceConfig(IGrantable grantee) {
        configBucketStack.grantWrite(grantee);
    }

    public void grantReadTablesConfig(IGrantable grantee) {
        configBucketStack.grantRead(grantee);
        tableIndexStack.grantRead(grantee);
    }

    public void grantReadTablesAndData(IGrantable grantee) {
        configBucketStack.grantRead(grantee);
        tableIndexStack.grantRead(grantee);
        stateStoreStacks.grantReadActiveFilesAndPartitions(grantee);
        dataStack.grantRead(grantee);
    }

    public void grantReadTablesMetadata(IGrantable grantee) {
        configBucketStack.grantRead(grantee);
        tableIndexStack.grantRead(grantee);
        stateStoreStacks.grantReadActiveFilesAndPartitions(grantee);
    }

    public void grantReadTablesStatus(IGrantable grantee) {
        configBucketStack.grantRead(grantee);
        tableIndexStack.grantRead(grantee);
    }

    // The Lambda IFunction.getRole method is annotated as nullable, even though it will never return null in practice.
    // This means SpotBugs complains if we pass that role into attachToRole.
    // The role parameter is marked as nullable to convince SpotBugs that it's fine to pass it into this method,
    // even though attachToRole really requires the role to be non-null.
    @SuppressFBWarnings("NP_PARAMETER_MUST_BE_NONNULL_BUT_MARKED_AS_NULLABLE")
    public void grantValidateBulkImport(@Nullable IRole nullableRole) {
        IRole grantee = Objects.requireNonNull(nullableRole);
        configBucketStack.grantRead(grantee);
        tableIndexStack.grantRead(grantee);
        stateStoreStacks.grantReadPartitions(grantee);
        policiesStack.grantReadIngestSources(grantee);
        ingestTracker.grantWriteJobEvent(grantee);
    }

    public void grantIngest(IRole grantee) {
        configBucketStack.grantRead(grantee);
        tableIndexStack.grantRead(grantee);
        stateStoreStacks.grantReadPartitionsReadWriteActiveFiles(grantee);
        dataStack.grantReadWrite(grantee);
        policiesStack.grantReadIngestSources(grantee);
        stateStoreCommitterStack.grantSendCommits(grantee);
        ingestTracker.grantWriteJobEvent(grantee);
        ingestTracker.grantWriteTaskEvent(grantee);
    }

    public void grantGarbageCollection(IGrantable grantee) {
        configBucketStack.grantRead(grantee);
        tableIndexStack.grantRead(grantee);
        stateStoreStacks.grantReadWriteReadyForGCFiles(grantee);
        dataStack.grantReadDelete(grantee);
        stateStoreCommitterStack.grantSendCommits(grantee);
    }

    public void grantCreateCompactionJobs(IGrantable grantee) {
        configBucketStack.grantRead(grantee);
        tableIndexStack.grantRead(grantee);
        stateStoreStacks.grantReadPartitionsReadWriteActiveFiles(grantee);
        compactionTracker.grantWriteJobEvent(grantee);
        stateStoreCommitterStack.grantSendCommits(grantee);
    }

    public void grantRunCompactionJobs(IGrantable grantee) {
        configBucketStack.grantRead(grantee);
        tableIndexStack.grantRead(grantee);
        stateStoreStacks.grantReadWriteActiveAndReadyForGCFiles(grantee);
        stateStoreStacks.grantReadPartitions(grantee);
        dataStack.grantReadWrite(grantee);
        stateStoreCommitterStack.grantSendCommits(grantee);
        compactionTracker.grantWriteJobEvent(grantee);
        compactionTracker.grantWriteTaskEvent(grantee);
    }

    public void grantSplitPartitions(IGrantable grantee) {
        configBucketStack.grantRead(grantee);
        tableIndexStack.grantRead(grantee);
        stateStoreStacks.grantReadWritePartitions(grantee);
        dataStack.grantRead(grantee);
        stateStoreCommitterStack.grantSendCommits(grantee);
    }

    // The Lambda IFunction.getRole method is annotated as nullable, even though it will never return null in practice.
    // This means SpotBugs complains if we pass that role into attachToRole.
    // The role parameter is marked as nullable to convince SpotBugs that it's fine to pass it into this method,
    // even though attachToRole really requires the role to be non-null.
    @SuppressFBWarnings("NP_PARAMETER_MUST_BE_NONNULL_BUT_MARKED_AS_NULLABLE")
    public void grantReadIngestSources(@Nullable IRole nullableRole) {
        IRole grantee = Objects.requireNonNull(nullableRole);
        policiesStack.grantReadIngestSources(grantee);
    }

    public IGrantable getIngestByQueuePolicyForGrants() {
        return policiesStack.getIngestByQueuePolicyForGrants();
    }

    public IGrantable getQueryPolicyForGrants() {
        return policiesStack.getQueryPolicyForGrants();
    }

    public ManagedPolicy getReportingPolicyForGrants() {
        return policiesStack.getReportingPolicyForGrants();
    }

    public void grantInvokeScheduled(IFunction function) {
        policiesStack.grantInvokeScheduled(function);
    }

    public void grantInvokeScheduled(IFunction triggerFunction, IQueue invokeQueue) {
        policiesStack.grantInvokeScheduled(triggerFunction, invokeQueue);
    }

    public ManagedPolicy getInvokeCompactionPolicyForGrants() {
        return policiesStack.getInvokeCompactionPolicyForGrants();
    }

    public IGrantable getPurgeQueuesPolicyForGrants() {
        return policiesStack.getPurgeQueuesPolicyForGrants();
    }
}
