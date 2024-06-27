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

package sleeper.cdk.stack;

import software.amazon.awscdk.services.iam.IGrantable;
import software.amazon.awscdk.services.iam.IRole;
import software.amazon.awscdk.services.iam.ManagedPolicy;
import software.amazon.awscdk.services.lambda.IFunction;
import software.amazon.awscdk.services.sqs.IQueue;

public class CoreStacks {

    private final ConfigBucketStack configBucketStack;
    private final TableIndexStack tableIndexStack;
    private final ManagedPoliciesStack policiesStack;
    private final StateStoreStacks stateStoreStacks;
    private final TableDataStack dataStack;
    private final StateStoreCommitterStack stateStoreCommitterStack;
    private final IngestStatusStoreResources ingestStatusStore;
    private final CompactionStatusStoreResources compactionStatusStore;

    public CoreStacks(ConfigBucketStack configBucketStack, TableIndexStack tableIndexStack,
            ManagedPoliciesStack policiesStack, StateStoreStacks stateStoreStacks, TableDataStack dataStack,
            StateStoreCommitterStack stateStoreCommitterStack,
            IngestStatusStoreResources ingestStatusStore,
            CompactionStatusStoreResources compactionStatusStore) {
        this.configBucketStack = configBucketStack;
        this.tableIndexStack = tableIndexStack;
        this.policiesStack = policiesStack;
        this.stateStoreStacks = stateStoreStacks;
        this.dataStack = dataStack;
        this.stateStoreCommitterStack = stateStoreCommitterStack;
        this.ingestStatusStore = ingestStatusStore;
        this.compactionStatusStore = compactionStatusStore;
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

    public void grantValidateBulkImport(IRole grantee) {
        configBucketStack.grantRead(grantee);
        tableIndexStack.grantRead(grantee);
        stateStoreStacks.grantReadPartitions(grantee);
        policiesStack.grantReadIngestSources(grantee);
        ingestStatusStore.grantWriteJobEvent(grantee);
    }

    public void grantIngest(IRole grantee) {
        configBucketStack.grantRead(grantee);
        tableIndexStack.grantRead(grantee);
        stateStoreStacks.grantReadPartitionsReadWriteActiveFiles(grantee);
        dataStack.grantReadWrite(grantee);
        policiesStack.grantReadIngestSources(grantee);
        stateStoreCommitterStack.grantSendCommits(grantee);
        ingestStatusStore.grantWriteJobEvent(grantee);
        ingestStatusStore.grantWriteTaskEvent(grantee);
    }

    public void grantGarbageCollection(IGrantable grantee) {
        configBucketStack.grantRead(grantee);
        tableIndexStack.grantRead(grantee);
        stateStoreStacks.grantReadWriteReadyForGCFiles(grantee);
        dataStack.grantReadDelete(grantee);
    }

    public void grantCreateCompactionJobs(IGrantable grantee) {
        configBucketStack.grantRead(grantee);
        tableIndexStack.grantRead(grantee);
        stateStoreStacks.grantReadPartitionsReadWriteActiveFiles(grantee);
        compactionStatusStore.grantWriteJobEvent(grantee);
    }

    public void grantRunCompactionJobs(IGrantable grantee) {
        configBucketStack.grantRead(grantee);
        tableIndexStack.grantRead(grantee);
        stateStoreStacks.grantReadWriteActiveAndReadyForGCFiles(grantee);
        stateStoreStacks.grantReadPartitions(grantee);
        dataStack.grantReadWrite(grantee);
        stateStoreCommitterStack.grantSendCommits(grantee);
        compactionStatusStore.grantWriteJobEvent(grantee);
        compactionStatusStore.grantWriteTaskEvent(grantee);
    }

    public void grantSplitPartitions(IGrantable grantee) {
        configBucketStack.grantRead(grantee);
        tableIndexStack.grantRead(grantee);
        stateStoreStacks.grantReadWritePartitions(grantee);
        dataStack.grantRead(grantee);
    }

    public void grantReadIngestSources(IRole grantee) {
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
