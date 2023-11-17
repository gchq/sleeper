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

package sleeper.cdk.stack;

import software.amazon.awscdk.services.iam.IGrantable;

public class CoreStacks {

    private final ConfigBucketStack configBucketStack;
    private final TableIndexStack tableIndexStack;
    private final IngestSourceBucketsStack.GrantBuckets sourceBucketsStack;
    private final ManagedPoliciesStack policiesStack;
    private final StateStoreStacks stateStoreStacks;
    private final TableDataStack dataStack;

    public CoreStacks(ConfigBucketStack configBucketStack, TableIndexStack tableIndexStack,
                      IngestSourceBucketsStack.GrantBuckets sourceBucketsStack, ManagedPoliciesStack policiesStack,
                      StateStoreStacks stateStoreStacks, TableDataStack dataStack) {
        this.configBucketStack = configBucketStack;
        this.tableIndexStack = tableIndexStack;
        this.sourceBucketsStack = sourceBucketsStack;
        this.policiesStack = policiesStack;
        this.stateStoreStacks = stateStoreStacks;
        this.dataStack = dataStack;
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

    public void grantReadConfigAndPartitions(IGrantable grantee) {
        configBucketStack.grantRead(grantee);
        tableIndexStack.grantRead(grantee);
        stateStoreStacks.grantReadPartitions(grantee);
    }

    public void grantIngest(IGrantable grantee) {
        configBucketStack.grantRead(grantee);
        tableIndexStack.grantRead(grantee);
        stateStoreStacks.grantReadPartitionsReadWriteActiveFiles(grantee);
        dataStack.grantReadWrite(grantee);
        sourceBucketsStack.grantReadIngestSources(grantee);
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
    }

    public void grantRunCompactionJobs(IGrantable grantee) {
        configBucketStack.grantRead(grantee);
        tableIndexStack.grantRead(grantee);
        stateStoreStacks.grantReadWriteActiveAndReadyForGCFiles(grantee);
        dataStack.grantReadWrite(grantee);
    }

    public void grantSplitPartitions(IGrantable grantee) {
        configBucketStack.grantRead(grantee);
        tableIndexStack.grantRead(grantee);
        stateStoreStacks.grantReadWritePartitions(grantee);
        dataStack.grantRead(grantee);
    }

    public void grantReadIngestSources(IGrantable grantee) {
        sourceBucketsStack.grantReadIngestSources(grantee);
    }

    public IGrantable getIngestPolicy() {
        return policiesStack.getIngestPolicy();
    }
}
