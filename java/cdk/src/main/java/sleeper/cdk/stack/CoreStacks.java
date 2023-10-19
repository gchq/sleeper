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

    private final ConfigurationStack configurationStack;
    private final TableIndexStack tableIndexStack;
    private final StateStoreStacks stateStoreStacks;
    private final TableDataStack dataStack;

    public CoreStacks(ConfigurationStack configurationStack, TableIndexStack tableIndexStack,
                      StateStoreStacks stateStoreStacks, TableDataStack dataStack) {
        this.configurationStack = configurationStack;
        this.tableIndexStack = tableIndexStack;
        this.stateStoreStacks = stateStoreStacks;
        this.dataStack = dataStack;
    }

    public void grantReadInstanceConfig(IGrantable grantee) {
        configurationStack.grantRead(grantee);
    }

    public void grantReadTablesConfig(IGrantable grantee) {
        configurationStack.grantRead(grantee);
        tableIndexStack.grantRead(grantee);
    }

    public void grantReadTablesAndData(IGrantable grantee) {
        configurationStack.grantRead(grantee);
        tableIndexStack.grantRead(grantee);
        stateStoreStacks.grantReadActiveFilesAndPartitions(grantee);
        dataStack.grantRead(grantee);
    }

    public void grantReadTablesMetadata(IGrantable grantee) {
        configurationStack.grantRead(grantee);
        tableIndexStack.grantRead(grantee);
        stateStoreStacks.grantReadActiveFilesAndPartitions(grantee);
    }

    public void grantReadConfigAndPartitions(IGrantable grantee) {
        configurationStack.grantRead(grantee);
        tableIndexStack.grantRead(grantee);
        stateStoreStacks.grantReadPartitions(grantee);
    }

    public void grantIngest(IGrantable grantee) {
        configurationStack.grantRead(grantee);
        tableIndexStack.grantRead(grantee);
        stateStoreStacks.grantReadPartitionsReadWriteActiveFiles(grantee);
        dataStack.grantReadWrite(grantee);
    }

    /**
     * Grants access to write data into a table without access to any configuration.
     * Requires that the configuration and table metadata should be read & written somewhere else,
     * eg. transmitted across a Spark cluster from the driver.
     * <p>
     * This has the advantage of keeping the configuration fixed for a whole bulk import job.
     *
     * @param grantee Entity to grant permissions to
     */
    public void grantWriteDataOnly(IGrantable grantee) {
        dataStack.grantReadWrite(grantee);
    }

    public void grantGarbageCollection(IGrantable grantee) {
        configurationStack.grantRead(grantee);
        tableIndexStack.grantRead(grantee);
        stateStoreStacks.grantReadWriteReadyForGCFiles(grantee);
        dataStack.grantReadDelete(grantee);
    }

    public void grantCreateCompactionJobs(IGrantable grantee) {
        configurationStack.grantRead(grantee);
        tableIndexStack.grantRead(grantee);
        stateStoreStacks.grantReadPartitionsReadWriteActiveFiles(grantee);
    }

    public void grantRunCompactionJobs(IGrantable grantee) {
        configurationStack.grantRead(grantee);
        tableIndexStack.grantRead(grantee);
        stateStoreStacks.grantReadWriteActiveAndReadyForGCFiles(grantee);
        dataStack.grantReadWrite(grantee);
    }

    public void grantSplitPartitions(IGrantable grantee) {
        configurationStack.grantRead(grantee);
        tableIndexStack.grantRead(grantee);
        stateStoreStacks.grantReadWritePartitions(grantee);
        dataStack.grantRead(grantee);
    }
}
