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

public class TableStacks {

    private final ConfigurationStack configurationStack;
    private final TableIndexStack tableIndexStack;
    private final StateStoreStacks stateStoreStacks;
    private final TableDataStack dataStack;

    public TableStacks(ConfigurationStack configurationStack, TableIndexStack tableIndexStack,
                       StateStoreStacks stateStoreStacks, TableDataStack dataStack) {
        this.configurationStack = configurationStack;
        this.tableIndexStack = tableIndexStack;
        this.stateStoreStacks = stateStoreStacks;
        this.dataStack = dataStack;
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
}
