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

package sleeper.systemtest.suite.dsl;

import sleeper.core.partition.Partition;
import sleeper.core.partition.PartitionTree;
import sleeper.core.statestore.FileInfo;
import sleeper.core.statestore.StateStoreException;
import sleeper.systemtest.drivers.instance.SleeperInstanceContext;

import java.util.List;

public class SystemTestStateStore {

    private final SleeperInstanceContext instance;

    public SystemTestStateStore(SleeperInstanceContext instance) {
        this.instance = instance;
    }

    public int numActiveFiles() {
        return activeFiles().size();
    }

    public List<FileInfo> activeFiles() {
        try {
            return instance.getStateStore().getActiveFiles();
        } catch (StateStoreException e) {
            throw new RuntimeException(e);
        }
    }

    public List<Partition> allPartitions() {
        try {
            return instance.getStateStore().getAllPartitions();
        } catch (StateStoreException e) {
            throw new RuntimeException(e);
        }
    }

    public void setPartitions(PartitionTree tree) {
        try {
            instance.getStateStore().initialise(tree.getAllPartitions());
        } catch (StateStoreException e) {
            throw new RuntimeException(e);
        }
    }
}
