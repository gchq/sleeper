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

package sleeper.core.statestore;

import sleeper.core.partition.Partition;

import java.util.List;

public class DelegatingStateStoreV2 implements StateStoreV2 {
    private final PartitionStore partitionStore;
    private final FileInfoStoreV2 fileStore;

    public DelegatingStateStoreV2(PartitionStore partitionStore, FileInfoStoreV2 fileStore) {
        this.partitionStore = partitionStore;
        this.fileStore = fileStore;
    }

    @Override
    public void atomicallyUpdatePartitionAndCreateNewOnes(Partition splitPartition, Partition newPartition1, Partition newPartition2) throws StateStoreException {
        partitionStore.atomicallyUpdatePartitionAndCreateNewOnes(splitPartition, newPartition1, newPartition2);
    }

    @Override
    public List<Partition> getAllPartitions() throws StateStoreException {
        return partitionStore.getAllPartitions();
    }

    @Override
    public List<Partition> getLeafPartitions() throws StateStoreException {
        return partitionStore.getLeafPartitions();
    }

    @Override
    public void initialise() throws StateStoreException {
        partitionStore.initialise();
    }

    @Override
    public void initialise(List<Partition> partitions) throws StateStoreException {
        partitionStore.initialise(partitions);
    }

    @Override
    public void completeIngest(AddFilesRequest request) {
        fileStore.completeIngest(request);
    }

    @Override
    public List<FileInfoV2> getPartitionFiles() {
        return fileStore.getPartitionFiles();
    }
}
