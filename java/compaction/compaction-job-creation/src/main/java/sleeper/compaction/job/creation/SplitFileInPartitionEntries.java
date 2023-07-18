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
package sleeper.compaction.job.creation;

import org.slf4j.Logger;
import org.slf4j.LoggerFactory;

import sleeper.core.partition.Partition;
import sleeper.core.partition.PartitionTree;
import sleeper.core.schema.Schema;
import sleeper.statestore.FileInfo;
import sleeper.statestore.StateStore;
import sleeper.statestore.StateStoreException;

import java.util.List;

public class SplitFileInPartitionEntries {
    private static final Logger LOGGER = LoggerFactory.getLogger(SplitFileInPartitionEntries.class);

    private final Schema schema;
    private final List<FileInfo> fileInPartitionEntries;
    private final List<Partition> allPartitions;
    private final StateStore stateStore;

    public SplitFileInPartitionEntries(Schema schema,
            List<FileInfo> fileInPartitionEntries,
            List<Partition> allPartitions,
            StateStore stateStore) {
        this.schema = schema;
        this.fileInPartitionEntries = fileInPartitionEntries;
        this.allPartitions = allPartitions;
        this.stateStore = stateStore;
    }

    public void run() throws StateStoreException {
        PartitionTree partitionTree = new PartitionTree(schema, allPartitions);
        for (FileInfo fileInfo : fileInPartitionEntries) {
            String partitionId = fileInfo.getPartitionId();
            Partition partition = partitionTree.getPartition(partitionId);
            if (!partition.isLeafPartition()) {
                LOGGER.info("File-in-partition entry for file {} and partition {} is being split as it is not in a leaf partition",
                    fileInfo.getFilename(), partitionId);
                List<String> childPartitionIds = partition.getChildPartitionIds();
                stateStore.atomicallySplitFileInPartitionRecord(fileInfo, childPartitionIds.get(0), childPartitionIds.get(1));
            }
        }
    }
}
