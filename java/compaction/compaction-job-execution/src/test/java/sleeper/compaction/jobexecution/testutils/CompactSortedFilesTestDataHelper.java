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
package sleeper.compaction.jobexecution.testutils;

import sleeper.compaction.job.CompactionJob;
import sleeper.core.partition.Partition;
import sleeper.core.partition.PartitionTree;
import sleeper.core.record.Record;
import sleeper.core.schema.Schema;
import sleeper.core.statestore.FileInfo;
import sleeper.core.statestore.FileInfoFactory;
import sleeper.core.statestore.StateStore;
import sleeper.core.statestore.StateStoreException;

import java.io.IOException;
import java.util.ArrayList;
import java.util.List;

import static sleeper.compaction.jobexecution.testutils.CompactSortedFilesTestData.writeDataFile;

public class CompactSortedFilesTestDataHelper {
    private final Schema schema;
    private final StateStore stateStore;
    private final PartitionTree partitionTree;
    private final FileInfoFactory fileInfoFactory;
    private final List<FileInfo> fileInfos = new ArrayList<>();

    public CompactSortedFilesTestDataHelper(Schema schema, StateStore stateStore) throws StateStoreException {
        this.schema = schema;
        this.stateStore = stateStore;
        this.partitionTree = new PartitionTree(schema, stateStore.getAllPartitions());
        this.fileInfoFactory = FileInfoFactory.from(partitionTree);
    }

    public void writeRootFile(String filename, List<Record> records) throws IOException {
        FileInfo fileInfo = fileInfoFactory.rootFile(filename, records.size());
        writeDataFile(schema, filename, records);
        fileInfos.add(fileInfo);
    }

    public FileInfo expectedRootFile(String filename, long records) {
        return fileInfoFactory.rootFile(filename, records);
    }

    public FileInfo expectedPartitionFile(String partitionId, String filename, long records) {
        return fileInfoFactory.partitionFile(partitionId, filename, records);
    }

    public void addFilesToStateStoreForJob(CompactionJob compactionJob) throws StateStoreException {
        stateStore.addFiles(fileInfos);
        stateStore.atomicallyUpdateJobStatusOfFiles(compactionJob.getId(), fileInfos);
    }

    public List<FileInfo> allFileInfos() {
        return fileInfos;
    }

    public Partition singlePartition() {
        Partition root = partitionTree.getRootPartition();
        if (!root.getChildPartitionIds().isEmpty()) {
            throw new IllegalArgumentException("Not a single partition");
        }
        return root;
    }
}
