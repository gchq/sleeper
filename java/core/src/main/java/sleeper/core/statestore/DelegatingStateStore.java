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

import java.time.Instant;
import java.util.Iterator;
import java.util.List;
import java.util.Map;

public class DelegatingStateStore implements StateStore {
    private final FileInfoStore fileInfoStore;
    private final PartitionStore partitionStore;

    public DelegatingStateStore(FileInfoStore fileInfoStore, PartitionStore partitionStore) {
        this.fileInfoStore = fileInfoStore;
        this.partitionStore = partitionStore;
    }

    @Override
    public void addFile(FileInfo fileInfo) throws StateStoreException {
        fileInfoStore.addFile(fileInfo);
    }

    @Override
    public void addFiles(List<FileInfo> fileInfos) throws StateStoreException {
        fileInfoStore.addFiles(fileInfos);
    }

    @Override
    public void atomicallyUpdateFilesToReadyForGCAndCreateNewActiveFile(List<FileInfo> filesToBeMarkedReadyForGC, FileInfo newActiveFile) throws StateStoreException {
        fileInfoStore.atomicallyUpdateFilesToReadyForGCAndCreateNewActiveFile(filesToBeMarkedReadyForGC, newActiveFile);
    }

    @Override
    public void atomicallyUpdateFilesToReadyForGCAndCreateNewActiveFiles(List<FileInfo> filesToBeMarkedReadyForGC, FileInfo leftFileInfo, FileInfo rightFileInfo) throws StateStoreException {
        fileInfoStore.atomicallyUpdateFilesToReadyForGCAndCreateNewActiveFiles(filesToBeMarkedReadyForGC, leftFileInfo, rightFileInfo);
    }

    @Override
    public void atomicallyUpdateFilesToReadyForGCAndCreateNewActiveFiles(List<FileInfo> filesToBeMarkedReadyForGC, List<FileInfo> newFiles) throws StateStoreException {
        fileInfoStore.atomicallyUpdateFilesToReadyForGCAndCreateNewActiveFiles(filesToBeMarkedReadyForGC, newFiles);
    }

    @Override
    public void atomicallyUpdateJobStatusOfFiles(String jobId, List<FileInfo> fileInfos) throws StateStoreException {
        fileInfoStore.atomicallyUpdateJobStatusOfFiles(jobId, fileInfos);
    }

    @Override
    public void deleteReadyForGCFile(FileInfo fileInfo) throws StateStoreException {
        fileInfoStore.deleteReadyForGCFile(fileInfo);
    }

    @Override
    public List<FileInfo> getActiveFiles() throws StateStoreException {
        return fileInfoStore.getActiveFiles();
    }

    @Override
    public Iterator<FileInfo> getReadyForGCFiles() throws StateStoreException {
        return fileInfoStore.getReadyForGCFiles();
    }

    @Override
    public List<FileInfo> getActiveFilesWithNoJobId() throws StateStoreException {
        return fileInfoStore.getActiveFilesWithNoJobId();
    }

    @Override
    public Map<String, List<String>> getPartitionToActiveFilesMap() throws StateStoreException {
        return fileInfoStore.getPartitionToActiveFilesMap();
    }

    @Override
    public void initialise() throws StateStoreException {
        if (!hasNoFiles()) {
            throw new StateStoreException("Cannot initialise state store when files are present");
        }
        partitionStore.initialise();
        fileInfoStore.initialise();
    }

    @Override
    public void initialise(List<Partition> partitions) throws StateStoreException {
        if (!hasNoFiles()) {
            throw new StateStoreException("Cannot initialise state store when files are present");
        }
        partitionStore.initialise(partitions);
        fileInfoStore.initialise();
    }

    public void setInitialFileInfos() throws StateStoreException {
        fileInfoStore.initialise();
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
    public boolean hasNoFiles() {
        return fileInfoStore.hasNoFiles();
    }

    @Override
    public void clearTable() {
        fileInfoStore.clearTable();
        partitionStore.clearTable();
    }

    @Override
    public void clearFiles() {
        fileInfoStore.clearTable();
    }

    /**
     * Used to set the current time. Should only be called during tests.
     *
     * @param now Time to set to be the current time
     */
    @Override
    public void fixTime(Instant now) {
        fileInfoStore.fixTime(now);
    }
}
