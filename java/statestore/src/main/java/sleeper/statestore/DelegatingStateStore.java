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
package sleeper.statestore;

import sleeper.core.partition.Partition;

import java.util.Iterator;
import java.util.List;
import java.util.Map;

public class DelegatingStateStore implements StateStore {
    protected final FileInfoStore fileInfoStore;
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
    public void setStatusToReadyForGarbageCollection(String filename) throws StateStoreException {
        fileInfoStore.setStatusToReadyForGarbageCollection(filename);
    }

    @Override
    public void setStatusToReadyForGarbageCollection(List<String> filenames) throws StateStoreException {
        fileInfoStore.setStatusToReadyForGarbageCollection(filenames);
    }

    @Override
    public void atomicallyRemoveFileInPartitionRecordsAndCreateNewActiveFile(
            List<FileInfo> fileInPartitionRecordsToBeDeleted, FileInfo newActiveFile) throws StateStoreException {
        fileInfoStore.atomicallyRemoveFileInPartitionRecordsAndCreateNewActiveFile(fileInPartitionRecordsToBeDeleted, newActiveFile);
    }

    @Override
    public void atomicallyRemoveFileInPartitionRecordsAndCreateNewActiveFiles(List<FileInfo> filesToBeMarkedReadyForGC, FileInfo leftFileInfo, FileInfo rightFileInfo) throws StateStoreException {
        fileInfoStore.atomicallyRemoveFileInPartitionRecordsAndCreateNewActiveFiles(filesToBeMarkedReadyForGC, leftFileInfo, rightFileInfo);
    }

    @Override
    public void atomicallyUpdateJobStatusOfFiles(String jobId, List<FileInfo> fileInfos) throws StateStoreException {
        fileInfoStore.atomicallyUpdateJobStatusOfFiles(jobId, fileInfos);
    }

    @Override
    public void deleteReadyForGCFiles(List<String> filenames) throws StateStoreException {
        fileInfoStore.deleteReadyForGCFiles(filenames);
    }

    @Override
    public List<FileInfo> getFileInPartitionList() throws StateStoreException {
        return fileInfoStore.getFileInPartitionList();
    }

    @Override
    public List<FileInfo> getFileLifecycleList() throws StateStoreException {
        return fileInfoStore.getFileLifecycleList();
    }

    @Override
    public List<FileInfo> getActiveFileList() throws StateStoreException {
        return fileInfoStore.getActiveFileList();
    }

    @Override
    public Iterator<FileInfo> getReadyForGCFileInfos() throws StateStoreException {
        return fileInfoStore.getReadyForGCFileInfos();
    }

    @Override
    public Iterator<String> getReadyForGCFiles() throws StateStoreException {
        return fileInfoStore.getReadyForGCFiles();
    }

    @Override
    public List<FileInfo> getFileInPartitionInfosWithNoJobId() throws StateStoreException {
        return fileInfoStore.getFileInPartitionInfosWithNoJobId();
    }

    @Override
    public Map<String, List<String>> getPartitionToActiveFilesMap() throws StateStoreException {
        return fileInfoStore.getPartitionToActiveFilesMap();
    }

    @Override
    public void initialise() throws StateStoreException {
        partitionStore.initialise();
        fileInfoStore.initialise();
    }

    @Override
    public void initialise(List<Partition> partitions) throws StateStoreException {
        partitionStore.initialise(partitions);
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
}
