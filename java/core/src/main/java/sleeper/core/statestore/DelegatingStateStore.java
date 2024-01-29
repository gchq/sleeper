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
package sleeper.core.statestore;

import sleeper.core.partition.Partition;

import java.time.Instant;
import java.util.List;
import java.util.Map;
import java.util.stream.Stream;

public class DelegatingStateStore implements StateStore {
    private final FileReferenceStore fileReferenceStore;
    private final PartitionStore partitionStore;

    public DelegatingStateStore(FileReferenceStore fileReferenceStore, PartitionStore partitionStore) {
        this.fileReferenceStore = fileReferenceStore;
        this.partitionStore = partitionStore;
    }

    @Override
    public void addFile(FileReference fileReference) throws StateStoreException {
        fileReferenceStore.addFile(fileReference);
    }

    @Override
    public void addFiles(List<FileReference> fileReferences) throws StateStoreException {
        fileReferenceStore.addFiles(fileReferences);
    }

    @Override
    public void addFilesWithReferences(List<AllReferencesToAFile> files) throws StateStoreException {
        fileReferenceStore.addFilesWithReferences(files);
    }

    @Override
    public void splitFileReferences(List<SplitFileReferenceRequest> splitRequests) throws StateStoreException {
        fileReferenceStore.splitFileReferences(splitRequests);
    }

    @Override
    public void atomicallyApplyJobFileReferenceUpdates(String jobId, String partitionId, List<String> filesProcessed, List<FileReference> newReferences) throws StateStoreException {
        fileReferenceStore.atomicallyApplyJobFileReferenceUpdates(jobId, partitionId, filesProcessed, newReferences);
    }

    @Override
    public void atomicallyAssignJobIdToFileReferences(String jobId, List<FileReference> fileReferences) throws StateStoreException {
        fileReferenceStore.atomicallyAssignJobIdToFileReferences(jobId, fileReferences);
    }

    @Override
    public void deleteGarbageCollectedFileReferenceCounts(List<String> filenames) throws StateStoreException {
        fileReferenceStore.deleteGarbageCollectedFileReferenceCounts(filenames);
    }

    @Override
    public List<FileReference> getFileReferences() throws StateStoreException {
        return fileReferenceStore.getFileReferences();
    }

    @Override
    public Stream<String> getReadyForGCFilenamesBefore(Instant maxUpdateTime) throws StateStoreException {
        return fileReferenceStore.getReadyForGCFilenamesBefore(maxUpdateTime);
    }

    @Override
    public List<FileReference> getFileReferencesWithNoJobId() throws StateStoreException {
        return fileReferenceStore.getFileReferencesWithNoJobId();
    }

    @Override
    public Map<String, List<String>> getPartitionToReferencedFilesMap() throws StateStoreException {
        return fileReferenceStore.getPartitionToReferencedFilesMap();
    }

    @Override
    public AllReferencesToAllFiles getAllFileReferencesWithMaxUnreferenced(int maxUnreferencedFiles) throws StateStoreException {
        return fileReferenceStore.getAllFileReferencesWithMaxUnreferenced(maxUnreferencedFiles);
    }

    @Override
    public void initialise() throws StateStoreException {
        if (!hasNoFiles()) {
            throw new StateStoreException("Cannot initialise state store when files are present");
        }
        partitionStore.initialise();
        fileReferenceStore.initialise();
    }

    @Override
    public void initialise(List<Partition> partitions) throws StateStoreException {
        if (!hasNoFiles()) {
            throw new StateStoreException("Cannot initialise state store when files are present");
        }
        partitionStore.initialise(partitions);
        fileReferenceStore.initialise();
    }

    public void setInitialFileReferences() throws StateStoreException {
        fileReferenceStore.initialise();
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
        return fileReferenceStore.hasNoFiles();
    }

    @Override
    public void clearFileData() {
        fileReferenceStore.clearFileData();
    }

    @Override
    public void clearPartitionData() {
        partitionStore.clearPartitionData();
    }

    @Override
    public void fixTime(Instant now) {
        fileReferenceStore.fixTime(now);
    }
}
