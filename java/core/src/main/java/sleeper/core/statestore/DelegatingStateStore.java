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

import org.slf4j.Logger;
import org.slf4j.LoggerFactory;

import sleeper.core.partition.Partition;
import sleeper.core.statestore.exception.ReplaceRequestsFailedException;
import sleeper.core.statestore.exception.SplitRequestsFailedException;

import java.time.Instant;
import java.util.List;
import java.util.Map;
import java.util.stream.Stream;

/**
 * A state store implementation that delegates to implementations of a file reference and partition store.
 */
public class DelegatingStateStore implements StateStore {
    public static final Logger LOGGER = LoggerFactory.getLogger(DelegatingStateStore.class);
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
        if (fileReferences.isEmpty()) {
            LOGGER.info("Ignoring addFiles call with no files");
            return;
        }
        fileReferenceStore.addFiles(fileReferences);
    }

    @Override
    public void addFilesWithReferences(List<AllReferencesToAFile> files) throws StateStoreException {
        if (files.isEmpty()) {
            LOGGER.info("Ignoring addFilesWithReferences call with no files");
            return;
        }
        fileReferenceStore.addFilesWithReferences(files);
    }

    @Override
    public void splitFileReferences(List<SplitFileReferenceRequest> splitRequests) throws SplitRequestsFailedException {
        if (splitRequests.isEmpty()) {
            LOGGER.info("Ignoring splitFileReferences call with no requests");
            return;
        }
        fileReferenceStore.splitFileReferences(splitRequests);
    }

    @Override
    public void atomicallyReplaceFileReferencesWithNewOnes(List<ReplaceFileReferencesRequest> requests) throws ReplaceRequestsFailedException {
        fileReferenceStore.atomicallyReplaceFileReferencesWithNewOnes(requests);
    }

    @Override
    public void assignJobIds(List<AssignJobIdRequest> requests) throws StateStoreException {
        if (requests.isEmpty()) {
            LOGGER.info("Ignoring assignJobIds call with no requests");
            return;
        }
        fileReferenceStore.assignJobIds(requests);
    }

    @Override
    public void deleteGarbageCollectedFileReferenceCounts(List<String> filenames) throws StateStoreException {
        if (filenames.isEmpty()) {
            LOGGER.info("Ignoring deleteGarbageCollectedFileReferenceCounts call with no files");
            return;
        }
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
    public AllReferencesToAllFiles getAllFilesWithMaxUnreferenced(int maxUnreferencedFiles) throws StateStoreException {
        return fileReferenceStore.getAllFilesWithMaxUnreferenced(maxUnreferencedFiles);
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

    /**
     * Initialises just the file reference store.
     *
     * @throws StateStoreException thrown if the initialisation fails
     */
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
    public boolean hasNoFiles() throws StateStoreException {
        return fileReferenceStore.hasNoFiles();
    }

    @Override
    public void clearFileData() throws StateStoreException {
        fileReferenceStore.clearFileData();
    }

    @Override
    public void clearPartitionData() throws StateStoreException {
        partitionStore.clearPartitionData();
    }

    @Override
    public void fixFileUpdateTime(Instant now) {
        fileReferenceStore.fixFileUpdateTime(now);
    }

    @Override
    public void fixPartitionUpdateTime(Instant now) {
        partitionStore.fixPartitionUpdateTime(now);
    }
}
