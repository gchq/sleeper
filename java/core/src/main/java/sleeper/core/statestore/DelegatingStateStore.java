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
import sleeper.core.statestore.transactionlog.AddTransactionRequest;

import java.time.Instant;
import java.util.List;
import java.util.Map;
import java.util.stream.Stream;

/**
 * A state store implementation that delegates to implementations of a file reference and partition store.
 */
public abstract class DelegatingStateStore implements StateStore {
    public static final Logger LOGGER = LoggerFactory.getLogger(DelegatingStateStore.class);
    private final FileReferenceStore fileReferenceStore;
    private final PartitionStore partitionStore;

    public DelegatingStateStore(FileReferenceStore fileReferenceStore, PartitionStore partitionStore) {
        this.fileReferenceStore = fileReferenceStore;
        this.partitionStore = partitionStore;
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
    public void atomicallyReplaceFileReferencesWithNewOnes(List<ReplaceFileReferencesRequest> requests) throws ReplaceRequestsFailedException {
        fileReferenceStore.atomicallyReplaceFileReferencesWithNewOnes(requests);
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
    public List<Partition> getAllPartitions() throws StateStoreException {
        return partitionStore.getAllPartitions();
    }

    @Override
    public List<Partition> getLeafPartitions() throws StateStoreException {
        return partitionStore.getLeafPartitions();
    }

    @Override
    public Partition getPartition(String partitionId) throws StateStoreException {
        return partitionStore.getPartition(partitionId);
    }

    @Override
    public boolean hasNoFiles() throws StateStoreException {
        return fileReferenceStore.hasNoFiles();
    }

    @Override
    public void fixFileUpdateTime(Instant now) {
        fileReferenceStore.fixFileUpdateTime(now);
    }

    @Override
    public void fixPartitionUpdateTime(Instant now) {
        partitionStore.fixPartitionUpdateTime(now);
    }

    @Override
    public void addFilesTransaction(AddTransactionRequest request) {
        if (request.checkBeforeAdd(this)) {
            fileReferenceStore.addFilesTransaction(request);
        }
    }

    @Override
    public void addPartitionsTransaction(AddTransactionRequest request) {
        if (request.checkBeforeAdd(this)) {
            partitionStore.addPartitionsTransaction(request);
        }
    }
}
