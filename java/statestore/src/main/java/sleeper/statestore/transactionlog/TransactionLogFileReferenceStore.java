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
package sleeper.statestore.transactionlog;

import sleeper.core.statestore.AllReferencesToAFile;
import sleeper.core.statestore.AllReferencesToAllFiles;
import sleeper.core.statestore.AssignJobIdRequest;
import sleeper.core.statestore.FileReference;
import sleeper.core.statestore.FileReferenceStore;
import sleeper.core.statestore.SplitFileReferenceRequest;
import sleeper.core.statestore.SplitRequestsFailedException;
import sleeper.core.statestore.StateStoreException;

import java.time.Instant;
import java.util.List;
import java.util.stream.Stream;

class TransactionLogFileReferenceStore implements FileReferenceStore {

    @Override
    public void addFilesWithReferences(List<AllReferencesToAFile> files) throws StateStoreException {
    }

    @Override
    public void assignJobIds(List<AssignJobIdRequest> requests) throws StateStoreException {
    }

    @Override
    public void atomicallyReplaceFileReferencesWithNewOne(String jobId, String partitionId, List<String> inputFiles, FileReference newReference) throws StateStoreException {
    }

    @Override
    public void clearFileData() {
    }

    @Override
    public void deleteGarbageCollectedFileReferenceCounts(List<String> filenames) throws StateStoreException {
    }

    @Override
    public void fixTime(Instant time) {
    }

    @Override
    public AllReferencesToAllFiles getAllFilesWithMaxUnreferenced(int maxUnreferencedFiles) throws StateStoreException {
        return null;
    }

    @Override
    public List<FileReference> getFileReferences() throws StateStoreException {
        return null;
    }

    @Override
    public List<FileReference> getFileReferencesWithNoJobId() throws StateStoreException {
        return null;
    }

    @Override
    public Stream<String> getReadyForGCFilenamesBefore(Instant maxUpdateTime) throws StateStoreException {
        return null;
    }

    @Override
    public boolean hasNoFiles() {
        return true;
    }

    @Override
    public void initialise() throws StateStoreException {
    }

    @Override
    public void splitFileReferences(List<SplitFileReferenceRequest> splitRequests) throws SplitRequestsFailedException {
    }

}
