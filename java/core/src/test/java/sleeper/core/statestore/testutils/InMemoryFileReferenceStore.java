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
package sleeper.core.statestore.testutils;

import org.slf4j.Logger;
import org.slf4j.LoggerFactory;

import sleeper.core.statestore.AllReferencesToAFile;
import sleeper.core.statestore.AllReferencesToAllFiles;
import sleeper.core.statestore.AssignJobIdRequest;
import sleeper.core.statestore.FileReference;
import sleeper.core.statestore.FileReferenceStore;
import sleeper.core.statestore.ReplaceFileReferencesRequest;
import sleeper.core.statestore.SplitFileReferenceRequest;
import sleeper.core.statestore.StateStoreException;
import sleeper.core.statestore.exception.FileAlreadyExistsException;
import sleeper.core.statestore.exception.FileHasReferencesException;
import sleeper.core.statestore.exception.FileNotFoundException;
import sleeper.core.statestore.exception.FileReferenceAlreadyExistsException;
import sleeper.core.statestore.exception.FileReferenceAssignedToJobException;
import sleeper.core.statestore.exception.FileReferenceNotAssignedToJobException;
import sleeper.core.statestore.exception.FileReferenceNotFoundException;
import sleeper.core.statestore.exception.NewReferenceSameAsOldReferenceException;
import sleeper.core.statestore.exception.ReplaceRequestsFailedException;
import sleeper.core.statestore.exception.SplitRequestsFailedException;
import sleeper.core.statestore.transactionlog.StateStoreFile;
import sleeper.core.statestore.transactionlog.StateStoreFiles;

import java.time.Clock;
import java.time.Instant;
import java.time.ZoneId;
import java.util.ArrayList;
import java.util.List;
import java.util.Optional;
import java.util.stream.Stream;

import static java.util.stream.Collectors.toUnmodifiableList;

/**
 * An in-memory file reference store implementation backed by a TreeMap.
 */
public class InMemoryFileReferenceStore implements FileReferenceStore {
    public static final Logger LOGGER = LoggerFactory.getLogger(InMemoryFileReferenceStore.class);

    private final StateStoreFiles state = new StateStoreFiles();
    private QueryFailureChecker queryFailureChecker = () -> Optional.empty();
    private Clock clock = Clock.systemUTC();

    @Override
    public void addFilesWithReferences(List<AllReferencesToAFile> files) throws StateStoreException {
        Instant updateTime = clock.instant();
        for (AllReferencesToAFile file : files) {
            if (state.file(file.getFilename()).isPresent()) {
                throw new FileAlreadyExistsException(file.getFilename());
            }
            state.add(StateStoreFile.newFile(updateTime, file));
        }
    }

    @Override
    public List<FileReference> getFileReferences() throws StateStoreException {
        return streamFileReferences().collect(toUnmodifiableList());
    }

    @Override
    public Stream<String> getReadyForGCFilenamesBefore(Instant maxUpdateTime) throws StateStoreException {
        checkQueryFailure();
        List<String> filenames = state.referencedAndUnreferenced().stream()
                .filter(file -> file.getReferences().isEmpty())
                .filter(file -> file.getLastStateStoreUpdateTime().isBefore(maxUpdateTime))
                .map(StateStoreFile::getFilename)
                .collect(toUnmodifiableList());
        return filenames.stream();
    }

    @Override
    public List<FileReference> getFileReferencesWithNoJobId() throws StateStoreException {
        return streamFileReferences()
                .filter(file -> file.getJobId() == null)
                .collect(toUnmodifiableList());
    }

    @Override
    public void splitFileReferences(List<SplitFileReferenceRequest> splitRequests) throws SplitRequestsFailedException {
        Instant updateTime = clock.instant();
        int firstUnappliedRequestIndex = 0;
        for (SplitFileReferenceRequest splitRequest : splitRequests) {
            Optional<StateStoreException> failure = validateSplitRequest(splitRequest);
            if (failure.isPresent()) {
                throw splitRequestsFailed(splitRequests, firstUnappliedRequestIndex, failure.get());
            }

            state.updateFile(splitRequest.getFilename(),
                    file -> file.splitReferenceFromPartition(
                            splitRequest.getFromPartitionId(),
                            splitRequest.getNewReferences(),
                            updateTime));
            firstUnappliedRequestIndex++;
        }
    }

    private Optional<StateStoreException> validateSplitRequest(SplitFileReferenceRequest request) {
        StateStoreFile existingFile = state.file(request.getFilename()).orElse(null);
        if (existingFile == null) {
            return Optional.of(new FileNotFoundException(request.getFilename()));
        }
        FileReference oldReference = existingFile.getReferenceForPartitionId(request.getFromPartitionId()).orElse(null);
        if (oldReference == null) {
            return Optional.of(new FileReferenceNotFoundException(request.getFilename(), request.getFromPartitionId()));
        }
        if (oldReference.getJobId() != null) {
            return Optional.of(new FileReferenceAssignedToJobException(oldReference));
        }
        for (FileReference newReference : request.getNewReferences()) {
            FileReference existingReference = existingFile.getReferenceForPartitionId(newReference.getPartitionId()).orElse(null);
            if (existingReference != null) {
                return Optional.of(new FileReferenceAlreadyExistsException(existingReference));
            }
        }
        return Optional.empty();
    }

    private static SplitRequestsFailedException splitRequestsFailed(
            List<SplitFileReferenceRequest> splitRequests, int firstUnappliedRequestIndex, StateStoreException cause) {
        return new SplitRequestsFailedException(
                splitRequests.subList(0, firstUnappliedRequestIndex),
                splitRequests.subList(firstUnappliedRequestIndex, splitRequests.size()), cause);
    }

    @Override
    public void atomicallyReplaceFileReferencesWithNewOnes(List<ReplaceFileReferencesRequest> requests) throws ReplaceRequestsFailedException {
        List<ReplaceFileReferencesRequest> succeeded = new ArrayList<>();
        List<ReplaceFileReferencesRequest> failed = new ArrayList<>();
        List<Exception> failures = new ArrayList<>();
        for (ReplaceFileReferencesRequest request : requests) {
            try {
                atomicallyReplaceFileReferencesWithNewOne(request);
                succeeded.add(request);
            } catch (RuntimeException e) {
                LOGGER.error("Failed replacing file references for job {}", request.getJobId(), e);
                failures.add(e);
                failed.add(request);
            }
        }
        if (!failures.isEmpty()) {
            throw new ReplaceRequestsFailedException(succeeded, failed, failures);
        }
    }

    private void atomicallyReplaceFileReferencesWithNewOne(ReplaceFileReferencesRequest request) throws StateStoreException {
        for (String filename : request.getInputFiles()) {
            StateStoreFile file = state.file(filename).orElse(null);
            if (file == null) {
                throw new FileNotFoundException(filename);
            }
            Optional<FileReference> referenceOpt = file.getReferences().stream()
                    .filter(ref -> request.getPartitionId().equals(ref.getPartitionId())).findFirst();
            if (referenceOpt.isEmpty()) {
                throw new FileReferenceNotFoundException(filename, request.getPartitionId());
            }
            FileReference reference = referenceOpt.get();
            if (!request.getJobId().equals(reference.getJobId())) {
                throw new FileReferenceNotAssignedToJobException(reference, request.getJobId());
            }
            if (filename.equals(request.getNewReference().getFilename())) {
                throw new NewReferenceSameAsOldReferenceException(filename);
            }
        }
        if (state.file(request.getNewReference().getFilename()).isPresent()) {
            throw new FileAlreadyExistsException(request.getNewReference().getFilename());
        }

        Instant updateTime = clock.instant();
        for (String filename : request.getInputFiles()) {
            state.updateFile(filename, file -> file.removeReferenceForPartition(request.getPartitionId(), updateTime));
        }
        state.add(StateStoreFile.newFile(updateTime, request.getNewReference()));
    }

    private Stream<FileReference> streamFileReferences() throws StateStoreException {
        checkQueryFailure();
        return state.references();
    }

    private void checkQueryFailure() throws StateStoreException {
        Optional<Exception> failureOpt = queryFailureChecker.getNextQueryFailure();
        if (!failureOpt.isPresent()) {
            return;
        }
        Exception failure = failureOpt.get();
        if (failure instanceof StateStoreException) {
            throw (StateStoreException) failure;
        } else if (failure instanceof RuntimeException) {
            throw (RuntimeException) failure;
        } else {
            throw new IllegalStateException("Unexpected exception type: " + failure.getClass());
        }
    }

    @Override
    public void assignJobIds(List<AssignJobIdRequest> requests) throws StateStoreException {
        Instant updateTime = clock.instant();
        for (AssignJobIdRequest request : requests) {
            assignJobId(request, updateTime);
        }
    }

    private void assignJobId(AssignJobIdRequest request, Instant updateTime) throws StateStoreException {
        for (String filename : request.getFilenames()) {
            StateStoreFile existingFile = state.file(filename).orElse(null);
            if (existingFile == null) {
                throw new FileReferenceNotFoundException(filename, request.getPartitionId());
            }
            FileReference existingReference = existingFile.getReferenceForPartitionId(request.getPartitionId()).orElse(null);
            if (existingReference == null) {
                throw new FileReferenceNotFoundException(filename, request.getPartitionId());
            }
            if (existingReference.getJobId() != null) {
                throw new FileReferenceAssignedToJobException(existingReference);
            }
        }
        for (String filename : request.getFilenames()) {
            state.updateFile(filename, file -> file.setJobIdForPartition(request.getJobId(), request.getPartitionId(), updateTime));
        }
    }

    @Override
    public void deleteGarbageCollectedFileReferenceCounts(List<String> filenames) throws StateStoreException {
        for (String filename : filenames) {
            StateStoreFile file = state.file(filename).orElse(null);
            if (file == null) {
                throw new FileNotFoundException(filename);
            } else if (!file.getReferences().isEmpty()) {
                throw new FileHasReferencesException(file.toModel());
            }
        }
        filenames.forEach(state::remove);
    }

    @Override
    public AllReferencesToAllFiles getAllFilesWithMaxUnreferenced(int maxUnreferencedFiles) {
        return state.allReferencesToAllFiles(maxUnreferencedFiles);
    }

    @Override
    public void initialise() {

    }

    @Override
    public boolean hasNoFiles() {
        return state.isEmpty();
    }

    @Override
    public void clearFileData() {
        state.clear();
    }

    @Override
    public void fixFileUpdateTime(Instant now) {
        clock = Clock.fixed(now, ZoneId.of("UTC"));
    }

    /**
     * Sets whether the expected queries will fail, which which exceptions. Any time files are queried against this
     * class, the next element in this list will be checked and the exception thrown if any. If more queries are made
     * those will fail with a NoSuchElementException.
     *
     * @param failures the failures for each expected query
     */
    public void setFailuresForExpectedQueries(List<Optional<Exception>> failures) {
        queryFailureChecker = failures.iterator()::next;
    }

    /**
     * Helper to check whether the next query to the file reference store should fail.
     */
    private interface QueryFailureChecker {

        Optional<Exception> getNextQueryFailure();
    }
}
