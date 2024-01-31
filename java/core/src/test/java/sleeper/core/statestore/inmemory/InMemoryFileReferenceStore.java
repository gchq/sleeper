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
package sleeper.core.statestore.inmemory;

import sleeper.core.statestore.AllReferencesToAFile;
import sleeper.core.statestore.AllReferencesToAllFiles;
import sleeper.core.statestore.FileReference;
import sleeper.core.statestore.FileReferenceStore;
import sleeper.core.statestore.SplitFileReferenceRequest;
import sleeper.core.statestore.SplitRequestsFailedException;
import sleeper.core.statestore.StateStoreException;
import sleeper.core.statestore.exception.FileAlreadyExistsException;
import sleeper.core.statestore.exception.FileHasReferencesException;
import sleeper.core.statestore.exception.FileNotFoundException;
import sleeper.core.statestore.exception.FileReferenceAlreadyExistsException;
import sleeper.core.statestore.exception.FileReferenceAssignedToJobException;
import sleeper.core.statestore.exception.FileReferenceNotAssignedToJobException;
import sleeper.core.statestore.exception.FileReferenceNotFoundException;
import sleeper.core.statestore.exception.NewReferenceSameAsOldReferenceException;

import java.time.Clock;
import java.time.Instant;
import java.time.ZoneId;
import java.util.LinkedHashMap;
import java.util.LinkedHashSet;
import java.util.List;
import java.util.Map;
import java.util.Optional;
import java.util.Set;
import java.util.TreeMap;
import java.util.function.Function;
import java.util.stream.Collectors;
import java.util.stream.Stream;

import static java.util.stream.Collectors.groupingBy;
import static java.util.stream.Collectors.mapping;
import static java.util.stream.Collectors.toList;
import static java.util.stream.Collectors.toUnmodifiableList;
import static sleeper.core.statestore.AllReferencesToAFile.fileWithOneReference;

public class InMemoryFileReferenceStore implements FileReferenceStore {

    private final Map<String, AllReferencesToAFile> filesByFilename = new TreeMap<>();
    private Clock clock = Clock.systemUTC();

    @Override
    public void addFile(FileReference fileReference) throws StateStoreException {
        addFiles(List.of(fileReference));
    }

    @Override
    public void addFiles(List<FileReference> fileReferences) throws StateStoreException {
        Instant updateTime = clock.instant();
        for (AllReferencesToAFile file : (Iterable<AllReferencesToAFile>)
                () -> AllReferencesToAFile.newFilesWithReferences(fileReferences.stream(), updateTime).iterator()) {
            AllReferencesToAFile existingFile = filesByFilename.get(file.getFilename());
            if (existingFile != null) {
                Set<String> existingPartitionIds = existingFile.getInternalReferences().stream()
                        .map(FileReference::getPartitionId)
                        .collect(Collectors.toSet());
                Optional<FileReference> fileInPartition = file.getInternalReferences().stream()
                        .filter(fileReference -> existingPartitionIds.contains(fileReference.getPartitionId()))
                        .findFirst();
                if (fileInPartition.isPresent()) {
                    throw new FileReferenceAlreadyExistsException(fileInPartition.get());
                }
                file = existingFile.addReferences(file.getInternalReferences(), updateTime);
            }
            filesByFilename.put(file.getFilename(), file);
        }
    }

    @Override
    public void addFilesWithReferences(List<AllReferencesToAFile> files) throws StateStoreException {
        Instant updateTime = clock.instant();
        for (AllReferencesToAFile file : files) {
            if (filesByFilename.containsKey(file.getFilename())) {
                throw new FileAlreadyExistsException(file.getFilename());
            }
            filesByFilename.put(file.getFilename(), file.withCreatedUpdateTime(updateTime));
        }
    }

    @Override
    public List<FileReference> getFileReferences() {
        return streamFileReferences().collect(toUnmodifiableList());
    }

    @Override
    public Stream<String> getReadyForGCFilenamesBefore(Instant maxUpdateTime) {
        List<String> filenames = filesByFilename.values().stream()
                .filter(file -> file.getTotalReferenceCount() < 1)
                .filter(file -> file.getLastStateStoreUpdateTime().isBefore(maxUpdateTime))
                .map(AllReferencesToAFile::getFilename)
                .collect(toUnmodifiableList());
        return filenames.stream();
    }

    @Override
    public List<FileReference> getFileReferencesWithNoJobId() {
        return streamFileReferences()
                .filter(file -> file.getJobId() == null)
                .collect(toUnmodifiableList());
    }

    @Override
    public Map<String, List<String>> getPartitionToReferencedFilesMap() {
        return streamFileReferences().collect(
                groupingBy(FileReference::getPartitionId,
                        mapping(FileReference::getFilename, toList())));
    }

    @Override
    public void splitFileReferences(List<SplitFileReferenceRequest> splitRequests) throws StateStoreException {
        Instant updateTime = clock.instant();
        int requestIndex = 0;
        for (SplitFileReferenceRequest splitRequest : splitRequests) {
            AllReferencesToAFile file = filesByFilename.get(splitRequest.getFilename());
            if (file == null) {
                throw splitRequestFailed(splitRequests, requestIndex, new FileNotFoundException(splitRequest.getFilename()));
            }
            Map<String, FileReference> referenceByPartitionId = file.getInternalReferences().stream()
                    .collect(Collectors.toMap(FileReference::getPartitionId, Function.identity()));
            if (!referenceByPartitionId.containsKey(splitRequest.getFromPartitionId())) {
                throw splitRequestFailed(splitRequests, requestIndex, new FileReferenceNotFoundException(file.getFilename(), splitRequest.getFromPartitionId()));
            }
            FileReference reference = referenceByPartitionId.get(splitRequest.getFromPartitionId());
            if (reference.getJobId() != null) {
                throw splitRequestFailed(splitRequests, requestIndex, new FileReferenceAssignedToJobException(reference));
            }
            for (FileReference newReference : splitRequest.getNewReferences()) {
                if (referenceByPartitionId.containsKey(newReference.getPartitionId())) {
                    throw splitRequestFailed(splitRequests, requestIndex, new FileReferenceAlreadyExistsException(referenceByPartitionId.get(newReference.getPartitionId())));
                }
            }

            filesByFilename.put(splitRequest.getFilename(),
                    file.splitReferenceFromPartition(
                            splitRequest.getFromPartitionId(),
                            splitRequest.getNewReferences(),
                            updateTime));
            requestIndex++;
        }
    }

    private static SplitRequestsFailedException splitRequestFailed(List<SplitFileReferenceRequest> splitRequests, int requestIndex, StateStoreException cause) {
        return new SplitRequestsFailedException(splitRequests.subList(0, requestIndex), splitRequests.subList(requestIndex, splitRequests.size()), cause);
    }

    @Override
    public void atomicallyReplaceFileReferencesWithNewOne(String jobId, String partitionId, List<String> inputFiles, FileReference newReference) throws StateStoreException {
        for (String filename : inputFiles) {
            AllReferencesToAFile file = filesByFilename.get(filename);
            if (file == null) {
                throw new FileNotFoundException(filename);
            }
            Optional<FileReference> referenceOpt = file.getInternalReferences().stream()
                    .filter(ref -> partitionId.equals(ref.getPartitionId())).findFirst();
            if (referenceOpt.isEmpty()) {
                throw new FileReferenceNotFoundException(filename, partitionId);
            }
            FileReference reference = referenceOpt.get();
            if (!jobId.equals(reference.getJobId())) {
                throw new FileReferenceNotAssignedToJobException(reference, jobId);
            }
            if (filename.equals(newReference.getFilename())) {
                throw new NewReferenceSameAsOldReferenceException(filename);
            }
        }

        Instant updateTime = clock.instant();
        for (String filename : inputFiles) {
            filesByFilename.put(filename, filesByFilename.get(filename)
                    .removeReferenceForPartition(partitionId, updateTime));
        }
        filesByFilename.put(newReference.getFilename(), fileWithOneReference(newReference, updateTime));
    }

    private Stream<FileReference> streamFileReferences() {
        return filesByFilename.values().stream()
                .flatMap(file -> file.getInternalReferences().stream());
    }

    @Override
    public void atomicallyAssignJobIdToFileReferences(String jobId, List<FileReference> fileReferences) throws StateStoreException {
        Instant updateTime = clock.instant();
        Map<String, Set<String>> partitionIdsByFilename = new LinkedHashMap<>();
        for (FileReference reference : fileReferences) {
            AllReferencesToAFile existingFile = filesByFilename.get(reference.getFilename());
            if (existingFile == null) {
                throw new FileNotFoundException(reference.getFilename());
            }
            FileReference existingReference = existingFile.getReferenceForPartitionId(reference.getPartitionId()).orElse(null);
            if (existingReference == null) {
                throw new FileReferenceNotFoundException(reference);
            }
            if (existingReference.getJobId() != null) {
                throw new FileReferenceAssignedToJobException(existingReference);
            }
            partitionIdsByFilename.computeIfAbsent(reference.getFilename(), filename -> new LinkedHashSet<>())
                    .add(reference.getPartitionId());
        }
        partitionIdsByFilename.forEach((filename, partitionIds) ->
                filesByFilename.put(filename, filesByFilename.get(filename)
                        .withJobIdForPartitions(jobId, partitionIds, updateTime)));
    }

    @Override
    public void deleteGarbageCollectedFileReferenceCounts(List<String> filenames) throws StateStoreException {
        for (String filename : filenames) {
            AllReferencesToAFile file = filesByFilename.get(filename);
            if (file == null) {
                throw new FileNotFoundException(filename);
            } else if (file.getTotalReferenceCount() > 0) {
                throw new FileHasReferencesException(file);
            }
        }
        filenames.forEach(filesByFilename::remove);
    }

    @Override
    public AllReferencesToAllFiles getAllFileReferencesWithMaxUnreferenced(int maxUnreferencedFiles) {
        List<AllReferencesToAFile> filesWithNoReferences = filesByFilename.values().stream()
                .filter(file -> file.getTotalReferenceCount() < 1)
                .collect(toUnmodifiableList());
        List<AllReferencesToAFile> files = Stream.concat(
                        filesByFilename.values().stream()
                                .filter(file -> file.getTotalReferenceCount() > 0),
                        filesWithNoReferences.stream().limit(maxUnreferencedFiles))
                .collect(toUnmodifiableList());
        return new AllReferencesToAllFiles(files, filesWithNoReferences.size() > maxUnreferencedFiles);
    }

    @Override
    public void initialise() {

    }

    @Override
    public boolean hasNoFiles() {
        return filesByFilename.isEmpty();
    }

    @Override
    public void clearFileData() {
        filesByFilename.clear();
    }

    @Override
    public void fixTime(Instant now) {
        clock = Clock.fixed(now, ZoneId.of("UTC"));
    }
}
