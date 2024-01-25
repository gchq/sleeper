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
import sleeper.core.statestore.StateStoreException;

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
                () -> AllReferencesToAFile.newFilesWithReferences(fileReferences, updateTime).iterator()) {
            AllReferencesToAFile existingFile = filesByFilename.get(file.getFilename());
            if (existingFile != null) {
                Set<String> existingPartitionIds = existingFile.getInternalReferences().stream()
                        .map(FileReference::getPartitionId)
                        .collect(Collectors.toSet());
                if (file.getInternalReferences().stream()
                        .map(FileReference::getPartitionId)
                        .anyMatch(existingPartitionIds::contains)) {
                    throw new StateStoreException("File already exists for partition: " + file.getFilename());
                }
                file = existingFile.addReferences(file.getInternalReferences(), updateTime);
            }
            filesByFilename.put(file.getFilename(), file);
        }
    }

    @Override
    public List<FileReference> getActiveFiles() {
        return activeFiles().collect(toUnmodifiableList());
    }

    @Override
    public Stream<String> getReadyForGCFilenamesBefore(Instant maxUpdateTime) {
        List<String> filenames = filesByFilename.values().stream()
                .filter(file -> file.getTotalReferenceCount() < 1)
                .filter(file -> file.getLastUpdateTime().isBefore(maxUpdateTime))
                .map(AllReferencesToAFile::getFilename)
                .collect(toUnmodifiableList());
        return filenames.stream();
    }

    @Override
    public List<FileReference> getActiveFilesWithNoJobId() {
        return activeFiles()
                .filter(file -> file.getJobId() == null)
                .collect(toUnmodifiableList());
    }

    @Override
    public Map<String, List<String>> getPartitionToActiveFilesMap() {
        return activeFiles().collect(
                groupingBy(FileReference::getPartitionId,
                        mapping(FileReference::getFilename, toList())));
    }

    @Override
    public void splitFileReferences(List<SplitFileReferenceRequest> splitRequests) throws StateStoreException {
        Instant updateTime = clock.instant();
        for (SplitFileReferenceRequest splitRequest : splitRequests) {
            AllReferencesToAFile file = filesByFilename.get(splitRequest.getFilename());
            if (file == null) {
                throw new StateStoreException("File not found: " + splitRequest.getFilename());
            }
            Map<String, FileReference> referenceByPartitionId = file.getInternalReferences().stream()
                    .collect(Collectors.toMap(FileReference::getPartitionId, Function.identity()));
            if (!referenceByPartitionId.containsKey(splitRequest.getFromPartitionId())) {
                throw new StateStoreException("File reference not found in partition: " + splitRequest.getFilename());
            }
            for (FileReference newReference : splitRequest.getNewReferences()) {
                if (referenceByPartitionId.containsKey(newReference.getPartitionId())) {
                    throw new StateStoreException("File already exists for partition: " + splitRequest.getFilename());
                }
            }

            filesByFilename.put(splitRequest.getFilename(),
                    file.splitReferenceFromPartition(
                            splitRequest.getFromPartitionId(),
                            splitRequest.getNewReferences(),
                            updateTime));
        }
    }

    @Override
    public void atomicallyUpdateFilesToReadyForGCAndCreateNewActiveFiles(String jobId, String partitionId, List<String> filesToBeMarkedReadyForGC, List<FileReference> newFiles) throws StateStoreException {
        Set<String> newFilenames = newFiles.stream().map(FileReference::getFilename).collect(Collectors.toSet());
        for (String filename : filesToBeMarkedReadyForGC) {
            AllReferencesToAFile file = filesByFilename.get(filename);
            if (file == null) {
                throw new StateStoreException("File not found: " + filename);
            }
            Optional<FileReference> referenceOpt = file.getInternalReferences().stream()
                    .filter(ref -> partitionId.equals(ref.getPartitionId())).findFirst();
            if (referenceOpt.isEmpty()) {
                throw new StateStoreException("File reference not found in partition: " + partitionId);
            }
            FileReference reference = referenceOpt.get();
            if (!jobId.equals(reference.getJobId())) {
                throw new StateStoreException("File reference not assigned to job: " + jobId);
            }
            if (newFilenames.contains(filename)) {
                throw new StateStoreException("File reference has same filename as new file: " + filename);
            }
        }
        Instant updateTime = clock.instant();
        for (String filename : filesToBeMarkedReadyForGC) {
            filesByFilename.put(filename, filesByFilename.get(filename)
                    .removeReferenceForPartition(partitionId, updateTime));
        }
        addFiles(newFiles);
    }

    private Stream<FileReference> activeFiles() {
        return filesByFilename.values().stream()
                .flatMap(file -> file.getInternalReferences().stream());
    }

    @Override
    public void atomicallyUpdateJobStatusOfFiles(String jobId, List<FileReference> fileReferences) throws StateStoreException {
        Instant updateTime = clock.instant();
        Map<String, Set<String>> partitionIdsByFilename = new LinkedHashMap<>();
        for (FileReference requestedFile : fileReferences) {
            AllReferencesToAFile file = filesByFilename.get(requestedFile.getFilename());
            if (file == null) {
                throw new StateStoreException("File not found: " + requestedFile.getFilename());
            }
            Optional<FileReference> referenceOpt = file.getInternalReferences().stream()
                    .filter(ref -> requestedFile.getPartitionId().equals(ref.getPartitionId())).findFirst();
            if (referenceOpt.isEmpty()) {
                throw new StateStoreException("File reference not found in partition: " + requestedFile.getPartitionId());
            }
            FileReference reference = referenceOpt.get();
            if (reference.getJobId() != null) {
                throw new StateStoreException("Job ID already set: " + file);
            }
            partitionIdsByFilename.computeIfAbsent(requestedFile.getFilename(), filename -> new LinkedHashSet<>())
                    .add(requestedFile.getPartitionId());
        }
        partitionIdsByFilename.forEach((filename, partitionIds) ->
                filesByFilename.put(filename, filesByFilename.get(filename)
                        .withJobIdForPartitions(jobId, partitionIds, updateTime)));
    }

    @Override
    public void deleteReadyForGCFiles(List<String> filenames) throws StateStoreException {
        for (String filename : filenames) {
            AllReferencesToAFile file = filesByFilename.get(filename);
            if (file == null || file.getTotalReferenceCount() > 0) {
                throw new StateStoreException("File is not ready for garbage collection: " + filename);
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
