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
package sleeper.core.statestore.inmemory;

import sleeper.core.statestore.AllFileReferences;
import sleeper.core.statestore.AssignJobToFilesRequest;
import sleeper.core.statestore.FileInfo;
import sleeper.core.statestore.FileInfoStore;
import sleeper.core.statestore.FileReferenceCount;
import sleeper.core.statestore.StateStoreException;

import java.time.Clock;
import java.time.Instant;
import java.time.ZoneId;
import java.util.Iterator;
import java.util.LinkedHashMap;
import java.util.List;
import java.util.Map;
import java.util.function.BiFunction;
import java.util.stream.Stream;

import static java.util.stream.Collectors.groupingBy;
import static java.util.stream.Collectors.mapping;
import static java.util.stream.Collectors.toList;
import static java.util.stream.Collectors.toUnmodifiableList;
import static sleeper.core.statestore.FileInfo.FileStatus.READY_FOR_GARBAGE_COLLECTION;

public class InMemoryFileInfoStore implements FileInfoStore {

    private final Map<String, PartitionFiles> partitionById = new LinkedHashMap<>();
    private final Map<String, FileReferenceCount> referenceCountByFilename = new LinkedHashMap<>();
    private Clock clock = Clock.systemUTC();

    private class PartitionFiles {
        private final Map<String, FileInfo> activeFiles = new LinkedHashMap<>();
        private final Map<String, FileInfo> readyForGCFiles = new LinkedHashMap<>();

        void add(FileInfo fileInfo) throws StateStoreException {
            if (activeFiles.containsKey(fileInfo.getFilename())) {
                throw new StateStoreException("File already exists for partition: " + fileInfo);
            }
            activeFiles.put(fileInfo.getFilename(), fileInfo.toBuilder().lastStateStoreUpdateTime(clock.millis()).build());
            incrementReferences(fileInfo);
        }

        void moveToGC(FileInfo file) throws StateStoreException {
            if (!activeFiles.containsKey(file.getFilename())) {
                throw new StateStoreException("Cannot move to ready for GC as file is not active: " + file.getFilename());
            }
            activeFiles.remove(file.getFilename());
            readyForGCFiles.put(file.getFilename(),
                    file.toBuilder().fileStatus(READY_FOR_GARBAGE_COLLECTION)
                            .lastStateStoreUpdateTime(clock.millis())
                            .build());
            decrementReferences(file);
        }

        boolean isEmpty() {
            return activeFiles.isEmpty() && readyForGCFiles.isEmpty();
        }
    }

    @Override
    public void addFile(FileInfo fileInfo) throws StateStoreException {
        partitionById.computeIfAbsent(fileInfo.getPartitionId(), partitionId -> new PartitionFiles())
                .add(fileInfo);
    }

    @Override
    public void addFiles(List<FileInfo> fileInfos) throws StateStoreException {
        for (FileInfo fileInfo : fileInfos) {
            addFile(fileInfo);
        }
    }

    @Override
    public List<FileInfo> getActiveFiles() {
        return activeFiles().collect(toUnmodifiableList());
    }

    @Override
    public Iterator<FileInfo> getReadyForGCFiles() {
        return partitionById.values().stream()
                .flatMap(partition -> partition.readyForGCFiles.values().stream())
                .iterator();
    }

    @Override
    public Stream<String> getReadyForGCFilenamesBefore(Instant maxUpdateTime) {
        return referenceCountByFilename.values().stream()
                .filter(file -> file.getReferences() < 1)
                .filter(file -> file.getLastUpdateTime().isBefore(maxUpdateTime))
                .map(FileReferenceCount::getFilename);
    }

    @Override
    public List<FileInfo> getActiveFilesWithNoJobId() {
        return activeFiles()
                .filter(file -> file.getJobId() == null)
                .collect(toUnmodifiableList());
    }

    @Override
    public Map<String, List<String>> getPartitionToActiveFilesMap() {
        return activeFiles().collect(
                groupingBy(FileInfo::getPartitionId,
                        mapping(FileInfo::getFilename, toList())));
    }

    @Override
    public void atomicallyUpdateFilesToReadyForGCAndCreateNewActiveFiles(List<FileInfo> filesToBeMarkedReadyForGC, List<FileInfo> newFiles) throws StateStoreException {
        for (FileInfo file : filesToBeMarkedReadyForGC) {
            partitionById.get(file.getPartitionId()).moveToGC(file);
        }
        addFiles(newFiles);
    }

    private Stream<FileInfo> activeFiles() {
        return partitionById.values().stream()
                .flatMap(partition -> partition.activeFiles.values().stream());
    }

    @Override
    public void atomicallyUpdateJobStatusOfFiles(String jobId, List<FileInfo> fileInfos) throws StateStoreException {
        String partitionId = fileInfos.get(0).getPartitionId();
        atomicallyUpdateJobStatusOfFiles(AssignJobToFilesRequest.builder()
                .tableId("unknown").jobId(jobId).partitionId(partitionId)
                .files(fileInfos.stream().map(FileInfo::getFilename).collect(toUnmodifiableList()))
                .build());
    }

    @Override
    public void atomicallyUpdateJobStatusOfFiles(AssignJobToFilesRequest job) throws StateStoreException {
        PartitionFiles partition = partitionById.get(job.getPartitionId());
        if (partition == null) {
            throw new StateStoreException("Partition contains no files: " + job.getPartitionId());
        }
        for (String filename : job.getFiles()) {
            FileInfo file = partition.activeFiles.get(filename);
            if (file == null) {
                throw new StateStoreException("File not found in partition " + job.getPartitionId() + ": " + filename);
            }
            if (file.getJobId() != null) {
                throw new StateStoreException("Job ID already set: " + file);
            }
        }
        for (String filename : job.getFiles()) {
            FileInfo file = partition.activeFiles.get(filename);
            partition.activeFiles.put(filename, file.toBuilder().jobId(job.getJobId())
                    .lastStateStoreUpdateTime(clock.millis()).build());
        }
    }

    @Override
    public void deleteReadyForGCFile(FileInfo fileInfo) throws StateStoreException {
        PartitionFiles partition = partitionById.get(fileInfo.getPartitionId());
        FileReferenceCount count = referenceCountByFilename.get(fileInfo.getFilename());
        if (count == null || count.getReferences() > 0) {
            throw new StateStoreException("File is not ready for garbage collection: " + fileInfo.getFilename());
        }
        partition.readyForGCFiles.remove(fileInfo.getFilename());
        if (partition.isEmpty()) {
            partitionById.remove(fileInfo.getPartitionId());
        }
        referenceCountByFilename.remove(fileInfo.getFilename());
    }

    @Override
    public void deleteReadyForGCFile(String filename) throws StateStoreException {
        FileReferenceCount count = referenceCountByFilename.get(filename);
        if (count == null || count.getReferences() > 0) {
            throw new StateStoreException("File is not ready for garbage collection: " + filename);
        }
        List<Map.Entry<String, PartitionFiles>> partitions = partitionById.entrySet().stream()
                .filter(entry -> entry.getValue().readyForGCFiles.containsKey(filename))
                .collect(toUnmodifiableList());
        partitions.forEach(entry -> {
            PartitionFiles files = entry.getValue();
            files.readyForGCFiles.remove(filename);
            if (files.isEmpty()) {
                partitionById.remove(entry.getKey());
            }
        });
        referenceCountByFilename.remove(filename);
    }

    @Override
    public AllFileReferences getAllFileReferences() {
        return AllFileReferences.fromActiveFilesAndReferenceCounts(
                partitionById.values().stream()
                        .flatMap(files -> files.activeFiles.values().stream()),
                referenceCountByFilename.values().stream());
    }

    @Override
    public void initialise() {

    }

    @Override
    public boolean hasNoFiles() {
        return partitionById.isEmpty();
    }

    @Override
    public void clearTable() {
        partitionById.clear();
    }

    @Override
    public void fixTime(Instant now) {
        clock = Clock.fixed(now, ZoneId.of("UTC"));
    }

    private void incrementReferences(FileInfo fileInfo) {
        updateReferenceCount(fileInfo.getFilename(), FileReferenceCount::increment);
    }

    private void decrementReferences(FileInfo fileInfo) {
        updateReferenceCount(fileInfo.getFilename(), FileReferenceCount::decrement);
    }

    private void updateReferenceCount(String filename, BiFunction<FileReferenceCount, Instant, FileReferenceCount> update) {
        FileReferenceCount before = referenceCountByFilename.get(filename);
        if (before == null) {
            before = FileReferenceCount.empty(filename);
        }
        referenceCountByFilename.put(filename, update.apply(before, clock.instant()));
    }
}
