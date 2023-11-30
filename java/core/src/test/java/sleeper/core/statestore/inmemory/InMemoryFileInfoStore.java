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
import java.util.stream.Collectors;
import java.util.stream.Stream;

import static java.util.stream.Collectors.groupingBy;
import static java.util.stream.Collectors.mapping;
import static java.util.stream.Collectors.toList;
import static java.util.stream.Collectors.toUnmodifiableList;
import static sleeper.core.statestore.FileInfo.FileStatus.READY_FOR_GARBAGE_COLLECTION;

public class InMemoryFileInfoStore implements FileInfoStore {
    private static final String DEFAULT_TABLE_ID = "test-table-id";
    private final Map<String, PartitionFiles> partitionById = new LinkedHashMap<>();
    private final Map<String, FileReferenceCount> fileReferenceCounts = new LinkedHashMap<>();
    private Clock clock = Clock.systemUTC();
    private final String tableId;

    private class PartitionFiles {
        private final Map<String, FileInfo> activeFiles = new LinkedHashMap<>();
        private final Map<String, FileInfo> readyForGCFiles = new LinkedHashMap<>();

        void add(FileInfo fileInfo) {
            activeFiles.put(fileInfo.getFilename(), fileInfo.toBuilder().lastStateStoreUpdateTime(clock.millis()).build());
        }

        void moveToGC(FileInfo file) {
            activeFiles.remove(file.getFilename());
            readyForGCFiles.put(file.getFilename(),
                    file.toBuilder().fileStatus(READY_FOR_GARBAGE_COLLECTION)
                            .lastStateStoreUpdateTime(clock.millis())
                            .build());
        }

        boolean isEmpty() {
            return activeFiles.isEmpty() && readyForGCFiles.isEmpty();
        }
    }

    public InMemoryFileInfoStore() {
        this(DEFAULT_TABLE_ID);
    }

    public InMemoryFileInfoStore(String tableId) {
        this.tableId = tableId;
    }

    @Override
    public void addFile(FileInfo fileInfo) {
        partitionById.computeIfAbsent(fileInfo.getPartitionId(), partitionId -> new PartitionFiles())
                .add(fileInfo.toBuilder().lastStateStoreUpdateTime(clock.millis()).build());
        FileReferenceCount fileReferenceCount = fileReferenceCounts.getOrDefault(fileInfo.getFilename(),
                FileReferenceCount.builder()
                        .tableId(tableId)
                        .filename(fileInfo.getFilename())
                        .lastUpdateTime(clock.millis())
                        .numberOfReferences(0L)
                        .build());
        fileReferenceCounts.put(fileInfo.getFilename(), fileReferenceCount.increment());
    }

    @Override
    public void addFiles(List<FileInfo> fileInfos) {
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
    public List<FileInfo> getActiveFilesWithNoJobId() {
        return activeFiles()
                .filter(file -> file.getJobId() == null)
                .collect(Collectors.toUnmodifiableList());
    }

    @Override
    public long getFileReferenceCount(String filename) {
        return fileReferenceCounts.get(filename).getNumberOfReferences();
    }

    @Override
    public Map<String, List<String>> getPartitionToActiveFilesMap() {
        return activeFiles().collect(
                groupingBy(FileInfo::getPartitionId,
                        mapping(FileInfo::getFilename, toList())));
    }

    @Override
    public void atomicallyUpdateFilesToReadyForGCAndCreateNewActiveFile(List<FileInfo> filesToBeMarkedReadyForGC, FileInfo newActiveFile) {
        filesToBeMarkedReadyForGC.forEach(fileReference -> {
            this.removeFileReference(fileReference);
            this.moveToGC(fileReference);
        });
        addFile(newActiveFile);
    }

    @Override
    public void atomicallyUpdateFilesToReadyForGCAndCreateNewActiveFiles(List<FileInfo> filesToBeMarkedReadyForGC, List<FileInfo> newFiles) {
        filesToBeMarkedReadyForGC.forEach(fileReference -> {
            this.removeFileReference(fileReference);
            this.moveToGC(fileReference);
        });
        addFiles(newFiles);
    }

    private Stream<FileInfo> activeFiles() {
        return partitionById.values().stream()
                .flatMap(partition -> partition.activeFiles.values().stream());
    }

    private void moveToGC(FileInfo file) {
        partitionById.get(file.getPartitionId()).moveToGC(file);
    }

    private void removeFileReference(FileInfo fileReference) {
        fileReferenceCounts.put(fileReference.getFilename(),
                fileReferenceCounts.get(fileReference.getFilename()).decrement());
    }

    @Override
    public void atomicallyUpdateJobStatusOfFiles(String jobId, List<FileInfo> fileInfos) throws StateStoreException {
        List<String> filenamesWithJobId = findFilenamesWithJobIdSet(fileInfos);
        if (!filenamesWithJobId.isEmpty()) {
            throw new StateStoreException("Job ID already set: " + filenamesWithJobId);
        }
        for (FileInfo file : fileInfos) {
            partitionById.get(file.getPartitionId())
                    .activeFiles.put(file.getFilename(), file.toBuilder().jobId(jobId)
                            .lastStateStoreUpdateTime(clock.millis()).build());
        }
    }

    private List<String> findFilenamesWithJobIdSet(List<FileInfo> fileInfos) {
        return fileInfos.stream()
                .filter(file -> partitionById.get(file.getPartitionId())
                        .activeFiles.getOrDefault(file.getFilename(), file).getJobId() != null)
                .map(FileInfo::getFilename)
                .collect(toList());
    }

    @Override
    public void deleteReadyForGCFile(FileInfo fileInfo) {
        PartitionFiles partition = partitionById.get(fileInfo.getPartitionId());
        partition.readyForGCFiles.remove(fileInfo.getFilename());
        if (partition.isEmpty()) {
            partitionById.remove(fileInfo.getPartitionId());
        }
        fileReferenceCounts.remove(fileInfo.getFilename());
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
        fileReferenceCounts.clear();
    }

    @Override
    public void fixTime(Instant now) {
        clock = Clock.fixed(now, ZoneId.of("UTC"));
    }
}
