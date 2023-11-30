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
import java.util.ArrayList;
import java.util.HashMap;
import java.util.Iterator;
import java.util.LinkedHashMap;
import java.util.List;
import java.util.Map;
import java.util.Objects;
import java.util.Set;
import java.util.stream.Collectors;

import static java.util.stream.Collectors.groupingBy;
import static java.util.stream.Collectors.mapping;
import static java.util.stream.Collectors.toList;
import static sleeper.core.statestore.FileInfo.FileStatus.READY_FOR_GARBAGE_COLLECTION;
import static sleeper.core.statestore.inmemory.InMemoryFileInfoStore.FileReferenceKey.keyFor;

public class InMemoryFileInfoStore implements FileInfoStore {
    private static final String DEFAULT_TABLE_ID = "test-table-id";
    private final List<FileInfo> activeFiles = new ArrayList<>();
    private final Map<String, FileInfo> readyForGCFiles = new HashMap<>();
    private final Map<String, FileReferenceCount> fileReferenceCounts = new LinkedHashMap<>();
    private Clock clock = Clock.systemUTC();
    private final String tableId;

    public InMemoryFileInfoStore() {
        this(DEFAULT_TABLE_ID);
    }

    public InMemoryFileInfoStore(String tableId) {
        this.tableId = tableId;
    }

    @Override
    public void addFile(FileInfo fileInfo) {
        activeFiles.add(fileInfo.toBuilder().lastStateStoreUpdateTime(clock.millis()).build());
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
        return activeFiles;
    }

    @Override
    public Iterator<FileInfo> getReadyForGCFiles() {
        return readyForGCFiles.values().iterator();
    }

    @Override
    public List<FileInfo> getActiveFilesWithNoJobId() {
        return activeFiles.stream()
                .filter(file -> file.getJobId() == null)
                .collect(Collectors.toUnmodifiableList());
    }

    @Override
    public long getFileReferenceCount(String filename) {
        return fileReferenceCounts.get(filename).getNumberOfReferences();
    }

    @Override
    public Map<String, List<String>> getPartitionToActiveFilesMap() {
        return activeFiles.stream()
                .collect(groupingBy(FileInfo::getPartitionId,
                        mapping(FileInfo::getFilename, toList())));
    }

    private void moveToGC(FileInfo file) {
        activeFiles.remove(file);
        readyForGCFiles.put(file.getFilename(),
                file.toBuilder().fileStatus(READY_FOR_GARBAGE_COLLECTION)
                        .lastStateStoreUpdateTime(clock.millis())
                        .build());
    }

    @Override
    public void atomicallyUpdateFilesToReadyForGCAndCreateNewActiveFiles(List<FileInfo> filesToBeMarkedReadyForGC, List<FileInfo> newFiles) {
        filesToBeMarkedReadyForGC.forEach(fileReference -> {
            this.removeFileReference(fileReference);
            this.moveToGC(fileReference);
        });
        this.addFiles(newFiles);
    }

    private void removeFileReference(FileInfo fileReference) {
        fileReferenceCounts.put(fileReference.getFilename(),
                fileReferenceCounts.get(fileReference.getFilename()).decrement());
    }

    @Override
    public void atomicallyUpdateJobStatusOfFiles(String jobId, List<FileInfo> fileInfos) throws StateStoreException {
        List<String> filenamesWithJobId = findFileReferencesWithJobIdSet(fileInfos);
        if (!filenamesWithJobId.isEmpty()) {
            throw new StateStoreException("Job ID already set: " + filenamesWithJobId);
        }
        for (FileInfo file : fileInfos) {
            activeFiles.remove(file);
            activeFiles.add(file.toBuilder().jobId(jobId)
                    .lastStateStoreUpdateTime(clock.millis()).build());
        }
    }

    private List<String> findFileReferencesWithJobIdSet(List<FileInfo> fileReferenceList) {
        Set<FileReferenceKey> fileReferenceKeys = fileReferenceList.stream()
                .map(FileReferenceKey::keyFor)
                .collect(Collectors.toSet());
        return activeFiles.stream()
                .filter(fileReference -> fileReferenceKeys.contains(keyFor(fileReference)))
                .filter(fileReference -> fileReference.getJobId() != null)
                .map(FileInfo::getFilename)
                .collect(toList());
    }

    @Override
    public void deleteReadyForGCFile(FileInfo fileInfo) {
        readyForGCFiles.remove(fileInfo.getFilename());
        fileReferenceCounts.remove(fileInfo.getFilename());
    }

    @Override
    public void initialise() {
    }

    @Override
    public boolean hasNoFiles() {
        return activeFiles.isEmpty();
    }

    @Override
    public void clearTable() {
        activeFiles.clear();
        readyForGCFiles.clear();
        fileReferenceCounts.clear();
    }

    @Override
    public void fixTime(Instant now) {
        clock = Clock.fixed(now, ZoneId.of("UTC"));
    }

    static class FileReferenceKey {
        private final String partitionId;
        private final String filename;

        FileReferenceKey(String partitionId, String filename) {
            this.partitionId = partitionId;
            this.filename = filename;
        }

        static FileReferenceKey keyFor(FileInfo fileInfo) {
            return new FileReferenceKey(fileInfo.getPartitionId(), fileInfo.getFilename());
        }

        @Override
        public boolean equals(Object o) {
            if (this == o) {
                return true;
            }
            if (o == null || getClass() != o.getClass()) {
                return false;
            }
            FileReferenceKey that = (FileReferenceKey) o;
            return Objects.equals(partitionId, that.partitionId) && Objects.equals(filename, that.filename);
        }

        @Override
        public int hashCode() {
            return Objects.hash(partitionId, filename);
        }

        @Override
        public String toString() {
            return "FileReferenceKey{" +
                    "partitionId='" + partitionId + '\'' +
                    ", filename='" + filename + '\'' +
                    '}';
        }
    }
}
