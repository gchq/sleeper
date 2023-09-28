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
import sleeper.core.statestore.StateStoreException;

import java.util.ArrayList;
import java.util.Collections;
import java.util.HashMap;
import java.util.Iterator;
import java.util.List;
import java.util.Map;

import static java.util.stream.Collectors.groupingBy;
import static java.util.stream.Collectors.mapping;
import static java.util.stream.Collectors.toList;
import static java.util.stream.Collectors.toUnmodifiableList;
import static sleeper.core.statestore.FileInfo.FileStatus.READY_FOR_GARBAGE_COLLECTION;

public class InMemoryFileInfoStore implements FileInfoStore {

    private final Map<String, FileInfo> activeFiles = new HashMap<>();
    private final Map<String, FileInfo> readyForGCFiles = new HashMap<>();

    @Override
    public void addFile(FileInfo fileInfo) {
        activeFiles.put(fileInfo.getFilename(), fileInfo);
    }

    @Override
    public void addFiles(List<FileInfo> fileInfos) {
        for (FileInfo fileInfo : fileInfos) {
            addFile(fileInfo);
        }
    }

    @Override
    public List<FileInfo> getActiveFiles() {
        return Collections.unmodifiableList(new ArrayList<>(activeFiles.values()));
    }

    @Override
    public Iterator<FileInfo> getReadyForGCFiles() {
        return readyForGCFiles.values().iterator();
    }

    @Override
    public List<FileInfo> getActiveFilesWithNoJobId() {
        return activeFiles.values().stream()
                .filter(file -> file.getJobId() == null)
                .collect(toUnmodifiableList());
    }

    @Override
    public Map<String, List<String>> getPartitionToActiveFilesMap() {
        return activeFiles.values().stream().collect(
                groupingBy(FileInfo::getPartitionId,
                        mapping(FileInfo::getFilename, toList())));
    }

    @Override
    public void atomicallyUpdateFilesToReadyForGCAndCreateNewActiveFile(List<FileInfo> filesToBeMarkedReadyForGC, FileInfo newActiveFile) {
        filesToBeMarkedReadyForGC.forEach(this::moveToGC);
        addFile(newActiveFile);
    }

    @Override
    public void atomicallyUpdateFilesToReadyForGCAndCreateNewActiveFiles(List<FileInfo> filesToBeMarkedReadyForGC, FileInfo leftFileInfo, FileInfo rightFileInfo) {
        filesToBeMarkedReadyForGC.forEach(this::moveToGC);
        addFile(leftFileInfo);
        addFile(rightFileInfo);
    }

    private void moveToGC(FileInfo file) {
        activeFiles.remove(file.getFilename());
        readyForGCFiles.put(file.getFilename(),
                file.toBuilder().fileStatus(READY_FOR_GARBAGE_COLLECTION).build());
    }

    @Override
    public void atomicallyUpdateJobStatusOfFiles(String jobId, List<FileInfo> fileInfos) throws StateStoreException {
        List<String> filenamesWithJobId = findFilenamesWithJobIdSet(fileInfos);
        if (!filenamesWithJobId.isEmpty()) {
            throw new StateStoreException("Job ID already set: " + filenamesWithJobId);
        }
        for (FileInfo file : fileInfos) {
            activeFiles.put(file.getFilename(), file.toBuilder().jobId(jobId).build());
        }
    }

    private List<String> findFilenamesWithJobIdSet(List<FileInfo> fileInfos) {
        return fileInfos.stream()
                .filter(file -> activeFiles.getOrDefault(file.getFilename(), file).getJobId() != null)
                .map(FileInfo::getFilename)
                .collect(toList());
    }

    @Override
    public void deleteReadyForGCFile(FileInfo fileInfo) {
        readyForGCFiles.remove(fileInfo.getFilename());
    }

    @Override
    public void initialise() {

    }

    @Override
    public boolean isHasNoFiles() {
        return activeFiles.isEmpty() && readyForGCFiles.isEmpty();
    }

    @Override
    public void clearTable() {
        activeFiles.clear();
        readyForGCFiles.clear();
    }
}
