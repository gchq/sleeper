/*
 * Copyright 2022 Crown Copyright
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
package sleeper.statestore.inmemory;

import sleeper.statestore.FileInfo;
import sleeper.statestore.FileInfoStore;
import sleeper.statestore.StateStoreException;

import java.util.ArrayList;
import java.util.Collections;
import java.util.HashMap;
import java.util.Iterator;
import java.util.List;
import java.util.Map;

import static java.util.stream.Collectors.groupingBy;
import static java.util.stream.Collectors.mapping;
import static java.util.stream.Collectors.toList;

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
        return Collections.unmodifiableList(new ArrayList<>(activeFiles.values()));
    }

    @Override
    public Map<String, List<String>> getPartitionToActiveFilesMap() {
        return activeFiles.values().stream().collect(
                groupingBy(FileInfo::getPartitionId,
                        mapping(FileInfo::getFilename, toList())));
    }

    @Override
    public void atomicallyUpdateFilesToReadyForGCAndCreateNewActiveFile(List<FileInfo> filesToBeMarkedReadyForGC, FileInfo newActiveFile) {
        for (FileInfo file : filesToBeMarkedReadyForGC) {
            activeFiles.remove(file.getFilename());
            readyForGCFiles.put(file.getFilename(), file);
        }
        activeFiles.put(newActiveFile.getFilename(), newActiveFile);
    }

    @Override
    public void atomicallyUpdateFilesToReadyForGCAndCreateNewActiveFiles(List<FileInfo> filesToBeMarkedReadyForGC, FileInfo leftFileInfo, FileInfo rightFileInfo) {
        for (FileInfo file : filesToBeMarkedReadyForGC) {
            activeFiles.remove(file.getFilename());
            readyForGCFiles.put(file.getFilename(), file);
        }
        activeFiles.put(leftFileInfo.getFilename(), leftFileInfo);
        activeFiles.put(rightFileInfo.getFilename(), rightFileInfo);
    }

    @Override
    public void atomicallyUpdateJobStatusOfFiles(String jobId, List<FileInfo> fileInfos) {
    }

    @Override
    public void deleteReadyForGCFile(FileInfo fileInfo) {
        readyForGCFiles.remove(fileInfo.getFilename());
    }

    @Override
    public void initialise() throws StateStoreException {

    }
}
