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

import java.time.Instant;
import java.util.Collection;
import java.util.Collections;
import java.util.LinkedHashMap;
import java.util.List;
import java.util.Map;
import java.util.Objects;
import java.util.Set;
import java.util.TreeMap;
import java.util.TreeSet;
import java.util.stream.Stream;

/**
 * This class contains a snapshot of files in the state store at a point in time, to be used to build a report.
 */
public class AllFileReferences {
    private final Set<String> filesWithNoReferences;
    private final Map<String, FileReference> activeFilesByPartitionAndFilename;
    private final Map<String, ReferencedFile> filesByFilename;
    private final boolean moreThanMax;

    public AllFileReferences(Collection<FileReference> activeFiles, Set<String> filesWithNoReferences, boolean moreThanMax) {
        this(activeFiles, filesWithNoReferences, null, moreThanMax);
    }

    public AllFileReferences(Collection<FileReference> activeFiles, Set<String> filesWithNoReferences, Instant updateTime, boolean moreThanMax) {
        this.filesWithNoReferences = filesWithNoReferences;
        this.activeFilesByPartitionAndFilename = activeFilesByFilenameAndPartition(activeFiles.stream()
                .map(reference -> reference.toBuilder().lastStateStoreUpdateTime(updateTime).build()));
        this.filesByFilename = filesByFilename(activeFiles, filesWithNoReferences, updateTime);
        this.moreThanMax = moreThanMax;
    }

    public AllFileReferences(Collection<ReferencedFile> files, boolean moreThanMax) {
        this.filesByFilename = filesByFilename(files);
        this.filesWithNoReferences = filesWithNoReferences(files);
        this.activeFilesByPartitionAndFilename = activeFilesByFilenameAndPartition(files.stream()
                .flatMap(file -> file.getInternalReferences().stream()));
        this.moreThanMax = moreThanMax;
    }

    public Collection<ReferencedFile> getFiles() {
        return filesByFilename.values();
    }

    public Set<String> getFilesWithNoReferences() {
        return filesWithNoReferences;
    }

    public Collection<FileReference> getActiveFiles() {
        return activeFilesByPartitionAndFilename.values();
    }

    public boolean isMoreThanMax() {
        return moreThanMax;
    }

    @Override
    public boolean equals(Object o) {
        if (this == o) {
            return true;
        }
        if (o == null || getClass() != o.getClass()) {
            return false;
        }
        AllFileReferences that = (AllFileReferences) o;
        return moreThanMax == that.moreThanMax && Objects.equals(filesByFilename, that.filesByFilename);
    }

    @Override
    public int hashCode() {
        return Objects.hash(filesByFilename, moreThanMax);
    }

    @Override
    public String toString() {
        return "AllFileReferences{" +
                "filesByFilename=" + filesByFilename +
                ", moreThanMax=" + moreThanMax +
                '}';
    }

    private static Map<String, FileReference> activeFilesByFilenameAndPartition(Stream<FileReference> activeFiles) {
        Map<String, FileReference> map = new LinkedHashMap<>();
        activeFiles.forEach(file -> map.put(file.getFilename() + "|" + file.getPartitionId(), file));
        return Collections.unmodifiableMap(map);
    }

    private static Map<String, ReferencedFile> filesByFilename(Collection<ReferencedFile> files) {
        Map<String, ReferencedFile> map = new TreeMap<>();
        files.forEach(file -> map.put(file.getFilename(), file));
        return Collections.unmodifiableMap(map);
    }

    private static Set<String> filesWithNoReferences(Collection<ReferencedFile> files) {
        Set<String> set = new TreeSet<>();
        files.stream()
                .filter(file -> file.getTotalReferenceCount() < 1)
                .forEach(file -> set.add(file.getFilename()));
        return Collections.unmodifiableSet(set);
    }

    private static Map<String, ReferencedFile> filesByFilename(
            Collection<FileReference> activeFiles, Set<String> filesWithNoReferences, Instant updateTime) {
        Map<String, ReferencedFile> map = new TreeMap<>();
        ReferencedFile.newFilesWithReferences(activeFiles).forEach(file ->
                map.put(file.getFilename(), file.toBuilder().lastUpdateTime(updateTime).build()));
        filesWithNoReferences.forEach(filename -> map.put(filename, ReferencedFile.builder()
                .filename(filename)
                .internalReferences(List.of())
                .totalReferenceCount(0)
                .lastUpdateTime(updateTime)
                .build()));
        return Collections.unmodifiableMap(map);
    }
}
