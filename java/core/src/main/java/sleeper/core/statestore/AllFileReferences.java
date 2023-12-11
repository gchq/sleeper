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

package sleeper.core.statestore;

import java.util.ArrayList;
import java.util.LinkedHashMap;
import java.util.List;
import java.util.Map;
import java.util.Objects;
import java.util.stream.Collectors;
import java.util.stream.Stream;

public class AllFileReferences {
    private final List<String> filesWithNoReferences;
    private final List<FileInfo> activeFiles;

    public AllFileReferences(List<String> filesWithNoReferences, List<FileInfo> activeFiles) {
        this.filesWithNoReferences = filesWithNoReferences;
        this.activeFiles = activeFiles;
    }

    public static AllFileReferences fromActiveFilesAndReferenceCounts(
            Stream<FileInfo> activeFiles,
            Stream<FileReferenceCount> fileReferenceCounts) {
        Map<String, List<FileInfo>> referencesByFilename = new LinkedHashMap<>();
        fileReferenceCounts.forEach(counts ->
                referencesByFilename.put(counts.getFilename(), new ArrayList<>()));
        activeFiles.forEach(file -> referencesByFilename
                .computeIfAbsent(file.getFilename(), name -> new ArrayList<>())
                .add(file));
        return new AllFileReferences(
                referencesByFilename.entrySet().stream()
                        .filter(entry -> entry.getValue().isEmpty())
                        .map(Map.Entry::getKey)
                        .collect(Collectors.toList()),
                referencesByFilename.values().stream()
                        .flatMap(List::stream)
                        .collect(Collectors.toList()));
    }

    public List<String> getFilesWithNoReferences() {
        return filesWithNoReferences;
    }

    public List<FileInfo> getActiveFiles() {
        return activeFiles;
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
        return Objects.equals(filesWithNoReferences, that.filesWithNoReferences) && Objects.equals(activeFiles, that.activeFiles);
    }

    @Override
    public int hashCode() {
        return Objects.hash(filesWithNoReferences, activeFiles);
    }

    @Override
    public String toString() {
        return "AllFileReferences{" +
                "filesWithNoReferences=" + filesWithNoReferences +
                ", activeFiles=" + activeFiles +
                '}';
    }
}
