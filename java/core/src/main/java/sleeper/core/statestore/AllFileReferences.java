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
import java.util.Collection;
import java.util.Comparator;
import java.util.LinkedHashMap;
import java.util.List;
import java.util.Map;
import java.util.Objects;
import java.util.Set;
import java.util.function.Function;
import java.util.stream.Collectors;
import java.util.stream.Stream;

public class AllFileReferences {

    private final Set<FileReferences> files;

    public AllFileReferences(Set<FileReferences> files) {
        this.files = files;
    }

    public static AllFileReferences fromActiveFilesAndReferenceCounts(
            Stream<FileInfo> activeFiles,
            Stream<FileReferenceCount> fileReferenceCounts) {
        Map<String, List<FileInfo>> referencesByFilename = new LinkedHashMap<>();
        Map<String, FileReferenceCount> referenceCountByFilename = fileReferenceCounts
                .peek(counts -> referencesByFilename.put(counts.getFilename(), new ArrayList<>()))
                .collect(Collectors.toMap(FileReferenceCount::getFilename, Function.identity()));
        activeFiles.forEach(file -> referencesByFilename
                .computeIfAbsent(file.getFilename(), name -> new ArrayList<>())
                .add(file));
        Set<FileReferences> fileReferences = referencesByFilename.entrySet().stream()
                .map(entry -> new FileReferences(entry.getKey(), referenceCountByFilename.get(entry.getKey()).getLastUpdateTime(), entry.getValue()))
                .collect(Collectors.toUnmodifiableSet());
        return new AllFileReferences(fileReferences);
    }

    public Collection<FileReferences> getFiles() {
        return files;
    }

    public List<String> getFilesWithNoReferences() {
        return getFiles().stream()
                .filter(fileReferences -> fileReferences.getReferences().isEmpty())
                .map(FileReferences::getFilename)
                .sorted(Comparator.naturalOrder())
                .collect(Collectors.toList());
    }

    public List<FileInfo> getActiveFiles() {
        return getFiles().stream()
                .flatMap(fileReferences -> fileReferences.getReferences().stream())
                .collect(Collectors.toList());
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
        return Objects.equals(files, that.files);
    }

    @Override
    public int hashCode() {
        return Objects.hash(files);
    }

    @Override
    public String toString() {
        return "FilesReport{" +
                "files=" + files +
                '}';
    }
}
