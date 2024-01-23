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

import java.util.Collection;
import java.util.Map;
import java.util.Objects;
import java.util.Set;
import java.util.stream.Collectors;
import java.util.stream.Stream;

import static java.util.function.Function.identity;

/**
 * This class contains a snapshot of files in the state store at a point in time, to be used to build a report.
 */
public class AllFileReferences {
    private final Set<String> filesWithNoReferences;
    private final Map<String, FileReference> activeFilesByFilename;
    private final Map<String, ReferencedFile> filesByFilename;
    private final boolean moreThanMax;

    public AllFileReferences(Collection<FileReference> activeFiles, Set<String> filesWithNoReferences, boolean moreThanMax) {
        this.filesWithNoReferences = filesWithNoReferences;
        this.activeFilesByFilename = activeFiles.stream()
                .collect(Collectors.toMap(FileReference::getFilename, identity()));
        this.filesByFilename = Stream.concat(
                        ReferencedFile.newFilesWithReferences(activeFiles),
                        filesWithNoReferences.stream().map(ReferencedFile::withNoReferences))
                .collect(Collectors.toMap(ReferencedFile::getFilename, identity()));
        this.moreThanMax = moreThanMax;
    }

    public AllFileReferences(Collection<ReferencedFile> files, boolean moreThanMax) {
        this.filesByFilename = files.stream()
                .collect(Collectors.toMap(ReferencedFile::getFilename, identity()));
        this.filesWithNoReferences = files.stream()
                .filter(file -> file.getTotalReferenceCount() == 0)
                .map(ReferencedFile::getFilename)
                .collect(Collectors.toUnmodifiableSet());
        this.activeFilesByFilename = files.stream()
                .flatMap(file -> file.getInternalReferences().stream())
                .collect(Collectors.toMap(FileReference::getFilename, identity()));
        this.moreThanMax = moreThanMax;
    }

    public Collection<ReferencedFile> getFiles() {
        return filesByFilename.values();
    }

    public Set<String> getFilesWithNoReferences() {
        return filesWithNoReferences;
    }

    public Collection<FileReference> getActiveFiles() {
        return activeFilesByFilename.values();
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
}
