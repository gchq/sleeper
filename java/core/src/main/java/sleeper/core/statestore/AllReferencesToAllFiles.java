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
import java.util.Collections;
import java.util.List;
import java.util.Map;
import java.util.Objects;
import java.util.TreeMap;
import java.util.stream.Stream;

import static java.util.stream.Collectors.toUnmodifiableList;

/**
 * This class contains a report of files in the state store at a point in time, to be used in a reporting client.
 */
public class AllReferencesToAllFiles {
    private final Map<String, AllReferencesToAFile> filesByFilename;
    private final Map<String, AllReferencesToAFile> filesWithReferencesByFilename;
    private final Map<String, AllReferencesToAFile> filesWithNoReferencesByFilename;
    private final boolean moreThanMax;

    public AllReferencesToAllFiles(Collection<AllReferencesToAFile> files, boolean moreThanMax) {
        this.filesByFilename = filesByFilename(files.stream());
        this.filesWithReferencesByFilename = filesByFilename(files.stream()
                .filter(file -> file.getReferenceCount() > 0));
        this.filesWithNoReferencesByFilename = filesByFilename(files.stream()
                .filter(file -> file.getReferenceCount() < 1));
        this.moreThanMax = moreThanMax;
    }

    public Collection<AllReferencesToAFile> getFiles() {
        return filesByFilename.values();
    }

    public Collection<AllReferencesToAFile> getFilesWithReferences() {
        return filesWithReferencesByFilename.values();
    }

    public Collection<AllReferencesToAFile> getFilesWithNoReferences() {
        return filesWithNoReferencesByFilename.values();
    }

    /**
     * Builds a list of all file references in the report.
     *
     * @return the list
     */
    public List<FileReference> listFileReferences() {
        return getFilesWithReferences().stream()
                .flatMap(file -> file.getReferences().stream())
                .collect(toUnmodifiableList());
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
        AllReferencesToAllFiles that = (AllReferencesToAllFiles) o;
        return moreThanMax == that.moreThanMax && Objects.equals(filesByFilename, that.filesByFilename);
    }

    @Override
    public int hashCode() {
        return Objects.hash(filesByFilename, moreThanMax);
    }

    @Override
    public String toString() {
        return "AllReferencesToAllFiles{" +
                "files=" + filesByFilename.values() +
                ", moreThanMax=" + moreThanMax +
                '}';
    }

    private static Map<String, AllReferencesToAFile> filesByFilename(Stream<AllReferencesToAFile> files) {
        Map<String, AllReferencesToAFile> map = new TreeMap<>();
        files.forEach(file -> map.put(file.getFilename(), file));
        return Collections.unmodifiableMap(map);
    }
}
