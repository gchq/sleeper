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
import java.util.List;
import java.util.Set;
import java.util.TreeSet;
import java.util.stream.Collectors;
import java.util.stream.Stream;

public class FilesReportTestHelper {

    private FilesReportTestHelper() {
    }

    public static AllReferencesToAllFiles noFilesReport() {
        return activeFilesReport();
    }

    public static AllReferencesToAllFiles activeFilesReport(FileReference... files) {
        return activeFilesReport(List.of(files));
    }

    public static AllReferencesToAllFiles activeFilesReport(List<FileReference> files) {
        return new AllReferencesToAllFiles(files, Set.of(), false);
    }

    public static AllReferencesToAllFiles activeFilesReport(Instant updateTime, FileReference... files) {
        return activeFilesReport(updateTime, List.of(files));
    }

    public static AllReferencesToAllFiles activeFilesReport(Instant updateTime, List<FileReference> references) {
        return new AllReferencesToAllFiles(AllReferencesToAFile.listNewFilesWithReferences(references, updateTime), false);
    }

    public static AllReferencesToAllFiles activeAndReadyForGCFilesReport(
            List<FileReference> activeFiles, List<String> readyForGCFiles) {
        return new AllReferencesToAllFiles(activeFiles, new TreeSet<>(readyForGCFiles), false);
    }

    public static AllReferencesToAllFiles activeAndReadyForGCFilesReport(
            Instant updateTime, List<FileReference> activeFiles, List<String> readyForGCFiles) {
        return new AllReferencesToAllFiles(activeAndReadyForGCFiles(updateTime, activeFiles, readyForGCFiles), false);
    }

    public static AllReferencesToAllFiles readyForGCFilesReport(Instant updateTime, String... filenames) {
        return new AllReferencesToAllFiles(activeAndReadyForGCFiles(updateTime, List.of(), List.of(filenames)), false);
    }

    public static AllReferencesToAllFiles readyForGCFilesReport(String... filename) {
        return new AllReferencesToAllFiles(List.of(), Set.of(filename), false);
    }

    public static AllReferencesToAllFiles partialReadyForGCFilesReport(String... filename) {
        return new AllReferencesToAllFiles(List.of(), Set.of(filename), true);
    }

    public static AllReferencesToAllFiles partialReadyForGCFilesReport(Instant updateTime, String... filenames) {
        return new AllReferencesToAllFiles(activeAndReadyForGCFiles(updateTime, List.of(), List.of(filenames)), true);
    }

    private static List<AllReferencesToAFile> activeAndReadyForGCFiles(
            Instant updateTime, List<FileReference> activeFiles, List<String> readyForGCFiles) {
        return Stream.concat(
                AllReferencesToAFile.newFilesWithReferences(activeFiles, updateTime),
                readyForGCFiles.stream().map(filename ->
                        AllReferencesToAFile.builder()
                                .filename(filename)
                                .internalReferences(List.of())
                                .totalReferenceCount(0)
                                .lastUpdateTime(updateTime)
                                .build())
        ).collect(Collectors.toUnmodifiableList());
    }
}
