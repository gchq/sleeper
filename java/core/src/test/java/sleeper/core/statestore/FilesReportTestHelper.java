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
import java.util.stream.Collectors;
import java.util.stream.Stream;

public class FilesReportTestHelper {

    private FilesReportTestHelper() {
    }

    public static final Instant DEFAULT_UPDATE_TIME = Instant.parse("2024-02-21T11:42:00Z");

    public static AllReferencesToAllFiles noFiles() {
        return noFilesReport();
    }

    public static AllReferencesToAllFiles noFilesReport() {
        return new AllReferencesToAllFiles(List.of(), false);
    }

    public static AllReferencesToAllFiles activeFiles(FileReference... files) {
        return activeFilesReport(DEFAULT_UPDATE_TIME, List.of(files));
    }

    public static AllReferencesToAllFiles activeFilesReport(Instant updateTime, FileReference... files) {
        return activeFilesReport(updateTime, List.of(files));
    }

    public static AllReferencesToAllFiles activeFilesReport(Instant updateTime, List<FileReference> references) {
        return new AllReferencesToAllFiles(AllReferencesToAFile
                .newFilesWithReferences(references.stream(), updateTime)
                .collect(Collectors.toUnmodifiableList()), false);
    }

    public static AllReferencesToAllFiles activeAndReadyForGCFiles(List<FileReference> activeFiles, List<String> readyForGCFiles) {
        return activeAndReadyForGCFilesReport(DEFAULT_UPDATE_TIME, activeFiles, readyForGCFiles);
    }

    public static AllReferencesToAllFiles activeAndReadyForGCFilesReport(
            Instant updateTime, List<FileReference> activeFiles, List<String> readyForGCFiles) {
        return new AllReferencesToAllFiles(activeAndReadyForGCFiles(updateTime, activeFiles, readyForGCFiles), false);
    }

    public static AllReferencesToAllFiles readyForGCFiles(String... filenames) {
        return readyForGCFilesReport(DEFAULT_UPDATE_TIME, filenames);
    }

    public static AllReferencesToAllFiles readyForGCFilesReport(Instant updateTime, String... filenames) {
        return new AllReferencesToAllFiles(activeAndReadyForGCFiles(updateTime, List.of(), List.of(filenames)), false);
    }

    public static AllReferencesToAllFiles partialReadyForGCFilesReport(Instant updateTime, String... filenames) {
        return new AllReferencesToAllFiles(activeAndReadyForGCFiles(updateTime, List.of(), List.of(filenames)), true);
    }

    private static List<AllReferencesToAFile> activeAndReadyForGCFiles(
            Instant updateTime, List<FileReference> activeFiles, List<String> readyForGCFiles) {
        return Stream.concat(
                AllReferencesToAFile.newFilesWithReferences(activeFiles.stream(), updateTime),
                readyForGCFiles.stream().map(filename -> AllReferencesToAFileTestHelper.fileWithNoReferences(filename, updateTime))).collect(Collectors.toUnmodifiableList());
    }
}
