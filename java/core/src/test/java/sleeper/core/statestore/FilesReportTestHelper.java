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

public class FilesReportTestHelper {

    private FilesReportTestHelper() {
    }

    public static AllFileReferences noFilesReport() {
        return activeFilesReport();
    }

    public static AllFileReferences activeFilesReport(FileReference... files) {
        return activeFilesReport(List.of(files));
    }

    public static AllFileReferences activeFilesReport(List<FileReference> files) {
        return new AllFileReferences(files, Set.of(), false);
    }

    public static AllFileReferences activeFilesReport(Instant updateTime, FileReference... files) {
        return activeFilesReport(updateTime, List.of(files));
    }

    public static AllFileReferences activeFilesReport(Instant updateTime, List<FileReference> files) {
        return new AllFileReferences(files, Set.of(), updateTime, false);
    }

    public static AllFileReferences activeAndReadyForGCFilesReport(
            List<FileReference> activeFiles, List<String> readyForGCFiles) {
        return new AllFileReferences(activeFiles, new TreeSet<>(readyForGCFiles), false);
    }

    public static AllFileReferences activeAndReadyForGCFilesReport(
            Instant updateTime, List<FileReference> activeFiles, List<String> readyForGCFiles) {
        return new AllFileReferences(activeFiles, new TreeSet<>(readyForGCFiles), updateTime, false);
    }

    public static AllFileReferences readyForGCFilesReport(Instant updateTime, String... filename) {
        return new AllFileReferences(List.of(), Set.of(filename), updateTime, false);
    }

    public static AllFileReferences readyForGCFilesReport(String... filename) {
        return new AllFileReferences(List.of(), Set.of(filename), false);
    }

    public static AllFileReferences partialReadyForGCFilesReport(String... filename) {
        return new AllFileReferences(List.of(), Set.of(filename), true);
    }

    public static AllFileReferences partialReadyForGCFilesReport(Instant updateTime, String... filename) {
        return new AllFileReferences(List.of(), Set.of(filename), updateTime, true);
    }
}
