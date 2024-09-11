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

/**
 * Test helpers for building a report of files in a Sleeper table.
 */
public class FilesReportTestHelper {

    private FilesReportTestHelper() {
    }

    public static final Instant DEFAULT_UPDATE_TIME = Instant.parse("2024-02-21T11:42:00Z");

    /**
     * Creates a report with no files.
     *
     * @return the report
     */
    public static AllReferencesToAllFiles noFiles() {
        return noFilesReport();
    }

    /**
     * Creates a report with no files.
     *
     * @return the report
     */
    public static AllReferencesToAllFiles noFilesReport() {
        return new AllReferencesToAllFiles(List.of(), false);
    }

    /**
     * Creates a report with the specified files referenced in partitions.
     *
     * @param  files the file references
     * @return       the report
     */
    public static AllReferencesToAllFiles activeFiles(FileReference... files) {
        return activeFiles(List.of(files));
    }

    /**
     * Creates a report with the specified files referenced in partitions.
     *
     * @param  files the file references
     * @return       the report
     */
    public static AllReferencesToAllFiles activeFiles(List<FileReference> files) {
        return activeFilesReport(DEFAULT_UPDATE_TIME, files);
    }

    /**
     * Creates a report with the specified files referenced in partitions. All files will be given the same last update
     * time.
     *
     * @param  updateTime the update time
     * @param  files      the file references
     * @return            the report
     */
    public static AllReferencesToAllFiles activeFilesReport(Instant updateTime, FileReference... files) {
        return activeFilesReport(updateTime, List.of(files));
    }

    /**
     * Creates a report with the specified files referenced in partitions. All files will be given the same last update
     * time.
     *
     * @param  updateTime the update time
     * @param  references the file references
     * @return            the report
     */
    public static AllReferencesToAllFiles activeFilesReport(Instant updateTime, List<FileReference> references) {
        return new AllReferencesToAllFiles(
                AllReferencesToAFile.newFilesWithReferences(references.stream())
                        .map(file -> file.withCreatedUpdateTime(updateTime))
                        .collect(Collectors.toUnmodifiableList()),
                false);
    }

    /**
     * Creates a report with specified files referenced in partitions, and files with no references.
     *
     * @param  activeFiles     the file references
     * @param  readyForGCFiles the filenames with no references
     * @return                 the report
     */
    public static AllReferencesToAllFiles activeAndReadyForGCFiles(List<FileReference> activeFiles, List<String> readyForGCFiles) {
        return activeAndReadyForGCFilesReport(DEFAULT_UPDATE_TIME, activeFiles, readyForGCFiles);
    }

    /**
     * Creates a report with specified files referenced in partitions, and files with no references.
     *
     * @param  activeFiles     the file references
     * @param  readyForGCFiles the filenames with no references
     * @return                 the report
     */
    public static AllReferencesToAllFiles activeAndReadyForGCFilesReport(
            Instant updateTime, List<FileReference> activeFiles, List<String> readyForGCFiles) {
        return new AllReferencesToAllFiles(activeAndReadyForGCFiles(updateTime, activeFiles, readyForGCFiles), false);
    }

    /**
     * Creates a report with the specified files with no references.
     *
     * @param  filenames the filenames
     * @return           the report
     */
    public static AllReferencesToAllFiles readyForGCFiles(String... filenames) {
        return readyForGCFilesReport(DEFAULT_UPDATE_TIME, filenames);
    }

    /**
     * Creates a report with the specified files with no references. All files will be given the same last update time.
     *
     * @param  updateTime the update time
     * @param  filenames  the filenames
     * @return            the report
     */
    public static AllReferencesToAllFiles readyForGCFilesReport(Instant updateTime, String... filenames) {
        return new AllReferencesToAllFiles(activeAndReadyForGCFiles(updateTime, List.of(), List.of(filenames)), false);
    }

    /**
     * Creates a report with the specified files with no references. All files will be given the same last update time.
     * This will be a partial report, which will record that there are more files with no references than the ones
     * specified here.
     *
     * @param  updateTime the update time
     * @param  filenames  the filenames
     * @return            the report
     */
    public static AllReferencesToAllFiles partialReadyForGCFilesReport(Instant updateTime, String... filenames) {
        return new AllReferencesToAllFiles(activeAndReadyForGCFiles(updateTime, List.of(), List.of(filenames)), true);
    }

    private static List<AllReferencesToAFile> activeAndReadyForGCFiles(
            Instant updateTime, List<FileReference> activeFiles, List<String> readyForGCFiles) {
        return Stream.concat(
                AllReferencesToAFile.newFilesWithReferences(activeFiles.stream()).map(file -> file.withCreatedUpdateTime(updateTime)),
                readyForGCFiles.stream().map(filename -> AllReferencesToAFileTestHelper.fileWithNoReferences(filename, updateTime))).collect(Collectors.toUnmodifiableList());
    }
}
