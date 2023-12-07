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

import java.time.Instant;
import java.util.Iterator;
import java.util.List;
import java.util.Map;
import java.util.stream.Stream;

/**
 * Stores information about the data files and their status (i.e. {@link FileInfo}s).
 */
public interface FileInfoStore {

    /**
     * Adds a {@link FileInfo}.
     *
     * @param fileInfo The fileInfo to be added
     * @throws StateStoreException if update fails
     */
    void addFile(FileInfo fileInfo) throws StateStoreException;

    /**
     * Adds a {@link List} of {@link FileInfo}s.
     *
     * @param fileInfos The fileInfos to be added
     * @throws StateStoreException if update fails
     */
    void addFiles(List<FileInfo> fileInfos) throws StateStoreException;

    /**
     * Atomically changes the status of some files from {@link FileInfo.FileStatus.ACTIVE}
     * to {@link FileInfo.FileStatus.READY_FOR_GARBAGE_COLLECTION}, and adds a new
     * {@link FileInfo} as an {@link FileInfo.FileStatus.ACTIVE} file.
     *
     * @param filesToBeMarkedReadyForGC The files to be marked as {@link FileInfo.FileStatus.READY_FOR_GARBAGE_COLLECTION}
     * @param newActiveFile             The file to be added as an {@link FileInfo.FileStatus.ACTIVE} file
     * @throws StateStoreException if update fails
     */
    default void atomicallyUpdateFilesToReadyForGCAndCreateNewActiveFile(
            List<FileInfo> filesToBeMarkedReadyForGC,
            FileInfo newActiveFile) throws StateStoreException {
        atomicallyUpdateFilesToReadyForGCAndCreateNewActiveFiles(
                filesToBeMarkedReadyForGC, List.of(newActiveFile));
    }

    /**
     * Atomically changes the status of some files from {@link FileInfo.FileStatus.ACTIVE}
     * to {@link FileInfo.FileStatus.READY_FOR_GARBAGE_COLLECTION}, and adds two new
     * {@link FileInfo}s as an {@link FileInfo.FileStatus.ACTIVE} file.
     *
     * @param filesToBeMarkedReadyForGC The files to be marked as {@link FileInfo.FileStatus.READY_FOR_GARBAGE_COLLECTION}.
     * @param leftFileInfo              The first file to be added as an {@link FileInfo.FileStatus.ACTIVE} file
     * @param rightFileInfo             The second file to be added as an {@link FileInfo.FileStatus.ACTIVE} file
     * @throws StateStoreException if update fails
     */
    default void atomicallyUpdateFilesToReadyForGCAndCreateNewActiveFiles(List<FileInfo> filesToBeMarkedReadyForGC,
                                                                          FileInfo leftFileInfo,
                                                                          FileInfo rightFileInfo) throws StateStoreException {
        atomicallyUpdateFilesToReadyForGCAndCreateNewActiveFiles(
                filesToBeMarkedReadyForGC, List.of(leftFileInfo, rightFileInfo));
    }

    void atomicallyUpdateFilesToReadyForGCAndCreateNewActiveFiles(List<FileInfo> filesToBeMarkedReadyForGC,
                                                                  List<FileInfo> newFiles) throws StateStoreException;

    /**
     * Atomically updates the job field of the input files of the compactionJob to the job
     * id, as long as the job field is currently null.
     *
     * @param jobId     The job id which will be added to the FileInfos
     * @param fileInfos The FileInfos whose status will be updated
     * @throws StateStoreException if update fails
     */
    void atomicallyUpdateJobStatusOfFiles(String jobId, List<FileInfo> fileInfos)
            throws StateStoreException;

    default void atomicallyUpdateEachJobStatusOfFiles(List<AssignJobToFilesRequest> jobs)
            throws StateStoreException {
    }

    /**
     * Deletes this file with the status of {@link FileInfo.FileStatus.READY_FOR_GARBAGE_COLLECTION}.
     *
     * @param fileInfo The FileInfo to be deleted.
     * @throws StateStoreException if update fails
     */
    void deleteReadyForGCFile(FileInfo fileInfo) throws StateStoreException;

    /**
     * Records that a file was garbage collected and has been deleted.
     *
     * @param filename The name of the file that was deleted.
     * @throws StateStoreException if update fails
     */
    void deleteReadyForGCFile(String filename) throws StateStoreException;

    /**
     * Returns all {@link FileInfo}s with a status of status.
     *
     * @return a {@code List} of {@code FileInfo.FileStatus}es with the matching status
     * @throws StateStoreException if query fails
     */
    List<FileInfo> getActiveFiles() throws StateStoreException;

    /**
     * Returns an {@link Iterator} of files that are ready for garbage collection, i.e. their status is
     * {@link FileInfo.FileStatus.READY_FOR_GARBAGE_COLLECTION} and the last update time is more than
     * <code>sleeper.table.gc.delay.minutes</code> ago.
     *
     * @return an iterator of {@code FileInfo.FileStatus}es with the matching status
     * @throws StateStoreException if query fails
     */
    Iterator<FileInfo> getReadyForGCFiles() throws StateStoreException;

    /**
     * Returns all {@link FileInfo}s with status {@link FileInfo.FileStatus} of
     * {@link FileInfo.FileStatus.ACTIVE} which have a null job id.
     *
     * @return a {@code List} of {@code FileInfo.FileStatus}es which are {@link FileInfo.FileStatus.ACTIVE} and have a null job id
     * @throws StateStoreException if query fails
     */
    List<FileInfo> getActiveFilesWithNoJobId() throws StateStoreException;

    /**
     * Returns a {@link Map} from the partition id to a {@link List} of the filenames.
     *
     * @return a {@link Map} from the partition id to a {@link List} of the filenames
     * @throws StateStoreException if query fails
     */
    Map<String, List<String>> getPartitionToActiveFilesMap() throws StateStoreException;

    /**
     * Returns a list of files that are ready for garbage collection, i.e. there are no active file records referencing
     * them and the last update time is before maxUpdateTime.
     *
     * @param maxUpdateTime The latest time at which a file can have been updated in order to be garbage collected
     * @return an iterator of filenames with the matching status
     * @throws StateStoreException if query fails
     */
    Stream<String> getReadyForGCFilenamesBefore(Instant maxUpdateTime) throws StateStoreException;

    /**
     * Returns a report of files in the system and their active references within partitions.
     *
     * @return the report
     * @throws StateStoreException if query fails
     */
    AllFileReferences getAllFileReferences() throws StateStoreException;

    void initialise() throws StateStoreException;

    boolean hasNoFiles();

    void clearTable();

    void fixTime(Instant time);
}
