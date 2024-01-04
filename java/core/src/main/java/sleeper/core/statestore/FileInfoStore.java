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
     * Atomically changes the status of some files from active to ready for GC
     * and adds new {@link FileInfo}s as active files.
     *
     * @param partitionId               The partition which the files to mark as ready for GC are in
     * @param filesToBeMarkedReadyForGC The filenames of files to be marked as ready for GC
     * @param newFiles                  The files to be added as active files
     * @throws StateStoreException if update fails
     */
    void atomicallyUpdateFilesToReadyForGCAndCreateNewActiveFiles(String partitionId, List<String> filesToBeMarkedReadyForGC,
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
     * Returns a stream of files that are ready for garbage collection, i.e. there are no active file records
     * referencing them and the last update time is before maxUpdateTime.
     *
     * @param maxUpdateTime The latest time at which a file can have been updated in order to be garbage collected
     * @return a stream of filenames with the matching status
     * @throws StateStoreException if query fails
     */
    Stream<String> getReadyForGCFilenamesBefore(Instant maxUpdateTime) throws StateStoreException;

    /**
     * Returns all {@link FileInfo}s with status of active which have a null job id.
     *
     * @return a {@code List} of {@code FileInfo.FileStatus}es which are active and have a null job id
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
     * Returns a report of files in the system and their active references within partitions.
     *
     * @param maxUnreferencedFiles Maximum number of files to return with no active references
     * @return the report
     * @throws StateStoreException if query fails
     */
    AllFileReferences getAllFileReferencesWithMaxUnreferenced(int maxUnreferencedFiles) throws StateStoreException;

    /**
     * Performs extra setup steps that are needed before the file info store can be used.
     *
     * @throws StateStoreException if initialisation fails
     */
    void initialise() throws StateStoreException;

    /**
     * Returns whether the file info store has files in it or not.
     *
     * @return a boolean representing whether the state store has files in it or not.
     */
    boolean hasNoFiles();

    /**
     * Clears all file data from the file info store.
     * <p>
     * Note that this does not delete any of the actual files
     */
    void clearSleeperTable();

    /**
     * Used to set the current time. Should only be called during tests.
     *
     * @param time Time to set to be the current time
     */
    void fixTime(Instant time);
}
