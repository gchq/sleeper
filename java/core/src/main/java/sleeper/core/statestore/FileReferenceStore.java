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
 * Stores information about the data files and their status (i.e. {@link FileReference}s).
 */
public interface FileReferenceStore {

    /**
     * Adds a {@link FileReference}.
     *
     * @param fileReference The fileReference to be added
     * @throws StateStoreException if update fails
     */
    void addFile(FileReference fileReference) throws StateStoreException;

    /**
     * Adds a {@link List} of {@link FileReference}s.
     *
     * @param fileReferences The fileReferences to be added
     * @throws StateStoreException if update fails
     */
    void addFiles(List<FileReference> fileReferences) throws StateStoreException;

    default void splitFileReferences(List<SplitFileReferenceRequest> splitRequests) throws StateStoreException {
    }

    /**
     * Atomically changes the status of some files from active to ready for GC
     * and adds new {@link FileReference}s as active files.
     *
     * @param jobId                     The job id which the filesToBeMarkedAsReadyForGC should be assigned to
     * @param partitionId               The partition which the files to mark as ready for GC are in
     * @param filesToBeMarkedReadyForGC The filenames of files to be marked as ready for GC
     * @param newFiles                  The files to be added as active files
     * @throws StateStoreException if update fails
     */
    void atomicallyUpdateFilesToReadyForGCAndCreateNewActiveFiles(String jobId, String partitionId, List<String> filesToBeMarkedReadyForGC,
                                                                  List<FileReference> newFiles) throws StateStoreException;

    /**
     * Atomically updates the job field of the input files of the compactionJob to the job
     * id, as long as the job field is currently null.
     *
     * @param jobId          The job id which will be added to the {@link FileInfo}
     * @param fileReferences The {@link FileInfo} whose status will be updated
     * @throws StateStoreException if update fails
     */
    void atomicallyUpdateJobStatusOfFiles(String jobId, List<FileReference> fileReferences)
            throws StateStoreException;

    /**
     * Records that files were garbage collected and have been deleted.
     *
     * @param filenames The names of files that were deleted.
     * @throws StateStoreException if update fails
     */
    void deleteReadyForGCFiles(List<String> filenames) throws StateStoreException;

    /**
     * Returns all {@link FileReference}s with a status of status.
     *
     * @return a {@code List} of {@code FileReference.FileStatus}es with the matching status
     * @throws StateStoreException if query fails
     */
    List<FileReference> getActiveFiles() throws StateStoreException;

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
     * Returns all {@link FileReference}s with status of active which have a null job id.
     *
     * @return a {@code List} of {@code FileReference}s which are active and have a null job id
     * @throws StateStoreException if query fails
     */
    List<FileReference> getActiveFilesWithNoJobId() throws StateStoreException;

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
     * Performs extra setup steps that are needed before the file reference store can be used.
     *
     * @throws StateStoreException if initialisation fails
     */
    void initialise() throws StateStoreException;

    /**
     * Returns whether the file reference store has files in it or not.
     *
     * @return a boolean representing whether the state store has files in it or not.
     */
    boolean hasNoFiles();

    /**
     * Clears all file data from the file reference store.
     * <p>
     * Note that this does not delete any of the actual files.
     */
    void clearFileData();

    /**
     * Used to set the current time. Should only be called during tests.
     *
     * @param time Time to set to be the current time
     */
    void fixTime(Instant time);
}
