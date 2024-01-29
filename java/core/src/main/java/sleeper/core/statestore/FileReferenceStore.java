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

    /**
     * Performs atomic updates to split file references. Replaces file references in non-leaf partitions with ones in
     * partitions that are children of the original partition, or further down the tree beneath the original partition.
     * <p>
     * Each {@link SplitFileReferenceRequest} will remove one file reference, and create new references to the same file
     * in partitions which are descendents of the original file reference.
     * <p>
     * Note that it is possible that the necessary updates may not fit in a single transaction. Each
     * {@link SplitFileReferenceRequest} is guaranteed to be done atomically in one transaction, but it is possible that
     * some may succeed and some may fail. If a single {@link SplitFileReferenceRequest} adds too many references to
     * apply in one transaction, this will also fail.
     *
     * @param splitRequests A list of {@link SplitFileReferenceRequest}s to apply
     * @throws StateStoreException          if update fails
     * @throws SplitRequestsFailedException if update fails when split into multiple transactions, and some requests may have succeeded
     */
    void splitFileReferences(List<SplitFileReferenceRequest> splitRequests) throws StateStoreException;

    /**
     * Atomically applies the results of a job. Removes file references for a job's input files, and adds references to
     * an output file. This will be used for compaction.
     * <p>
     * This should validate that the input files were assigned to the job.
     * <p>
     * This should result in the input files becoming available for garbage collection, if no other references exist
     * for those files.
     * <p>
     * This should support one output file reference, with a single output file in one partition. This is also used in
     * some test cases to remove a file from the system, with an empty list of new references. If we add direct support
     * for that, we may simplify this method signature.
     *
     * @param jobId         The ID of the job
     * @param partitionId   The partition which the job operated on
     * @param inputFiles    The filenames of the input files, whose references in this partition should be removed
     * @param newReferences The references to a new file, including metadata in the output partition
     * @throws StateStoreException if update fails
     */
    void atomicallyApplyJobFileReferenceUpdates(String jobId, String partitionId, List<String> inputFiles,
                                                List<FileReference> newReferences) throws StateStoreException;

    /**
     * Atomically updates the job field of file references, as long as the job field is currently unset. This will be
     * used for compaction job input files.
     *
     * @param jobId          The job id which will be added to the {@link AllReferencesToAFile}
     * @param fileReferences The {@link AllReferencesToAFile} whose status will be updated
     * @throws StateStoreException if update fails
     */
    void atomicallyAssignJobIdToFileReferences(String jobId, List<FileReference> fileReferences)
            throws StateStoreException;

    /**
     * Records that files were garbage collected and have been deleted. The reference counts for those files should be
     * deleted.
     * <p>
     * If there are any remaining internal references for the files on partitions, this should fail, as it should not be
     * possible to reach that state.
     * <p>
     * If the reference count is non-zero for any other reason, it may be that the count was incremented after the file
     * was ready for garbage collection. This should fail in that case as well, as we would like this to not be
     * possible.
     *
     * @param filenames The names of files that were deleted.
     * @throws StateStoreException if update fails
     */
    void deleteGarbageCollectedFileReferenceCounts(List<String> filenames) throws StateStoreException;

    /**
     * Returns all {@link FileReference}s for files which are active in any partition.
     * <p>
     * This may return multiple references for a single file if it contains records in more than one partition.
     * <p>
     * This must never return references for the same file on partitions where one is the ancestor of the other. This
     * means that every record in a file must only be referenced once.
     *
     * @return a list of all {@link FileReference}s in the Sleeper table
     * @throws StateStoreException if query fails
     */
    List<FileReference> getFileReferences() throws StateStoreException;

    /**
     * Returns a stream of files that are ready for garbage collection, i.e. they have no references and the last update
     * time is before maxUpdateTime.
     *
     * @param maxUpdateTime The latest time at which a file can have been updated in order to be garbage collected
     * @return a stream of filenames with the matching status
     * @throws StateStoreException if query fails
     */
    Stream<String> getReadyForGCFilenamesBefore(Instant maxUpdateTime) throws StateStoreException;

    /**
     * Returns all {@link FileReference}s which are not assigned to any job.
     *
     * @return a list of {@link FileReference}s which are not assigned to any job
     * @throws StateStoreException if query fails
     */
    List<FileReference> getFileReferencesWithNoJobId() throws StateStoreException;

    /**
     * Returns a {@link Map} from the partition id to a {@link List} of all files referenced against that partition.
     * <p>
     * Each file may be included multiple times in this map, as it may be referenced in more than one partition.
     *
     * @return a {@link Map} from the partition id to a {@link List} of all files referenced against that partition
     * @throws StateStoreException if query fails
     */
    Map<String, List<String>> getPartitionToReferencedFilesMap() throws StateStoreException;

    /**
     * Returns a report of files in the system and their active references within partitions.
     *
     * @param maxUnreferencedFiles Maximum number of files to return with no active references
     * @return the report
     * @throws StateStoreException if query fails
     */
    AllReferencesToAllFiles getAllFileReferencesWithMaxUnreferenced(int maxUnreferencedFiles) throws StateStoreException;

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
