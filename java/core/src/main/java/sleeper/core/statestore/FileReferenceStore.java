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

import sleeper.core.statestore.exception.FileAlreadyExistsException;
import sleeper.core.statestore.exception.FileHasReferencesException;
import sleeper.core.statestore.exception.FileNotFoundException;
import sleeper.core.statestore.exception.FileReferenceAssignedToJobException;
import sleeper.core.statestore.exception.FileReferenceNotFoundException;
import sleeper.core.statestore.exception.ReplaceRequestsFailedException;
import sleeper.core.statestore.exception.SplitRequestsFailedException;

import java.time.Instant;
import java.util.ArrayList;
import java.util.HashMap;
import java.util.List;
import java.util.Map;
import java.util.Objects;
import java.util.stream.Stream;

import static java.util.stream.Collectors.toMap;

/**
 * Stores information about the data files in a Sleeper table. This includes a count of the number of references
 * to the file, and internal references which assign all the data in the file to non-overlapping partitions.
 */
public interface FileReferenceStore {

    /**
     * Adds a file to the table, with one reference.
     *
     * @param  fileReference              the reference to be added
     * @throws FileAlreadyExistsException if the file already exists
     * @throws StateStoreException        if the update fails for another reason
     */
    default void addFile(FileReference fileReference) throws StateStoreException {
        addFiles(List.of(fileReference));
    }

    /**
     * Adds files to the Sleeper table, with any number of references. Each reference to be added should be for a file
     * which does not yet exist in the table.
     * <p>
     * When adding multiple references for a file, a file must never be referenced in two partitions where one is a
     * descendent of another. This means each record in a file must only be covered by one reference. A partition covers
     * a range of records. A partition which is the child of another covers a sub-range within the parent partition.
     *
     * @param  fileReferences             The file references to be added
     * @throws FileAlreadyExistsException if a file already exists
     * @throws StateStoreException        if the update fails for another reason
     */
    default void addFiles(List<FileReference> fileReferences) throws StateStoreException {
        addFilesWithReferences(AllReferencesToAFile.newFilesWithReferences(fileReferences));
    }

    /**
     * Adds files to the Sleeper table, with any number of references. Each new file should be specified once, with all
     * its references.
     * <p>
     * A file must never be referenced in two partitions where one is a descendent of another. This means each record in
     * a file must only be covered by one reference. A partition covers a range of records. A partition which is the
     * child of another covers a sub-range within the parent partition.
     *
     * @param  files                      The files to be added
     * @throws FileAlreadyExistsException if a file already exists
     * @throws StateStoreException        if the update fails for another reason
     */
    void addFilesWithReferences(List<AllReferencesToAFile> files) throws StateStoreException;

    /**
     * Performs atomic updates to split file references. This is used to push file references down the partition tree,
     * eg. where records are ingested to a non-leaf partition, or when a partition is split. A file referenced in a
     * larger, non-leaf partition may be split between smaller partitions which cover non-overlapping sub-ranges of the
     * original partition. This includes these records in compactions of the descendent partitions.
     * <p>
     * The aim is to combine all records into a small number of files for each leaf partition, where the leaves of the
     * partition tree should represent a separation of the data into manageable chunks. Compaction operates on file
     * references to pull records from multiple files into one, when they are referenced in the same partition. This
     * reduces the number of files in the system, and improves statistics and indexing within each partition. This
     * should result in faster queries, and more accurate partitioning when a partition is split.
     * <p>
     * Each {@link SplitFileReferenceRequest} will remove one file reference, and create new references to the same file
     * in descendent partitions. The reference counts will be tracked accordingly.
     * <p>
     * The ranges covered by the partitions of the new references must not overlap, so there
     * must never be two references to the same file where one partition is a descendent of the other.
     * <p>
     * Note that it is possible that the necessary updates may not fit in a single transaction. Each
     * {@link SplitFileReferenceRequest} is guaranteed to be done atomically in one transaction, but it is possible that
     * some may succeed and some may fail. If a single {@link SplitFileReferenceRequest} adds too many references to
     * apply in one transaction, this will also fail.
     *
     * @param  splitRequests                A list of {@link SplitFileReferenceRequest}s to apply
     * @throws SplitRequestsFailedException if any of the requests fail, even if some succeeded
     */
    void splitFileReferences(List<SplitFileReferenceRequest> splitRequests) throws SplitRequestsFailedException;

    /**
     * Atomically applies the results of jobs. Removes file references for a job's input files, and adds a reference to
     * an output file. This will be used for compaction.
     * <p>
     * This will validate that the input files were assigned to the job.
     * <p>
     * This will decrement the number of references for each of the input files. If no other references exist for those
     * files, they will become available for garbage collection.
     *
     * @param  requests                       requests for jobs to each have their results atomically applied
     * @throws ReplaceRequestsFailedException if any of the updates fail
     */
    void atomicallyReplaceFileReferencesWithNewOnes(List<ReplaceFileReferencesRequest> requests) throws ReplaceRequestsFailedException;

    /**
     * Atomically updates the job field of file references, as long as the job field is currently unset. This will be
     * used for compaction job input files.
     *
     * @param  requests                            A list of {@link AssignJobIdRequest}s which should each be applied
     *                                             atomically
     * @throws FileReferenceNotFoundException      if a reference does not exist
     * @throws FileReferenceAssignedToJobException if a reference is already assigned to a job
     * @throws StateStoreException                 if the update fails for another reason
     */
    void assignJobIds(List<AssignJobIdRequest> requests) throws StateStoreException;

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
     * @param  filenames                  The names of files that were deleted.
     * @throws FileNotFoundException      if a file does not exist
     * @throws FileHasReferencesException if a file still has references
     * @throws StateStoreException        if the update fails for another reason
     */
    void deleteGarbageCollectedFileReferenceCounts(List<String> filenames) throws StateStoreException;

    /**
     * Returns all references for files in any partition. This may return multiple references for a single file if it
     * contains records in more than one partition.
     * <p>
     * This must never return references for the same file on partitions where one is the ancestor of the other. This
     * means that every record in a file must only be referenced once.
     *
     * @return                     a list of all {@link FileReference}s in the Sleeper table
     * @throws StateStoreException if query fails
     */
    List<FileReference> getFileReferences() throws StateStoreException;

    /**
     * Returns a stream of files that are ready for garbage collection, i.e. they have no references and the last update
     * time is before maxUpdateTime.
     *
     * @param  maxUpdateTime       The latest time at which a file can have been updated in order to be garbage
     *                             collected
     * @return                     a stream of filenames with the matching status
     * @throws StateStoreException if query fails
     */
    Stream<String> getReadyForGCFilenamesBefore(Instant maxUpdateTime) throws StateStoreException;

    /**
     * Returns all file references which are not assigned to any job.
     *
     * @return                     a list of {@link FileReference}s which are not assigned to any job
     * @throws StateStoreException if query fails
     */
    List<FileReference> getFileReferencesWithNoJobId() throws StateStoreException;

    /**
     * Checks if files on a given partition are assigned to a certain job.
     *
     * @param  partitionId         the ID of the partition to query
     * @return                     a list of {@link FileReference}s on the partition
     * @throws StateStoreException if query fails
     */
    default boolean isPartitionFilesAssignedToJob(String partitionId, List<String> filenames, String jobId) throws StateStoreException {
        List<FileReference> fileReferences = getFileReferences();
        Map<String, FileReference> partitionFileByName = fileReferences.stream()
                .filter(reference -> Objects.equals(partitionId, reference.getPartitionId()))
                .collect(toMap(FileReference::getFilename, f -> f));
        boolean allAssigned = true;
        for (String filename : filenames) {
            FileReference reference = partitionFileByName.get(filename);
            if (reference == null) {
                throw new FileReferenceNotFoundException(filename, partitionId);
            } else if (reference.getJobId() == null) {
                allAssigned = false;
            } else if (!reference.getJobId().equals(jobId)) {
                throw new FileReferenceAssignedToJobException(reference);
            }
        }
        return allAssigned;
    }

    /**
     * Returns a map from the partition id to a list of file references in that partition. Each file may be included
     * multiple times in this map, as it may be referenced in more than one partition.
     *
     * @return                     a {@link Map} from the partition id to a {@link List} of all files referenced against
     *                             that partition
     * @throws StateStoreException if query fails
     */
    default Map<String, List<String>> getPartitionToReferencedFilesMap() throws StateStoreException {
        List<FileReference> fileReferences = getFileReferences();
        Map<String, List<String>> partitionToFiles = new HashMap<>();
        for (FileReference fileReference : fileReferences) {
            String partition = fileReference.getPartitionId();
            if (!partitionToFiles.containsKey(partition)) {
                partitionToFiles.put(partition, new ArrayList<>());
            }
            partitionToFiles.get(partition).add(fileReference.getFilename());
        }
        return partitionToFiles;
    }

    /**
     * Returns a report of files tracked in the store and their references. This includes reference counts, and internal
     * references against Sleeper partitions. This will include all files whose reference count is tracked against the
     * Sleeper table, whether it is referenced against partitions or not.
     * <p>
     * Files with internal references against partitions have records in the Sleeper table. Files with no internal
     * references are either in use by long-running operations, or are waiting to be garbage collected.
     *
     * @param  maxUnreferencedFiles Maximum number of files to return with no active references
     * @return                      the report
     * @throws StateStoreException  if query fails
     */
    AllReferencesToAllFiles getAllFilesWithMaxUnreferenced(int maxUnreferencedFiles) throws StateStoreException;

    /**
     * Performs extra setup steps that are needed before the file reference store can be used.
     *
     * @throws StateStoreException if initialisation fails
     */
    void initialise() throws StateStoreException;

    /**
     * Returns whether the file reference store has files in it or not. This includes files where no references are
     * stored, but the reference count is tracked.
     *
     * @return a boolean representing whether the state store has files in it or not.
     */
    boolean hasNoFiles() throws StateStoreException;

    /**
     * Clears all file data from the file reference store. Note that this does not delete any of the actual files.
     */
    void clearFileData() throws StateStoreException;

    /**
     * Used to fix the time of file updates. Should only be called during tests.
     *
     * @param time the time that any future file updates will be considered to occur
     */
    void fixFileUpdateTime(Instant time);
}
