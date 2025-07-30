/*
 * Copyright 2022-2025 Crown Copyright
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

import sleeper.core.statestore.exception.FileReferenceAssignedToJobException;
import sleeper.core.statestore.exception.FileReferenceNotFoundException;

import java.time.Instant;
import java.util.ArrayList;
import java.util.HashMap;
import java.util.List;
import java.util.Map;
import java.util.stream.Stream;

import static java.util.stream.Collectors.groupingBy;

/**
 * Serves queries about the data files in a Sleeper table. This includes a count of the number of references
 * to the file, and internal references which assign all the data in the file to non-overlapping partitions.
 */
public interface FileReferenceStoreQueries {

    /**
     * Returns all references for files in any partition. This may return multiple references for a single file if it
     * contains rows in more than one partition.
     * <p>
     * This must never return references for the same file on partitions where one is the ancestor of the other. This
     * means that every row in a file must only be referenced once.
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
     * Checks if files are assigned to jobs.
     *
     * @param  requests            the definitions of which files should be assigned to which jobs
     * @return                     true if all files are assigned to all jobs
     * @throws StateStoreException if query fails
     */
    default boolean isAssigned(List<CheckFileAssignmentsRequest> requests) throws StateStoreException {
        List<FileReference> fileReferences = getFileReferences();
        Map<String, List<FileReference>> referencesByPartitionId = fileReferences.stream()
                .collect(groupingBy(FileReference::getPartitionId));
        boolean allAssigned = true;
        for (CheckFileAssignmentsRequest request : requests) {
            List<FileReference> references = referencesByPartitionId.getOrDefault(request.getPartitionId(), List.of());
            for (String filename : request.getFilenames()) {
                FileReference reference = references.stream().filter(ref -> filename.equals(ref.getFilename())).findFirst().orElse(null);
                if (reference == null) {
                    throw new FileReferenceNotFoundException(filename, request.getPartitionId());
                } else if (reference.getJobId() == null) {
                    allAssigned = false;
                } else if (!reference.getJobId().equals(request.getJobId())) {
                    throw new FileReferenceAssignedToJobException(reference);
                }
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
     * Files with internal references against partitions have rows in the Sleeper table. Files with no internal
     * references are either in use by long-running operations, or are waiting to be garbage collected.
     *
     * @param  maxUnreferencedFiles maximum number of files to return with no references
     * @return                      the report
     * @throws StateStoreException  if query fails
     */
    AllReferencesToAllFiles getAllFilesWithMaxUnreferenced(int maxUnreferencedFiles) throws StateStoreException;

    /**
     * Returns whether the file reference store has files in it or not. This includes files where no references are
     * stored, but the reference count is tracked.
     *
     * @return a boolean representing whether the state store has files in it or not.
     */
    boolean hasNoFiles() throws StateStoreException;

}
