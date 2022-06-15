/*
 * Copyright 2022 Crown Copyright
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
package sleeper.statestore;

import sleeper.core.partition.Partition;
import sleeper.core.schema.type.PrimitiveType;

import java.util.Iterator;
import java.util.List;
import java.util.Map;

/**
 * Stores information about the data files and their status (i.e. {@link FileInfo}s,
 * and the {@link Partition}s).
 */
public interface StateStore {

    /**
     * Returns the {@link PrimitiveType}s of the row keys.
     */
    List<PrimitiveType> getRowKeyTypes();
    
    /**
     * Adds a {@link FileInfo}.
     * 
     * @param fileInfo The fileInfo to be added
     * @throws StateStoreException 
     */
    void addFile(FileInfo fileInfo) throws StateStoreException;

    /**
     * Adds a {@link List} of {@link FileInfo}s.
     *
     * @param fileInfos The fileInfos to be added
     * @throws StateStoreException
     */
    void addFiles(List<FileInfo> fileInfos) throws StateStoreException;

    /**
     * Atomically changes the status of some files from {@link FileInfo.FileStatus.ACTIVE}
     * to {@link FileInfo.FileStatus.READY_FOR_GARBAGE_COLLECTION}, and adds a new
     * {@link FileInfo} as an {@link FileInfo.FileStatus.ACTIVE} file.
     * 
     * @param filesToBeMarkedReadyForGC The files to be marked as {@link FileInfo.FileStatus.READY_FOR_GARBAGE_COLLECTION}
     * @param newActiveFile The file to be added as an {@link FileInfo.FileStatus.ACTIVE} file
     * @throws StateStoreException 
     */
    void atomicallyUpdateFilesToReadyForGCAndCreateNewActiveFile(
            List<FileInfo> filesToBeMarkedReadyForGC,
            FileInfo newActiveFile) throws StateStoreException;

    /**
     * Atomically changes the status of some files from {@link FileInfo.FileStatus.ACTIVE}
     * to {@link FileInfo.FileStatus.READY_FOR_GARBAGE_COLLECTION}, and adds two new
     * {@link FileInfo}s as an {@link FileInfo.FileStatus.ACTIVE} file.
     * 
     * @param filesToBeMarkedReadyForGC The files to be marked as {@link FileInfo.FileStatus.READY_FOR_GARBAGE_COLLECTION}.
     * @param leftFileInfo The first file to be added as an {@link FileInfo.FileStatus.ACTIVE} file
     * @param rightFileInfo The second file to be added as an {@link FileInfo.FileStatus.ACTIVE} file
     * @throws StateStoreException 
     */
    void atomicallyUpdateFilesToReadyForGCAndCreateNewActiveFiles(List<FileInfo> filesToBeMarkedReadyForGC,
                                                                  FileInfo leftFileInfo,
                                                                  FileInfo rightFileInfo) throws StateStoreException;

    /**
     * Atomically updates the job field of the input files of the compactionJob to the job
     * id, as long as the job field is currently null.
     * 
     * @param jobId The job id which will be added to the FileInfos
     * @param fileInfos The FileInfos whose status will be updated
     * @throws StateStoreException 
     */
    void atomicallyUpdateJobStatusOfFiles(String jobId, List<FileInfo> fileInfos)
            throws StateStoreException;

    /**
     * Deletes this file with the status of {@link FileInfo.FileStatus.READY_FOR_GARBAGE_COLLECTION}.
     *
     * @param fileInfo The FileInfo to be deleted.
     * @throws StateStoreException
     */
    void deleteReadyForGCFile(FileInfo fileInfo) throws StateStoreException;

    /**
     * Returns all {@link FileInfo}s with a status of status.
     *
     * @return a {@code List} of {@code FileInfo.FileStatus}es with the matching status
     * @throws StateStoreException 
     */
    List<FileInfo> getActiveFiles() throws StateStoreException;

    /**
     * Returns an {@link Iterator} of files that are ready for garbage collection, i.e. their status is
     * {@link FileInfo.FileStatus.READY_FOR_GARBAGE_COLLECTION} and the last update time is more than
     * <code>delayBeforeGarbageCollectionInSeconds</code> seconds ago (where
     * <code>delayBeforeGarbageCollectionInSeconds</code> is taken from the SleeperProperties).
     *
     * @return a {@code List} of size of most max of {@code FileInfo.FileStatus}es with the matching status
     * @throws StateStoreException 
     */
    Iterator<FileInfo> getReadyForGCFiles() throws StateStoreException;

    /**
     * Returns all {@link FileInfo}s with status {@link FileInfo.FileStatus} of
     * {@link FileInfo.FileStatus.ACTIVE} which have a null job id.
     * 
     * @return a {@code List} of {@code FileInfo.FileStatus}es which are {@link FileInfo.FileStatus.ACTIVE} and have a null job id
     * @throws StateStoreException 
     */
    List<FileInfo> getActiveFilesWithNoJobId() throws StateStoreException;

    /**
     * Returns a {@link Map} from the partition id to a {@List} of the filenames.
     * 
     * @return a {@link Map} from the partition id to a {@List} of the filenames
     * @throws StateStoreException 
     */
    Map<String, List<String>> getPartitionToActiveFilesMap() throws StateStoreException;
    
    /**
     * Atomically updates a {@link Partition} and adds two new ones, conditional
     * on the splitPartition being marked as a leaf partition.
     *
     * @param splitPartition The {@link Partition} to be updated
     * @param newPartition1 The first new {@link Partition}
     * @param newPartition2 The second new {@link Partition}
     * @throws StateStoreException
     */
    void atomicallyUpdatePartitionAndCreateNewOnes(Partition splitPartition,
                                                   Partition newPartition1,
                                                   Partition newPartition2) throws StateStoreException;

    /**
     * Returns all the {@link Partition}s.
     * 
     * @return All the {@link Partition}s
     * @throws StateStoreException 
     */
    List<Partition> getAllPartitions() throws StateStoreException;

    /**
     * Returns all the [@link Partition}s which are leaf partitions.
     * 
     * @return All the {@link Partition}s which are leaf partitions.
     * @throws StateStoreException 
     */
    List<Partition> getLeafPartitions() throws StateStoreException;

    /**
     * Initialises the store with a single root {@link Partition} covering all
     * keys.
     * 
     * @throws StateStoreException 
     */
    void initialise() throws StateStoreException;
    
    /**
     * Initialises store with the list of {@link Partition}s.
     * 
     * @param partitions The initial list of {@link Partition}s
     * @throws StateStoreException 
     */
    void initialise(List<Partition> partitions) throws StateStoreException;
}
