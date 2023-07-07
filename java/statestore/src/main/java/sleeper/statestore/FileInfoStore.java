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
package sleeper.statestore;

import java.util.Iterator;
import java.util.List;
import java.util.Map;

/**
 * A FileInfoStore stores metadata about the files in a Sleeper table. Two types of
 * information are stored: file-in-partition metadata which records that a file
 * contains records in a particular partition; file-lifecyle metadata which records
 * that a file is present in the system and its status. File-in-partition metadata
 * exists as {@link FileInfo} objects where the {@link FileInfo.FileStatus} value
 * is {@link FileInfo.FileStatus.FILE_IN_PARTITION}. File-lifecyle metadata exists
 * as {@link FileInfo} objects where the {@link FileInfo.FileStatus} value is
 * either {@link FileInfo.FileStatus.ACTIVE} or
 * {@link FileInfo.FileStatus.GARBAGE_COLLECTION_PENDING}.
 */
public interface FileInfoStore {

    /**
     * This is used when a file of data is added to a table. The provided {@FileInfo}
     * should indicate which partition the data is in - the file should only contain
     * data for that partition. A record is added to the file-in-partition table and
     * a record indicating the file exists is added to the file-lifecycle table.
     *
     * @param fileInfo The fileInfo to be added
     * @throws StateStoreException if the update fails
     */
    void addFile(FileInfo fileInfo) throws StateStoreException;

    /**
     * This is used when a multiple files are added to a table. Each provided {@FileInfo}
     * should indicate which partition the data in that file is in - the file should only
     * contain data for that partition. For each {@FileInfo} a record is added to the
     * file-in-partition table and a record indicating the file exists is added to the
     * file-lifecycle table.
     *
     * For some implementations of <code>FileInfoStore</code> it may be more efficient
     * to use this method than to repeatedly call <code>addFile(FileInfo fileInfo)</code>.
     *
     * @param fileInfos The fileInfos to be added
     * @throws StateStoreException if the update fails
     */
    void addFiles(List<FileInfo> fileInfos) throws StateStoreException;

    /**
     * This method is used to commit the results of a compaction job. The relevant file-in-partition
     * records for the input files are deleted and information about the output file is added.
     * (As a file may be present in multiple partitions, not all file-in-partition records for
     * the file are deleted.) This operation is done atomically, conditional on the file-in-partition
     * records still existing. Doing it atomically guarantees that any client that reads the file-in
     * -partition records will either get results from before this method is called, or after it
     * is called, but will not see a partially complete version. Performing this operation conditionally
     * on the file-in-partition records still existing guarantees that if two tasks are executing
     * the same compaction job then only one can succeed.
     *
     * Note that this method does not set the status of any files to be garbage collection pending. If the last
     * file-in-partition record for a file is deleted then there will still be an
     * {@link FileInfo.FileStatus.ACTIVE} file-lifecycle record for the file. Identifying that that
     * active file should now have its status set to {@link FileInfo.FileStatus.GARBAGE_COLLECTION_PENDING}
     * is an asynchronous operation that is performed using findFilesThatShouldHaveStatusOfGCPending.
     *
     * @param fileInPartitionRecordsToBeDeleted The file-in-partition records to be deleted
     * @param newActiveFile                     The file to be added as an {@link FileInfo.FileStatus.ACTIVE} file
     * @throws StateStoreException if the update fails
     */
    void atomicallyRemoveFileInPartitionRecordsAndCreateNewActiveFile(
            List<FileInfo> fileInPartitionRecordsToBeDeleted,
            FileInfo newActiveFile) throws StateStoreException;

    /**
     * This method is used to commit the results of a splitting compaction job. The relevant file-in-partition
     * records for the input files are deleted and information about the output files is added.
     * (As a file may be present in multiple partitions, not all file-in-partition records for
     * the file are deleted.) This operation is done atomically, conditional on the file-in-partition
     * records still existing. Doing it atomically guarantees that any client that reads the file-in
     * -partition records will either get results from before this method is called, or after it
     * is called, but will not see a partially complete version. Performing this operation conditionally
     * on the file-in-partition records still existing guarantees that if two tasks are executing
     * the same compaction job then only one can succeed.
     *
     * Note that this method does not set any files to be ready for garbage collection. If the last
     * file-in-partition record for a file is deleted then there will still be an
     * {@link FileInfo.FileStatus.ACTIVE} file-lifecycle record for the file. Identifying that that
     * active file should now have its status set to {@link FileInfo.FileStatus.GARBAGE_COLLECTION_PENDING}
     * is an asynchronous operation that is performed using findFilesThatShouldHaveStatusOfGCPending.
     *
     * @param fileInPartitionRecordsToBeDeleted The file-in-partition records to be deleted
     * @param leftFileInfo                      The first file to be added as an {@link FileInfo.FileStatus.ACTIVE} file
     * @param rightFileInfo                     The second file to be added as an {@link FileInfo.FileStatus.ACTIVE} file
     * @throws StateStoreException if the update fails
     */
    void atomicallyRemoveFileInPartitionRecordsAndCreateNewActiveFiles(List<FileInfo> fileInPartitionRecordsToBeDeleted,
                                                                  FileInfo leftFileInfo,
                                                                  FileInfo rightFileInfo) throws StateStoreException;

    /**
     * This method is used when compaction jobs are created. It sets the jobId field to the provided
     * value for the given file-in-partition records. This is done atomically, conditional on
     * the job id fields being null. This condition ensures that each file can only have one
     * job id added to it.
     *
     * @param jobId     The job id which will be added to the FileInfos
     * @param fileInfos The FileInfos whose status will be updated
     * @throws StateStoreException if the update fails
     */
    void atomicallyUpdateJobStatusOfFiles(String jobId, List<FileInfo> fileInfos)
            throws StateStoreException;

    /**
     * Deletes the file-lifecyle record for the files with the given filenames.
     *
     * @param filenames The name of the file to be deleted.
     * @throws StateStoreException if the delete fails
     */
    void deleteFileLifecycleEntries(List<String> filenames) throws StateStoreException;

    /**
     * Returns all file-in-partition {@link FileInfo}s.
     *
     * @return a {@code List} of {@code FileInfo}s for the file-in-partition records
     * @throws StateStoreException if the query fails
     */
    List<FileInfo> getFileInPartitionList() throws StateStoreException;

    /**
     * Returns all file-lifecycle {@link FileInfo}s.
     *
     * @return a {@code List} of {@code FileInfo}s for the file-lifecycle records
     * @throws StateStoreException if the query fails
     */
     List<FileInfo> getFileLifecycleList() throws StateStoreException;

    /**
     * Returns all file-lifecycle {@link FileInfo}s with a status of ACTIVE.
     *
     * @return a {@code} List} of the {@link FileInfo}s with a status of ACTIVE
     * @throws StateStoreException if retrieving the active file list fails
     */
     List<FileInfo> getActiveFileList() throws StateStoreException;

    /**
     * Returns an {@link Iterator} of filenames of files that are ready
     * for garbage collection, i.e. their file-lifecycle status is
     * {@link FileInfo.FileStatus.GARBAGE_COLLECTION_PENDING} and the last update
     * time is more than <code>delayBeforeGarbageCollectionInSeconds</code> seconds ago (where
     * <code>delayBeforeGarbageCollectionInSeconds</code> is taken from the table properties).
     *
     * @return an {@link Iterator} of filenames of files that are ready to be garbage collected
     * @throws StateStoreException if query fails
     */
    Iterator<String> getReadyForGCFiles() throws StateStoreException;

   /**
     * Returns an {@link Iterator} of {@link FileInfo}s file-lifecycle records of files that
     * are ready for garbage collection, i.e. their file-lifecycle status is
     * {@link FileInfo.FileStatus.READY_FOR_GARBAGE_COLLECTION} and the last update
     * time is more than <code>delayBeforeGarbageCollectionInSeconds</code> seconds ago (where
     * <code>delayBeforeGarbageCollectionInSeconds</code> is taken from the table properties).
     *
     * @return an {@link Iterator} of {@link FileInfo}s file-lifecyle records of files that are ready to be garbage collected
     * @throws StateStoreException if query fails
     */
    Iterator<FileInfo> getReadyForGCFileInfos() throws StateStoreException;

    /**
     * Identifies files which should have a status of GARBAGE_COLLECTION_PENDING. This means
     * that there is a file-lifecycle record for them but no file-in-partition records. This
     * method should set the status of any such files to {@link FileInfo.FileStatus.GARBAGE_COLLECTION_PENDING}
     * in the file-lifecycle store.
     *
     * @throws StateStoreException if operation fails
     */
    void findFilesThatShouldHaveStatusOfGCPending() throws StateStoreException;

    /**
     * Returns all file-in-partition records for files which have a null job id.
     *
     * @return a {@link List} of {@link FileInfo}s which are {@link FileInfo.FileStatus.ACTIVE} and have a null job id
     * @throws StateStoreException if the query fails
     */
    List<FileInfo> getFileInPartitionInfosWithNoJobId() throws StateStoreException;

    /**
     * Returns a {@link Map} from the partition id to a {@link List} of the filenames
     * of files for which there are file-in-partition records for the file.
     *
     * @return a {@link Map} from the partition id to a {@link List} of the filenames
     * @throws StateStoreException if the query fails
     */
    Map<String, List<String>> getPartitionToFileInPartitionMap() throws StateStoreException;

    /**
     * Used to initialise the store.
     *
     * @throws StateStoreException if the initialisation fails
     */
    void initialise() throws StateStoreException;
}
