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
package sleeper.core.statestore.testutils;

import org.slf4j.Logger;
import org.slf4j.LoggerFactory;

import sleeper.core.statestore.AllReferencesToAFile;
import sleeper.core.statestore.AssignJobIdRequest;
import sleeper.core.statestore.FileReference;
import sleeper.core.statestore.ReplaceFileReferencesRequest;
import sleeper.core.statestore.SplitFileReferenceRequest;
import sleeper.core.statestore.StateStore;
import sleeper.core.statestore.StateStoreException;
import sleeper.core.statestore.exception.FileAlreadyExistsException;
import sleeper.core.statestore.exception.FileHasReferencesException;
import sleeper.core.statestore.exception.FileNotFoundException;
import sleeper.core.statestore.exception.FileReferenceAssignedToJobException;
import sleeper.core.statestore.exception.FileReferenceNotFoundException;
import sleeper.core.statestore.exception.ReplaceRequestsFailedException;
import sleeper.core.statestore.exception.SplitRequestsFailedException;
import sleeper.core.statestore.transactionlog.AddTransactionRequest;
import sleeper.core.statestore.transactionlog.state.StateListenerBeforeApply;
import sleeper.core.statestore.transactionlog.transaction.StateStoreTransaction;
import sleeper.core.statestore.transactionlog.transaction.impl.AddFilesTransaction;
import sleeper.core.statestore.transactionlog.transaction.impl.AssignJobIdsTransaction;
import sleeper.core.statestore.transactionlog.transaction.impl.DeleteFilesTransaction;
import sleeper.core.statestore.transactionlog.transaction.impl.ReplaceFileReferencesTransaction;
import sleeper.core.statestore.transactionlog.transaction.impl.SplitFileReferencesTransaction;

import java.util.List;

/**
 * Wraps a state store and exposes methods for shortcuts during tests.
 */
public class StateStoreUpdatesWrapper {
    public static final Logger LOGGER = LoggerFactory.getLogger(StateStoreUpdatesWrapper.class);

    private final StateStore stateStore;

    private StateStoreUpdatesWrapper(StateStore stateStore) {
        this.stateStore = stateStore;
    }

    /**
     * Wraps the given state store. Has a short name for ease of use in tests when it's statically imported.
     *
     * @param  stateStore the state store
     * @return            the wrapper
     */
    public static StateStoreUpdatesWrapper update(StateStore stateStore) {
        return new StateStoreUpdatesWrapper(stateStore);
    }

    /**
     * Adds a file to the table, with one reference.
     *
     * @param  fileReference              the reference to be added
     * @throws FileAlreadyExistsException if the file already exists
     * @throws StateStoreException        if the update fails for another reason
     */
    public void addFile(FileReference fileReference) throws StateStoreException {
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
    public void addFiles(List<FileReference> fileReferences) throws StateStoreException {
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
    public void addFilesWithReferences(List<AllReferencesToAFile> files) throws StateStoreException {
        AddFilesTransaction transaction = new AddFilesTransaction(files);
        stateStore.addTransaction(AddTransactionRequest.withTransaction(transaction)
                .beforeApplyListener(StateListenerBeforeApply.withFilesState(state -> transaction.validateFiles(state)))
                .build());
    }

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
    public void splitFileReferences(List<SplitFileReferenceRequest> splitRequests) throws SplitRequestsFailedException {
        try {
            addTransaction(new SplitFileReferencesTransaction(splitRequests));
        } catch (StateStoreException e) {
            throw new SplitRequestsFailedException(List.of(), splitRequests, e);
        }
    }

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
    public void assignJobIds(List<AssignJobIdRequest> requests) throws StateStoreException {
        AssignJobIdsTransaction.ignoringEmptyRequests(requests).ifPresent(this::addTransaction);
    }

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
    public void atomicallyReplaceFileReferencesWithNewOnes(List<ReplaceFileReferencesRequest> requests) throws ReplaceRequestsFailedException {
        try {
            ReplaceFileReferencesTransaction transaction = new ReplaceFileReferencesTransaction(requests);
            stateStore.addTransaction(AddTransactionRequest.withTransaction(transaction)
                    .beforeApplyListener(StateListenerBeforeApply.withFilesState(state -> transaction.validateStateChange(state)))
                    .build());
        } catch (StateStoreException e) {
            throw new ReplaceRequestsFailedException(requests, e);
        }
    }

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
    public void deleteGarbageCollectedFileReferenceCounts(List<String> filenames) throws StateStoreException {
        addTransaction(new DeleteFilesTransaction(filenames));
    }

    private void addTransaction(StateStoreTransaction<?> transaction) {
        stateStore.addTransaction(AddTransactionRequest.withTransaction(transaction).build());
    }

}
