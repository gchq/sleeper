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

import sleeper.core.statestore.AllReferencesToAFile;
import sleeper.core.statestore.FileReference;
import sleeper.core.statestore.StateStore;
import sleeper.core.statestore.StateStoreException;
import sleeper.core.statestore.exception.FileAlreadyExistsException;
import sleeper.core.statestore.transactionlog.AddTransactionRequest;
import sleeper.core.statestore.transactionlog.state.StateListenerBeforeApply;
import sleeper.core.statestore.transactionlog.transaction.impl.AddFilesTransaction;

import java.util.List;

/**
 * Wraps a state store and exposes methods for shortcuts during tests.
 */
public class StateStoreUpdatesWrapper {

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

}
