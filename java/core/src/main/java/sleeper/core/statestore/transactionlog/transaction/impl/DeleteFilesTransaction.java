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
package sleeper.core.statestore.transactionlog.transaction.impl;

import sleeper.core.statestore.StateStore;
import sleeper.core.statestore.StateStoreException;
import sleeper.core.statestore.exception.FileHasReferencesException;
import sleeper.core.statestore.exception.FileNotFoundException;
import sleeper.core.statestore.transactionlog.AddTransactionRequest;
import sleeper.core.statestore.transactionlog.state.StateStoreFile;
import sleeper.core.statestore.transactionlog.state.StateStoreFiles;
import sleeper.core.statestore.transactionlog.transaction.FileReferenceTransaction;

import java.time.Instant;
import java.util.List;
import java.util.Objects;

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
 */
public class DeleteFilesTransaction implements FileReferenceTransaction {

    private final List<String> filenames;

    public DeleteFilesTransaction(List<String> filenames) {
        this.filenames = filenames;
    }

    /**
     * Commit this transaction directly to the state store without going to the commit queue. This will throw any
     * validation exceptions immediately, even if they wouldn't be as part of an asynchronous commit.
     *
     * @param  stateStore                 the state store
     * @throws FileNotFoundException      if a file does not exist
     * @throws FileHasReferencesException if a file still has references
     * @throws StateStoreException        if the update fails for another reason
     */
    public void synchronousCommit(StateStore stateStore) throws StateStoreException {
        stateStore.addFilesTransaction(AddTransactionRequest.withTransaction(this).build());
    }

    @Override
    public void validate(StateStoreFiles stateStoreFiles) throws StateStoreException {
        for (String filename : filenames) {
            StateStoreFile file = stateStoreFiles.file(filename)
                    .orElseThrow(() -> new FileNotFoundException(filename));
            if (!file.getReferences().isEmpty()) {
                throw new FileHasReferencesException(file.toModel());
            }
        }
    }

    @Override
    public void apply(StateStoreFiles stateStoreFiles, Instant updateTime) {
        filenames.forEach(stateStoreFiles::remove);
    }

    @Override
    public boolean isEmpty() {
        return filenames.isEmpty();
    }

    @Override
    public int hashCode() {
        return Objects.hash(filenames);
    }

    @Override
    public boolean equals(Object obj) {
        if (this == obj) {
            return true;
        }
        if (!(obj instanceof DeleteFilesTransaction)) {
            return false;
        }
        DeleteFilesTransaction other = (DeleteFilesTransaction) obj;
        return Objects.equals(filenames, other.filenames);
    }

    @Override
    public String toString() {
        return "DeleteFilesTransaction{filenames=" + filenames + "}";
    }
}
