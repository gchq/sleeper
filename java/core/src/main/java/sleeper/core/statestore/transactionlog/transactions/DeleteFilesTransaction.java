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
package sleeper.core.statestore.transactionlog.transactions;

import sleeper.core.statestore.AllReferencesToAFile;
import sleeper.core.statestore.StateStoreException;
import sleeper.core.statestore.exception.FileHasReferencesException;
import sleeper.core.statestore.exception.FileNotFoundException;
import sleeper.core.statestore.transactionlog.FileReferenceTransaction;
import sleeper.core.statestore.transactionlog.StateStoreFiles;

import java.time.Instant;
import java.util.List;
import java.util.Objects;

/**
 * A transaction to delete files that have no remaining references. This is used to perform garbage collection.
 */
public class DeleteFilesTransaction implements FileReferenceTransaction {

    private final List<String> filenames;

    public DeleteFilesTransaction(List<String> filenames) {
        this.filenames = filenames;
    }

    @Override
    public void validate(StateStoreFiles stateStoreFiles) throws StateStoreException {
        for (String filename : filenames) {
            AllReferencesToAFile file = stateStoreFiles.file(filename)
                    .orElseThrow(() -> new FileNotFoundException(filename));
            if (file.getReferenceCount() > 0) {
                throw new FileHasReferencesException(file);
            }
        }
    }

    @Override
    public void apply(StateStoreFiles stateStoreFiles, Instant updateTime) {
        filenames.forEach(stateStoreFiles::remove);
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
