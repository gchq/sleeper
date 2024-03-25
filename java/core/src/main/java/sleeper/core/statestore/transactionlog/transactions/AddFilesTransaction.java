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
import sleeper.core.statestore.exception.FileAlreadyExistsException;
import sleeper.core.statestore.transactionlog.StateStoreTransaction;
import sleeper.core.statestore.transactionlog.TransactionLogHead;

import java.util.List;

public class AddFilesTransaction implements StateStoreTransaction {

    private final List<AllReferencesToAFile> files;

    public AddFilesTransaction(List<AllReferencesToAFile> files) {
        this.files = files;
    }

    @Override
    public void validate(TransactionLogHead state) throws StateStoreException {
        for (AllReferencesToAFile file : files) {
            if (state.files().file(file.getFilename()).isPresent()) {
                throw new FileAlreadyExistsException(file.getFilename());
            }
        }
    }

    @Override
    public void apply(TransactionLogHead state) {
        state.files().add(this.files);
    }

}
