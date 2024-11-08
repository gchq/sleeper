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
import sleeper.core.statestore.transactionlog.FileReferenceTransaction;
import sleeper.core.statestore.transactionlog.StateStoreFile;
import sleeper.core.statestore.transactionlog.StateStoreFiles;

import java.time.Instant;
import java.util.List;
import java.util.Objects;

import static java.util.stream.Collectors.toUnmodifiableList;

/**
 * A transaction to add files to the state store.
 */
public class AddFilesTransaction implements FileReferenceTransaction {

    private final List<AllReferencesToAFile> files;

    public AddFilesTransaction(List<AllReferencesToAFile> files) {
        this.files = files.stream().map(file -> file.withCreatedUpdateTime(null)).collect(toUnmodifiableList());
    }

    @Override
    public void validate(StateStoreFiles stateStoreFiles) throws StateStoreException {
        for (AllReferencesToAFile file : files) {
            if (stateStoreFiles.file(file.getFilename()).isPresent()) {
                throw new FileAlreadyExistsException(file.getFilename());
            }
        }
    }

    @Override
    public void apply(StateStoreFiles stateStoreFiles, Instant updateTime) {
        for (AllReferencesToAFile file : files) {
            stateStoreFiles.add(StateStoreFile.newFile(updateTime, file));
        }
    }

    @Override
    public int hashCode() {
        return Objects.hash(files);
    }

    @Override
    public boolean equals(Object obj) {
        if (this == obj) {
            return true;
        }
        if (!(obj instanceof AddFilesTransaction)) {
            return false;
        }
        AddFilesTransaction other = (AddFilesTransaction) obj;
        return Objects.equals(files, other.files);
    }

    @Override
    public String toString() {
        return "AddFilesTransaction{files=" + files + "}";
    }

}
