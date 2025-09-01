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
package sleeper.core.statestore.transactionlog;

import org.slf4j.Logger;
import org.slf4j.LoggerFactory;

import sleeper.core.statestore.AllReferencesToAllFiles;
import sleeper.core.statestore.FileReference;
import sleeper.core.statestore.FileReferenceStore;
import sleeper.core.statestore.StateStoreException;
import sleeper.core.statestore.transactionlog.log.TransactionLogEntry;
import sleeper.core.statestore.transactionlog.state.StateListenerBeforeApply;
import sleeper.core.statestore.transactionlog.state.StateStoreFiles;
import sleeper.core.statestore.transactionlog.transaction.impl.ClearFilesTransaction;

import java.time.Instant;
import java.util.List;
import java.util.function.Supplier;
import java.util.stream.Stream;

import static java.util.stream.Collectors.toUnmodifiableList;

/**
 * A file reference store backed by a log of transactions. Part of {@link TransactionLogStateStore}.
 */
class TransactionLogFileReferenceStore implements FileReferenceStore {

    public static final Logger LOGGER = LoggerFactory.getLogger(TransactionLogFileReferenceStore.class);
    private final TransactionLogHead<StateStoreFiles> head;
    private Supplier<Instant> timeSupplier = Instant::now;

    TransactionLogFileReferenceStore(TransactionLogHead<StateStoreFiles> head) {
        this.head = head;
    }

    @Override
    public void fixFileUpdateTime(Instant time) {
        timeSupplier = () -> time;
    }

    @Override
    public AllReferencesToAllFiles getAllFilesWithMaxUnreferenced(int maxUnreferencedFiles) throws StateStoreException {
        return files().allReferencesToAllFiles(maxUnreferencedFiles);
    }

    @Override
    public List<FileReference> getFileReferences() throws StateStoreException {
        return files().references().collect(toUnmodifiableList());
    }

    @Override
    public List<FileReference> getFileReferencesWithNoJobId() throws StateStoreException {
        return files().references()
                .filter(file -> file.getJobId() == null)
                .collect(toUnmodifiableList());
    }

    @Override
    public Stream<String> getReadyForGCFilenamesBefore(Instant maxUpdateTime) throws StateStoreException {
        return files().unreferencedBefore(maxUpdateTime);
    }

    @Override
    public boolean hasNoFiles() throws StateStoreException {
        return files().isEmpty();
    }

    void updateFromLog() throws StateStoreException {
        head.update();
    }

    @Override
    public void addFilesTransaction(AddTransactionRequest request) {
        head.addTransaction(timeSupplier.get(), request);
    }

    void applyEntryFromLog(TransactionLogEntry logEntry, StateListenerBeforeApply listener) {
        head.applyTransactionUpdatingIfNecessary(logEntry, listener);
    }

    void clearTransactionLog() {
        head.clearTransactionLog(new ClearFilesTransaction(), timeSupplier.get());
    }

    private StateStoreFiles files() throws StateStoreException {
        head.update();
        return head.state();
    }
}
