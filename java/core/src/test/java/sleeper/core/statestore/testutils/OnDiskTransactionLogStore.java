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

import sleeper.core.statestore.transactionlog.log.DuplicateTransactionNumberException;
import sleeper.core.statestore.transactionlog.log.TransactionLogEntry;
import sleeper.core.statestore.transactionlog.log.TransactionLogRange;
import sleeper.core.statestore.transactionlog.log.TransactionLogStore;
import sleeper.core.statestore.transactionlog.transaction.TransactionSerDe;

import java.io.IOException;
import java.io.PrintWriter;
import java.io.UncheckedIOException;
import java.nio.file.Files;
import java.nio.file.Path;
import java.util.stream.Stream;

/**
 * An implementation of a transaction log store holding transactions on disk. Can be used to cache a transaction log
 * locally.
 */
public class OnDiskTransactionLogStore implements TransactionLogStore {

    private final Path directory;
    private final TransactionSerDe serDe;

    private OnDiskTransactionLogStore(Path directory, TransactionSerDe serDe) {
        this.directory = directory;
        this.serDe = serDe;
    }

    @Override
    public void addTransaction(TransactionLogEntry entry) throws DuplicateTransactionNumberException {
        try (PrintWriter writer = new PrintWriter(Files.newBufferedWriter(directory.resolve(entry.getTransactionNumber() + ".transaction")))) {
            writer.println(entry.getTransactionNumber());
            writer.println(entry.getTransactionType());
            writer.println(entry.getBodyKey().orElse(""));
            writer.println(entry.getTransaction().map(serDe::toJson).orElse(""));
        } catch (IOException e) {
            throw new UncheckedIOException(e);
        }
    }

    @Override
    public Stream<TransactionLogEntry> readTransactions(TransactionLogRange range) {
        return Stream.of();
    }

    @Override
    public void deleteTransactionsAtOrBefore(long transactionNumber) {
        // TODO Auto-generated method stub
        throw new UnsupportedOperationException("Unimplemented method 'deleteTransactionsAtOrBefore'");
    }

    /**
     * Creates a store to hold the transaction log in a specific directory. The directory should not contain anything
     * else.
     *
     * @param  directory the directory
     * @return           the store
     */
    public static TransactionLogStore inDirectory(Path directory, TransactionSerDe serDe) {
        return new OnDiskTransactionLogStore(directory, serDe);
    }

}
