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
import sleeper.core.statestore.transactionlog.transaction.StateStoreTransaction;
import sleeper.core.statestore.transactionlog.transaction.TransactionSerDe;
import sleeper.core.statestore.transactionlog.transaction.TransactionType;

import java.io.BufferedReader;
import java.io.IOException;
import java.io.PrintWriter;
import java.io.StringWriter;
import java.io.UncheckedIOException;
import java.nio.file.Files;
import java.nio.file.Path;
import java.time.Instant;
import java.util.Comparator;
import java.util.List;
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
            writer.println(entry.getUpdateTime());
            writer.println(entry.getBodyKey().orElse(""));
            writer.println(entry.getTransaction().map(serDe::toJson).orElse(""));
        } catch (IOException e) {
            throw new UncheckedIOException(e);
        }
    }

    @Override
    public Stream<TransactionLogEntry> readTransactions(TransactionLogRange range) {
        List<TransactionFile> files = listFiles(range);
        return files.stream()
                .sorted(Comparator.comparing(TransactionFile::transactionNumber))
                .map(file -> readEntry(file.file()));
    }

    @Override
    public void deleteTransactionsAtOrBefore(long transactionNumber) {
        List<TransactionFile> files = listFiles(new TransactionLogRange(1, transactionNumber + 1));
        for (TransactionFile file : files) {
            try {
                Files.delete(file.file());
            } catch (IOException e) {
                throw new UncheckedIOException(e);
            }
        }
    }

    private List<TransactionFile> listFiles(TransactionLogRange range) {
        try (Stream<Path> stream = Files.list(directory)) {
            return stream.map(TransactionFile::from)
                    .filter(file -> isInRange(file.transactionNumber(), range))
                    .toList();
        } catch (IOException e) {
            throw new UncheckedIOException(e);
        }
    }

    private boolean isInRange(long transactionNumber, TransactionLogRange range) {
        return transactionNumber >= range.startInclusive()
                && (!range.isMaxTransactionBounded() || transactionNumber < range.endExclusive());
    }

    private TransactionLogEntry readEntry(Path file) {
        try (BufferedReader reader = Files.newBufferedReader(file)) {
            long transactionNumber = Long.parseLong(reader.readLine());
            TransactionType transactionType = TransactionType.valueOf(reader.readLine());
            Instant updateTime = Instant.parse(reader.readLine());
            String bodyKey = reader.readLine();
            if (bodyKey.length() > 0) {
                return new TransactionLogEntry(transactionNumber, updateTime, transactionType, bodyKey);
            } else {
                StringWriter stringWriter = new StringWriter();
                reader.transferTo(stringWriter);
                StateStoreTransaction<?> transaction = serDe.toTransaction(transactionType, stringWriter.toString());
                return new TransactionLogEntry(transactionNumber, updateTime, transaction);
            }
        } catch (IOException e) {
            throw new UncheckedIOException(e);
        }
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

    /**
     * A holder of a transaction file where the transaction number has been detected from the file name.
     *
     * @param file              the path to the file
     * @param transactionNumber the transaction number in the file name
     */
    private record TransactionFile(Path file, long transactionNumber) {

        static TransactionFile from(Path file) {
            String filename = file.getFileName().toString();
            long transactionNumber = Long.parseLong(filename.substring(0, filename.indexOf('.')));
            return new TransactionFile(file, transactionNumber);
        }
    }

}
