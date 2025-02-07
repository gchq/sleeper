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
package sleeper.core.statestore.transactionlog;

import java.util.stream.Stream;

/**
 * A store of a transaction log that can be used to derive the state of a Sleeper table. Used by
 * {@link TransactionLogStateStore}.
 */
public interface TransactionLogStore {

    /**
     * Adds a new transaction at the end of the log.
     *
     * @param  entry                               the entry
     * @throws DuplicateTransactionNumberException thrown if a transaction already exists with the given number,
     *                                             potentially because another process updated the state store at the
     *                                             same time
     */
    void addTransaction(TransactionLogEntry entry) throws DuplicateTransactionNumberException;

    /**
     * Streams through transactions to the end of the log, starting after a given transaction.
     *
     * @param  lastTransactionNumber the last transaction number that should not be read
     * @return                       all transactions in order, starting at the one after the specified number
     */
    Stream<TransactionLogEntry> readTransactionsAfter(long lastTransactionNumber);

    /**
     * Streams through transactions to before a certain point, starting after a given transaction. This will be used
     * when we want to apply some transaction locally but we are not yet up to date with the log before that
     * transaction. In that case we want to avoid re-reading or seeking beyond the transaction we're processing.
     *
     * @param  lastTransactionNumber the last transaction number that should not be read
     * @param  nextTransactionNumber the next transaction number that we are trying to process
     * @return                       the requested transactions in order
     */
    Stream<TransactionLogEntry> readTransactionsBetween(long lastTransactionNumber, long nextTransactionNumber);

    /**
     * Deletes transactions from the log that are at or before the provided transaction number.
     *
     * @param transactionNumber the transaction number
     */
    void deleteTransactionsAtOrBefore(long transactionNumber);
}
