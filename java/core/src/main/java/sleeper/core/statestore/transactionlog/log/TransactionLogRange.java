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
package sleeper.core.statestore.transactionlog.log;

/**
 * Range of transaction numbers to be read. The next transaction number field will be used when we want to apply some
 * transaction locally but we are not yet up to date with the log before that transaction.
 * In that case we want to avoid re-reading or seeking beyond the transaction we're processing.
 *
 * @param startInclusive the first transaction number
 * @param endExclusive   the exclusive upper bound (the first transaction number after the range)
 */
public record TransactionLogRange(long startInclusive, long endExclusive) {

    /**
     * Returns the range of transactions needed to update the local state to the latest transaction.
     *
     * @param  startExclusive the last transaction number that was applied to the local state
     * @return                range of transaction numbers to load
     */
    public static TransactionLogRange toUpdateLocalStateAt(long startExclusive) {
        return new TransactionLogRange(startExclusive + 1, -1);
    }

    /**
     * Returns the range of transactions needed to update the local state to apply a specific transaction. This is used
     * when we already have a transaction in memory that we want to apply, so we only need to load the transactions in
     * between the local state and the target transaction.
     *
     * @param  startExclusive the last transaction number that was applied to the local state
     * @param  endExclusive   the transaction number that is already held, to be applied to the local state
     * @return                range of transaction numbers to load
     */
    public static TransactionLogRange toUpdateLocalStateToApply(long startExclusive, long endExclusive) {
        return new TransactionLogRange(startExclusive + 1, endExclusive);
    }

    /**
     * Returns the range of transactions from the given number to the latest transaction.
     *
     * @param  startInclusive the first transaction number
     * @return                the range
     */
    public static TransactionLogRange fromMinimum(long startInclusive) {
        return new TransactionLogRange(startInclusive, -1);
    }

    public boolean isMaxTransactionBounded() {
        return endExclusive >= 0;
    }
}
