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

import org.slf4j.Logger;
import org.slf4j.LoggerFactory;

/**
 * Tracks and logs which transactions have been deleted. Used in TransactionLogStore implementations when deleting
 * transactions.
 */
public class TransactionLogDeletionTracker {
    public static final Logger LOGGER = LoggerFactory.getLogger(TransactionLogDeletionTracker.class);

    private final String transactionDescription;
    private long minTransactionNumber = Long.MAX_VALUE;
    private long maxTransactionNumber = -1;
    private int numDeletedLargeBody;
    private int numDeletedFromLog;

    public TransactionLogDeletionTracker(String transactionDescription) {
        this.transactionDescription = transactionDescription;
    }

    /**
     * Records that a large transaction's body has been deleted from the large tranactions store (e.g. S3).
     *
     * @param transactionNumber the transaction number
     */
    public void deletedLargeTransactionBody(long transactionNumber) {
        numDeletedLargeBody++;
    }

    /**
     * Records that a transaction has been deleted from the transaction log (e.g. DynamoDB).
     *
     * @param transactionNumber the transaction number
     */
    public void deletedFromLog(long transactionNumber) {
        minTransactionNumber = Math.min(minTransactionNumber, transactionNumber);
        maxTransactionNumber = Math.max(maxTransactionNumber, transactionNumber);
        numDeletedFromLog++;
        if (numDeletedFromLog % 100 == 0) {
            LOGGER.info("Deleted {} {} transactions from Dynamo, {} from S3, numbers between {} and {} inclusive",
                    numDeletedFromLog, transactionDescription, numDeletedLargeBody, minTransactionNumber, maxTransactionNumber);
        }
    }

    /**
     * Retrieves a summary of the state at the end of deletion, for inclusion in a log message.
     *
     * @return a summary object implementing toString
     */
    public Object summary() {
        return new Summary();
    }

    /**
     * Implements toString for inclusion in a log message.
     */
    private class Summary {
        @Override
        public String toString() {
            if (numDeletedFromLog == 0) {
                return "Deleted no " + transactionDescription + " transactions from DynamoDB or S3.";
            } else {
                return "Deleted " + numDeletedFromLog + " " + transactionDescription + " transactions from DynamoDB, "
                        + numDeletedLargeBody + " from S3, " +
                        "numbers between " + minTransactionNumber + " and " + maxTransactionNumber + " inclusive.";
            }
        }
    }
}
