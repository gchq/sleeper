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

import org.junit.jupiter.api.Test;

import static org.assertj.core.api.Assertions.assertThat;

public class TransactionLogDeletionTrackerTest {

    @Test
    void shouldReportNoTransactionsWereDeleted() {
        // When no deletions are reported
        TransactionLogDeletionTracker tracker = forFiles();

        // Then
        assertThat(tracker.summary()).hasToString(
                "Deleted no file transactions from DynamoDB or S3.");
    }

    @Test
    void shouldReportOneTransactionWasDeletedFromLog() {
        // Given
        TransactionLogDeletionTracker tracker = forFiles();

        // When
        tracker.deletedFromLog(1);

        // Then
        assertThat(tracker.summary()).hasToString(
                "Deleted 1 file transactions from DynamoDB, 0 from S3, numbers between 1 and 1 inclusive.");
    }

    @Test
    void shouldReportOneLargeTransactionWasDeleted() {
        // Given
        TransactionLogDeletionTracker tracker = forFiles();

        // When
        tracker.deletedLargeTransactionBody(1);
        tracker.deletedFromLog(1);

        // Then
        assertThat(tracker.summary()).hasToString(
                "Deleted 1 file transactions from DynamoDB, 1 from S3, numbers between 1 and 1 inclusive.");
    }

    @Test
    void shouldReportTwoTransactionsWereDeletedAndOneWasLarge() {
        // Given
        TransactionLogDeletionTracker tracker = forFiles();

        // When
        tracker.deletedLargeTransactionBody(1);
        tracker.deletedFromLog(1);
        tracker.deletedFromLog(2);

        // Then
        assertThat(tracker.summary()).hasToString(
                "Deleted 2 file transactions from DynamoDB, 1 from S3, numbers between 1 and 2 inclusive.");
    }

    private TransactionLogDeletionTracker forFiles() {
        return new TransactionLogDeletionTracker("file");
    }
}
