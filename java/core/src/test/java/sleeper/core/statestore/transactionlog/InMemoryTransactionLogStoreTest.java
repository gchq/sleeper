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

import sleeper.core.statestore.transactionlog.transaction.StateStoreTransaction;
import sleeper.core.statestore.transactionlog.transaction.impl.ClearFilesTransaction;
import sleeper.core.statestore.transactionlog.transaction.impl.DeleteFilesTransaction;

import java.time.Instant;
import java.util.ArrayList;
import java.util.List;
import java.util.concurrent.atomic.AtomicInteger;

import static org.assertj.core.api.Assertions.assertThat;
import static org.assertj.core.api.Assertions.assertThatThrownBy;

public class InMemoryTransactionLogStoreTest {

    private static final Instant DEFAULT_UPDATE_TIME = Instant.parse("2024-04-09T14:01:01Z");
    private final InMemoryTransactionLogStore store = new InMemoryTransactionLogStore();

    private TransactionLogEntry logEntry(long number, StateStoreTransaction<?> transaction) {
        return new TransactionLogEntry(number, DEFAULT_UPDATE_TIME, transaction);
    }

    @Test
    void shouldAddFirstTransaction() throws Exception {
        // Given
        TransactionLogEntry entry = logEntry(1, new ClearFilesTransaction());

        // When
        store.addTransaction(entry);

        // Then
        assertThat(store.readTransactionsAfter(0))
                .containsExactly(entry);
    }

    @Test
    void shouldFailToAddFirstTransactionWithTooHighNumber() throws Exception {
        // Given
        TransactionLogEntry entry = logEntry(2, new ClearFilesTransaction());

        // When / Then
        assertThatThrownBy(() -> store.addTransaction(entry))
                .isInstanceOf(IllegalStateException.class)
                .hasMessage("Attempted to add transaction 2 when we only have 0");
        assertThat(store.readTransactionsAfter(0)).isEmpty();
    }

    @Test
    void shouldFailToAddTransactionWithTooLowNumber() throws Exception {
        // Given
        TransactionLogEntry entry1 = logEntry(1,
                new DeleteFilesTransaction(List.of("file1.parquet")));
        TransactionLogEntry entry2 = logEntry(1,
                new DeleteFilesTransaction(List.of("file2.parquet")));
        store.addTransaction(entry1);

        // When / Then
        assertThatThrownBy(() -> store.addTransaction(entry2))
                .isInstanceOf(DuplicateTransactionNumberException.class)
                .hasMessage("Unread transaction found. Adding transaction number 1, but it already exists.");
        assertThat(store.readTransactionsAfter(0))
                .containsExactly(entry1);
    }

    @Test
    void shouldFailToAddTransactionWhenAnotherWasAddedAtSameTime() throws Exception {
        // Given
        TransactionLogEntry entry1 = logEntry(1,
                new DeleteFilesTransaction(List.of("file1.parquet")));
        TransactionLogEntry entry2 = logEntry(1,
                new DeleteFilesTransaction(List.of("file2.parquet")));
        store.atStartOfNextAddTransaction(
                () -> store.addTransaction(entry1));

        // When / Then
        assertThatThrownBy(() -> store.addTransaction(entry2))
                .isInstanceOf(DuplicateTransactionNumberException.class)
                .hasMessage("Unread transaction found. Adding transaction number 1, but it already exists.");
        assertThat(store.readTransactionsAfter(0))
                .containsExactly(entry1);
    }

    @Test
    void shouldFailToAddTransactionWhenExceptionWasThrownExplicitly() throws Exception {
        // Given
        RuntimeException failure = new RuntimeException("Fail test");
        store.atStartOfNextAddTransaction(() -> {
            throw failure;
        });

        // When / Then
        assertThatThrownBy(() -> store.addTransaction(logEntry(1, new ClearFilesTransaction())))
                .isSameAs(failure);
        assertThat(store.readTransactionsAfter(0))
                .isEmpty();
    }

    @Test
    void shouldFailToReadTransactionsWhenExceptionWasThrownExplicitly() throws Exception {
        // Given
        RuntimeException failure = new RuntimeException("Fail test");
        store.atStartOfNextReadTransactions(() -> {
            throw failure;
        });

        // When / Then
        assertThatThrownBy(() -> store.readTransactionsAfter(0))
                .isSameAs(failure);
    }

    @Test
    void shouldReportOneTransactionWasRead() throws Exception {
        // Given
        TransactionLogEntry entry = logEntry(1, new ClearFilesTransaction());
        store.addTransaction(entry);
        List<TransactionLogEntry> readEntries = new ArrayList<>();
        store.onReadTransactionLogEntry(readEntries::add);

        // When
        store.readTransactionsAfter(0).toList();

        // Then
        assertThat(readEntries).containsExactly(entry);
    }

    @Test
    void shouldReportNoTransactionsWereReadWhenRequestedTransactionsAfterLatest() throws Exception {
        // Given
        TransactionLogEntry entry = logEntry(1, new ClearFilesTransaction());
        store.addTransaction(entry);
        List<TransactionLogEntry> readEntries = new ArrayList<>();
        store.onReadTransactionLogEntry(readEntries::add);

        // When
        store.readTransactionsAfter(1).toList();

        // Then
        assertThat(readEntries).isEmpty();
    }

    @Test
    void shouldReturnTransactionsInBetweenTwoEntries() throws Exception {
        // Given
        TransactionLogEntry entry1 = logEntry(1, new ClearFilesTransaction());
        TransactionLogEntry entry2 = logEntry(2, new ClearFilesTransaction());
        TransactionLogEntry entry3 = logEntry(3, new ClearFilesTransaction());
        store.addTransaction(entry1);
        store.addTransaction(entry2);
        store.addTransaction(entry3);
        AtomicInteger readRequests = new AtomicInteger(0);
        store.atStartOfReadTransactions(readRequests::incrementAndGet);
        List<TransactionLogEntry> readEntries = new ArrayList<>();
        store.onReadTransactionLogEntry(readEntries::add);

        // When / Then
        assertThat(store.readTransactionsBetween(1, 3))
                .containsExactly(entry2);
        assertThat(readEntries).containsExactly(entry2);
        assertThat(readRequests.get()).isEqualTo(1);
    }

    @Test
    void shouldThrowExceptionReadingTransactionsBetweenWhenAlreadyUpToDate() throws Exception {
        // Given
        TransactionLogEntry entry1 = logEntry(1, new ClearFilesTransaction());
        TransactionLogEntry entry2 = logEntry(2, new ClearFilesTransaction());
        TransactionLogEntry entry3 = logEntry(3, new ClearFilesTransaction());
        store.addTransaction(entry1);
        store.addTransaction(entry2);
        store.addTransaction(entry3);
        List<TransactionLogEntry> readEntries = new ArrayList<>();
        store.onReadTransactionLogEntry(readEntries::add);

        // When / Then
        assertThatThrownBy(() -> store.readTransactionsBetween(2, 2))
                .isInstanceOf(RuntimeException.class);
        assertThat(readEntries).isEmpty();
    }

    @Test
    void shouldThrowExceptionReadingTransactionsBetweenWhenTargetTransactionIsBeforeCurrent() throws Exception {
        // Given
        TransactionLogEntry entry1 = logEntry(1, new ClearFilesTransaction());
        TransactionLogEntry entry2 = logEntry(2, new ClearFilesTransaction());
        TransactionLogEntry entry3 = logEntry(3, new ClearFilesTransaction());
        store.addTransaction(entry1);
        store.addTransaction(entry2);
        store.addTransaction(entry3);
        List<TransactionLogEntry> readEntries = new ArrayList<>();
        store.onReadTransactionLogEntry(readEntries::add);

        // When / Then
        assertThatThrownBy(() -> store.readTransactionsBetween(3, 2))
                .isInstanceOf(RuntimeException.class);
        assertThat(readEntries).isEmpty();
    }
}
