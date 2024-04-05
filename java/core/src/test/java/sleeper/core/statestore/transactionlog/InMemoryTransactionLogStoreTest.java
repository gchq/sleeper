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

import sleeper.core.statestore.transactionlog.transactions.ClearFilesTransaction;
import sleeper.core.statestore.transactionlog.transactions.DeleteFilesTransaction;

import java.util.List;

import static org.assertj.core.api.Assertions.assertThat;
import static org.assertj.core.api.Assertions.assertThatThrownBy;

public class InMemoryTransactionLogStoreTest {

    private final InMemoryTransactionLogStore store = new InMemoryTransactionLogStore();

    @Test
    void shouldAddFirstTransaction() throws Exception {
        // When
        store.addTransaction(new ClearFilesTransaction(), 1);

        // Then
        assertThat(store.readTransactionsAfter(0))
                .containsExactly(new ClearFilesTransaction());
    }

    @Test
    void shouldFailToAddFirstTransactionWithTooHighNumber() throws Exception {
        // When / Then
        assertThatThrownBy(() -> store.addTransaction(new ClearFilesTransaction(), 2))
                .isInstanceOf(IllegalStateException.class)
                .hasMessage("Attempted to add transaction 2 when we only have 0");
        assertThat(store.readTransactionsAfter(0)).isEmpty();
    }

    @Test
    void shouldFailToAddTransactionWithTooLowNumber() throws Exception {
        // Given
        store.addTransaction(new DeleteFilesTransaction(List.of("file1.parquet")), 1);

        // When / Then
        assertThatThrownBy(() -> store.addTransaction(new DeleteFilesTransaction(List.of("file2.parquet")), 1))
                .isInstanceOf(DuplicateTransactionNumberException.class)
                .hasMessage("Unread transaction found. Adding transaction number 1, but it already exists.");
        assertThat(store.readTransactionsAfter(0))
                .containsExactly(new DeleteFilesTransaction(List.of("file1.parquet")));
    }

    @Test
    void shouldFailToAddTransactionWhenAnotherWasAddedAtSameTime() throws Exception {
        // Given
        store.beforeNextAddTransaction(
                () -> store.addTransaction(new ClearFilesTransaction(), 1));

        // When / Then
        assertThatThrownBy(() -> store.addTransaction(new DeleteFilesTransaction(List.of("file.parquet")), 1))
                .isInstanceOf(DuplicateTransactionNumberException.class)
                .hasMessage("Unread transaction found. Adding transaction number 1, but it already exists.");
        assertThat(store.readTransactionsAfter(0))
                .containsExactly(new ClearFilesTransaction());
    }

    @Test
    void shouldFailToAddTransactionWhenExceptionWasThrownExplicitly() throws Exception {
        // Given
        RuntimeException failure = new RuntimeException("Fail test");
        store.beforeNextAddTransaction(() -> {
            throw failure;
        });

        // When / Then
        assertThatThrownBy(() -> store.addTransaction(new ClearFilesTransaction(), 1))
                .isSameAs(failure);
        assertThat(store.readTransactionsAfter(0))
                .isEmpty();
    }

    @Test
    void shouldFailToReadTransactionsWhenExceptionWasThrownExplicitly() throws Exception {
        // Given
        RuntimeException failure = new RuntimeException("Fail test");
        store.beforeNextReadTransactions(() -> {
            throw failure;
        });

        // When / Then
        assertThatThrownBy(() -> store.readTransactionsAfter(0))
                .isSameAs(failure);
    }
}
