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
package sleeper.core.statestore.testutils;

import org.junit.jupiter.api.Test;

import sleeper.core.statestore.transactionlog.log.StoreTransactionBodyResult;
import sleeper.core.statestore.transactionlog.transaction.StateStoreTransaction;
import sleeper.core.statestore.transactionlog.transaction.TransactionType;
import sleeper.core.statestore.transactionlog.transaction.impl.ClearFilesTransaction;

import java.util.List;

import static org.assertj.core.api.Assertions.assertThat;

public class InMemoryTransactionBodyStoreTest {

    private final InMemoryTransactionBodyStore store = new InMemoryTransactionBodyStore();

    @Test
    void shouldStoreTransactionWhenSet() {
        // Given
        store.setStoreTransactions(true);
        ClearFilesTransaction transaction = new ClearFilesTransaction();

        // When
        StoreTransactionBodyResult found = store.storeIfTooBig("test-table", transaction);

        // Then
        StateStoreTransaction<?> foundTransaction = store.getBody(found.getBodyKey().orElseThrow(), "test-table", TransactionType.CLEAR_FILES);
        assertThat(foundTransaction).isEqualTo(transaction);
    }

    @Test
    void shouldNotStoreTransactionByDefault() {
        // Given
        ClearFilesTransaction transaction = new ClearFilesTransaction();

        // When
        StoreTransactionBodyResult found = store.storeIfTooBig("test-table", transaction);

        // Then
        assertThat(found).isEqualTo(StoreTransactionBodyResult.notStored());
    }

    @Test
    void shouldFixObjectKeys() {
        store.setStoreTransactionsWithObjectKeys(List.of("key-1", "key-2"));
        ClearFilesTransaction transaction = new ClearFilesTransaction();

        // When
        StoreTransactionBodyResult found1 = store.storeIfTooBig("test-table", transaction);
        StoreTransactionBodyResult found2 = store.storeIfTooBig("test-table", transaction);

        // Then
        assertThat(List.of(found1, found2)).containsExactly(
                StoreTransactionBodyResult.stored("key-1"),
                StoreTransactionBodyResult.stored("key-2"));
    }

    @Test
    void shouldTurnOffStoringTransactions() {
        // Given
        ClearFilesTransaction transaction = new ClearFilesTransaction();
        store.setStoreTransactionsWithObjectKeys(List.of("some-key"));
        StoreTransactionBodyResult found1 = store.storeIfTooBig("test-table", transaction);

        // When
        store.setStoreTransactions(false);
        StoreTransactionBodyResult found2 = store.storeIfTooBig("test-table", transaction);

        // Then
        assertThat(found1).isEqualTo(StoreTransactionBodyResult.stored("some-key"));
        assertThat(found2).isEqualTo(StoreTransactionBodyResult.notStored());
    }

    @Test
    void shouldIncludeSerialisedTransactionInResponseWhenNotStoringTransaction() {
        // Given
        ClearFilesTransaction transaction = new ClearFilesTransaction();
        store.setReturnSerialisedTransactions(List.of("serialised-transaction"));
        store.setStoreTransactions(false);

        // When
        StoreTransactionBodyResult found = store.storeIfTooBig("test-table", transaction);

        // Then
        assertThat(found).isEqualTo(StoreTransactionBodyResult.notStored("serialised-transaction"));
    }

    @Test
    void shouldNotIncludeSerialisedTransactionInResponseWhenStoringTransaction() {
        // Given
        ClearFilesTransaction transaction1 = new ClearFilesTransaction();
        ClearFilesTransaction transaction2 = new ClearFilesTransaction();
        store.setReturnSerialisedTransactions(List.of("transaction-1", "transaction-2"));

        // When
        store.setStoreTransactions(true);
        StoreTransactionBodyResult found1 = store.storeIfTooBig("test-table", transaction1);
        store.setStoreTransactions(false);
        StoreTransactionBodyResult found2 = store.storeIfTooBig("test-table", transaction2);

        // Then
        assertThat(found1.getSerialisedTransaction()).isEmpty();
        assertThat(found2.getSerialisedTransaction()).containsSame("transaction-2");
    }
}
