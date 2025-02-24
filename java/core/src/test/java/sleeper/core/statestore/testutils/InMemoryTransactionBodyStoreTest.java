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

import org.junit.jupiter.api.Test;

import sleeper.core.statestore.transactionlog.AddTransactionRequest;
import sleeper.core.statestore.transactionlog.state.StateListenerBeforeApply;
import sleeper.core.statestore.transactionlog.transaction.TransactionType;
import sleeper.core.statestore.transactionlog.transaction.impl.ClearFilesTransaction;

import java.util.List;
import java.util.Optional;

import static org.assertj.core.api.Assertions.assertThat;

public class InMemoryTransactionBodyStoreTest {

    private final InMemoryTransactionBodyStore store = new InMemoryTransactionBodyStore();

    @Test
    void shouldStoreTransactionWhenSet() {
        // Given
        store.setStoreTransactions(true);
        ClearFilesTransaction transaction = new ClearFilesTransaction();
        AddTransactionRequest request = AddTransactionRequest.withTransaction(transaction).build();

        // When
        AddTransactionRequest found = store.storeIfTooBig("test-table", request);

        // Then
        assertThat(found).isNotSameAs(request);
        assertThat(found.<ClearFilesTransaction>getTransaction()).isEqualTo(transaction);
        assertThat(store.<ClearFilesTransaction>getBody(found.getBodyKey().orElseThrow(), "test-table", TransactionType.CLEAR_FILES)).isEqualTo(transaction);
    }

    @Test
    void shouldNotStoreTransactionByDefault() {
        // Given
        ClearFilesTransaction transaction = new ClearFilesTransaction();
        AddTransactionRequest request = AddTransactionRequest.withTransaction(transaction).build();

        // When
        AddTransactionRequest found = store.storeIfTooBig("test-table", request);

        // Then
        assertThat(found).isSameAs(request);
    }

    @Test
    void shouldFixObjectKeys() {
        store.setStoreTransactionsWithObjectKeys(List.of("key-1", "key-2"));
        ClearFilesTransaction transaction = new ClearFilesTransaction();
        AddTransactionRequest request = AddTransactionRequest.withTransaction(transaction).build();

        // When
        AddTransactionRequest found1 = store.storeIfTooBig("test-table", request);
        AddTransactionRequest found2 = store.storeIfTooBig("test-table", request);

        // Then
        assertThat(List.of(found1.getBodyKey(), found2.getBodyKey()))
                .extracting(Optional::orElseThrow)
                .containsExactly("key-1", "key-2");
    }

    @Test
    void shouldRetainListenerWhenTransactionIsStored() {
        // Given
        store.setStoreTransactions(true);
        ClearFilesTransaction transaction = new ClearFilesTransaction();
        StateListenerBeforeApply listener = StateListenerBeforeApply.withFilesState(state -> {
            state.references().count();
        });
        AddTransactionRequest request = AddTransactionRequest.withTransaction(transaction).beforeApplyListener(listener).build();

        // When
        AddTransactionRequest found = store.storeIfTooBig("test-table", request);

        // Then
        assertThat(found).isNotSameAs(request);
        assertThat(found.getBeforeApplyListener()).isSameAs(listener);
    }

    @Test
    void shouldTurnOffStoringTransactions() {
        // Given
        ClearFilesTransaction transaction = new ClearFilesTransaction();
        AddTransactionRequest request = AddTransactionRequest.withTransaction(transaction).build();
        store.setStoreTransactions(true);
        AddTransactionRequest found1 = store.storeIfTooBig("test-table", request);

        // When
        store.setStoreTransactions(false);
        AddTransactionRequest found2 = store.storeIfTooBig("test-table", request);

        // Then
        assertThat(found1).isNotSameAs(request);
        assertThat(found2).isSameAs(request);
    }

    @Test
    void shouldIncludeSerialisedTransactionInResponseWhenNotStoringTransaction() {
        // Given
        ClearFilesTransaction transaction = new ClearFilesTransaction();
        AddTransactionRequest request = AddTransactionRequest.withTransaction(transaction).build();
        store.setReturnSerialisedTransactions(List.of("serialised-transaction"));
        store.setStoreTransactions(false);

        // When
        AddTransactionRequest found = store.storeIfTooBig("test-table", request);

        // Then
        assertThat(found.getSerialisedTransaction()).containsSame("serialised-transaction");
    }

    @Test
    void shouldNotIncludeSerialisedTransactionInResponseWhenStoringTransaction() {
        // Given
        ClearFilesTransaction transaction1 = new ClearFilesTransaction();
        ClearFilesTransaction transaction2 = new ClearFilesTransaction();
        store.setReturnSerialisedTransactions(List.of("transaction-1", "transaction-2"));

        // When
        store.setStoreTransactions(true);
        AddTransactionRequest found1 = store.storeIfTooBig("test-table", AddTransactionRequest.withTransaction(transaction1).build());
        store.setStoreTransactions(false);
        AddTransactionRequest found2 = store.storeIfTooBig("test-table", AddTransactionRequest.withTransaction(transaction2).build());

        // Then
        assertThat(found1.getSerialisedTransaction()).isEmpty();
        assertThat(found2.getSerialisedTransaction()).containsSame("transaction-2");
    }
}
