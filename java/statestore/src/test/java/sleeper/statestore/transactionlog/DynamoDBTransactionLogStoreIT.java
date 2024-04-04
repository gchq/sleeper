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
package sleeper.statestore.transactionlog;

import org.junit.jupiter.api.Test;

import sleeper.configuration.properties.table.TableProperties;
import sleeper.core.statestore.transactionlog.TransactionLogStore;
import sleeper.core.statestore.transactionlog.UnreadTransactionException;
import sleeper.core.statestore.transactionlog.transactions.ClearFilesTransaction;
import sleeper.core.statestore.transactionlog.transactions.DeleteFilesTransaction;

import java.util.List;

import static org.assertj.core.api.Assertions.assertThat;
import static org.assertj.core.api.Assertions.assertThatThrownBy;
import static sleeper.configuration.properties.instance.CdkDefinedInstanceProperty.FILE_TRANSACTION_LOG_TABLENAME;
import static sleeper.configuration.properties.table.TablePropertiesTestHelper.createTestTableProperties;
import static sleeper.core.schema.SchemaTestHelper.schemaWithKey;

public class DynamoDBTransactionLogStoreIT extends TransactionLogStateStoreTestBase {

    protected final TableProperties tableProperties = createTestTableProperties(instanceProperties, schemaWithKey("key"));
    private final TransactionLogStore store = new DynamoDBTransactionLogStore(
            instanceProperties.get(FILE_TRANSACTION_LOG_TABLENAME),
            tableProperties, dynamoDBClient);

    @Test
    void shouldAddFirstTransaction() throws Exception {
        // When
        store.addTransaction(new ClearFilesTransaction(), 1);

        // Then
        assertThat(store.readTransactionsAfter(0))
                .containsExactly(new ClearFilesTransaction());
    }

    @Test
    void shouldFailToAddTransactionWhenOneAlreadyExistsWithSameNumber() throws Exception {
        // Given
        store.addTransaction(new DeleteFilesTransaction(List.of("file1.parquet")), 1);

        // When / Then
        assertThatThrownBy(() -> store.addTransaction(new DeleteFilesTransaction(List.of("file2.parquet")), 1))
                .isInstanceOf(UnreadTransactionException.class)
                .hasMessage("Unread transaction found. Adding transaction number 1, but it already exists.");
        assertThat(store.readTransactionsAfter(0))
                .containsExactly(new DeleteFilesTransaction(List.of("file1.parquet")));
    }

}
