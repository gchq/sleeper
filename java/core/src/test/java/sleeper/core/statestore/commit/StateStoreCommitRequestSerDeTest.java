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
package sleeper.core.statestore.commit;

import org.approvaltests.Approvals;
import org.approvaltests.core.Options;
import org.junit.jupiter.api.Test;

import sleeper.core.partition.PartitionsBuilder;
import sleeper.core.properties.instance.InstanceProperties;
import sleeper.core.properties.table.TableProperties;
import sleeper.core.properties.testutils.FixedTablePropertiesProvider;
import sleeper.core.schema.Schema;
import sleeper.core.schema.type.LongType;
import sleeper.core.statestore.transactionlog.InMemoryTransactionBodyStore;
import sleeper.core.statestore.transactionlog.TransactionBodyStore;
import sleeper.core.statestore.transactionlog.transaction.PartitionTransaction;
import sleeper.core.statestore.transactionlog.transaction.TransactionType;
import sleeper.core.statestore.transactionlog.transaction.impl.ClearFilesTransaction;
import sleeper.core.statestore.transactionlog.transaction.impl.InitialisePartitionsTransaction;

import java.time.Instant;

import static org.assertj.core.api.Assertions.assertThat;
import static sleeper.core.properties.table.TableProperty.TABLE_ID;
import static sleeper.core.properties.table.TableProperty.TABLE_NAME;
import static sleeper.core.properties.testutils.InstancePropertiesTestHelper.createTestInstanceProperties;
import static sleeper.core.properties.testutils.TablePropertiesTestHelper.createTestTableProperties;
import static sleeper.core.schema.SchemaTestHelper.schemaWithKey;

public class StateStoreCommitRequestSerDeTest {

    Schema schema = schemaWithKey("key", new LongType());
    TableProperties tableProperties = createTableProperties(schema);
    StateStoreCommitRequestSerDe serDe = new StateStoreCommitRequestSerDe(new FixedTablePropertiesProvider(tableProperties));
    TransactionBodyStore bodyStore = new InMemoryTransactionBodyStore();

    @Test
    void shouldSerialiseCompactionCommitInS3() {
        // Given
        String key = TransactionBodyStore.createObjectKey("test-table", Instant.parse("2025-01-14T15:30:00Z"), "test-transaction");
        StateStoreCommitRequest commitRequest = StateStoreCommitRequest.create(
                "test-table", key, TransactionType.REPLACE_FILE_REFERENCES);

        // When
        String json = serDe.toJsonPrettyPrint(commitRequest);

        // Then
        assertThat(serDe.fromJson(json)).isEqualTo(commitRequest);
        Approvals.verify(json, new Options().forFile().withExtension(".json"));
    }

    @Test
    void shouldSerialiseInitialisePartitionsInS3() {
        // Given
        String key = TransactionBodyStore.createObjectKey("test-table", Instant.parse("2025-01-14T15:30:00Z"), "test-transaction");
        StateStoreCommitRequest commitRequest = StateStoreCommitRequest.create(
                "test-table", key, TransactionType.INITIALISE_PARTITIONS);

        // When
        String json = serDe.toJsonPrettyPrint(commitRequest);

        // Then
        assertThat(serDe.fromJson(json)).isEqualTo(commitRequest);
        Approvals.verify(json, new Options().forFile().withExtension(".json"));
    }

    @Test
    void shouldSerialiseInitialisePartitionsDirectly() {
        // Given
        PartitionTransaction transaction = new InitialisePartitionsTransaction(new PartitionsBuilder(schema)
                .rootFirst("root")
                .splitToNewChildren("root", "L", "R", 50L)
                .buildList());
        StateStoreCommitRequest commitRequest = StateStoreCommitRequest.create("test-table", transaction);

        // When
        String json = serDe.toJsonPrettyPrint(commitRequest);

        // Then
        assertThat(serDe.fromJson(json)).isEqualTo(commitRequest);
        Approvals.verify(json, new Options().forFile().withExtension(".json"));
    }

    @Test
    void shouldSeraliseFileTransactionWithoutTableProperties() {
        // Given
        StateStoreCommitRequestSerDe serDe = StateStoreCommitRequestSerDe.forFileTransactions();
        StateStoreCommitRequest commitRequest = StateStoreCommitRequest.create("test-table", new ClearFilesTransaction());

        // When
        String json = serDe.toJson(commitRequest);

        // Then
        assertThat(serDe.fromJson(json)).isEqualTo(commitRequest);
    }

    private TableProperties createTableProperties(Schema schema) {
        InstanceProperties instanceProperties = createTestInstanceProperties();
        TableProperties tableProperties = createTestTableProperties(instanceProperties, schema);
        tableProperties.set(TABLE_ID, "test-table");
        tableProperties.set(TABLE_NAME, "test.table");
        return tableProperties;
    }

}
