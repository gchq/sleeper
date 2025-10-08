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
package sleeper.core.statestore.commit;

import org.approvaltests.Approvals;
import org.approvaltests.core.Options;
import org.junit.jupiter.api.BeforeEach;
import org.junit.jupiter.api.Test;

import sleeper.core.partition.PartitionTree;
import sleeper.core.partition.PartitionsBuilder;
import sleeper.core.properties.instance.InstanceProperties;
import sleeper.core.properties.table.TableProperties;
import sleeper.core.properties.testutils.FixedTablePropertiesProvider;
import sleeper.core.schema.Schema;
import sleeper.core.schema.type.LongType;
import sleeper.core.statestore.FileReferenceFactory;
import sleeper.core.statestore.transactionlog.log.TransactionBodyStore;
import sleeper.core.statestore.transactionlog.transaction.FileReferenceTransaction;
import sleeper.core.statestore.transactionlog.transaction.PartitionTransaction;
import sleeper.core.statestore.transactionlog.transaction.StateStoreTransaction;
import sleeper.core.statestore.transactionlog.transaction.TransactionType;
import sleeper.core.statestore.transactionlog.transaction.impl.AddFilesTransaction;
import sleeper.core.statestore.transactionlog.transaction.impl.ClearFilesTransaction;
import sleeper.core.statestore.transactionlog.transaction.impl.InitialisePartitionsTransaction;

import java.time.Instant;
import java.util.List;

import static org.assertj.core.api.Assertions.assertThat;
import static sleeper.core.properties.table.TableProperty.TABLE_ID;
import static sleeper.core.properties.table.TableProperty.TABLE_NAME;
import static sleeper.core.properties.testutils.InstancePropertiesTestHelper.createTestInstancePropertiesWithId;
import static sleeper.core.properties.testutils.TablePropertiesTestHelper.createTestTableProperties;
import static sleeper.core.schema.SchemaTestHelper.createSchemaWithKey;

public class StateStoreCommitRequestSerDeTest {

    Schema schema = createSchemaWithKey("key", new LongType());
    InstanceProperties instanceProperties = createTestInstancePropertiesWithId("instanceid");
    TableProperties tableProperties = createTestTableProperties(instanceProperties, schema);
    StateStoreCommitRequestSerDe serDe;

    @BeforeEach
    void setUp() {
        tableProperties.set(TABLE_ID, "933cd595");
        tableProperties.set(TABLE_NAME, "test.table");
        serDe = new StateStoreCommitRequestSerDe(new FixedTablePropertiesProvider(tableProperties));
    }

    @Test
    void shouldSerialiseCompactionCommitInS3() {
        // Given
        String key = createTransactionObjectKey(Instant.parse("2025-01-14T15:30:00Z"), "test-transaction");
        StateStoreCommitRequest commitRequest = createCommitRequestInS3(key, TransactionType.REPLACE_FILE_REFERENCES);

        // When
        String json = serDe.toJsonPrettyPrint(commitRequest);

        // Then
        assertThat(serDe.fromJson(json)).isEqualTo(commitRequest);
        Approvals.verify(json, new Options().forFile().withExtension(".json"));
    }

    @Test
    void shouldSerialiseInitialisePartitionsInS3() {
        // Given
        String key = createTransactionObjectKey(Instant.parse("2025-01-14T15:30:00Z"), "test-transaction");
        StateStoreCommitRequest commitRequest = createCommitRequestInS3(key, TransactionType.INITIALISE_PARTITIONS);

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
        StateStoreCommitRequest commitRequest = createDirectCommitRequest(transaction);

        // When
        String json = serDe.toJsonPrettyPrint(commitRequest);

        // Then
        assertThat(serDe.fromJson(json)).isEqualTo(commitRequest);
        Approvals.verify(json, new Options().forFile().withExtension(".json"));
    }

    @Test
    void shouldSerialiseAddFilesDirectly() {
        // Given
        PartitionTree partitions = new PartitionsBuilder(schema).singlePartition("root").buildTree();
        FileReferenceTransaction transaction = AddFilesTransaction.fromReferences(List.of(
                fileFactory(partitions).rootFile("file1", 100),
                fileFactory(partitions).rootFile("file2", 200)));
        StateStoreCommitRequest commitRequest = createDirectCommitRequest(transaction);

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

    private String createTransactionObjectKey(Instant now, String uuid) {
        return TransactionBodyStore.createObjectKey(tableProperties.get(TABLE_ID), now, uuid);
    }

    private StateStoreCommitRequest createCommitRequestInS3(String objectKey, TransactionType transactionType) {
        return StateStoreCommitRequest.create(tableProperties.get(TABLE_ID), objectKey, transactionType);
    }

    private StateStoreCommitRequest createDirectCommitRequest(StateStoreTransaction<?> transaction) {
        return StateStoreCommitRequest.create(tableProperties.get(TABLE_ID), transaction);
    }

    private FileReferenceFactory fileFactory(PartitionTree partitions) {
        return FileReferenceFactory.from(instanceProperties, tableProperties, partitions);
    }

}
