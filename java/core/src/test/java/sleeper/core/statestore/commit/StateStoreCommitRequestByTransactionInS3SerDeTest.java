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

import sleeper.core.partition.PartitionTree;
import sleeper.core.partition.PartitionsBuilder;
import sleeper.core.properties.instance.InstanceProperties;
import sleeper.core.properties.table.TableProperties;
import sleeper.core.schema.Schema;
import sleeper.core.schema.type.LongType;
import sleeper.core.statestore.FileReference;
import sleeper.core.statestore.FileReferenceFactory;
import sleeper.core.statestore.transactionlog.FileReferenceTransaction;
import sleeper.core.statestore.transactionlog.InMemoryTransactionBodyStore;
import sleeper.core.statestore.transactionlog.PartitionTransaction;
import sleeper.core.statestore.transactionlog.TransactionBodyStore;
import sleeper.core.statestore.transactionlog.transactions.InitialisePartitionsTransaction;
import sleeper.core.statestore.transactionlog.transactions.ReplaceFileReferencesTransaction;

import java.util.List;

import static org.assertj.core.api.Assertions.assertThat;
import static sleeper.core.properties.table.TableProperty.TABLE_ID;
import static sleeper.core.properties.table.TableProperty.TABLE_NAME;
import static sleeper.core.properties.testutils.InstancePropertiesTestHelper.createTestInstanceProperties;
import static sleeper.core.properties.testutils.TablePropertiesTestHelper.createTestTableProperties;
import static sleeper.core.schema.SchemaTestHelper.schemaWithKey;
import static sleeper.core.statestore.ReplaceFileReferencesRequest.replaceJobFileReferences;

public class StateStoreCommitRequestByTransactionInS3SerDeTest {

    Schema schema = schemaWithKey("key", new LongType());
    TableProperties tableProperties = createTableProperties(schema);
    StateStoreCommitRequestByTransactionInS3SerDe serDe = new StateStoreCommitRequestByTransactionInS3SerDe();
    TransactionBodyStore bodyStore = new InMemoryTransactionBodyStore();

    @Test
    void shouldSerialiseCompactionCommitInS3() {
        // Given
        PartitionTree partitions = new PartitionsBuilder(schema).singlePartition("root").buildTree();
        FileReference file = FileReferenceFactory.from(partitions).rootFile("new.parquet", 100);
        FileReferenceTransaction transaction = new ReplaceFileReferencesTransaction(List.of(
                replaceJobFileReferences("test-job", List.of("old.parquet"), file)));
        StateStoreCommitRequestByTransactionInS3 commitRequest = StateStoreCommitRequestByTransactionInS3.create(
                "test-table", "test-table/transactions/commit-compaction-transaction.json", transaction);

        // When
        String json = serDe.toJsonPrettyPrint(commitRequest);

        // Then
        assertThat(serDe.fromJson(json)).isEqualTo(commitRequest);
        Approvals.verify(json, new Options().forFile().withExtension(".json"));
    }

    @Test
    void shouldSerialiseInitialisePartitionsInS3() {
        // Given
        PartitionTransaction transaction = new InitialisePartitionsTransaction(new PartitionsBuilder(schema)
                .rootFirst("root")
                .splitToNewChildren("root", "L", "R", 50L)
                .buildList());
        StateStoreCommitRequestByTransactionInS3 commitRequest = StateStoreCommitRequestByTransactionInS3.create(
                "test-table", "test-table/transactions/initialise-transaction.json", transaction);

        // When
        String json = serDe.toJsonPrettyPrint(commitRequest);

        // Then
        assertThat(serDe.fromJson(json)).isEqualTo(commitRequest);
        Approvals.verify(json, new Options().forFile().withExtension(".json"));
    }

    private TableProperties createTableProperties(Schema schema) {
        InstanceProperties instanceProperties = createTestInstanceProperties();
        TableProperties tableProperties = createTestTableProperties(instanceProperties, schema);
        tableProperties.set(TABLE_ID, "test-table");
        tableProperties.set(TABLE_NAME, "test.table");
        return tableProperties;
    }

}
