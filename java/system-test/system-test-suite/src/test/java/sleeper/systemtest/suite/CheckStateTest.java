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
package sleeper.systemtest.suite;

import org.junit.jupiter.api.BeforeEach;
import org.junit.jupiter.api.Test;

import sleeper.core.properties.instance.InstanceProperties;
import sleeper.core.properties.table.TableProperties;
import sleeper.core.schema.Schema;
import sleeper.core.statestore.FileReference;
import sleeper.core.statestore.FileReferenceFactory;
import sleeper.core.statestore.StateStore;
import sleeper.core.statestore.testutils.InMemoryTransactionLogStateStore;
import sleeper.core.statestore.testutils.InMemoryTransactionLogs;

import java.time.Instant;

import static org.assertj.core.api.Assertions.assertThat;
import static sleeper.core.properties.testutils.InstancePropertiesTestHelper.createTestInstanceProperties;
import static sleeper.core.properties.testutils.TablePropertiesTestHelper.createTestTableProperties;
import static sleeper.core.schema.SchemaTestHelper.schemaWithKey;
import static sleeper.core.statestore.testutils.StateStoreUpdatesWrapper.update;

public class CheckStateTest {

    static final Instant UPDATE_TIME = Instant.parse("2025-02-27T12:43:00Z");
    InstanceProperties instanceProperties = createTestInstanceProperties();
    Schema schema = schemaWithKey("key");
    TableProperties tableProperties = createTestTableProperties(instanceProperties, schemaWithKey("key"));
    InMemoryTransactionLogs transactionLogs = new InMemoryTransactionLogs();
    StateStore stateStore = InMemoryTransactionLogStateStore.createAndInitialise(tableProperties, transactionLogs);

    @BeforeEach
    void setUp() {
        stateStore.fixFileUpdateTime(UPDATE_TIME);
    }

    @Test
    void shouldLoadFilesStateWithTransactionBody() {
        // Given
        FileReference file1 = fileFactory().rootFile("test1.parquet", 100);
        FileReference file2 = fileFactory().rootFile("test2.parquet", 100);
        transactionLogs.getTransactionBodyStore().setStoreTransactions(true);
        update(stateStore).addFile(file1);
        update(stateStore).addFile(file2);

        // When
        CheckState check = checkState();

        // Then
        assertThat(check.totalRecordsAtTransaction(2)).isEqualTo(200);
    }

    private FileReferenceFactory fileFactory() {
        return FileReferenceFactory.fromUpdatedAt(stateStore, UPDATE_TIME);
    }

    private CheckState checkState() {
        return CheckState.load(tableProperties, transactionLogs.getFilesLogStore(), transactionLogs.getTransactionBodyStore());
    }

}
