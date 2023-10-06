/*
 * Copyright 2022-2023 Crown Copyright
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

package sleeper.compaction.job.batcher;

import org.junit.jupiter.api.Test;

import sleeper.configuration.properties.instance.InstanceProperties;
import sleeper.configuration.properties.table.TableProperties;
import sleeper.configuration.properties.table.TableProperty;

import java.util.List;
import java.util.Map;

import static org.assertj.core.api.Assertions.assertThat;
import static sleeper.compaction.job.batcher.TableBatch.batchWithTables;
import static sleeper.configuration.properties.instance.CompactionProperty.TABLE_BATCHER_BATCH_SIZE;
import static sleeper.configuration.properties.instance.SystemDefinedInstanceProperty.TABLE_BATCHER_QUEUE_URL;
import static sleeper.core.schema.SchemaTestHelper.schemaWithKey;

public class TableBatcherTest {
    private final InstanceProperties instanceProperties = createInstanceProperties();
    private final TableBatcherQueuesInMemory queueClient = new TableBatcherQueuesInMemory();

    @Test
    void shouldCreateOneBatchOfThreeTables() {
        // Given
        instanceProperties.setNumber(TABLE_BATCHER_BATCH_SIZE, 3);
        TableProperties table1 = createTable("test-table-1");
        TableProperties table2 = createTable("test-table-2");
        TableProperties table3 = createTable("test-table-3");

        // When
        new TableBatcher(instanceProperties, List.of(table1, table2, table3), queueClient).batchTables();

        // Then
        assertThat(queueClient.getMessagesByQueueUrl()).isEqualTo(Map.of(
                "table-batcher-queue", List.of(
                        batchWithTables("test-table-1", "test-table-2", "test-table-3"))
        ));
    }

    @Test
    void shouldCreateTwoBatchesOfTwoTables() {
        // Given
        instanceProperties.setNumber(TABLE_BATCHER_BATCH_SIZE, 2);
        TableProperties table1 = createTable("test-table-1");
        TableProperties table2 = createTable("test-table-2");
        TableProperties table3 = createTable("test-table-3");
        TableProperties table4 = createTable("test-table-4");

        // When
        new TableBatcher(instanceProperties, List.of(table1, table2, table3, table4), queueClient).batchTables();

        // Then
        assertThat(queueClient.getMessagesByQueueUrl()).isEqualTo(Map.of(
                "table-batcher-queue", List.of(
                        batchWithTables("test-table-1", "test-table-2"),
                        batchWithTables("test-table-3", "test-table-4"))
        ));
    }

    @Test
    void shouldCreateOneBatchWhenTableCountDoesNotMeetBatchSize() {
        // Given
        instanceProperties.setNumber(TABLE_BATCHER_BATCH_SIZE, 2);
        TableProperties table1 = createTable("test-table-1");

        // When
        new TableBatcher(instanceProperties, List.of(table1), queueClient).batchTables();

        // Then
        assertThat(queueClient.getMessagesByQueueUrl()).isEqualTo(Map.of(
                "table-batcher-queue", List.of(
                        batchWithTables("test-table-1"))
        ));
    }

    private static InstanceProperties createInstanceProperties() {
        InstanceProperties instanceProperties = new InstanceProperties();
        instanceProperties.set(TABLE_BATCHER_QUEUE_URL, "table-batcher-queue");
        return instanceProperties;
    }

    private TableProperties createTable(String tableName) {
        TableProperties tableProperties = new TableProperties(instanceProperties);
        tableProperties.setSchema(schemaWithKey("key"));
        tableProperties.set(TableProperty.TABLE_NAME, tableName);
        return tableProperties;
    }
}
