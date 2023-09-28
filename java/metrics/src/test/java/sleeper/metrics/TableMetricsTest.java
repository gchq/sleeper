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

package sleeper.metrics;

import org.junit.jupiter.api.Test;

import sleeper.configuration.properties.instance.InstanceProperties;
import sleeper.configuration.properties.table.TableProperties;
import sleeper.core.schema.Schema;
import sleeper.core.statestore.StateStore;
import sleeper.core.statestore.StateStoreException;

import static org.assertj.core.api.Assertions.assertThat;
import static sleeper.configuration.properties.InstancePropertiesTestHelper.createTestInstanceProperties;
import static sleeper.configuration.properties.instance.CommonProperty.ID;
import static sleeper.configuration.properties.table.TablePropertiesTestHelper.createTestTableProperties;
import static sleeper.configuration.properties.table.TableProperty.TABLE_NAME;
import static sleeper.core.schema.SchemaTestHelper.schemaWithKey;
import static sleeper.core.statestore.inmemory.StateStoreTestHelper.inMemoryStateStoreWithFixedSinglePartition;

public class TableMetricsTest {
    private final InstanceProperties instanceProperties = createTestInstanceProperties();

    @Test
    void shouldReportMetricsWithEmptyTable() {
        // Given
        instanceProperties.set(ID, "test-instance");
        Schema schema = schemaWithKey("key");
        TableProperties tableProperties = namedTable("test-table", schema);

        // When
        TableMetrics metrics = tableMetrics(tableProperties, inMemoryStateStoreWithFixedSinglePartition(schema));

        // Then
        assertThat(metrics).isEqualTo(TableMetrics.builder()
                .instanceId("test-instance")
                .tableName("test-table")
                .fileCount(0).recordCount(0)
                .partitionCount(1).leafPartitionCount(1)
                .averageActiveFilesPerPartition(0)
                .build());
    }

    private TableProperties namedTable(String tableName, Schema schema) {
        TableProperties tableProperties = createTestTableProperties(instanceProperties, schema);
        tableProperties.set(TABLE_NAME, tableName);
        return tableProperties;
    }

    private TableMetrics tableMetrics(TableProperties table, StateStore stateStore) {
        try {
            return TableMetrics.from(instanceProperties, table, stateStore);
        } catch (StateStoreException e) {
            throw new RuntimeException(e);
        }
    }
}
