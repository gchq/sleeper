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

import org.junit.jupiter.api.DisplayName;
import org.junit.jupiter.api.Nested;
import org.junit.jupiter.api.Test;

import sleeper.configuration.properties.instance.InstanceProperties;
import sleeper.configuration.properties.table.TableProperties;
import sleeper.core.partition.Partition;
import sleeper.core.partition.PartitionsBuilder;
import sleeper.core.schema.Schema;
import sleeper.core.schema.type.LongType;
import sleeper.core.statestore.StateStore;
import sleeper.core.statestore.StateStoreException;
import sleeper.core.statestore.inmemory.StateStoreTestBuilder;
import sleeper.statestore.FixedStateStoreProvider;

import java.util.ArrayList;
import java.util.HashMap;
import java.util.List;
import java.util.Map;

import static org.assertj.core.api.Assertions.assertThat;
import static sleeper.configuration.properties.InstancePropertiesTestHelper.createTestInstanceProperties;
import static sleeper.configuration.properties.instance.CommonProperty.ID;
import static sleeper.configuration.properties.table.TablePropertiesTestHelper.createTestTableProperties;
import static sleeper.configuration.properties.table.TableProperty.TABLE_NAME;
import static sleeper.core.schema.SchemaTestHelper.schemaWithKey;
import static sleeper.core.statestore.inmemory.StateStoreTestHelper.inMemoryStateStoreWithFixedPartitions;
import static sleeper.core.statestore.inmemory.StateStoreTestHelper.inMemoryStateStoreWithSinglePartition;

/* Tests:
x Empty Sleeper table w/1 partition
x Multiple partitions
x Single file
x Files with different record counts in one partition
x Partitions with different file counts
One partition has no files and calculate average
Multiple tables
 */
public class TableMetricsTest {
    private final InstanceProperties instanceProperties = createTestInstanceProperties();
    private final Schema schema = schemaWithKey("key", new LongType());
    private final List<TableProperties> tables = new ArrayList<>();
    private final Map<String, StateStore> stateStoreByTableName = new HashMap<>();

    @Nested
    @DisplayName("Single table")
    class SingleTable {
        @Test
        void shouldReportMetricsWithEmptyTable() {
            // Given
            instanceProperties.set(ID, "test-instance");
            createTable("test-table");

            // When
            List<TableMetrics> metrics = tableMetrics();

            // Then
            assertThat(metrics).containsExactly(TableMetrics.builder()
                    .instanceId("test-instance")
                    .tableName("test-table")
                    .fileCount(0).recordCount(0)
                    .partitionCount(1).leafPartitionCount(1)
                    .averageActiveFilesPerPartition(0)
                    .build());
        }

        @Test
        void shouldReportMetricsWithMultiplePartitions() {
            // Given
            instanceProperties.set(ID, "test-instance");
            List<Partition> partitions = new PartitionsBuilder(schema)
                    .rootFirst("root")
                    .splitToNewChildren("root", "left", "right", 10L)
                    .buildList();
            createTable("test-table", inMemoryStateStoreWithFixedPartitions(partitions));

            // When
            List<TableMetrics> metrics = tableMetrics();

            // Then
            assertThat(metrics).containsExactly(TableMetrics.builder()
                    .instanceId("test-instance")
                    .tableName("test-table")
                    .fileCount(0).recordCount(0)
                    .partitionCount(3).leafPartitionCount(2)
                    .averageActiveFilesPerPartition(0)
                    .build());
        }

        @Test
        void shouldReportMetricsWithOneFileInOnePartition() {
            // Given
            instanceProperties.set(ID, "test-instance");
            createTable("test-table", StateStoreTestBuilder.withSinglePartition(schema)
                    .singleFileInEachLeafPartitionWithRecords(100L)
                    .buildStateStore());

            // When
            List<TableMetrics> metrics = tableMetrics();

            // Then
            assertThat(metrics).containsExactly(TableMetrics.builder()
                    .instanceId("test-instance")
                    .tableName("test-table")
                    .fileCount(1).recordCount(100)
                    .partitionCount(1).leafPartitionCount(1)
                    .averageActiveFilesPerPartition(1)
                    .build());
        }

        @Test
        void shouldReportMetricsForMultipleFilesWithDifferentRecordCounts() {
            // Given
            instanceProperties.set(ID, "test-instance");
            PartitionsBuilder partitionsBuilder = new PartitionsBuilder(schema)
                    .singlePartition("root");
            createTable("test-table", StateStoreTestBuilder.from(partitionsBuilder)
                    .partitionFileWithRecords("root", "file1.parquet", 100L)
                    .partitionFileWithRecords("root", "file2.parquet", 200L)
                    .buildStateStore());

            // When
            List<TableMetrics> metrics = tableMetrics();

            // Then
            assertThat(metrics).containsExactly(TableMetrics.builder()
                    .instanceId("test-instance")
                    .tableName("test-table")
                    .fileCount(2).recordCount(300)
                    .partitionCount(1).leafPartitionCount(1)
                    .averageActiveFilesPerPartition(2)
                    .build());
        }

        @Test
        void shouldReportMetricsForMultiplePartitionsWithDifferentFileCounts() {
            // Given
            instanceProperties.set(ID, "test-instance");
            PartitionsBuilder partitionsBuilder = new PartitionsBuilder(schema)
                    .rootFirst("root")
                    .splitToNewChildren("root", "left", "right", 10L);
            createTable("test-table", StateStoreTestBuilder.from(partitionsBuilder)
                    .partitionFileWithRecords("left", "file1.parquet", 10L)
                    .partitionFileWithRecords("left", "file2.parquet", 10L)
                    .partitionFileWithRecords("right", "file3.parquet", 10L)
                    .buildStateStore());

            // When
            List<TableMetrics> metrics = tableMetrics();

            // Then
            assertThat(metrics).containsExactly(TableMetrics.builder()
                    .instanceId("test-instance")
                    .tableName("test-table")
                    .fileCount(3).recordCount(30)
                    .partitionCount(3).leafPartitionCount(2)
                    .averageActiveFilesPerPartition(1.5)
                    .build());
        }

        @Test
        void shouldReportMetricsForMultiplePartitionsWhenOneLeafPartitionHasNoFiles() {
            // Given
            instanceProperties.set(ID, "test-instance");
            PartitionsBuilder partitionsBuilder = new PartitionsBuilder(schema)
                    .rootFirst("root")
                    .splitToNewChildren("root", "left", "right", 10L);
            createTable("test-table", StateStoreTestBuilder.from(partitionsBuilder)
                    .partitionFileWithRecords("left", "file1.parquet", 10L)
                    .buildStateStore());

            // When
            List<TableMetrics> metrics = tableMetrics();

            // Then
            assertThat(metrics).containsExactly(TableMetrics.builder()
                    .instanceId("test-instance")
                    .tableName("test-table")
                    .fileCount(1).recordCount(10)
                    .partitionCount(3).leafPartitionCount(2)
                    .averageActiveFilesPerPartition(1)
                    .build());
        }
    }

    private void createTable(String tableName) {
        createTable(tableName, inMemoryStateStoreWithSinglePartition(schema));
    }

    private void createTable(String tableName, StateStore stateStore) {
        TableProperties tableProperties = createTestTableProperties(instanceProperties, schema);
        tableProperties.set(TABLE_NAME, tableName);
        tables.add(tableProperties);
        stateStoreByTableName.put(tableName, stateStore);
    }

    private List<TableMetrics> tableMetrics() {
        try {
            return TableMetrics.from(instanceProperties, tables, new FixedStateStoreProvider(stateStoreByTableName));
        } catch (StateStoreException e) {
            throw new RuntimeException(e);
        }
    }
}
