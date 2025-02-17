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

package sleeper.core.metrics;

import org.junit.jupiter.api.DisplayName;
import org.junit.jupiter.api.Nested;
import org.junit.jupiter.api.Test;

import sleeper.core.partition.Partition;
import sleeper.core.partition.PartitionsBuilder;
import sleeper.core.properties.instance.InstanceProperties;
import sleeper.core.properties.table.TableProperties;
import sleeper.core.schema.Schema;
import sleeper.core.schema.type.LongType;
import sleeper.core.statestore.StateStore;
import sleeper.core.statestore.testutils.StateStoreTestBuilder;

import java.util.List;

import static org.assertj.core.api.Assertions.assertThat;
import static sleeper.core.properties.instance.CommonProperty.ID;
import static sleeper.core.properties.table.TableProperty.TABLE_NAME;
import static sleeper.core.properties.testutils.InstancePropertiesTestHelper.createTestInstanceProperties;
import static sleeper.core.properties.testutils.TablePropertiesTestHelper.createTestTableProperties;
import static sleeper.core.schema.SchemaTestHelper.schemaWithKey;
import static sleeper.core.statestore.testutils.StateStoreTestHelper.inMemoryStateStoreWithFixedPartitions;
import static sleeper.core.statestore.testutils.StateStoreTestHelper.inMemoryStateStoreWithFixedSinglePartition;

public class TableMetricsTest {
    private final Schema schema = schemaWithKey("key", new LongType());
    private StateStore stateStore;
    private final InstanceProperties instanceProperties = createTestInstanceProperties();
    private final TableProperties tableProperties = createTestTableProperties(instanceProperties, schema);

    @Nested
    @DisplayName("One partition")
    class OnePartition {
        @Test
        void shouldReportMetricsWithEmptyTable() {
            // Given
            createInstance("test-instance");
            createTable("test-table", inMemoryStateStoreWithFixedSinglePartition(schema));

            // When
            TableMetrics metrics = tableMetrics();

            // Then
            assertThat(metrics).isEqualTo(TableMetrics.builder()
                    .instanceId("test-instance")
                    .tableName("test-table")
                    .fileCount(0).recordCount(0)
                    .partitionCount(1).leafPartitionCount(1)
                    .averageFileReferencesPerPartition(0)
                    .build());
        }

        @Test
        void shouldReportMetricsWithOneFileInOnePartition() {
            // Given
            createInstance("test-instance");
            createTable("test-table", StateStoreTestBuilder.withSinglePartition(schema)
                    .singleFileInEachLeafPartitionWithRecords(100L)
                    .buildStateStore());

            // When
            TableMetrics metrics = tableMetrics();

            // Then
            assertThat(metrics).isEqualTo(TableMetrics.builder()
                    .instanceId("test-instance")
                    .tableName("test-table")
                    .fileCount(1).recordCount(100)
                    .partitionCount(1).leafPartitionCount(1)
                    .averageFileReferencesPerPartition(1)
                    .build());
        }

        @Test
        void shouldReportMetricsForMultipleFilesWithDifferentRecordCounts() {
            // Given
            createInstance("test-instance");
            PartitionsBuilder partitionsBuilder = new PartitionsBuilder(schema)
                    .singlePartition("root");
            createTable("test-table", StateStoreTestBuilder.from(partitionsBuilder)
                    .partitionFileWithRecords("root", "file1.parquet", 100L)
                    .partitionFileWithRecords("root", "file2.parquet", 200L)
                    .buildStateStore());

            // When
            TableMetrics metrics = tableMetrics();

            // Then
            assertThat(metrics).isEqualTo(TableMetrics.builder()
                    .instanceId("test-instance")
                    .tableName("test-table")
                    .fileCount(2).recordCount(300)
                    .partitionCount(1).leafPartitionCount(1)
                    .averageFileReferencesPerPartition(2)
                    .build());
        }
    }

    @Nested
    @DisplayName("Multiple partitions")
    class MultiplePartitions {

        @Test
        void shouldReportMetricsWithMultiplePartitions() {
            // Given
            createInstance("test-instance");
            List<Partition> partitions = new PartitionsBuilder(schema)
                    .rootFirst("root")
                    .splitToNewChildren("root", "left", "right", 10L)
                    .buildList();
            createTable("test-table", inMemoryStateStoreWithFixedPartitions(partitions));

            // When
            TableMetrics metrics = tableMetrics();

            // Then
            assertThat(metrics).isEqualTo(TableMetrics.builder()
                    .instanceId("test-instance")
                    .tableName("test-table")
                    .fileCount(0).recordCount(0)
                    .partitionCount(3).leafPartitionCount(2)
                    .averageFileReferencesPerPartition(0)
                    .build());
        }

        @Test
        void shouldReportMetricsWithTwoFilesInOnePartitionAndOneFileInOther() {
            // Given
            createInstance("test-instance");
            PartitionsBuilder partitionsBuilder = new PartitionsBuilder(schema)
                    .rootFirst("root")
                    .splitToNewChildren("root", "L", "R", 100L);
            createTable("test-table", StateStoreTestBuilder.from(partitionsBuilder)
                    .partitionFileWithRecords("L", "left.parquet", 50L)
                    .partitionFileWithRecords("R", "right1.parquet", 50L)
                    .partitionFileWithRecords("R", "right2.parquet", 23L)
                    .buildStateStore());

            // When
            TableMetrics metrics = tableMetrics();

            // Then
            assertThat(metrics).isEqualTo(TableMetrics.builder()
                    .instanceId("test-instance")
                    .tableName("test-table")
                    .fileCount(3).recordCount(123)
                    .partitionCount(3).leafPartitionCount(2)
                    .averageFileReferencesPerPartition(1.5)
                    .build());
        }

        @Test
        void shouldReportMetricsForMultiplePartitionsWithDifferentFileCounts() {
            // Given
            createInstance("test-instance");
            PartitionsBuilder partitionsBuilder = new PartitionsBuilder(schema)
                    .rootFirst("root")
                    .splitToNewChildren("root", "left", "right", 10L);
            createTable("test-table", StateStoreTestBuilder.from(partitionsBuilder)
                    .partitionFileWithRecords("left", "file1.parquet", 10L)
                    .partitionFileWithRecords("left", "file2.parquet", 10L)
                    .partitionFileWithRecords("right", "file3.parquet", 10L)
                    .buildStateStore());

            // When
            TableMetrics metrics = tableMetrics();

            // Then
            assertThat(metrics).isEqualTo(TableMetrics.builder()
                    .instanceId("test-instance")
                    .tableName("test-table")
                    .fileCount(3).recordCount(30)
                    .partitionCount(3).leafPartitionCount(2)
                    .averageFileReferencesPerPartition(1.5)
                    .build());
        }

        @Test
        void shouldReportMetricsForMultiplePartitionsWhenOneLeafPartitionHasNoFiles() {
            // Given
            createInstance("test-instance");
            PartitionsBuilder partitionsBuilder = new PartitionsBuilder(schema)
                    .rootFirst("root")
                    .splitToNewChildren("root", "left", "right", 10L);
            createTable("test-table", StateStoreTestBuilder.from(partitionsBuilder)
                    .partitionFileWithRecords("left", "file1.parquet", 10L)
                    .buildStateStore());

            // When
            TableMetrics metrics = tableMetrics();

            // Then
            assertThat(metrics).isEqualTo(TableMetrics.builder()
                    .instanceId("test-instance")
                    .tableName("test-table")
                    .fileCount(1).recordCount(10)
                    .partitionCount(3).leafPartitionCount(2)
                    .averageFileReferencesPerPartition(1)
                    .build());
        }
    }

    @Nested
    @DisplayName("Files split into multiple references")
    class SplitFileReferences {

        @Test
        void shouldReportMetricsWithOneFileInMultiplePartitions() {
            // Given
            createInstance("test-instance");
            PartitionsBuilder partitionsBuilder = new PartitionsBuilder(schema)
                    .rootFirst("root")
                    .splitToNewChildren("root", "L", "R", 100L);
            createTable("test-table", StateStoreTestBuilder.from(partitionsBuilder)
                    .partitionFileWithRecords("root", "test.parquet", 100L)
                    .splitFileToPartitions("test.parquet", "L", "R")
                    .buildStateStore());

            // When
            TableMetrics metrics = tableMetrics();

            // Then
            assertThat(metrics).isEqualTo(TableMetrics.builder()
                    .instanceId("test-instance")
                    .tableName("test-table")
                    .fileCount(1).recordCount(100)
                    .partitionCount(3).leafPartitionCount(2)
                    .averageFileReferencesPerPartition(1)
                    .build());
        }

        @Test
        void shouldReportMetricsWithOneFileInMultiplePartitionsAndOneFileInOnePartition() {
            // Given
            createInstance("test-instance");
            PartitionsBuilder partitionsBuilder = new PartitionsBuilder(schema)
                    .rootFirst("root")
                    .splitToNewChildren("root", "L", "R", 100L);
            createTable("test-table", StateStoreTestBuilder.from(partitionsBuilder)
                    .partitionFileWithRecords("root", "test.parquet", 100L)
                    .splitFileToPartitions("test.parquet", "L", "R")
                    .partitionFileWithRecords("L", "left.parquet", 23L)
                    .buildStateStore());

            // When
            TableMetrics metrics = tableMetrics();

            // Then
            assertThat(metrics).isEqualTo(TableMetrics.builder()
                    .instanceId("test-instance")
                    .tableName("test-table")
                    .fileCount(2).recordCount(123)
                    .partitionCount(3).leafPartitionCount(2)
                    .averageFileReferencesPerPartition(1.5)
                    .build());
        }
    }

    private void createInstance(String instanceId) {
        instanceProperties.set(ID, instanceId);
    }

    private void createTable(String tableName, StateStore stateStore) {
        tableProperties.set(TABLE_NAME, tableName);
        this.stateStore = stateStore;
    }

    private TableMetrics tableMetrics() {
        return TableMetrics.from(instanceProperties.get(ID), tableProperties.getStatus(), stateStore);
    }
}
