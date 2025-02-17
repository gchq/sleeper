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

import org.junit.jupiter.api.BeforeEach;
import org.junit.jupiter.api.DisplayName;
import org.junit.jupiter.api.Nested;
import org.junit.jupiter.api.Test;

import sleeper.core.partition.PartitionsBuilder;
import sleeper.core.properties.instance.InstanceProperties;
import sleeper.core.properties.table.TableProperties;
import sleeper.core.schema.Schema;
import sleeper.core.schema.type.LongType;
import sleeper.core.statestore.FileReference;
import sleeper.core.statestore.FileReferenceFactory;
import sleeper.core.statestore.StateStore;
import sleeper.core.statestore.transactionlog.InMemoryTransactionLogStateStore;
import sleeper.core.statestore.transactionlog.InMemoryTransactionLogs;

import java.util.List;

import static org.assertj.core.api.Assertions.assertThat;
import static sleeper.core.properties.instance.CommonProperty.ID;
import static sleeper.core.properties.table.TableProperty.TABLE_NAME;
import static sleeper.core.properties.testutils.InstancePropertiesTestHelper.createTestInstanceProperties;
import static sleeper.core.properties.testutils.TablePropertiesTestHelper.createTestTableProperties;
import static sleeper.core.schema.SchemaTestHelper.schemaWithKey;
import static sleeper.core.statestore.FileReferenceTestData.splitFile;

public class TableMetricsTest {
    private final Schema schema = schemaWithKey("key", new LongType());
    private final InstanceProperties instanceProperties = createTestInstanceProperties();
    private final TableProperties tableProperties = createTestTableProperties(instanceProperties, schema);
    private StateStore stateStore = InMemoryTransactionLogStateStore.createAndInitialise(tableProperties, new InMemoryTransactionLogs());

    @BeforeEach
    void setUp() {
        instanceProperties.set(ID, "test-instance");
        tableProperties.set(TABLE_NAME, "test-table");
    }

    @Nested
    @DisplayName("One partition")
    class OnePartition {
        @Test
        void shouldReportMetricsWithEmptyTable() {
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
            stateStore.addFile(fileFactory().rootFile(100L));

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
            stateStore.addFile(fileFactory().rootFile("file1.parquet", 100L));
            stateStore.addFile(fileFactory().rootFile("file2.parquet", 200L));

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
            stateStore.initialise(new PartitionsBuilder(schema)
                    .rootFirst("root")
                    .splitToNewChildren("root", "left", "right", 10L)
                    .buildList());

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
            stateStore.initialise(new PartitionsBuilder(schema)
                    .rootFirst("root")
                    .splitToNewChildren("root", "L", "R", 100L)
                    .buildList());
            stateStore.addFile(fileFactory().partitionFile("L", "left.parquet", 50));
            stateStore.addFile(fileFactory().partitionFile("R", "right1.parquet", 50));
            stateStore.addFile(fileFactory().partitionFile("R", "right2.parquet", 23));

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
            stateStore.initialise(new PartitionsBuilder(schema)
                    .rootFirst("root")
                    .splitToNewChildren("root", "left", "right", 10L)
                    .buildList());
            stateStore.addFile(fileFactory().partitionFile("left", "file1.parquet", 10));
            stateStore.addFile(fileFactory().partitionFile("left", "file2.parquet", 10));
            stateStore.addFile(fileFactory().partitionFile("right", "file3.parquet", 10));

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
            stateStore.initialise(new PartitionsBuilder(schema)
                    .rootFirst("root")
                    .splitToNewChildren("root", "left", "right", 10L)
                    .buildList());
            stateStore.addFile(fileFactory().partitionFile("left", "file1.parquet", 10));

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
            stateStore.initialise(new PartitionsBuilder(schema)
                    .rootFirst("root")
                    .splitToNewChildren("root", "L", "R", 100L)
                    .buildList());
            FileReference splitFile = fileFactory().rootFile("test.parquet", 100);
            stateStore.addFiles(List.of(
                    splitFile(splitFile, "L"),
                    splitFile(splitFile, "R")));

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
            stateStore.initialise(new PartitionsBuilder(schema)
                    .rootFirst("root")
                    .splitToNewChildren("root", "L", "R", 100L)
                    .buildList());
            FileReference splitFile = fileFactory().rootFile("test.parquet", 100);
            stateStore.addFiles(List.of(
                    fileFactory().partitionFile("L", "left.parquet", 23),
                    splitFile(splitFile, "L"),
                    splitFile(splitFile, "R")));

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

    private TableMetrics tableMetrics() {
        return TableMetrics.from(instanceProperties.get(ID), tableProperties.getStatus(), stateStore);
    }

    private FileReferenceFactory fileFactory() {
        return FileReferenceFactory.from(stateStore);
    }
}
