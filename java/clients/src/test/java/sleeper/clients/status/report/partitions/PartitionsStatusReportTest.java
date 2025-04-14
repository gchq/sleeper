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

package sleeper.clients.status.report.partitions;

import org.junit.jupiter.api.Test;

import sleeper.clients.testutil.ToStringConsoleOutput;
import sleeper.core.partition.PartitionsBuilder;
import sleeper.core.properties.instance.InstanceProperties;
import sleeper.core.properties.table.TableProperties;
import sleeper.core.schema.Field;
import sleeper.core.schema.Schema;
import sleeper.core.schema.type.ByteArrayType;
import sleeper.core.schema.type.LongType;
import sleeper.core.schema.type.StringType;
import sleeper.core.statestore.FileReference;
import sleeper.core.statestore.FileReferenceFactory;
import sleeper.core.statestore.StateStore;
import sleeper.core.statestore.testutils.InMemoryTransactionLogStateStore;
import sleeper.core.statestore.testutils.InMemoryTransactionLogs;
import sleeper.splitter.core.status.PartitionsStatus;

import java.util.List;

import static org.assertj.core.api.Assertions.assertThat;
import static sleeper.clients.testutil.ClientTestUtils.example;
import static sleeper.core.properties.table.TableProperty.PARTITION_SPLIT_THRESHOLD;
import static sleeper.core.properties.testutils.InstancePropertiesTestHelper.createTestInstanceProperties;
import static sleeper.core.properties.testutils.TablePropertiesTestHelper.createTestTableProperties;
import static sleeper.core.schema.SchemaTestHelper.createSchemaWithKey;
import static sleeper.core.statestore.AssignJobIdRequest.assignJobOnPartitionToFiles;
import static sleeper.core.statestore.FileReferenceTestData.splitFile;
import static sleeper.core.statestore.testutils.StateStoreUpdatesWrapper.update;

class PartitionsStatusReportTest {
    InstanceProperties instanceProperties = createTestInstanceProperties();
    TableProperties tableProperties = createTestTableProperties(instanceProperties, createSchemaWithKey("key", new StringType()));
    StateStore stateStore = InMemoryTransactionLogStateStore.create(tableProperties, new InMemoryTransactionLogs());
    PartitionsBuilder partitions;

    @Test
    void shouldReportNoPartitions() throws Exception {
        // Given
        tableProperties.setNumber(PARTITION_SPLIT_THRESHOLD, 10);
        update(stateStore).initialise(List.of());

        // When
        assertThat(getStandardReport()).hasToString(
                example("reports/partitions/noPartitions.txt"));
    }

    @Test
    void shouldReportRootPartitionWithNoChildrenAndNoSplitNeeded() throws Exception {
        // Given
        tableProperties.setNumber(PARTITION_SPLIT_THRESHOLD, 10);
        update(stateStore).initialise(partitionsBuilder().singlePartition("root").buildList());
        update(stateStore).addFiles(fileFactory().singleFileInEachLeafPartitionWithRecords(5).toList());

        // When
        assertThat(getStandardReport()).isEqualTo(
                example("reports/partitions/rootWithNoChildren.txt"));
    }

    @Test
    void shouldReportRootPartitionWithTwoChildrenAndNoSplitsNeeded() throws Exception {
        // Given
        tableProperties.setNumber(PARTITION_SPLIT_THRESHOLD, 10);
        update(stateStore).initialise(partitionsBuilder().rootFirst("parent")
                .splitToNewChildren("parent", "A", "B", "aaa")
                .buildList());
        update(stateStore).addFiles(fileFactory().singleFileInEachLeafPartitionWithRecords(5).toList());

        // When
        assertThat(getStandardReport()).hasToString(
                example("reports/partitions/rootWithTwoChildren.txt"));
    }

    @Test
    void shouldReportRootPartitionWithTwoChildrenBothNeedSplitting() throws Exception {
        // Given
        tableProperties.setNumber(PARTITION_SPLIT_THRESHOLD, 10);
        update(stateStore).initialise(partitionsBuilder().rootFirst("parent")
                .splitToNewChildren("parent", "A", "B", "aaa")
                .buildList());
        update(stateStore).addFiles(fileFactory().singleFileInEachLeafPartitionWithRecords(100).toList());

        // When
        assertThat(getStandardReport()).hasToString(
                example("reports/partitions/rootWithTwoChildrenBothNeedSplitting.txt"));
    }

    @Test
    void shouldReportRootPartitionSplitOnByteArray() throws Exception {
        // Given
        tableProperties.setSchema(createSchemaWithKey("key", new ByteArrayType()));
        tableProperties.setNumber(PARTITION_SPLIT_THRESHOLD, 10);
        update(stateStore).initialise(partitionsBuilder().rootFirst("parent")
                .splitToNewChildren("parent", "A", "B", new byte[42])
                .buildList());
        update(stateStore).addFiles(fileFactory().singleFileInEachLeafPartitionWithRecords(5).toList());

        // When
        assertThat(getStandardReport()).hasToString(
                example("reports/partitions/rootWithTwoChildrenSplitOnByteArray.txt"));
    }

    @Test
    void shouldReportRootPartitionSplitOnLongStringHidingMiddle() throws Exception {
        // Given
        tableProperties.setSchema(createSchemaWithKey("key", new StringType()));
        tableProperties.setNumber(PARTITION_SPLIT_THRESHOLD, 10);
        update(stateStore).initialise(partitionsBuilder().rootFirst("parent")
                .splitToNewChildren("parent", "A", "B", "abcdefghijklmnopqrstuvwxyzABCDEFGHIJKLMNOPQRSTUVWXYZ")
                .buildList());
        update(stateStore).addFiles(fileFactory().singleFileInEachLeafPartitionWithRecords(5).toList());

        // When
        assertThat(getStandardReport()).hasToString(
                example("reports/partitions/rootWithTwoChildrenSplitOnLongString.txt"));
    }

    @Test
    void shouldReportRootIsSplitOnFirstFieldAndLeavesAreSplitOnSecondField() throws Exception {
        // Given
        tableProperties.setSchema(Schema.builder()
                .rowKeyFields(
                        new Field("first-key", new LongType()),
                        new Field("another-key", new StringType()))
                .build());
        tableProperties.setNumber(PARTITION_SPLIT_THRESHOLD, 10);
        update(stateStore).initialise(partitionsBuilder().rootFirst("parent")
                .splitToNewChildrenOnDimension("parent", "A", "B", 0, 123L)
                .splitToNewChildrenOnDimension("B", "C", "D", 1, "aaa")
                .buildList());
        update(stateStore).addFiles(fileFactory().singleFileInEachLeafPartitionWithRecords(5).toList());

        // When
        assertThat(getStandardReport()).hasToString(
                example("reports/partitions/rootWithNestedChildrenSplitOnDifferentFields.txt"));
    }

    @Test
    void shouldReportApproxAndKnownNumberOfRecordsWithSplitFilesInPartition() throws Exception {
        // Given
        tableProperties.setNumber(PARTITION_SPLIT_THRESHOLD, 10);
        update(stateStore).initialise(partitionsBuilder().rootFirst("parent")
                .splitToNewChildren("parent", "A", "B", "aaa")
                .buildList());
        FileReferenceFactory fileFactory = fileFactory();
        update(stateStore).addFile(fileFactory.partitionFile("A", "file-a1.parquet", 5L));
        update(stateStore).addFile(fileFactory.partitionFile("B", "file-b1.parquet", 5L));
        FileReference splitFile = fileFactory.partitionFile("parent", "file-split.parquet", 10L);
        update(stateStore).addFiles(List.of(
                splitFile(splitFile, "A"),
                splitFile(splitFile, "B")));

        // When
        assertThat(getStandardReport()).isEqualTo(
                example("reports/partitions/rootWithTwoChildrenWithSplitFiles.txt"));
    }

    @Test
    void shouldReportWhenNonLeafPartitionRecordCountExceedsSplitThreshold() throws Exception {
        // Given
        tableProperties.setNumber(PARTITION_SPLIT_THRESHOLD, 10);
        tableProperties.setSchema(createSchemaWithKey("key", new StringType()));
        update(stateStore).initialise(partitionsBuilder().rootFirst("root")
                .splitToNewChildren("root", "L", "R", "abc")
                .buildList());
        update(stateStore).addFile(fileFactory().partitionFile("root", "not-split-yet.parquet", 100L));

        // When
        assertThat(getStandardReport()).isEqualTo(
                example("reports/partitions/nonLeafPartitionRecordCountExceedsThreshold.txt"));
    }

    @Test
    void shouldReportWhenNonLeafPartitionRecordCountExceedsSplitThresholdWithRecordsFurtherUpTree() throws Exception {
        tableProperties.setNumber(PARTITION_SPLIT_THRESHOLD, 100);
        tableProperties.setSchema(createSchemaWithKey("key", new StringType()));
        update(stateStore).initialise(partitionsBuilder().rootFirst("root")
                .splitToNewChildren("root", "L", "R", "abc")
                .splitToNewChildren("R", "RL", "RR", "def")
                .buildList());
        update(stateStore).addFile(fileFactory().partitionFile("root", 100L));
        update(stateStore).addFile(fileFactory().partitionFile("R", 100L));
        update(stateStore).addFile(fileFactory().partitionFile("RL", 24L));
        update(stateStore).addFile(fileFactory().partitionFile("RR", 26L));

        // When
        assertThat(getStandardReport()).isEqualTo(
                example("reports/partitions/combinedPartitionRecordCountExceedsThreshold.txt"));
    }

    @Test
    void shouldReportSomeFilesAssignedToAJob() throws Exception {
        tableProperties.setNumber(PARTITION_SPLIT_THRESHOLD, 10);
        tableProperties.setSchema(createSchemaWithKey("key", new StringType()));
        update(stateStore).initialise(partitionsBuilder().singlePartition("root").buildList());
        update(stateStore).addFile(fileFactory().partitionFile("root", "1.parquet", 100L));
        update(stateStore).addFile(fileFactory().partitionFile("root", "2.parquet", 100L));
        update(stateStore).addFile(fileFactory().partitionFile("root", "3.parquet", 100L));
        update(stateStore).assignJobIds(List.of(assignJobOnPartitionToFiles("test-job", "root", List.of("1.parquet", "2.parquet"))));

        // When
        assertThat(getStandardReport()).isEqualTo(
                example("reports/partitions/rootWithSomeFilesOnJob.txt"));
    }

    public static String getStandardReport(TableProperties tableProperties, StateStore stateStore) {
        ToStringConsoleOutput output = new ToStringConsoleOutput();
        PartitionsStatusReporter reporter = new PartitionsStatusReporter(output.getPrintStream());
        reporter.report(PartitionsStatus.from(tableProperties, stateStore));
        return output.toString();
    }

    String getStandardReport() {
        ToStringConsoleOutput output = new ToStringConsoleOutput();
        PartitionsStatusReporter reporter = new PartitionsStatusReporter(output.getPrintStream());
        reporter.report(PartitionsStatus.from(tableProperties, stateStore));
        return output.toString();
    }

    PartitionsBuilder partitionsBuilder() {
        partitions = new PartitionsBuilder(tableProperties.getSchema());
        return partitions;
    }

    FileReferenceFactory fileFactory() {
        return FileReferenceFactory.from(partitions.buildTree());
    }
}
