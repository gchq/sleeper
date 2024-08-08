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

package sleeper.clients.status.report.partitions;

import org.junit.jupiter.api.Test;

import sleeper.configuration.properties.table.TableProperties;
import sleeper.core.partition.PartitionsBuilder;
import sleeper.core.partition.PartitionsBuilderSplitsFirst;
import sleeper.core.schema.Field;
import sleeper.core.schema.Schema;
import sleeper.core.schema.type.ByteArrayType;
import sleeper.core.schema.type.LongType;
import sleeper.core.schema.type.StringType;
import sleeper.core.statestore.StateStore;
import sleeper.core.statestore.inmemory.StateStoreTestBuilder;

import java.util.Collections;
import java.util.List;

import static org.assertj.core.api.Assertions.assertThat;
import static sleeper.clients.status.report.partitions.PartitionStatusReportTestHelper.createRootPartitionWithNoChildren;
import static sleeper.clients.status.report.partitions.PartitionStatusReportTestHelper.createRootPartitionWithTwoChildren;
import static sleeper.clients.status.report.partitions.PartitionStatusReportTestHelper.createTablePropertiesWithSplitThreshold;
import static sleeper.clients.status.report.partitions.PartitionStatusReportTestHelper.getStandardReport;
import static sleeper.clients.testutil.ClientTestUtils.example;
import static sleeper.core.statestore.inmemory.StateStoreTestHelper.inMemoryStateStoreWithFixedPartitions;

class PartitionsStatusReportTest {
    @Test
    void shouldReportNoPartitions() throws Exception {
        // Given
        TableProperties properties = createTablePropertiesWithSplitThreshold(10);
        StateStore store = inMemoryStateStoreWithFixedPartitions(Collections.emptyList());

        // When
        assertThat(getStandardReport(properties, store)).hasToString(
                example("reports/partitions/noPartitions.txt"));
    }

    @Test
    void shouldReportRootPartitionWithNoChildrenAndNoSplitNeeded() throws Exception {
        // Given
        TableProperties properties = createTablePropertiesWithSplitThreshold(10);
        StateStore store = createRootPartitionWithNoChildren()
                .singleFileInEachLeafPartitionWithRecords(5).buildStateStore();

        // When
        assertThat(getStandardReport(properties, store)).isEqualTo(
                example("reports/partitions/rootWithNoChildren.txt"));
    }

    @Test
    void shouldReportRootPartitionWithTwoChildrenAndNoSplitsNeeded() throws Exception {
        // Given
        TableProperties properties = createTablePropertiesWithSplitThreshold(10);
        StateStore store = createRootPartitionWithTwoChildren()
                .singleFileInEachLeafPartitionWithRecords(5).buildStateStore();

        // When
        assertThat(getStandardReport(properties, store)).hasToString(
                example("reports/partitions/rootWithTwoChildren.txt"));
    }

    @Test
    void shouldReportRootPartitionWithTwoChildrenBothNeedSplitting() throws Exception {
        // Given
        TableProperties properties = createTablePropertiesWithSplitThreshold(10);
        StateStore store = createRootPartitionWithTwoChildren()
                .singleFileInEachLeafPartitionWithRecords(100).buildStateStore();

        // When
        assertThat(getStandardReport(properties, store)).hasToString(
                example("reports/partitions/rootWithTwoChildrenBothNeedSplitting.txt"));
    }

    @Test
    void shouldReportRootPartitionSplitOnByteArray() throws Exception {
        // Given
        Schema schema = Schema.builder()
                .rowKeyFields(new Field("key", new ByteArrayType()))
                .build();
        TableProperties tableProperties = createTablePropertiesWithSplitThreshold(schema, 10);
        StateStore store = StateStoreTestBuilder.from(PartitionsBuilderSplitsFirst.leavesWithSplits(schema,
                List.of("A", "B"), List.of(new byte[42]))
                .parentJoining("parent", "A", "B"))
                .singleFileInEachLeafPartitionWithRecords(5)
                .buildStateStore();

        // When
        assertThat(getStandardReport(tableProperties, store)).hasToString(
                example("reports/partitions/rootWithTwoChildrenSplitOnByteArray.txt"));
    }

    @Test
    void shouldReportRootPartitionSplitOnLongStringHidingMiddle() throws Exception {
        // Given
        Schema schema = Schema.builder()
                .rowKeyFields(new Field("key", new StringType()))
                .build();
        TableProperties tableProperties = createTablePropertiesWithSplitThreshold(10);
        StateStore store = StateStoreTestBuilder.from(PartitionsBuilderSplitsFirst.leavesWithSplits(schema,
                List.of("A", "B"), List.of("abcdefghijklmnopqrstuvwxyzABCDEFGHIJKLMNOPQRSTUVWXYZ"))
                .parentJoining("parent", "A", "B"))
                .singleFileInEachLeafPartitionWithRecords(5).buildStateStore();

        // When
        assertThat(getStandardReport(tableProperties, store)).hasToString(
                example("reports/partitions/rootWithTwoChildrenSplitOnLongString.txt"));
    }

    @Test
    void shouldReportRootIsSplitOnFirstFieldAndLeavesAreSplitOnSecondField() throws Exception {
        // Given
        Schema schema = Schema.builder()
                .rowKeyFields(
                        new Field("first-key", new LongType()),
                        new Field("another-key", new StringType()))
                .build();
        TableProperties tableProperties = createTablePropertiesWithSplitThreshold(schema, 10);
        StateStore store = StateStoreTestBuilder.from(new PartitionsBuilder(schema)
                .rootFirst("parent")
                .splitToNewChildrenOnDimension("parent", "A", "B", 0, 123L)
                .splitToNewChildrenOnDimension("B", "C", "D", 1, "aaa"))
                .singleFileInEachLeafPartitionWithRecords(5)
                .buildStateStore();

        // When
        assertThat(getStandardReport(tableProperties, store)).hasToString(
                example("reports/partitions/rootWithNestedChildrenSplitOnDifferentFields.txt"));
    }

    @Test
    void shouldReportApproxAndKnownNumberOfRecordsWithSplitFilesInPartition() throws Exception {
        // Given
        TableProperties properties = createTablePropertiesWithSplitThreshold(10);
        StateStore store = createRootPartitionWithTwoChildren()
                .partitionFileWithRecords("A", "file-a1.parquet", 5L)
                .partitionFileWithRecords("B", "file-b1.parquet", 5L)
                .partitionFileWithRecords("parent", "file-split.parquet", 10L)
                .splitFileToPartitions("file-split.parquet", "A", "B")
                .buildStateStore();

        // When
        assertThat(getStandardReport(properties, store)).isEqualTo(
                example("reports/partitions/rootWithTwoChildrenWithSplitFiles.txt"));
    }

    @Test
    void shouldReportWhenNonLeafPartitionRecordCountExceedsSplitThreshold() throws Exception {
        Schema schema = Schema.builder()
                .rowKeyFields(new Field("key", new StringType()))
                .build();
        TableProperties properties = createTablePropertiesWithSplitThreshold(10);
        StateStore store = StateStoreTestBuilder.from(new PartitionsBuilder(schema)
                .rootFirst("root")
                .splitToNewChildren("root", "L", "R", "abc"))
                .partitionFileWithRecords("root", "not-split-yet.parquet", 100L)
                .buildStateStore();

        // When
        assertThat(getStandardReport(properties, store)).isEqualTo(
                example("reports/partitions/nonLeafPartitionRecordCountExceedsThreshold.txt"));
    }
}
