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
import sleeper.core.partition.PartitionTree;
import sleeper.core.partition.PartitionsBuilderSplitsFirst;
import sleeper.core.schema.Schema;
import sleeper.core.schema.type.StringType;
import sleeper.core.statestore.StateStore;
import sleeper.core.statestore.StateStoreException;
import sleeper.core.statestore.inmemory.StateStoreTestBuilder;

import java.util.List;
import java.util.stream.Collectors;
import java.util.stream.Stream;

import static org.assertj.core.api.Assertions.assertThat;
import static org.assertj.core.groups.Tuple.tuple;
import static sleeper.clients.status.report.partitions.PartitionStatusReportTestHelper.createPartitionsBuilder;
import static sleeper.clients.status.report.partitions.PartitionStatusReportTestHelper.createRootPartitionWithTwoChildren;
import static sleeper.clients.status.report.partitions.PartitionStatusReportTestHelper.createTableProperties;
import static sleeper.clients.status.report.partitions.PartitionStatusReportTestHelper.createTablePropertiesWithSplitThreshold;
import static sleeper.core.schema.SchemaTestHelper.schemaWithKey;
import static sleeper.core.statestore.inmemory.StateStoreTestHelper.inMemoryStateStoreWithFixedPartitions;

class PartitionsStatusTest {

    @Test
    void shouldCountRecordsInPartitions() throws StateStoreException {
        // Given
        StateStore store = createRootPartitionWithTwoChildren()
                .partitionFileWithRecords("A", "file-a1.parquet", 5)
                .partitionFileWithRecords("parent", "file-b1.parquet", 10)
                .splitFileToPartitions("file-b1.parquet", "A", "B")
                .buildStateStore();

        // When
        PartitionsStatus status = PartitionsStatus.from(createTableProperties(), store);

        // Then
        assertThat(status.getPartitions())
                .extracting("partition.id", "numberOfFiles", "approxRecords", "knownRecords")
                .containsExactlyInAnyOrder(
                        tuple("parent", 0, 0L, 0L),
                        tuple("A", 2, 10L, 5L),
                        tuple("B", 1, 5L, 0L));
    }

    @Test
    void shouldCountLeafPartitions() throws StateStoreException {
        // Given
        StateStore store = createRootPartitionWithTwoChildren().buildStateStore();

        // When
        PartitionsStatus status = PartitionsStatus.from(createTableProperties(), store);

        // Then
        assertThat(status.getNumLeafPartitions()).isEqualTo(2);
    }

    @Test
    void shouldFindNoSplittingPartitionsWhenThresholdNotExceeded() throws StateStoreException {
        // Given
        TableProperties tableProperties = createTablePropertiesWithSplitThreshold(10);
        StateStore store = createRootPartitionWithTwoChildren()
                .singleFileInEachLeafPartitionWithRecords(5).buildStateStore();

        // When
        PartitionsStatus status = PartitionsStatus.from(tableProperties, store);

        // Then
        assertThat(status.getNumLeafPartitionsThatWillBeSplit()).isZero();
    }

    @Test
    void shouldFindSplittingPartitionsWhenThresholdExceeded() throws StateStoreException {
        // Given
        TableProperties tableProperties = createTablePropertiesWithSplitThreshold(10);
        StateStore store = createRootPartitionWithTwoChildren()
                .singleFileInEachLeafPartitionWithRecords(100).buildStateStore();

        // When
        PartitionsStatus status = PartitionsStatus.from(tableProperties, store);

        // Then
        assertThat(status.getNumLeafPartitionsThatWillBeSplit()).isEqualTo(2);
    }

    @Test
    void shouldExcludeNonLeafPartitionsInNeedsSplittingCount() throws StateStoreException {
        // Given
        TableProperties tableProperties = createTablePropertiesWithSplitThreshold(10);
        StateStore store = StateStoreTestBuilder.from(createPartitionsBuilder()
                .rootFirst("root")
                .splitToNewChildren("root", "L", "R", "abc"))
                .partitionFileWithRecords("root", "not-split-yet.parquet", 100L)
                .buildStateStore();

        // When
        PartitionsStatus status = PartitionsStatus.from(tableProperties, store);

        // Then
        assertThat(status.getNumLeafPartitionsThatWillBeSplit()).isEqualTo(0);
    }

    @Test
    void shouldOrderPartitionsByTreeLeavesFirst() throws StateStoreException {
        // Given
        Schema schema = schemaWithKey("key", new StringType());
        PartitionTree partitions = PartitionsBuilderSplitsFirst.leavesWithSplits(schema,
                List.of("some-leaf", "other-leaf", "third-leaf", "fourth-leaf"),
                List.of("aaa", "bbb", "ccc"))
                .parentJoining("some-middle", "some-leaf", "other-leaf")
                .parentJoining("other-middle", "third-leaf", "fourth-leaf")
                .parentJoining("root", "some-middle", "other-middle")
                .buildTree();
        StateStore store = inMemoryStateStoreWithFixedPartitions(
                Stream.of("other-leaf", "fourth-leaf", "root", "other-middle", "some-leaf", "third-leaf", "some-middle")
                        .map(partitions::getPartition).collect(Collectors.toList()));

        // When
        PartitionsStatus status = PartitionsStatus.from(createTableProperties(), store);

        // Then
        assertThat(status.getPartitions()).extracting("partition.id").containsExactly(
                "some-leaf", "other-leaf", "third-leaf", "fourth-leaf",
                "some-middle", "other-middle", "root");
    }
}
