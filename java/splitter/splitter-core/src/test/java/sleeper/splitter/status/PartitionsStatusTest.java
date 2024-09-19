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

package sleeper.splitter.status;

import org.junit.jupiter.api.Disabled;
import org.junit.jupiter.api.DisplayName;
import org.junit.jupiter.api.Nested;
import org.junit.jupiter.api.Test;

import sleeper.configuration.properties.table.TableProperties;
import sleeper.core.partition.PartitionTree;
import sleeper.core.partition.PartitionsBuilder;
import sleeper.core.partition.PartitionsBuilderSplitsFirst;
import sleeper.core.schema.Field;
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
import static sleeper.core.schema.SchemaTestHelper.schemaWithKey;
import static sleeper.core.statestore.inmemory.StateStoreTestHelper.inMemoryStateStoreWithFixedPartitions;
import static sleeper.splitter.status.PartitionsStatusTestHelper.createPartitionsBuilder;
import static sleeper.splitter.status.PartitionsStatusTestHelper.createRootPartitionWithTwoChildren;
import static sleeper.splitter.status.PartitionsStatusTestHelper.createTableProperties;
import static sleeper.splitter.status.PartitionsStatusTestHelper.createTablePropertiesWithSplitThreshold;

class PartitionsStatusTest {

    private final Schema schema = Schema.builder()
            .rowKeyFields(new Field("key", new StringType()))
            .build();

    @Nested
    @DisplayName("Count known & estimated records")
    class CountRecords {

        @Test
        void shouldCountRecordsFromReferencesOnLeaves() throws Exception {
            // Given
            StateStore store = StateStoreTestBuilder.from(new PartitionsBuilder(schema)
                    .rootFirst("parent")
                    .splitToNewChildren("parent", "A", "B", "aaa"))
                    .partitionFileWithRecords("A", "file-a.parquet", 5)
                    .partitionFileWithRecords("parent", "file-ab.parquet", 10)
                    .splitFileToPartitions("file-ab.parquet", "A", "B")
                    .buildStateStore();

            // When
            PartitionsStatus status = PartitionsStatus.from(createTableProperties(), store);

            // Then
            assertThat(status.getPartitions())
                    .extracting("partition.id", "numberOfFiles", "knownRecords", "approxRecords", "approxRecordsAfterCompaction")
                    .containsExactlyInAnyOrder(
                            tuple("parent", 0, 0L, 0L, 0L),
                            tuple("A", 2, 5L, 10L, 10L),
                            tuple("B", 1, 0L, 5L, 5L));
        }

        @Test
        @Disabled("TODO")
        void shouldCountRecordsFromReferenceOnLeafAndRoot() throws Exception {
            // Given
            StateStore store = StateStoreTestBuilder.from(new PartitionsBuilder(schema)
                    .rootFirst("parent")
                    .splitToNewChildren("parent", "A", "B", "aaa"))
                    .partitionFileWithRecords("A", "file-a.parquet", 5)
                    .partitionFileWithRecords("parent", "file-parent.parquet", 10)
                    .buildStateStore();

            // When
            PartitionsStatus status = PartitionsStatus.from(createTableProperties(), store);

            // Then
            assertThat(status.getPartitions())
                    .extracting("partition.id", "numberOfFiles", "knownRecords", "approxRecords", "approxRecordsAfterCompaction")
                    .containsExactlyInAnyOrder(
                            tuple("parent", 1, 10L, 10L, 0L),
                            tuple("A", 1, 5L, 5L, 10L),
                            tuple("B", 0, 0L, 0L, 5L));
        }
    }

    @Nested
    @DisplayName("Count partitions")
    class CountPartitions {

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
