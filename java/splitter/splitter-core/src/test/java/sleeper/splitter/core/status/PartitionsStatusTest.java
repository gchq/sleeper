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

package sleeper.splitter.core.status;

import org.junit.jupiter.api.DisplayName;
import org.junit.jupiter.api.Nested;
import org.junit.jupiter.api.Test;

import sleeper.core.partition.PartitionsBuilder;
import sleeper.core.properties.instance.InstanceProperties;
import sleeper.core.properties.table.TableProperties;
import sleeper.core.schema.Field;
import sleeper.core.schema.Schema;
import sleeper.core.schema.type.StringType;
import sleeper.core.statestore.FileReference;
import sleeper.core.statestore.FileReferenceFactory;
import sleeper.core.statestore.StateStore;
import sleeper.core.statestore.testutils.InMemoryTransactionLogStateStore;
import sleeper.core.statestore.testutils.InMemoryTransactionLogs;

import java.util.List;

import static org.assertj.core.api.Assertions.assertThat;
import static org.assertj.core.groups.Tuple.tuple;
import static sleeper.core.properties.table.TableProperty.PARTITION_SPLIT_THRESHOLD;
import static sleeper.core.properties.testutils.InstancePropertiesTestHelper.createTestInstanceProperties;
import static sleeper.core.properties.testutils.TablePropertiesTestHelper.createTestTableProperties;
import static sleeper.core.statestore.FileReferenceTestData.splitFile;
import static sleeper.core.statestore.testutils.StateStoreUpdatesWrapper.update;

class PartitionsStatusTest {

    private final Schema schema = Schema.builder()
            .rowKeyFields(new Field("key", new StringType()))
            .build();
    private final InstanceProperties instanceProperties = createTestInstanceProperties();
    private final TableProperties tableProperties = createTestTableProperties(instanceProperties, schema);
    private final StateStore stateStore = InMemoryTransactionLogStateStore.create(tableProperties, new InMemoryTransactionLogs());

    @Nested
    @DisplayName("Count known & estimated records")
    class CountRecords {

        @Test
        void shouldCountRecordsFromReferencesOnLeaves() throws Exception {
            // Given
            stateStore.initialise(new PartitionsBuilder(schema)
                    .rootFirst("parent")
                    .splitToNewChildren("parent", "A", "B", "aaa")
                    .buildList());
            update(stateStore).addFile(fileFactory().partitionFile("A", "file-a.parquet", 5));
            FileReference splitFile = fileFactory().rootFile("file-ab.parquet", 10);
            update(stateStore).addFiles(List.of(
                    splitFile(splitFile, "A"),
                    splitFile(splitFile, "B")));

            // When
            PartitionsStatus status = PartitionsStatus.from(tableProperties, stateStore);

            // Then
            assertThat(status.getPartitions())
                    .extracting("partition.id", "numberOfFiles", "exactRecordsReferenced", "approxRecordsReferenced", "approxRecords")
                    .containsExactlyInAnyOrder(
                            tuple("parent", 0, 0L, 0L, 15L),
                            tuple("A", 2, 5L, 10L, 10L),
                            tuple("B", 1, 0L, 5L, 5L));
        }

        @Test
        void shouldCountRecordsFromReferenceOnLeafAndRoot() throws Exception {
            // Given
            stateStore.initialise(new PartitionsBuilder(schema)
                    .rootFirst("parent")
                    .splitToNewChildren("parent", "A", "B", "aaa")
                    .buildList());
            update(stateStore).addFile(fileFactory().partitionFile("A", "file-a.parquet", 5));
            update(stateStore).addFile(fileFactory().rootFile("file-parent.parquet", 10));

            // When
            PartitionsStatus status = PartitionsStatus.from(tableProperties, stateStore);

            // Then
            assertThat(status.getPartitions())
                    .extracting("partition.id", "numberOfFiles", "exactRecordsReferenced", "approxRecordsReferenced", "approxRecords")
                    .containsExactlyInAnyOrder(
                            tuple("parent", 1, 10L, 10L, 15L),
                            tuple("A", 1, 5L, 5L, 10L),
                            tuple("B", 0, 0L, 0L, 5L));
        }

        @Test
        void shouldCountRecordsFromNestedTree() throws Exception {
            // Given
            stateStore.initialise(new PartitionsBuilder(schema)
                    .rootFirst("root")
                    .splitToNewChildren("root", "L", "R", "aaa")
                    .splitToNewChildren("R", "RL", "RR", "bbb")
                    .buildList());
            update(stateStore).addFile(fileFactory().rootFile("file-root.parquet", 20));
            update(stateStore).addFile(fileFactory().partitionFile("R", "file-R.parquet", 10));
            update(stateStore).addFile(fileFactory().partitionFile("RR", "file-RR.parquet", 5));

            // When
            PartitionsStatus status = PartitionsStatus.from(tableProperties, stateStore);

            // Then
            assertThat(status.getPartitions())
                    .extracting("partition.id", "numberOfFiles", "exactRecordsReferenced", "approxRecordsReferenced", "approxRecords")
                    .containsExactlyInAnyOrder(
                            tuple("root", 1, 20L, 20L, 35L),
                            tuple("L", 0, 0L, 0L, 10L),
                            tuple("R", 1, 10L, 10L, 25L),
                            tuple("RL", 0, 0L, 0L, 10L),
                            tuple("RR", 1, 5L, 5L, 15L));
        }
    }

    @Nested
    @DisplayName("Count partitions")
    class CountPartitions {

        @Test
        void shouldCountLeafPartitions() {
            // Given
            stateStore.initialise(new PartitionsBuilder(schema)
                    .rootFirst("parent")
                    .splitToNewChildren("parent", "A", "B", "aaa")
                    .buildList());

            // When
            PartitionsStatus status = PartitionsStatus.from(tableProperties, stateStore);

            // Then
            assertThat(status.getNumLeafPartitions()).isEqualTo(2);
        }

        @Test
        void shouldFindNoSplittingPartitionsWhenThresholdNotExceeded() {
            // Given
            tableProperties.setNumber(PARTITION_SPLIT_THRESHOLD, 10);
            stateStore.initialise(new PartitionsBuilder(schema)
                    .rootFirst("parent")
                    .splitToNewChildren("parent", "A", "B", "aaa")
                    .buildList());
            update(stateStore).addFiles(fileFactory().singleFileInEachLeafPartitionWithRecords(5).toList());

            // When
            PartitionsStatus status = PartitionsStatus.from(tableProperties, stateStore);

            // Then
            assertThat(status.getNumLeafPartitionsThatWillBeSplit()).isZero();
        }

        @Test
        void shouldFindSplittingPartitionsWhenThresholdExceeded() {
            // Given
            tableProperties.setNumber(PARTITION_SPLIT_THRESHOLD, 10);
            stateStore.initialise(new PartitionsBuilder(schema)
                    .rootFirst("parent")
                    .splitToNewChildren("parent", "A", "B", "aaa")
                    .buildList());
            update(stateStore).addFiles(fileFactory().singleFileInEachLeafPartitionWithRecords(100).toList());

            // When
            PartitionsStatus status = PartitionsStatus.from(tableProperties, stateStore);

            // Then
            assertThat(status.getNumLeafPartitionsThatWillBeSplit()).isEqualTo(2);
        }

        @Test
        void shouldExcludeNonLeafPartitionsInNeedsSplittingCount() {
            // Given
            tableProperties.setNumber(PARTITION_SPLIT_THRESHOLD, 10);
            stateStore.initialise(new PartitionsBuilder(schema)
                    .rootFirst("root")
                    .splitToNewChildren("root", "L", "R", "abc")
                    .buildList());
            update(stateStore).addFile(fileFactory().rootFile("not-split-yet.parquet", 100));

            // When
            PartitionsStatus status = PartitionsStatus.from(tableProperties, stateStore);

            // Then
            assertThat(status.getNumLeafPartitionsThatWillBeSplit()).isEqualTo(0);
        }
    }

    @Test
    void shouldOrderPartitionsByTreeLeavesFirst() {
        // Given
        stateStore.initialise(new PartitionsBuilder(schema)
                .rootFirst("root")
                .splitToNewChildren("root", "some-middle", "other-middle", "aaa")
                .splitToNewChildren("some-middle", "some-leaf", "other-leaf", "bbb")
                .splitToNewChildren("other-middle", "third-leaf", "fourth-leaf", "ccc")
                .buildList());

        // When
        PartitionsStatus status = PartitionsStatus.from(tableProperties, stateStore);

        // Then
        assertThat(status.getPartitions()).extracting("partition.id").containsExactly(
                "some-leaf", "other-leaf", "third-leaf", "fourth-leaf",
                "some-middle", "other-middle", "root");
    }

    private FileReferenceFactory fileFactory() {
        return FileReferenceFactory.from(stateStore);
    }
}
