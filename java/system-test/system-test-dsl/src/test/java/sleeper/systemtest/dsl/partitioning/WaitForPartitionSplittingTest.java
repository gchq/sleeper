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

package sleeper.systemtest.dsl.partitioning;

import org.junit.jupiter.api.DisplayName;
import org.junit.jupiter.api.Nested;
import org.junit.jupiter.api.Test;

import sleeper.core.partition.PartitionsBuilder;
import sleeper.core.properties.instance.InstanceProperties;
import sleeper.core.properties.table.TableProperties;
import sleeper.core.schema.type.StringType;
import sleeper.core.statestore.FileReferenceFactory;
import sleeper.core.statestore.StateStore;
import sleeper.core.statestore.testutils.InMemoryTransactionLogStateStore;
import sleeper.core.statestore.testutils.InMemoryTransactionLogs;

import java.util.stream.Stream;

import static org.assertj.core.api.Assertions.assertThat;
import static sleeper.core.properties.table.TableProperty.PARTITION_SPLIT_THRESHOLD;
import static sleeper.core.properties.testutils.InstancePropertiesTestHelper.createTestInstanceProperties;
import static sleeper.core.properties.testutils.TablePropertiesTestHelper.createTestTableProperties;
import static sleeper.core.schema.SchemaTestHelper.schemaWithKey;
import static sleeper.core.statestore.testutils.StateStoreUpdatesWrapper.update;

class WaitForPartitionSplittingTest {

    InstanceProperties instanceProperties = createTestInstanceProperties();
    TableProperties tableProperties = createTestTableProperties(instanceProperties, schemaWithKey("key", new StringType()));
    StateStore stateStore = InMemoryTransactionLogStateStore.create(tableProperties, new InMemoryTransactionLogs());
    PartitionsBuilder partitions;

    @Nested
    @DisplayName("The system is doing partition splitting and nothing else")
    class SystemIsOnlySplitting {
        @Test
        void shouldFindSplitsNotFinishedWhenOnePartitionStillNeedsSplitting() {
            // Given
            tableProperties.setNumber(PARTITION_SPLIT_THRESHOLD, 10);
            update(stateStore).initialise(new PartitionsBuilder(tableProperties).singlePartition("root").buildList());
            update(stateStore).addFile(fileFactory().partitionFile("root", "test.parquet", 11));

            // When
            WaitForPartitionSplitting waitForPartitionSplitting = waitWithStateBefore(tableProperties, stateStore);

            // Then
            assertThat(isSplitFinishedWithState(waitForPartitionSplitting, tableProperties, stateStore))
                    .isFalse();
        }

        @Test
        void shouldFindSplitsFinishedWhenNoPartitionsNeedSplitting() {
            // Given
            tableProperties.setNumber(PARTITION_SPLIT_THRESHOLD, 10);
            update(stateStore).initialise(new PartitionsBuilder(tableProperties).singlePartition("root").buildList());
            update(stateStore).addFile(fileFactory().partitionFile("root", "test.parquet", 5));

            // When
            WaitForPartitionSplitting waitForPartitionSplitting = waitWithStateBefore(tableProperties, stateStore);

            // Then
            assertThat(isSplitFinishedWithState(waitForPartitionSplitting, tableProperties, stateStore))
                    .isTrue();
        }

        @Test
        void shouldFindSplitFinishedWhenOnePartitionWasSplitButSplittingCompactionHasNotHappenedYet() {
            // Given
            tableProperties.setNumber(PARTITION_SPLIT_THRESHOLD, 10);

            StateStore before = InMemoryTransactionLogStateStore.create(tableProperties, new InMemoryTransactionLogs());
            update(before).initialise(new PartitionsBuilder(tableProperties).singlePartition("root").buildList());
            update(before).addFile(fileFactory(before).partitionFile("root", "test.parquet", 11));

            StateStore after = InMemoryTransactionLogStateStore.create(tableProperties, new InMemoryTransactionLogs());
            update(after).initialise(new PartitionsBuilder(tableProperties)
                    .rootFirst("root")
                    .splitToNewChildren("root", "left", "right", "split point")
                    .buildList());
            update(after).addFile(fileFactory(after).partitionFile("root", "test.parquet", 11));

            // When
            WaitForPartitionSplitting waitForPartitionSplitting = waitWithStateBefore(tableProperties, before);

            // Then
            assertThat(isSplitFinishedWithState(waitForPartitionSplitting, tableProperties, after))
                    .isTrue();
        }

        @Test
        void shouldFindSplitsNotFinishedWhenTwoPartitionsNeededSplittingAndOneIsFinished() {
            // Given
            tableProperties.setNumber(PARTITION_SPLIT_THRESHOLD, 10);

            StateStore before = InMemoryTransactionLogStateStore.create(tableProperties, new InMemoryTransactionLogs());
            update(before).initialise(new PartitionsBuilder(tableProperties)
                    .rootFirst("root")
                    .splitToNewChildren("root", "left", "right", "split point")
                    .buildList());
            update(before).addFile(fileFactory(before).partitionFile("left", "left.parquet", 11));
            update(before).addFile(fileFactory(before).partitionFile("right", "right.parquet", 11));

            StateStore after = InMemoryTransactionLogStateStore.create(tableProperties, new InMemoryTransactionLogs());
            update(after).initialise(new PartitionsBuilder(tableProperties)
                    .rootFirst("root")
                    .splitToNewChildren("root", "left", "right", "split point")
                    .splitToNewChildren("left", "left left", "left right", "left split")
                    .buildList());
            update(after).addFile(fileFactory(after).partitionFile("left", "left.parquet", 11));
            update(after).addFile(fileFactory(after).partitionFile("right", "right.parquet", 11));

            // When
            WaitForPartitionSplitting waitForPartitionSplitting = waitWithStateBefore(tableProperties, before);

            // Then
            assertThat(isSplitFinishedWithState(waitForPartitionSplitting, tableProperties, after))
                    .isFalse();
        }

        @Test
        void shouldFindSplitsFinishedWhenTwoPartitionsNeededSplittingAndBothAreFinished() {
            // Given
            tableProperties.setNumber(PARTITION_SPLIT_THRESHOLD, 10);

            StateStore before = InMemoryTransactionLogStateStore.create(tableProperties, new InMemoryTransactionLogs());
            update(before).initialise(new PartitionsBuilder(tableProperties)
                    .rootFirst("root")
                    .splitToNewChildren("root", "left", "right", "split point")
                    .buildList());
            update(before).addFile(fileFactory(before).partitionFile("left", "left.parquet", 11));
            update(before).addFile(fileFactory(before).partitionFile("right", "right.parquet", 11));

            StateStore after = InMemoryTransactionLogStateStore.create(tableProperties, new InMemoryTransactionLogs());
            update(after).initialise(new PartitionsBuilder(tableProperties)
                    .rootFirst("root")
                    .splitToNewChildren("root", "left", "right", "split point")
                    .splitToNewChildren("left", "left left", "left right", "split left")
                    .splitToNewChildren("right", "right left", "right right", "split right")
                    .buildList());
            update(after).addFile(fileFactory(after).partitionFile("left", "left.parquet", 11));
            update(after).addFile(fileFactory(after).partitionFile("right", "right.parquet", 11));

            // When
            WaitForPartitionSplitting waitForPartitionSplitting = waitWithStateBefore(tableProperties, before);

            // Then
            assertThat(isSplitFinishedWithState(waitForPartitionSplitting, tableProperties, after))
                    .isTrue();
        }
    }

    @Nested
    @DisplayName("The system is doing other things at the same time as partition splitting")
    class SystemIsDoingOtherThings {
        @Test
        void shouldFindSplitFinishedWhenOnePartitionWasSplitButANewSplitIsNeeded() {
            // Given
            tableProperties.setNumber(PARTITION_SPLIT_THRESHOLD, 10);

            StateStore before = InMemoryTransactionLogStateStore.create(tableProperties, new InMemoryTransactionLogs());
            update(before).initialise(new PartitionsBuilder(tableProperties).singlePartition("root").buildList());
            update(before).addFile(fileFactory(before).partitionFile("root", "test.parquet", 11));

            StateStore after = InMemoryTransactionLogStateStore.create(tableProperties, new InMemoryTransactionLogs());
            update(after).initialise(new PartitionsBuilder(tableProperties)
                    .rootFirst("root")
                    .splitToNewChildren("root", "left", "right", "split point")
                    .buildList());
            update(after).addFile(fileFactory(after).partitionFile("root", "test.parquet", 11));
            update(after).addFile(fileFactory(after).partitionFile("left", "left.parquet", 11));

            // When
            WaitForPartitionSplitting waitForPartitionSplitting = waitWithStateBefore(tableProperties, before);

            // Then
            assertThat(isSplitFinishedWithState(waitForPartitionSplitting, tableProperties, after))
                    .isTrue();
        }

        @Test
        void shouldFindSplitFinishedWhenTableIsReinitialisedAndDataMovedToRoot() {
            // Given
            tableProperties.setNumber(PARTITION_SPLIT_THRESHOLD, 10);

            StateStore before = InMemoryTransactionLogStateStore.create(tableProperties, new InMemoryTransactionLogs());
            update(before).initialise(new PartitionsBuilder(tableProperties)
                    .rootFirst("root")
                    .splitToNewChildren("root", "left", "right", "split point")
                    .buildList());
            update(before).addFile(fileFactory(before).partitionFile("left", "left.parquet", 11));

            StateStore after = InMemoryTransactionLogStateStore.create(tableProperties, new InMemoryTransactionLogs());
            update(after).initialise(new PartitionsBuilder(tableProperties).singlePartition("root").buildList());
            update(after).addFile(fileFactory(after).partitionFile("root", "test.parquet", 11));

            // When
            WaitForPartitionSplitting waitForPartitionSplitting = waitWithStateBefore(tableProperties, before);

            // Then
            assertThat(isSplitFinishedWithState(waitForPartitionSplitting, tableProperties, after))
                    .isTrue();
        }

        @Test
        void shouldFindSplitNotFinishedWhenTableIsReinitialisedChangingRegionButPartitionStillNeedsSplitting() {
            // Given
            tableProperties.setNumber(PARTITION_SPLIT_THRESHOLD, 10);

            StateStore before = InMemoryTransactionLogStateStore.create(tableProperties, new InMemoryTransactionLogs());
            update(before).initialise(new PartitionsBuilder(tableProperties)
                    .rootFirst("root")
                    .splitToNewChildren("root", "left", "right", "split point before")
                    .buildList());
            update(before).addFile(fileFactory(before).partitionFile("left", "left.parquet", 11));

            StateStore after = InMemoryTransactionLogStateStore.create(tableProperties, new InMemoryTransactionLogs());
            update(after).initialise(new PartitionsBuilder(tableProperties)
                    .rootFirst("root")
                    .splitToNewChildren("root", "left", "right", "split point after")
                    .buildList());
            update(after).addFile(fileFactory(after).partitionFile("left", "left.parquet", 11));

            // When
            WaitForPartitionSplitting waitForPartitionSplitting = waitWithStateBefore(tableProperties, before);

            // Then
            assertThat(isSplitFinishedWithState(waitForPartitionSplitting, tableProperties, after))
                    .isFalse();
        }
    }

    private WaitForPartitionSplitting waitWithStateBefore(TableProperties properties, StateStore stateStore) {
        return WaitForPartitionSplitting.forCurrentPartitionsNeedingSplitting(Stream.of(properties), table -> stateStore);
    }

    private boolean isSplitFinishedWithState(WaitForPartitionSplitting wait, TableProperties properties, StateStore stateStore) {
        return wait.isSplitFinished(properties, stateStore);
    }

    private FileReferenceFactory fileFactory() {
        return FileReferenceFactory.from(stateStore);
    }

    private FileReferenceFactory fileFactory(StateStore stateStore) {
        return FileReferenceFactory.from(stateStore);
    }
}
