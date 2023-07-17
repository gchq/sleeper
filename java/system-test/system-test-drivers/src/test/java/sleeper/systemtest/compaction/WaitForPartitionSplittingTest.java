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

package sleeper.systemtest.compaction;

import org.junit.jupiter.api.DisplayName;
import org.junit.jupiter.api.Nested;
import org.junit.jupiter.api.Test;

import sleeper.configuration.properties.InstanceProperties;
import sleeper.configuration.properties.table.TableProperties;
import sleeper.core.partition.PartitionsBuilder;
import sleeper.core.schema.Field;
import sleeper.core.schema.Schema;
import sleeper.core.schema.type.StringType;
import sleeper.statestore.StateStore;
import sleeper.statestore.inmemory.StateStoreTestBuilder;
import sleeper.systemtest.drivers.compaction.WaitForPartitionSplitting;

import static org.assertj.core.api.Assertions.assertThat;
import static sleeper.configuration.properties.InstancePropertiesTestHelper.createTestInstanceProperties;
import static sleeper.configuration.properties.table.TablePropertiesTestHelper.createTestTableProperties;
import static sleeper.configuration.properties.table.TableProperty.PARTITION_SPLIT_THRESHOLD;

class WaitForPartitionSplittingTest {

    @Nested
    @DisplayName("The system is doing partition splitting and nothing else")
    class SystemIsOnlySplitting {
        @Test
        void shouldFindSplitsNotFinishedWhenOnePartitionStillNeedsSplitting() throws Exception {
            // Given
            TableProperties tableProperties = createTablePropertiesWithSplitThreshold("10");
            StateStore stateStore = StateStoreTestBuilder.from(partitionsBuilder(tableProperties)
                            .singlePartition("root"))
                    .partitionFileWithRecords("root", "test.parquet", 11)
                    .buildStateStore();

            // When
            WaitForPartitionSplitting waitForPartitionSplitting = WaitForPartitionSplitting
                    .forCurrentPartitionsNeedingSplitting(tableProperties, stateStore);

            // Then
            assertThat(waitForPartitionSplitting.isSplitFinished(stateStore)).isFalse();
        }

        @Test
        void shouldFindSplitsFinishedWhenNoPartitionsNeedSplitting() throws Exception {
            // Given
            TableProperties tableProperties = createTablePropertiesWithSplitThreshold("10");
            StateStore stateStore = StateStoreTestBuilder.from(partitionsBuilder(tableProperties)
                            .singlePartition("root"))
                    .partitionFileWithRecords("root", "test.parquet", 5)
                    .buildStateStore();

            // When
            WaitForPartitionSplitting waitForPartitionSplitting = WaitForPartitionSplitting
                    .forCurrentPartitionsNeedingSplitting(tableProperties, stateStore);

            // Then
            assertThat(waitForPartitionSplitting.isSplitFinished(stateStore)).isTrue();
        }

        @Test
        void shouldFindSplitFinishedWhenOnePartitionWasSplitButSplittingCompactionHasNotHappenedYet() throws Exception {
            // Given
            TableProperties tableProperties = createTablePropertiesWithSplitThreshold("10");
            StateStore before = StateStoreTestBuilder.from(partitionsBuilder(tableProperties)
                            .singlePartition("root"))
                    .partitionFileWithRecords("root", "test.parquet", 11)
                    .buildStateStore();
            StateStore after = StateStoreTestBuilder.from(partitionsBuilder(tableProperties)
                            .rootFirst("root")
                            .splitToNewChildren("root", "left", "right", "split point"))
                    .partitionFileWithRecords("root", "test.parquet", 11)
                    .buildStateStore();

            // When
            WaitForPartitionSplitting waitForPartitionSplitting = WaitForPartitionSplitting
                    .forCurrentPartitionsNeedingSplitting(tableProperties, before);

            // Then
            assertThat(waitForPartitionSplitting.isSplitFinished(after)).isTrue();
        }

        @Test
        void shouldFindSplitsNotFinishedWhenTwoPartitionsNeededSplittingAndOneIsFinished() throws Exception {
            // Given
            TableProperties tableProperties = createTablePropertiesWithSplitThreshold("10");
            StateStore before = StateStoreTestBuilder.from(partitionsBuilder(tableProperties)
                            .rootFirst("root")
                            .splitToNewChildren("root", "left", "right", "split point"))
                    .partitionFileWithRecords("left", "left.parquet", 11)
                    .partitionFileWithRecords("right", "right.parquet", 11)
                    .buildStateStore();
            StateStore after = StateStoreTestBuilder.from(partitionsBuilder(tableProperties)
                            .rootFirst("root")
                            .splitToNewChildren("root", "left", "right", "split point")
                            .splitToNewChildren("left", "left left", "left right", "left split"))
                    .partitionFileWithRecords("left", "left.parquet", 11)
                    .partitionFileWithRecords("right", "right.parquet", 11)
                    .buildStateStore();

            // When
            WaitForPartitionSplitting waitForPartitionSplitting = WaitForPartitionSplitting
                    .forCurrentPartitionsNeedingSplitting(tableProperties, before);

            // Then
            assertThat(waitForPartitionSplitting.isSplitFinished(after)).isFalse();
        }

        @Test
        void shouldFindSplitsFinishedWhenTwoPartitionsNeededSplittingAndBothAreFinished() throws Exception {
            // Given
            TableProperties tableProperties = createTablePropertiesWithSplitThreshold("10");
            StateStore before = StateStoreTestBuilder.from(partitionsBuilder(tableProperties)
                            .rootFirst("root")
                            .splitToNewChildren("root", "left", "right", "split point"))
                    .partitionFileWithRecords("left", "left.parquet", 11)
                    .partitionFileWithRecords("right", "right.parquet", 11)
                    .buildStateStore();
            StateStore after = StateStoreTestBuilder.from(partitionsBuilder(tableProperties)
                            .rootFirst("root")
                            .splitToNewChildren("root", "left", "right", "split point")
                            .splitToNewChildren("left", "left left", "left right", "left split")
                            .splitToNewChildren("right", "right left", "right right", "right split"))
                    .partitionFileWithRecords("left", "left.parquet", 11)
                    .partitionFileWithRecords("right", "right.parquet", 11)
                    .buildStateStore();

            // When
            WaitForPartitionSplitting waitForPartitionSplitting = WaitForPartitionSplitting
                    .forCurrentPartitionsNeedingSplitting(tableProperties, before);

            // Then
            assertThat(waitForPartitionSplitting.isSplitFinished(after)).isTrue();
        }
    }

    @Nested
    @DisplayName("The system is doing other things at the same time as partition splitting")
    class SystemIsDoingOtherThings {
        @Test
        void shouldFindSplitFinishedWhenOnePartitionWasSplitButANewSplitIsNeeded() throws Exception {
            // Given
            TableProperties tableProperties = createTablePropertiesWithSplitThreshold("10");
            StateStore before = StateStoreTestBuilder.from(partitionsBuilder(tableProperties)
                            .singlePartition("root"))
                    .partitionFileWithRecords("root", "test.parquet", 11)
                    .buildStateStore();
            StateStore after = StateStoreTestBuilder.from(partitionsBuilder(tableProperties)
                            .rootFirst("root")
                            .splitToNewChildren("root", "left", "right", "split point"))
                    .partitionFileWithRecords("root", "test.parquet", 11)
                    .partitionFileWithRecords("left", "left.parquet", 11)
                    .buildStateStore();

            // When
            WaitForPartitionSplitting waitForPartitionSplitting = WaitForPartitionSplitting
                    .forCurrentPartitionsNeedingSplitting(tableProperties, before);

            // Then
            assertThat(waitForPartitionSplitting.isSplitFinished(after)).isTrue();
        }

        @Test
        void shouldFindSplitFinishedWhenTableIsReinitialisedAndDataMovedToRoot() throws Exception {
            // Given
            TableProperties tableProperties = createTablePropertiesWithSplitThreshold("10");
            StateStore before = StateStoreTestBuilder.from(partitionsBuilder(tableProperties)
                            .rootFirst("root")
                            .splitToNewChildren("root", "left", "right", "split point"))
                    .partitionFileWithRecords("left", "left.parquet", 11)
                    .buildStateStore();
            StateStore after = StateStoreTestBuilder.from(partitionsBuilder(tableProperties)
                            .singlePartition("root"))
                    .partitionFileWithRecords("root", "test.parquet", 11)
                    .buildStateStore();

            // When
            WaitForPartitionSplitting waitForPartitionSplitting = WaitForPartitionSplitting
                    .forCurrentPartitionsNeedingSplitting(tableProperties, before);

            // Then
            assertThat(waitForPartitionSplitting.isSplitFinished(after)).isTrue();
        }

        @Test
        void shouldFindSplitNotFinishedWhenTableIsReinitialisedChangingRegionButPartitionStillNeedsSplitting() throws Exception {
            // Given
            TableProperties tableProperties = createTablePropertiesWithSplitThreshold("10");
            StateStore before = StateStoreTestBuilder.from(partitionsBuilder(tableProperties)
                            .rootFirst("root")
                            .splitToNewChildren("root", "left", "right", "split point before"))
                    .partitionFileWithRecords("left", "left.parquet", 11)
                    .buildStateStore();
            StateStore after = StateStoreTestBuilder.from(partitionsBuilder(tableProperties)
                            .rootFirst("root")
                            .splitToNewChildren("root", "left", "right", "split point after"))
                    .partitionFileWithRecords("left", "left.parquet", 11)
                    .buildStateStore();

            // When
            WaitForPartitionSplitting waitForPartitionSplitting = WaitForPartitionSplitting
                    .forCurrentPartitionsNeedingSplitting(tableProperties, before);

            // Then
            assertThat(waitForPartitionSplitting.isSplitFinished(after)).isFalse();
        }
    }

    private PartitionsBuilder partitionsBuilder(TableProperties tableProperties) {
        return new PartitionsBuilder(tableProperties.getSchema());
    }

    private TableProperties createTablePropertiesWithSplitThreshold(String threshold) {
        InstanceProperties instanceProperties = createTestInstanceProperties();
        TableProperties tableProperties = createTestTableProperties(instanceProperties,
                Schema.builder().rowKeyFields(new Field("key", new StringType())).build());
        tableProperties.set(PARTITION_SPLIT_THRESHOLD, threshold);
        return tableProperties;
    }
}
