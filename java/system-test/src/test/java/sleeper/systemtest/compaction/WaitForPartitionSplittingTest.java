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

import org.junit.jupiter.api.Test;

import sleeper.configuration.properties.InstanceProperties;
import sleeper.configuration.properties.table.TableProperties;
import sleeper.core.partition.PartitionsBuilder;
import sleeper.statestore.StateStore;
import sleeper.statestore.inmemory.StateStoreTestBuilder;

import static org.assertj.core.api.Assertions.assertThat;
import static sleeper.configuration.properties.InstancePropertiesTestHelper.createTestInstanceProperties;
import static sleeper.configuration.properties.table.TablePropertiesTestHelper.createTestTableProperties;
import static sleeper.configuration.properties.table.TableProperty.PARTITION_SPLIT_THRESHOLD;
import static sleeper.core.schema.SchemaTestHelper.schemaWithKey;

class WaitForPartitionSplittingTest {
    @Test
    void shouldWaitWhenOnePartitionStillNeedsSplitting() throws Exception {
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
    void shouldNotWaitWhenNoPartitionsNeedSplitting() throws Exception {
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

    private PartitionsBuilder partitionsBuilder(TableProperties tableProperties) {
        return new PartitionsBuilder(tableProperties.getSchema());
    }

    private TableProperties createTablePropertiesWithSplitThreshold(String threshold) {
        InstanceProperties instanceProperties = createTestInstanceProperties();
        TableProperties tableProperties = createTestTableProperties(instanceProperties, schemaWithKey("key"));
        tableProperties.set(PARTITION_SPLIT_THRESHOLD, threshold);
        return tableProperties;
    }
}
