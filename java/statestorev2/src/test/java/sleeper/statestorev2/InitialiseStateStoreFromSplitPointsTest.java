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

package sleeper.statestorev2;

import org.junit.jupiter.api.Test;

import sleeper.core.partition.PartitionsBuilder;
import sleeper.core.properties.instance.InstanceProperties;
import sleeper.core.properties.table.TableProperties;
import sleeper.core.schema.Schema;
import sleeper.core.statestore.StateStore;
import sleeper.core.statestore.StateStoreProvider;
import sleeper.core.statestore.testutils.InMemoryTransactionLogStateStore;
import sleeper.core.statestore.testutils.InMemoryTransactionLogsPerTable;

import java.util.List;

import static org.assertj.core.api.Assertions.assertThat;
import static sleeper.core.properties.testutils.InstancePropertiesTestHelper.createTestInstanceProperties;
import static sleeper.core.properties.testutils.TablePropertiesTestHelper.createTestTableProperties;
import static sleeper.core.schema.SchemaTestHelper.createSchemaWithKey;

public class InitialiseStateStoreFromSplitPointsTest {
    private final InstanceProperties instanceProperties = createTestInstanceProperties();
    private final Schema schema = createSchemaWithKey("key");
    private final TableProperties tableProperties = createTestTableProperties(instanceProperties, schema);
    private final StateStoreProvider stateStoreProvider = InMemoryTransactionLogStateStore.createProvider(instanceProperties, new InMemoryTransactionLogsPerTable());

    @Test
    void shouldInitialiseStateStoreFromSplitPoints() throws Exception {
        // Given / When
        new InitialiseStateStoreFromSplitPoints(stateStoreProvider, tableProperties, List.of(123L)).run();

        // Then
        StateStore stateStore = stateStoreProvider.getStateStore(tableProperties);
        assertThat(stateStore.getAllPartitions())
                .usingRecursiveFieldByFieldElementComparatorIgnoringFields("id", "parentPartitionId", "childPartitionIds")
                .containsExactlyInAnyOrderElementsOf(
                        new PartitionsBuilder(schema)
                                .rootFirst("root")
                                .splitToNewChildren("root", "left", "right", 123L)
                                .buildList());
    }

    @Test
    void shouldInitialiseStateStoreWithRootLeafPartitionIfSplitPointsNotProvided() throws Exception {
        // Given / When
        new InitialiseStateStoreFromSplitPoints(stateStoreProvider, tableProperties, null).run();

        // Then
        StateStore stateStore = stateStoreProvider.getStateStore(tableProperties);
        assertThat(stateStore.getAllPartitions())
                .containsExactlyInAnyOrderElementsOf(
                        new PartitionsBuilder(schema)
                                .singlePartition("root")
                                .buildList());
    }
}
