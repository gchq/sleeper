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
package sleeper.clients.status.partitions;

import org.junit.jupiter.api.Test;

import sleeper.core.partition.PartitionsFromSplitPoints;
import sleeper.core.properties.instance.InstanceProperties;
import sleeper.core.properties.table.TableProperties;
import sleeper.core.schema.Field;
import sleeper.core.schema.Schema;
import sleeper.core.schema.type.ByteArrayType;
import sleeper.core.schema.type.IntType;
import sleeper.core.schema.type.LongType;
import sleeper.core.schema.type.PrimitiveType;
import sleeper.core.schema.type.StringType;
import sleeper.core.statestore.StateStore;
import sleeper.core.statestore.testutils.InMemoryTransactionLogStateStore;
import sleeper.core.statestore.testutils.InMemoryTransactionLogs;

import java.util.ArrayList;
import java.util.List;

import static org.assertj.core.api.Assertions.assertThat;
import static sleeper.core.properties.testutils.InstancePropertiesTestHelper.createTestInstanceProperties;
import static sleeper.core.properties.testutils.TablePropertiesTestHelper.createTestTableProperties;
import static sleeper.core.statestore.testutils.StateStoreUpdatesWrapper.update;

public class ExportSplitPointsTest {
    InstanceProperties instanceProperties = createTestInstanceProperties();

    private StateStore getStateStore(Schema schema) {
        TableProperties tableProperties = createTestTableProperties(instanceProperties, schema);
        return InMemoryTransactionLogStateStore.create(tableProperties, new InMemoryTransactionLogs());
    }

    private Schema schemaWithKeyType(PrimitiveType type) {
        return Schema.builder()
                .rowKeyFields(new Field("key", type))
                .sortKeyFields(new Field("sort", new LongType()))
                .valueFields(new Field("value", new ByteArrayType()))
                .build();
    }

    @Test
    public void shouldExportCorrectSplitPointsIntType() {
        // Given
        Schema schema = schemaWithKeyType(new IntType());
        StateStore stateStore = getStateStore(schema);
        List<Object> splitPoints = new ArrayList<>();
        splitPoints.add(-10);
        splitPoints.add(1000);
        update(stateStore).initialise(new PartitionsFromSplitPoints(schema, splitPoints).construct());
        ExportSplitPoints exportSplitPoints = new ExportSplitPoints(stateStore, schema);

        // When
        List<Object> exportedSplitPoints = exportSplitPoints.getSplitPoints();

        // Then
        assertThat(exportedSplitPoints).containsExactly(-10, 1000);
    }

    @Test
    public void shouldExportCorrectSplitPointsLongType() {
        // Given
        Schema schema = schemaWithKeyType(new LongType());
        StateStore stateStore = getStateStore(schema);
        List<Object> splitPoints = new ArrayList<>();
        splitPoints.add(-10L);
        splitPoints.add(1000L);
        update(stateStore).initialise(new PartitionsFromSplitPoints(schema, splitPoints).construct());
        ExportSplitPoints exportSplitPoints = new ExportSplitPoints(stateStore, schema);

        // When
        List<Object> exportedSplitPoints = exportSplitPoints.getSplitPoints();

        // Then
        assertThat(exportedSplitPoints).containsExactly(-10L, 1000L);
    }

    @Test
    public void shouldExportCorrectSplitPointsStringType() {
        // Given
        Schema schema = schemaWithKeyType(new StringType());
        StateStore stateStore = getStateStore(schema);
        List<Object> splitPoints = new ArrayList<>();
        splitPoints.add("A");
        splitPoints.add("T");
        update(stateStore).initialise(new PartitionsFromSplitPoints(schema, splitPoints).construct());
        ExportSplitPoints exportSplitPoints = new ExportSplitPoints(stateStore, schema);

        // When
        List<Object> exportedSplitPoints = exportSplitPoints.getSplitPoints();

        // Then
        assertThat(exportedSplitPoints).containsExactly("A", "T");
    }

    @Test
    public void shouldExportCorrectSplitPointsByteArrayType() {
        // Given
        Schema schema = schemaWithKeyType(new ByteArrayType());
        StateStore stateStore = getStateStore(schema);
        List<Object> splitPoints = new ArrayList<>();
        splitPoints.add(new byte[]{10});
        splitPoints.add(new byte[]{100});
        update(stateStore).initialise(new PartitionsFromSplitPoints(schema, splitPoints).construct());
        ExportSplitPoints exportSplitPoints = new ExportSplitPoints(stateStore, schema);

        // When
        List<Object> exportedSplitPoints = exportSplitPoints.getSplitPoints();

        // Then
        assertThat(exportedSplitPoints).containsExactly(new byte[]{10}, new byte[]{100});
    }
}
