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
package sleeper.query.core.rowretrieval;

import org.junit.jupiter.api.Test;

import sleeper.core.properties.instance.InstanceProperties;
import sleeper.core.properties.model.DataEngine;
import sleeper.core.properties.table.TableProperties;
import sleeper.core.row.testutils.InMemoryRowStore;
import sleeper.core.schema.type.LongType;

import static org.assertj.core.api.Assertions.assertThat;
import static sleeper.core.properties.table.TableProperty.DATA_ENGINE;
import static sleeper.core.properties.testutils.InstancePropertiesTestHelper.createTestInstanceProperties;
import static sleeper.core.properties.testutils.TablePropertiesTestHelper.createTestTableProperties;
import static sleeper.core.schema.SchemaTestHelper.createSchemaWithKey;

public class QueryEngineSelectorTest {

    InstanceProperties instanceProperties = createTestInstanceProperties();
    TableProperties tableProperties = createTestTableProperties(instanceProperties, createSchemaWithKey("key", new LongType()));
    InMemoryRowStore rowStore = new InMemoryRowStore();
    InMemoryLeafPartitionRowRetriever javaRowRetriever = new InMemoryLeafPartitionRowRetriever(rowStore);
    InMemoryLeafPartitionRowRetriever dataFusionRowRetriever = new InMemoryLeafPartitionRowRetriever(rowStore);

    @Test
    void shouldSetDataFusionEngine() {
        // Given
        tableProperties.setEnum(DATA_ENGINE, DataEngine.DATAFUSION_EXPERIMENTAL);

        // When / Then
        assertThat(createRowRetriever()).isSameAs(dataFusionRowRetriever);
    }

    @Test
    void shouldSetJavaEngine() {
        // Given
        tableProperties.setEnum(DATA_ENGINE, DataEngine.JAVA);

        // When / Then
        assertThat(createRowRetriever()).isSameAs(javaRowRetriever);
    }

    @Test
    void shouldSetDataFusionForCompactionOnly() {
        // Given
        tableProperties.setEnum(DATA_ENGINE, DataEngine.DATAFUSION);

        // When / Then
        assertThat(createRowRetriever()).isSameAs(javaRowRetriever);
    }

    private LeafPartitionRowRetriever createRowRetriever() {
        return QueryEngineSelector.javaAndDataFusion(javaRowRetriever, dataFusionRowRetriever)
                .getRowRetriever(tableProperties);
    }

}
