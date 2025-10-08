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
package sleeper.query.runner.rowretrieval;

import org.apache.hadoop.conf.Configuration;
import org.junit.jupiter.api.Test;

import sleeper.core.properties.instance.InstanceProperties;
import sleeper.core.properties.model.DataEngine;
import sleeper.core.properties.table.TableProperties;
import sleeper.core.schema.type.LongType;
import sleeper.foreign.bridge.FFIContext;
import sleeper.query.core.rowretrieval.LeafPartitionRowRetriever;
import sleeper.query.datafusion.DataFusionLeafPartitionRowRetriever;

import java.util.concurrent.ForkJoinPool;

import static org.assertj.core.api.Assertions.assertThat;
import static org.mockito.Mockito.mock;
import static sleeper.core.properties.table.TableProperty.DATA_ENGINE;
import static sleeper.core.properties.testutils.InstancePropertiesTestHelper.createTestInstanceProperties;
import static sleeper.core.properties.testutils.TablePropertiesTestHelper.createTestTableProperties;
import static sleeper.core.schema.SchemaTestHelper.createSchemaWithKey;

public class QueryEngineSelectorTest {

    InstanceProperties instanceProperties = createTestInstanceProperties();
    TableProperties tableProperties = createTestTableProperties(instanceProperties, createSchemaWithKey("key", new LongType()));

    @Test
    void shouldSetDataFusionEngine() {
        // Given
        tableProperties.setEnum(DATA_ENGINE, DataEngine.DATAFUSION_EXPERIMENTAL);

        // When / Then
        assertThat(createRowRetriever()).isInstanceOf(DataFusionLeafPartitionRowRetriever.class);
    }

    @Test
    void shouldSetJavaEngine() {
        // Given
        tableProperties.setEnum(DATA_ENGINE, DataEngine.JAVA);

        // When / Then
        assertThat(createRowRetriever()).isInstanceOf(LeafPartitionRowRetrieverImpl.class);
    }

    @Test
    void shouldSetDataFusionForCompactionOnly() {
        // Given
        tableProperties.setEnum(DATA_ENGINE, DataEngine.DATAFUSION);

        // When / Then
        assertThat(createRowRetriever()).isInstanceOf(LeafPartitionRowRetrieverImpl.class);
    }

    private LeafPartitionRowRetriever createRowRetriever() {
        return new QueryEngineSelector(ForkJoinPool.commonPool(), new Configuration(), null, mock(FFIContext.class))
                .getRowRetriever(tableProperties);
    }

}
