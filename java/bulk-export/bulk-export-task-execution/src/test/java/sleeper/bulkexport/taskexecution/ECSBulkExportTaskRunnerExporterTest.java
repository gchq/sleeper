/*
 * Copyright 2022-2026 Crown Copyright
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
package sleeper.bulkexport.taskexecution;

import org.junit.jupiter.api.Test;

import sleeper.bulkexport.core.model.BulkExportLeafPartitionQuery;
import sleeper.bulkexport.taskexecution.ECSBulkExportTaskRunner.BulkExporter;
import sleeper.bulkexport.taskexecution.ECSBulkExportTaskRunner.DataFusionQueryExporter;
import sleeper.bulkexport.taskexecution.ECSBulkExportTaskRunner.JavaCompactionExporter;
import sleeper.core.properties.instance.InstanceProperties;
import sleeper.core.properties.model.DataEngine;
import sleeper.core.properties.table.TableProperties;
import sleeper.core.range.Range.RangeFactory;
import sleeper.core.range.Region;
import sleeper.core.schema.Field;
import sleeper.core.schema.Schema;
import sleeper.core.schema.type.StringType;
import sleeper.foreign.datafusion.DataFusionAwsConfig;

import java.util.List;

import static org.assertj.core.api.Assertions.assertThat;
import static sleeper.core.properties.table.TableProperty.DATA_ENGINE;
import static sleeper.core.properties.testutils.InstancePropertiesTestHelper.createTestInstanceProperties;
import static sleeper.core.properties.testutils.TablePropertiesTestHelper.createTestTableProperties;

public class ECSBulkExportTaskRunnerExporterTest {

    private final InstanceProperties instanceProperties = createTestInstanceProperties();
    private final Field keyField = new Field("key", new StringType());
    private final Schema schema = Schema.builder().rowKeyFields(keyField).build();
    private final TableProperties tableProperties = createTestTableProperties(instanceProperties, schema);
    private final Region partitionRegion = new Region(new RangeFactory(schema).createExactRange(keyField, "a"));
    private final BulkExportLeafPartitionQuery query = BulkExportLeafPartitionQuery.builder()
            .tableId("test-table-id")
            .exportId("export-id")
            .subExportId("sub-export-id")
            .leafPartitionId("partition-id")
            .partitionRegion(partitionRegion)
            .files(List.of())
            .regions(List.of())
            .build();
    private final DataFusionAwsConfig awsConfig = DataFusionAwsConfig.getDefault(instanceProperties);

    @Test
    void shouldCreateJavaCompactionExporterForJavaEngine() {
        // Given
        tableProperties.setEnum(DATA_ENGINE, DataEngine.JAVA);

        // When
        BulkExporter exporter = exporterFor();

        // Then
        assertThat(exporter).isInstanceOf(JavaCompactionExporter.class);
    }

    @Test
    void shouldCreateDataFusionQueryExporterForDataFusionEngine() {
        // Given
        tableProperties.setEnum(DATA_ENGINE, DataEngine.DATAFUSION);

        // When
        BulkExporter exporter = exporterFor();

        // Then
        assertThat(exporter).isInstanceOf(DataFusionQueryExporter.class);
    }

    @Test
    void shouldCreateDataFusionQueryExporterForDataFusionExperimentalEngine() {
        // Given
        tableProperties.setEnum(DATA_ENGINE, DataEngine.DATAFUSION_EXPERIMENTAL);

        // When
        BulkExporter exporter = exporterFor();

        // Then
        assertThat(exporter).isInstanceOf(DataFusionQueryExporter.class);
    }

    private BulkExporter exporterFor() {
        return ECSBulkExportTaskRunner.exporterFor(
                query, instanceProperties, null, null, awsConfig, "output-file", tableProperties);
    }
}
