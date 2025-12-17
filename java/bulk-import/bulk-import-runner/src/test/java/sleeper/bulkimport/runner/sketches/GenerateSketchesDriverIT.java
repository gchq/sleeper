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
package sleeper.bulkimport.runner.sketches;

import org.junit.jupiter.api.AfterAll;
import org.junit.jupiter.api.BeforeAll;
import org.junit.jupiter.api.BeforeEach;
import org.junit.jupiter.api.Test;
import org.junit.jupiter.api.io.TempDir;

import sleeper.bulkimport.runner.BulkImportContext;
import sleeper.core.properties.instance.InstanceProperties;
import sleeper.core.properties.table.TableProperties;
import sleeper.core.schema.Field;
import sleeper.core.schema.Schema;
import sleeper.core.schema.type.IntType;
import sleeper.core.schema.type.ListType;
import sleeper.core.schema.type.LongType;
import sleeper.core.schema.type.MapType;
import sleeper.core.schema.type.StringType;
import sleeper.core.statestore.StateStore;
import sleeper.core.statestore.testutils.InMemoryTransactionLogStateStore;
import sleeper.core.statestore.testutils.InMemoryTransactionLogs;
import sleeper.sketches.Sketches;
import sleeper.sketches.store.LocalFileSystemSketchesStore;
import sleeper.sketches.store.SketchesStore;

import java.util.List;
import java.util.Map;

import static org.assertj.core.api.Assertions.assertThat;
import static sleeper.core.properties.instance.CdkDefinedInstanceProperty.DATA_BUCKET;
import static sleeper.core.properties.instance.CommonProperty.FILE_SYSTEM;
import static sleeper.core.properties.testutils.InstancePropertiesTestHelper.createTestInstanceProperties;
import static sleeper.core.properties.testutils.TablePropertiesTestHelper.createTestTablePropertiesWithNoSchema;
import static sleeper.core.statestore.testutils.StateStoreUpdatesWrapper.update;

public class GenerateSketchesDriverIT {

    private final InstanceProperties instanceProperties = createTestInstanceProperties();
    private final TableProperties tableProperties = createTestTablePropertiesWithNoSchema(instanceProperties);
    private final SketchesStore sketchesStore = new LocalFileSystemSketchesStore();
    private final StateStore stateStore = InMemoryTransactionLogStateStore.create(tableProperties, new InMemoryTransactionLogs());
    @TempDir
    public java.nio.file.Path tempDir;

    @BeforeEach
    void setUpBase() {
        instanceProperties.set(DATA_BUCKET, tempDir.toString());
        instanceProperties.set(FILE_SYSTEM, "file://");
        tableProperties.setSchema(Schema.builder()
                .rowKeyFields(new Field("key", new IntType()))
                .sortKeyFields(new Field("sort", new LongType()))
                .valueFields(
                        new Field("value1", new StringType()),
                        new Field("value2", new ListType(new IntType())),
                        new Field("value3", new MapType(new StringType(), new LongType())))
                .build());
        update(stateStore).initialise(tableProperties);
    }

    @BeforeAll
    public static void setSparkProperties() {
        System.setProperty("spark.master", "local");
        System.setProperty("spark.app.name", "bulk import");
    }

    @AfterAll
    public static void clearSparkProperties() {
        System.clearProperty("spark.master");
        System.clearProperty("spark.app.name");
    }

    @Test
    void shouldGenerateNoSketches() {
        // Given
        List<String> filenames = List.of();

        // When
        Map<String, Sketches> partitionIdToSketches = generateSketches(filenames);

        // Then
        assertThat(partitionIdToSketches).isEmpty();
    }

    private Map<String, Sketches> generateSketches(List<String> filenames) {
        try (BulkImportContext context = BulkImportContext.create(
                instanceProperties, tableProperties, stateStore.getAllPartitions(), filenames)) {
            return GenerateSketchesDriver.generatePartitionIdToSketches(context);
        }
    }

}
