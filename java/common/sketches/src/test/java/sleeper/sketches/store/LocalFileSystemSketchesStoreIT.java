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
package sleeper.sketches.store;

import org.junit.jupiter.api.BeforeEach;
import org.junit.jupiter.api.Test;
import org.junit.jupiter.api.io.TempDir;

import sleeper.core.properties.instance.InstanceProperties;
import sleeper.core.properties.table.TableProperties;
import sleeper.core.schema.Schema;
import sleeper.core.table.TableFilePaths;
import sleeper.sketches.Sketches;
import sleeper.sketches.testutils.SketchesDeciles;
import sleeper.sketches.testutils.SketchesTestData;

import java.nio.file.Path;

import static org.assertj.core.api.Assertions.assertThat;
import static sleeper.core.properties.instance.CdkDefinedInstanceProperty.DATA_BUCKET;
import static sleeper.core.properties.instance.CommonProperty.FILE_SYSTEM;
import static sleeper.core.properties.testutils.InstancePropertiesTestHelper.createTestInstanceProperties;
import static sleeper.core.properties.testutils.TablePropertiesTestHelper.createTestTableProperties;

public class LocalFileSystemSketchesStoreIT {

    InstanceProperties instanceProperties = createTestInstanceProperties();
    Schema schema = SketchesTestData.SCHEMA;
    TableProperties tableProperties = createTestTableProperties(instanceProperties, schema);
    SketchesStore store = new LocalFileSystemSketchesStore();

    @TempDir
    Path tempDir;

    @BeforeEach
    void setUp() {
        instanceProperties.set(FILE_SYSTEM, "file://");
        instanceProperties.set(DATA_BUCKET, tempDir.toString());
    }

    @Test
    void shouldWriteSketchesToFile() {
        Sketches sketches = SketchesTestData.createExampleSketches();
        String filename = filePaths().constructPartitionParquetFilePath("test-partition", "test-file");

        // When
        store.saveFileSketches(filename, schema, sketches);
        Sketches found = store.loadFileSketches(filename, schema);

        // Then
        assertThat(SketchesDeciles.from(found)).isEqualTo(SketchesTestData.createExampleDeciles());
        assertThat(tempDir).isDirectoryRecursivelyContaining("glob:**/test-file.sketches");
    }

    private TableFilePaths filePaths() {
        return TableFilePaths.buildDataFilePathPrefix(instanceProperties, tableProperties);
    }

}
