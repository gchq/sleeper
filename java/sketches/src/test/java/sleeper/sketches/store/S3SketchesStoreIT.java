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

import sleeper.core.properties.instance.InstanceProperties;
import sleeper.core.properties.table.TableProperties;
import sleeper.core.schema.Schema;
import sleeper.core.table.TableFilePaths;
import sleeper.localstack.test.LocalStackTestBase;
import sleeper.sketches.Sketches;
import sleeper.sketches.testutils.SketchesDeciles;
import sleeper.sketches.testutils.SketchesTestData;

import static org.assertj.core.api.Assertions.assertThat;
import static sleeper.core.properties.instance.CdkDefinedInstanceProperty.DATA_BUCKET;
import static sleeper.core.properties.testutils.InstancePropertiesTestHelper.createTestInstanceProperties;
import static sleeper.core.properties.testutils.TablePropertiesTestHelper.createTestTableProperties;

public class S3SketchesStoreIT extends LocalStackTestBase {
    InstanceProperties instanceProperties = createTestInstanceProperties();
    Schema schema = SketchesTestData.SCHEMA;
    TableProperties tableProperties = createTestTableProperties(instanceProperties, schema);
    SketchesStore store = new S3SketchesStore(s3Client, s3TransferManager);

    @BeforeEach
    void setUp() {
        createBucket(instanceProperties.get(DATA_BUCKET));
    }

    @Test
    void shouldWriteSketchesToS3() {
        // Given
        Sketches sketches = SketchesTestData.createExampleSketches();
        String filename = filePaths().constructPartitionParquetFilePath("test-partition", "test-file");

        // When
        store.saveFileSketches(filename, schema, sketches);
        Sketches found = store.loadFileSketches(filename, schema);

        // Then
        assertThat(SketchesDeciles.from(found)).isEqualTo(SketchesTestData.createExampleDeciles());
    }

    private TableFilePaths filePaths() {
        return TableFilePaths.buildDataFilePathPrefix(instanceProperties, tableProperties);
    }

}
