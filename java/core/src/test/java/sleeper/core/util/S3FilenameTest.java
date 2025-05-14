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
package sleeper.core.util;

import org.junit.jupiter.api.Test;

import sleeper.core.properties.instance.InstanceProperties;
import sleeper.core.properties.table.TableProperties;
import sleeper.core.table.TableFilePaths;

import static org.assertj.core.api.Assertions.assertThat;
import static org.assertj.core.api.Assertions.assertThatThrownBy;
import static sleeper.core.properties.instance.CdkDefinedInstanceProperty.DATA_BUCKET;
import static sleeper.core.properties.table.TableProperty.TABLE_ID;
import static sleeper.core.properties.testutils.InstancePropertiesTestHelper.createTestInstanceProperties;
import static sleeper.core.properties.testutils.TablePropertiesTestHelper.createTestTableProperties;
import static sleeper.core.schema.SchemaTestHelper.createSchemaWithKey;

public class S3FilenameTest {

    @Test
    void shouldParseFilename() {
        // Given
        InstanceProperties instanceProperties = createTestInstanceProperties();
        TableProperties tableProperties = createTestTableProperties(instanceProperties, createSchemaWithKey("key"));
        instanceProperties.set(DATA_BUCKET, "test-bucket");
        tableProperties.set(TABLE_ID, "test-table");
        TableFilePaths filePaths = TableFilePaths.buildDataFilePathPrefix(instanceProperties, tableProperties);
        String fullPath = filePaths.constructPartitionParquetFilePath("test-partition", "test-file");

        // When
        S3Filename filename = S3Filename.parse(fullPath);

        // Then
        assertThat(filename).isEqualTo(new S3Filename(fullPath,
                "test-bucket",
                "test-table/data/partition_test-partition/test-file.parquet"));
    }

    @Test
    void shouldRefuseInvalidFilename() {
        // Given
        String filename = "%^<";

        // When / Then
        assertThatThrownBy(() -> S3Filename.parse(filename))
                .isInstanceOf(IllegalArgumentException.class);
    }

    @Test
    void shouldRefuseFilenameWithMissingScheme() {
        // Given
        String filename = "test-bucket/test-file.parquet";

        // When / Then
        assertThatThrownBy(() -> S3Filename.parse(filename))
                .isInstanceOf(IllegalArgumentException.class)
                .hasMessage("Filename is missing scheme");
    }

    @Test
    void shouldRefuseFilenameWithNonS3Scheme() {
        // Given
        String filename = "file://test-bucket/test-file.parquet";

        // When / Then
        assertThatThrownBy(() -> S3Filename.parse(filename))
                .isInstanceOf(IllegalArgumentException.class)
                .hasMessage("Unexpected scheme: file");
    }

    @Test
    void shouldRefuseFilenameWithNoObjectKey() {
        // Given
        String filename = "s3a://test-bucket";

        // When / Then
        assertThatThrownBy(() -> S3Filename.parse(filename))
                .isInstanceOf(IllegalArgumentException.class)
                .hasMessage("Filename is missing object key");
    }

    @Test
    void shouldRefuseFilenameWithNoBucketNameOrObjectKey() {
        // Given
        String filename = "s3a://";

        // When / Then
        assertThatThrownBy(() -> S3Filename.parse(filename))
                .isInstanceOf(IllegalArgumentException.class)
                .hasMessage("Filename is missing object key");
    }

}
