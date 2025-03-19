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
package sleeper.garbagecollector;

import org.junit.jupiter.api.Test;

import sleeper.core.properties.instance.InstanceProperties;
import sleeper.core.properties.table.TableProperties;
import sleeper.core.schema.Schema;
import sleeper.core.table.TableFilePaths;

import java.util.List;
import java.util.Map;

import static org.assertj.core.api.Assertions.assertThat;
import static sleeper.core.properties.instance.CdkDefinedInstanceProperty.DATA_BUCKET;
import static sleeper.core.properties.testutils.InstancePropertiesTestHelper.createTestInstanceProperties;
import static sleeper.core.properties.testutils.TablePropertiesTestHelper.createTestTableProperties;
import static sleeper.core.schema.SchemaTestHelper.schemaWithKey;

public class FilesToDeleteTest {
    Schema schema = schemaWithKey("key");
    InstanceProperties instanceProperties = createTestInstanceProperties();
    TableProperties tableProperties = createTestTableProperties(instanceProperties, schema);

    // Tests:
    // - Read bucket name and object key from one file
    // - Read multiple files for one bucket
    // - Read files on multiple buckets
    // - Handle when file system scheme is missing
    // - Handle when file system scheme is not s3/s3a
    // - Fail when path includes bucket name but not object key
    // - Fail when path includes scheme but not bucket name or object key

    @Test
    void shouldReadBucketNameAndObjectKey() {
        // Given
        instanceProperties.set(DATA_BUCKET, "test-bucket");
        String filename = buildFullFilename("root", "test-file");

        // When
        FilesToDelete filesToDelete = FilesToDelete.from(List.of(filename));

        // Then
        assertThat(filesToDelete.getBucketNameToObjectKey()).isEqualTo(Map.of("test-bucket",
                List.of(buildObjectKey("root", "test-file"),
                        buildObjectKeyForSketches("root", "test-file"))));
        assertThat(filesToDelete.getObjectKeyToFilename()).isEqualTo(Map.of(
                buildObjectKey("root", "test-file"), filename,
                buildObjectKeyForSketches("root", "test-file"), filename));
    }

    @Test
    void shouldReadMultipleFiles() {
        // Given
        instanceProperties.set(DATA_BUCKET, "test-bucket");
        String filename1 = buildFullFilename("root", "test-file1");
        String filename2 = buildFullFilename("root", "test-file2");

        // When
        FilesToDelete filesToDelete = FilesToDelete.from(List.of(filename1, filename2));

        // Then
        assertThat(filesToDelete.getBucketNameToObjectKey()).isEqualTo(Map.of("test-bucket",
                List.of(buildObjectKey("root", "test-file1"),
                        buildObjectKeyForSketches("root", "test-file1"),
                        buildObjectKey("root", "test-file2"),
                        buildObjectKeyForSketches("root", "test-file2"))));

        assertThat(filesToDelete.getObjectKeyToFilename()).isEqualTo(Map.of(
                buildObjectKey("root", "test-file1"), filename1,
                buildObjectKeyForSketches("root", "test-file1"), filename1,
                buildObjectKey("root", "test-file2"), filename2,
                buildObjectKeyForSketches("root", "test-file2"), filename2));
    }

    @Test
    void shouldReadMultipleBuckets() {
        // Given
        InstanceProperties instance1 = createTestInstanceProperties();
        TableProperties table1 = createTestTableProperties(instance1, schema);
        instance1.set(DATA_BUCKET, "test-bucket1");
        String filename1 = buildFullFilename(instance1, table1, "root", "test-file1");

        InstanceProperties instance2 = createTestInstanceProperties();
        TableProperties table2 = createTestTableProperties(instance2, schema);
        instance2.set(DATA_BUCKET, "test-bucket2");
        String filename2 = buildFullFilename(instance2, table2, "root", "test-file2");

        // When
        FilesToDelete filesToDelete = FilesToDelete.from(List.of(filename1, filename2));

        // Then
        assertThat(filesToDelete.getBucketNameToObjectKey())
                .isEqualTo(Map.of(
                        "test-bucket1",
                        List.of(buildObjectKey(table1, "root", "test-file1"),
                                buildObjectKeyForSketches(table1, "root", "test-file1")),
                        "test-bucket2",
                        List.of(buildObjectKey(table2, "root", "test-file2"),
                                buildObjectKeyForSketches(table2, "root", "test-file2"))));

        assertThat(filesToDelete.getObjectKeyToFilename()).isEqualTo(Map.of(
                buildObjectKey(table1, "root", "test-file1"), filename1,
                buildObjectKeyForSketches(table1, "root", "test-file1"), filename1,
                buildObjectKey(table2, "root", "test-file2"), filename2,
                buildObjectKeyForSketches(table2, "root", "test-file2"), filename2));
    }

    @Test
    void shouldIgnoreFileWithMissingScheme() {
        // Given
        String filename = "test-bucket/test-file.parquet";

        // When
        FilesToDelete filesToDelete = FilesToDelete.from(List.of(filename));

        // Then
        assertThat(filesToDelete.getBucketNameToObjectKey()).isEmpty();
        assertThat(filesToDelete.getObjectKeyToFilename()).isEmpty();
    }

    private String buildFullFilename(String partitionId, String fileName) {
        return TableFilePaths.buildDataFilePathPrefix(instanceProperties, tableProperties).constructPartitionParquetFilePath(partitionId, fileName);
    }

    private String buildFullFilename(InstanceProperties instPropIn, TableProperties tablePropIn, String partitionId, String fileName) {
        return TableFilePaths.buildDataFilePathPrefix(instPropIn, tablePropIn).constructPartitionParquetFilePath(partitionId, fileName);
    }

    private String buildObjectKey(String partitionId, String fileName) {
        return TableFilePaths.buildObjectKeyInDataBucket(tableProperties).constructPartitionParquetFilePath(partitionId, fileName);
    }

    private String buildObjectKey(TableProperties tableProperties, String partitionId, String fileName) {
        return TableFilePaths.buildObjectKeyInDataBucket(tableProperties).constructPartitionParquetFilePath(partitionId, fileName);
    }

    private String buildObjectKeyForSketches(String partitionId, String fileName) {
        return TableFilePaths.buildObjectKeyInDataBucket(tableProperties).constructQuantileSketchesFilePath(partitionId, fileName);
    }

    private String buildObjectKeyForSketches(TableProperties tableProperties, String partitionId, String fileName) {
        return TableFilePaths.buildObjectKeyInDataBucket(tableProperties).constructQuantileSketchesFilePath(partitionId, fileName);
    }

}
