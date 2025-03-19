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

import org.junit.jupiter.api.DisplayName;
import org.junit.jupiter.api.Nested;
import org.junit.jupiter.api.Test;

import sleeper.core.properties.instance.InstanceProperties;
import sleeper.core.properties.table.TableProperties;
import sleeper.core.schema.Schema;
import sleeper.core.table.TableFilePaths;

import java.util.HashSet;
import java.util.List;
import java.util.Map;
import java.util.Set;

import static org.assertj.core.api.Assertions.assertThat;
import static org.assertj.core.api.Assertions.assertThatThrownBy;
import static sleeper.core.properties.instance.CdkDefinedInstanceProperty.DATA_BUCKET;
import static sleeper.core.properties.testutils.InstancePropertiesTestHelper.createTestInstanceProperties;
import static sleeper.core.properties.testutils.TablePropertiesTestHelper.createTestTableProperties;
import static sleeper.core.schema.SchemaTestHelper.schemaWithKey;

public class FilesToDeleteTest {
    Schema schema = schemaWithKey("key");
    InstanceProperties instanceProperties = createTestInstanceProperties();
    TableProperties tableProperties = createTestTableProperties(instanceProperties, schema);

    @Nested
    @DisplayName("Index files for deletion")
    class IndexFiles {

        @Test
        void shouldReadBucketNameAndObjectKey() {
            // Given
            instanceProperties.set(DATA_BUCKET, "test-bucket");
            String filename = buildFullFilename("root", "test-file");

            // When
            FilesToDelete filesToDelete = FilesToDelete.from(List.of(filename));

            // Then
            assertThat(filesToDelete.getBuckets()).containsExactly(
                    new FilesToDeleteInBucket("test-bucket",
                            Map.of(
                                    buildObjectKey("root", "test-file"), filename,
                                    buildObjectKeyForSketches("root", "test-file"), filename)));
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
            assertThat(filesToDelete.getBuckets()).containsExactly(
                    new FilesToDeleteInBucket("test-bucket",
                            Map.of(
                                    buildObjectKey("root", "test-file1"), filename1,
                                    buildObjectKeyForSketches("root", "test-file1"), filename1,
                                    buildObjectKey("root", "test-file2"), filename2,
                                    buildObjectKeyForSketches("root", "test-file2"), filename2)));
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
            assertThat(new HashSet<>(filesToDelete.getBuckets())).isEqualTo(Set.of(
                    new FilesToDeleteInBucket("test-bucket1",
                            Map.of(
                                    buildObjectKey(table1, "root", "test-file1"), filename1,
                                    buildObjectKeyForSketches(table1, "root", "test-file1"), filename1)),
                    new FilesToDeleteInBucket("test-bucket2",
                            Map.of(
                                    buildObjectKey(table2, "root", "test-file2"), filename2,
                                    buildObjectKeyForSketches(table2, "root", "test-file2"), filename2))));
        }
    }

    @Nested
    @DisplayName("Handle invalid filenames")
    class InvalidFilenames {

        @Test
        void shouldIgnoreInvalidFilename() {
            // Given
            String filename = "%^<";

            // When
            FilesToDelete filesToDelete = FilesToDelete.from(List.of(filename));

            // Then
            assertThat(filesToDelete.getBuckets()).isEmpty();
        }

        @Test
        void shouldRefuseFilenameWithMissingScheme() {
            // Given
            String filename = "test-bucket/test-file.parquet";

            // When / Then
            assertThatThrownBy(() -> FileToDelete.fromFilename(filename))
                    .isInstanceOf(IllegalArgumentException.class)
                    .hasMessage("Filename is missing scheme");
        }

        @Test
        void shouldRefuseFilenameWithNonS3Scheme() {
            // Given
            String filename = "file://test-bucket/test-file.parquet";

            // When / Then
            assertThatThrownBy(() -> FileToDelete.fromFilename(filename))
                    .isInstanceOf(IllegalArgumentException.class)
                    .hasMessage("Unexpected scheme: file");
        }

        @Test
        void shouldRefuseFilenameWithNoObjectKey() {
            // Given
            String filename = "s3a://test-bucket";

            // When / Then
            assertThatThrownBy(() -> FileToDelete.fromFilename(filename))
                    .isInstanceOf(IllegalArgumentException.class)
                    .hasMessage("Filename is missing object key");
        }

        @Test
        void shouldRefuseFilenameWithNoBucketNameOrObjectKey() {
            // Given
            String filename = "s3a://";

            // When / Then
            assertThatThrownBy(() -> FileToDelete.fromFilename(filename))
                    .isInstanceOf(IllegalArgumentException.class)
                    .hasMessage("Filename is missing object key");
        }
    }

    @Nested
    @DisplayName("Split object keys to delete into batches for S3")
    class BatchesOfObjectKeys {

        @Test
        void shouldRetrieveSingleBatchBelowBatchSize() {
            // Given
            FilesToDeleteInBucket files = new FilesToDeleteInBucket("test-bucket",
                    Map.of("object-key", "test-file"));

            // When / Then
            assertThat(files.objectKeysInBatchesOf(2))
                    .containsExactly(List.of("object-key"));
        }

        @Test
        void shouldRetrieveTwoBatches() {
            // Given
            FilesToDeleteInBucket files = new FilesToDeleteInBucket("test-bucket",
                    Map.of("key-1", "file-1", "key-2", "file-2", "key-3", "file-3"));

            // When / Then
            assertThat(files.objectKeysInBatchesOf(2))
                    .hasSize(2) // 2 batches
                    .flatExtracting(batch -> batch)
                    .containsExactlyInAnyOrder("key-1", "key-2", "key-3");
        }
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
