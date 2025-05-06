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
package sleeper.statestorev2.transactionlog.snapshots;

import org.junit.jupiter.api.BeforeEach;
import org.junit.jupiter.api.Test;

import sleeper.core.properties.instance.InstanceProperties;
import sleeper.core.properties.table.TableProperties;

import static org.assertj.core.api.Assertions.assertThat;
import static sleeper.core.properties.instance.CdkDefinedInstanceProperty.DATA_BUCKET;
import static sleeper.core.properties.table.TableProperty.TABLE_ID;
import static sleeper.core.properties.testutils.InstancePropertiesTestHelper.createTestInstanceProperties;
import static sleeper.core.properties.testutils.TablePropertiesTestHelper.createTestTableProperties;
import static sleeper.core.schema.SchemaTestHelper.createSchemaWithKey;

public class TransactionLogSnapshotMetadataTest {
    InstanceProperties instanceProperties = createTestInstanceProperties();
    TableProperties tableProperties = createTestTableProperties(instanceProperties, createSchemaWithKey("key"));

    @BeforeEach
    void setUp() {
        instanceProperties.set(DATA_BUCKET, "test-bucket");
        tableProperties.set(TABLE_ID, "test-table-id");
    }

    @Test
    void shouldCreateBasePathForTable() {
        // When / Then
        assertThat(TransactionLogSnapshotMetadata.getBasePath(instanceProperties, tableProperties))
                .isEqualTo("s3a://test-bucket/test-table-id");
    }

    @Test
    void shouldGetFilesPath() {
        // Given
        String basePath = TransactionLogSnapshotMetadata.getBasePath(instanceProperties, tableProperties);

        // When
        TransactionLogSnapshotMetadata metadata = TransactionLogSnapshotMetadata.forFiles(basePath, 123);

        // Then
        assertThat(metadata.getPath()).isEqualTo(
                "s3a://test-bucket/test-table-id/statestore/snapshots/123-files.arrow");
        assertThat(metadata.getObjectKey()).isEqualTo(
                "test-table-id/statestore/snapshots/123-files.arrow");
    }

    @Test
    void shouldGetPartitionsPath() {
        // Given
        String basePath = TransactionLogSnapshotMetadata.getBasePath(instanceProperties, tableProperties);

        // When
        TransactionLogSnapshotMetadata metadata = TransactionLogSnapshotMetadata.forPartitions(basePath, 123);

        // Then
        assertThat(metadata.getPath()).isEqualTo(
                "s3a://test-bucket/test-table-id/statestore/snapshots/123-partitions.arrow");
        assertThat(metadata.getObjectKey()).isEqualTo(
                "test-table-id/statestore/snapshots/123-partitions.arrow");
    }
}
