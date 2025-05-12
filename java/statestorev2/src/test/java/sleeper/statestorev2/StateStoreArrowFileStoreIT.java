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
package sleeper.statestorev2;

import org.junit.jupiter.api.BeforeEach;
import org.junit.jupiter.api.Test;

import sleeper.core.partition.Partition;
import sleeper.core.partition.PartitionsBuilder;
import sleeper.core.properties.instance.InstanceProperties;
import sleeper.core.properties.table.TableProperties;
import sleeper.core.schema.type.StringType;
import sleeper.core.statestore.FileReference;
import sleeper.core.statestore.FileReferenceFactory;
import sleeper.core.statestore.transactionlog.snapshot.TransactionLogSnapshot;
import sleeper.core.statestore.transactionlog.state.StateStoreFile;
import sleeper.core.statestore.transactionlog.state.StateStoreFiles;
import sleeper.core.statestore.transactionlog.state.StateStorePartitions;
import sleeper.statestorev2.transactionlog.snapshots.TransactionLogSnapshotMetadata;
import sleeper.statestorev2.transactionlog.snapshots.TransactionLogSnapshotTestBase;

import java.time.Instant;
import java.util.List;

import static org.assertj.core.api.Assertions.assertThat;
import static sleeper.core.properties.instance.CdkDefinedInstanceProperty.DATA_BUCKET;
import static sleeper.core.properties.testutils.InstancePropertiesTestHelper.createTestInstanceProperties;
import static sleeper.core.properties.testutils.TablePropertiesTestHelper.createTestTableProperties;
import static sleeper.core.schema.SchemaTestHelper.createSchemaWithKey;
import static sleeper.core.statestore.AllReferencesToAFileTestHelper.fileWithOneReference;

public class StateStoreArrowFileStoreIT extends TransactionLogSnapshotTestBase {

    InstanceProperties instanceProperties = createTestInstanceProperties();
    TableProperties tableProperties = createTestTableProperties(instanceProperties, createSchemaWithKey("key", new StringType()));
    String basePath = TransactionLogSnapshotMetadata.getBasePath(instanceProperties, tableProperties);

    @BeforeEach
    void setUp() {
        createBucket(instanceProperties.get(DATA_BUCKET));
    }

    @Test
    void shouldWritePartitionsToS3() throws Exception {
        // Given
        List<Partition> partitions = new PartitionsBuilder(tableProperties)
                .rootFirst("root")
                .splitToNewChildren("root", "L", "R", "aaa")
                .buildList();
        TransactionLogSnapshotMetadata metadata = TransactionLogSnapshotMetadata.forPartitions(basePath, 1);

        // When
        store().savePartitions(metadata.getObjectKey(), partitions);

        // Then
        assertThat(store().loadSnapshot(metadata)).isEqualTo(
                new TransactionLogSnapshot(StateStorePartitions.from(partitions), 1));
    }

    @Test
    void shouldWriteFileReferencesToS3() throws Exception {
        // Given
        FileReferenceFactory fileFactory = FileReferenceFactory.forSinglePartition("test-partition", tableProperties);
        FileReference fileRef = fileFactory.rootFile("test-file", 10);
        Instant updateTime = Instant.parse("2025-05-06T13:36:00Z");

        StateStoreFiles files = new StateStoreFiles();
        files.add(StateStoreFile.from(fileWithOneReference(fileRef, updateTime)));
        TransactionLogSnapshotMetadata metadata = TransactionLogSnapshotMetadata.forFiles(basePath, 1);

        // When
        store().saveFiles(metadata.getObjectKey(), files);

        // Then
        assertThat(store().loadSnapshot(metadata)).isEqualTo(new TransactionLogSnapshot(files, 1));
    }

    @Test
    void shouldFindEmptyFile() throws Exception {
        // Given
        StateStoreFiles files = new StateStoreFiles();

        // When
        store().saveFiles("test/file-references.arrow", files);

        // Then
        assertThat(store().isEmpty("test/file-references.arrow")).isTrue();
    }

    @Test
    void shouldDeleteObjectWhenRequested() throws Exception {
        // Given
        FileReferenceFactory fileFactory = FileReferenceFactory.forSinglePartition("test-partition", tableProperties);
        FileReference fileRef = fileFactory.rootFile("test-file", 10);
        Instant updateTime = Instant.parse("2025-05-06T13:36:00Z");

        StateStoreFiles files = new StateStoreFiles();
        files.add(StateStoreFile.from(fileWithOneReference(fileRef, updateTime)));
        TransactionLogSnapshotMetadata metadata = TransactionLogSnapshotMetadata.forFiles(basePath, 1);
        store().saveFiles(metadata.getObjectKey(), files);

        // When
        store().deleteSnapshotFile(metadata);

        // Then
        assertThat(filesInDataBucket()).isEmpty();
    }

    private StateStoreArrowFileStore store() {
        return new StateStoreArrowFileStore(instanceProperties, tableProperties, s3ClientV2, s3TransferManager);
    }

}
