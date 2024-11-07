/*
 * Copyright 2022-2024 Crown Copyright
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
package sleeper.statestore.transactionlog.snapshots;

import org.apache.hadoop.conf.Configuration;
import org.junit.jupiter.api.Test;
import org.junit.jupiter.api.io.TempDir;

import sleeper.core.partition.PartitionTree;
import sleeper.core.partition.PartitionsBuilder;
import sleeper.core.schema.Schema;
import sleeper.core.schema.type.StringType;
import sleeper.core.statestore.AllReferencesToAFile;
import sleeper.core.statestore.FileReferenceFactory;
import sleeper.core.statestore.transactionlog.StateStoreFiles;
import sleeper.core.statestore.transactionlog.StateStorePartitions;

import java.nio.file.Path;

import static org.assertj.core.api.Assertions.assertThat;
import static sleeper.core.schema.SchemaTestHelper.schemaWithKey;
import static sleeper.core.statestore.AllReferencesToAFile.fileWithOneReference;
import static sleeper.core.statestore.FileReferenceTestData.DEFAULT_UPDATE_TIME;

public class TransactionLogSnapshotSerDeIT {
    @TempDir
    private Path tempDir;
    private final Schema schema = schemaWithKey("key", new StringType());
    private final PartitionsBuilder partitions = new PartitionsBuilder(schema).singlePartition("root");
    private final Configuration configuration = new Configuration();
    private final TransactionLogSnapshotSerDe snapshotSerDe = new TransactionLogSnapshotSerDe(schema, configuration);

    @Test
    void shouldSaveAndLoadPartitionsState() throws Exception {
        // Given
        PartitionTree splitTree = partitions.splitToNewChildren("root", "L", "R", "l").buildTree();
        StateStorePartitions state = new StateStorePartitions();
        splitTree.getAllPartitions().forEach(state::put);

        // When
        snapshotSerDe.savePartitions(partitionsSnapshot(1), state);

        // Then
        assertThat(snapshotSerDe.loadPartitions(partitionsSnapshot(1))).isEqualTo(state);
    }

    @Test
    void shouldSaveAndLoadFilesState() throws Exception {
        // Given
        AllReferencesToAFile file = fileWithOneReference(fileFactory().rootFile(123L), DEFAULT_UPDATE_TIME);
        StateStoreFiles state = new StateStoreFiles();
        state.add(file);

        // When
        snapshotSerDe.saveFiles(filesSnapshot(1), state);

        // Then
        assertThat(snapshotSerDe.loadFiles(filesSnapshot(1))).isEqualTo(state);
    }

    private FileReferenceFactory fileFactory() {
        return FileReferenceFactory.fromUpdatedAt(partitions.buildTree(), DEFAULT_UPDATE_TIME);
    }

    private TransactionLogSnapshotMetadata filesSnapshot(long transactionNumber) {
        return TransactionLogSnapshotMetadata.forFiles(tempDir.toString(), transactionNumber);
    }

    private TransactionLogSnapshotMetadata partitionsSnapshot(long transactionNumber) {
        return TransactionLogSnapshotMetadata.forPartitions(tempDir.toString(), transactionNumber);
    }
}
