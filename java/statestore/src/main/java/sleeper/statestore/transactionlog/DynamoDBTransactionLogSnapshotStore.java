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
package sleeper.statestore.transactionlog;

import org.apache.hadoop.conf.Configuration;
import org.apache.hadoop.fs.FileSystem;
import org.apache.hadoop.fs.Path;
import org.slf4j.Logger;
import org.slf4j.LoggerFactory;

import sleeper.configuration.properties.instance.InstanceProperties;
import sleeper.configuration.properties.table.TableProperties;
import sleeper.configuration.properties.table.TableProperty;
import sleeper.core.statestore.transactionlog.TransactionLogSnapshot;
import sleeper.core.statestore.transactionlog.TransactionLogSnapshotLoader;

import java.io.IOException;
import java.util.Optional;

import static sleeper.configuration.properties.instance.CdkDefinedInstanceProperty.DATA_BUCKET;
import static sleeper.configuration.properties.instance.CommonProperty.FILE_SYSTEM;

public class DynamoDBTransactionLogSnapshotStore implements TransactionLogSnapshotLoader {
    public static final Logger LOGGER = LoggerFactory.getLogger(DynamoDBTransactionLogSnapshotStore.class);

    private final SnapshotSaver metadataStore;
    private final TransactionLogSnapshotSerDe snapshotSerDe;
    private final InstanceProperties instanceProperties;
    private final TableProperties tableProperties;
    private final Configuration configuration;

    public DynamoDBTransactionLogSnapshotStore(
            DynamoDBTransactionLogSnapshotMetadataStore metadataStore, TransactionLogSnapshotSerDe snapshotSerDe,
            InstanceProperties instanceProperties, TableProperties tableProperties, Configuration configuration) {
        this(metadataStore::saveSnapshot, snapshotSerDe, instanceProperties, tableProperties, configuration);
    }

    public DynamoDBTransactionLogSnapshotStore(
            SnapshotSaver metadataStore, TransactionLogSnapshotSerDe snapshotSerDe,
            InstanceProperties instanceProperties, TableProperties tableProperties, Configuration configuration) {
        this.metadataStore = metadataStore;
        this.snapshotSerDe = snapshotSerDe;
        this.instanceProperties = instanceProperties;
        this.tableProperties = tableProperties;
        this.configuration = configuration;
    }

    @Override
    public Optional<TransactionLogSnapshot> loadLatestSnapshotIfAtMinimumTransaction(long transactionNumber) {
        //metadataStore.getLatestSnapshots();
        // TODO Auto-generated method stub
        throw new UnsupportedOperationException("Unimplemented method 'loadLatestSnapshotIfAtMinimumTransaction'");
    }

    public void saveFilesSnapshot(TransactionLogSnapshot snapshot) throws IOException, DuplicateSnapshotException {
        TransactionLogSnapshotMetadata snapshotMetadata = TransactionLogSnapshotMetadata.forFiles(
                getBasePath(), snapshot.getTransactionNumber());

        snapshotSerDe.saveFiles(snapshotMetadata, snapshot.getState());
        try {
            metadataStore.save(snapshotMetadata);
        } catch (Exception e) {
            LOGGER.info("Failed to save snapshot to Dynamo DB. Deleting snapshot file.");
            Path path = new Path(snapshotMetadata.getPath());
            FileSystem fs = path.getFileSystem(configuration);
            fs.delete(path, false);
            throw e;
        }
    }

    public void savePartitionsSnapshot(TransactionLogSnapshot snapshot) throws IOException, DuplicateSnapshotException {
        TransactionLogSnapshotMetadata snapshotMetadata = TransactionLogSnapshotMetadata.forPartitions(
                getBasePath(), snapshot.getTransactionNumber());
        snapshotSerDe.savePartitions(snapshotMetadata, snapshot.getState());
        try {
            metadataStore.save(snapshotMetadata);
        } catch (Exception e) {
            LOGGER.info("Failed to save snapshot to Dynamo DB. Deleting snapshot file.");
            Path path = new Path(snapshotMetadata.getPath());
            FileSystem fs = path.getFileSystem(configuration);
            fs.delete(path, false);
            throw e;
        }
    }

    private String getBasePath() {
        return instanceProperties.get(FILE_SYSTEM)
                + instanceProperties.get(DATA_BUCKET) + "/"
                + tableProperties.get(TableProperty.TABLE_ID);
    }

    public interface SnapshotSaver {
        void save(TransactionLogSnapshotMetadata snapshot) throws DuplicateSnapshotException;
    }
}
