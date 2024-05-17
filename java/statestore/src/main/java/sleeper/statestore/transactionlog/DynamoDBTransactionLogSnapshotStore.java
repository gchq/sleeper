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

import com.amazonaws.services.dynamodbv2.AmazonDynamoDB;
import org.apache.hadoop.conf.Configuration;
import org.apache.hadoop.fs.FileSystem;
import org.apache.hadoop.fs.Path;
import org.slf4j.Logger;
import org.slf4j.LoggerFactory;

import sleeper.configuration.properties.instance.InstanceProperties;
import sleeper.configuration.properties.table.TableProperties;
import sleeper.configuration.properties.table.TableProperty;
import sleeper.core.statestore.transactionlog.StateStoreFiles;
import sleeper.core.statestore.transactionlog.StateStorePartitions;
import sleeper.core.statestore.transactionlog.TransactionLogSnapshot;
import sleeper.statestore.transactionlog.DynamoDBTransactionLogSnapshotMetadataStore.LatestSnapshots;

import java.io.IOException;
import java.io.UncheckedIOException;
import java.util.Optional;

import static sleeper.configuration.properties.instance.CdkDefinedInstanceProperty.DATA_BUCKET;
import static sleeper.configuration.properties.instance.CommonProperty.FILE_SYSTEM;

public class DynamoDBTransactionLogSnapshotStore {
    public static final Logger LOGGER = LoggerFactory.getLogger(DynamoDBTransactionLogSnapshotStore.class);

    private final LatestSnapshotsMetadataLoader latestMetadataLoader;
    private final SnapshotMetadataSaver metadataSaver;
    private final TransactionLogSnapshotSerDe snapshotSerDe;
    private final InstanceProperties instanceProperties;
    private final TableProperties tableProperties;
    private final Configuration configuration;

    public DynamoDBTransactionLogSnapshotStore(
            InstanceProperties instanceProperties, TableProperties tableProperties, AmazonDynamoDB dynamo, Configuration configuration) {
        this(new DynamoDBTransactionLogSnapshotMetadataStore(instanceProperties, tableProperties, dynamo),
                new TransactionLogSnapshotSerDe(tableProperties.getSchema(), configuration),
                instanceProperties, tableProperties, configuration);
    }

    private DynamoDBTransactionLogSnapshotStore(
            DynamoDBTransactionLogSnapshotMetadataStore metadataStore, TransactionLogSnapshotSerDe snapshotSerDe,
            InstanceProperties instanceProperties, TableProperties tableProperties, Configuration configuration) {
        this(metadataStore::getLatestSnapshots, metadataStore::saveSnapshot, snapshotSerDe, instanceProperties, tableProperties, configuration);
    }

    DynamoDBTransactionLogSnapshotStore(
            LatestSnapshotsMetadataLoader latestMetadataLoader, SnapshotMetadataSaver metadataSaver,
            TransactionLogSnapshotSerDe snapshotSerDe,
            InstanceProperties instanceProperties, TableProperties tableProperties, Configuration configuration) {
        this.latestMetadataLoader = latestMetadataLoader;
        this.metadataSaver = metadataSaver;
        this.snapshotSerDe = snapshotSerDe;
        this.instanceProperties = instanceProperties;
        this.tableProperties = tableProperties;
        this.configuration = configuration;
    }

    public Optional<TransactionLogSnapshot> loadLatestFilesSnapshotIfAtMinimumTransaction(long transactionNumber) {
        return latestMetadataLoader.load().getFilesSnapshot()
                .filter(metadata -> metadata.getTransactionNumber() >= transactionNumber)
                .map(this::loadFilesSnapshot);
    }

    public Optional<TransactionLogSnapshot> loadLatestPartitionsSnapshotIfAtMinimumTransaction(long transactionNumber) {
        return latestMetadataLoader.load().getPartitionsSnapshot()
                .filter(metadata -> metadata.getTransactionNumber() >= transactionNumber)
                .map(this::loadPartitionsSnapshot);
    }

    public TransactionLogSnapshot loadFilesSnapshot(LatestSnapshots snapshots) {
        return snapshots.getFilesSnapshot()
                .map(this::loadFilesSnapshot)
                .orElseGet(() -> new TransactionLogSnapshot(new StateStoreFiles(), 0));
    }

    public TransactionLogSnapshot loadPartitionsSnapshot(LatestSnapshots snapshots) {
        return snapshots.getPartitionsSnapshot()
                .map(this::loadPartitionsSnapshot)
                .orElseGet(() -> new TransactionLogSnapshot(new StateStorePartitions(), 0));
    }

    private TransactionLogSnapshot loadFilesSnapshot(TransactionLogSnapshotMetadata metadata) {
        try {
            return new TransactionLogSnapshot(snapshotSerDe.loadFiles(metadata), metadata.getTransactionNumber());
        } catch (IOException e) {
            throw new UncheckedIOException(e);
        }
    }

    private TransactionLogSnapshot loadPartitionsSnapshot(TransactionLogSnapshotMetadata metadata) {
        try {
            return new TransactionLogSnapshot(snapshotSerDe.loadPartitions(metadata), metadata.getTransactionNumber());
        } catch (IOException e) {
            throw new UncheckedIOException(e);
        }
    }

    public void saveFilesSnapshot(TransactionLogSnapshot snapshot) throws IOException, DuplicateSnapshotException {
        TransactionLogSnapshotMetadata snapshotMetadata = TransactionLogSnapshotMetadata.forFiles(
                getBasePath(), snapshot.getTransactionNumber());

        snapshotSerDe.saveFiles(snapshotMetadata, snapshot.getState());
        try {
            metadataSaver.save(snapshotMetadata);
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
            metadataSaver.save(snapshotMetadata);
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

    public interface LatestSnapshotsMetadataLoader {
        LatestSnapshots load();
    }

    public interface SnapshotMetadataSaver {
        void save(TransactionLogSnapshotMetadata snapshot) throws DuplicateSnapshotException;
    }
}
