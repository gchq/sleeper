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
import sleeper.core.statestore.transactionlog.TransactionLogSnapshot;
import sleeper.statestore.transactionlog.DynamoDBTransactionLogSnapshotMetadataStore.LatestSnapshots;

import java.io.IOException;
import java.io.UncheckedIOException;
import java.util.Optional;

import static sleeper.configuration.properties.instance.CdkDefinedInstanceProperty.DATA_BUCKET;
import static sleeper.configuration.properties.instance.CommonProperty.FILE_SYSTEM;

/**
 * Stores snapshots derived from a transaction log. Holds an index of snapshots in DynamoDB, and stores snapshot data in
 * S3.
 */
public class DynamoDBTransactionLogSnapshotStore {
    public static final Logger LOGGER = LoggerFactory.getLogger(DynamoDBTransactionLogSnapshotStore.class);

    private final LatestSnapshotsMetadataLoader latestMetadataLoader;
    private final SnapshotMetadataSaver metadataSaver;
    private final TransactionLogSnapshotSerDe snapshotSerDe;
    private final Configuration configuration;
    private final String basePath;

    public DynamoDBTransactionLogSnapshotStore(
            InstanceProperties instanceProperties, TableProperties tableProperties, AmazonDynamoDB dynamo, Configuration configuration) {
        this(new DynamoDBTransactionLogSnapshotMetadataStore(instanceProperties, tableProperties, dynamo),
                instanceProperties, tableProperties, configuration);
    }

    private DynamoDBTransactionLogSnapshotStore(
            DynamoDBTransactionLogSnapshotMetadataStore metadataStore,
            InstanceProperties instanceProperties, TableProperties tableProperties, Configuration configuration) {
        this(metadataStore::getLatestSnapshots, metadataStore::saveSnapshot, instanceProperties, tableProperties, configuration);
    }

    DynamoDBTransactionLogSnapshotStore(
            LatestSnapshotsMetadataLoader latestMetadataLoader, SnapshotMetadataSaver metadataSaver,
            InstanceProperties instanceProperties, TableProperties tableProperties, Configuration configuration) {
        this.latestMetadataLoader = latestMetadataLoader;
        this.metadataSaver = metadataSaver;
        this.snapshotSerDe = new TransactionLogSnapshotSerDe(tableProperties.getSchema(), configuration);
        this.configuration = configuration;
        this.basePath = getBasePath(instanceProperties, tableProperties);
    }

    /**
     * Loads the latest snapshot of files if it meets a minimum transaction number. Used by the state store to implement
     * {@link sleeper.core.statestore.transactionlog.TransactionLogSnapshotLoader}.
     *
     * @param  transactionNumber the minimum transaction number to load snapshot data from S3
     * @return                   the latest snapshot if there is one that meets the minimum transaction number
     */
    public Optional<TransactionLogSnapshot> loadLatestFilesSnapshotIfAtMinimumTransaction(long transactionNumber) {
        return latestMetadataLoader.load().getFilesSnapshot()
                .filter(metadata -> metadata.getTransactionNumber() >= transactionNumber)
                .map(this::loadFilesSnapshot);
    }

    /**
     * Loads the latest snapshot of partitions if it meets a minimum transaction number. Used by the state store to
     * implement {@link sleeper.core.statestore.transactionlog.TransactionLogSnapshotLoader}.
     *
     * @param  transactionNumber the minimum transaction number to load snapshot data from S3
     * @return                   the latest snapshot if there is one that meets the minimum transaction number
     */
    public Optional<TransactionLogSnapshot> loadLatestPartitionsSnapshotIfAtMinimumTransaction(long transactionNumber) {
        return latestMetadataLoader.load().getPartitionsSnapshot()
                .filter(metadata -> metadata.getTransactionNumber() >= transactionNumber)
                .map(this::loadPartitionsSnapshot);
    }

    /**
     * Loads the snapshot of files based on metadata for the latest snapshot held in the index.
     *
     * @param  snapshots the metadata
     * @return           the snapshot, or the initial state if there was no snapshot in the index
     */
    public TransactionLogSnapshot loadFilesSnapshot(LatestSnapshots snapshots) {
        return snapshots.getFilesSnapshot()
                .map(this::loadFilesSnapshot)
                .orElseGet(TransactionLogSnapshot::filesInitialState);
    }

    /**
     * Loads the snapshot of partitions based on metadata for the latest snapshot held in the index.
     *
     * @param  snapshots the metadata
     * @return           the snapshot, or the initial state if there was no snapshot in the index
     */
    public TransactionLogSnapshot loadPartitionsSnapshot(LatestSnapshots snapshots) {
        return snapshots.getPartitionsSnapshot()
                .map(this::loadPartitionsSnapshot)
                .orElseGet(TransactionLogSnapshot::partitionsInitialState);
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

    /**
     * Saves a snapshot of files to S3 and to the index. Deletes the file if the metadata fails to save in the index.
     *
     * @param  snapshot                   the snapshot
     * @throws IOException                if the snapshot fails to save to S3
     * @throws DuplicateSnapshotException if there is already a snapshot for the given transaction number
     */
    public void saveFilesSnapshot(TransactionLogSnapshot snapshot) throws IOException, DuplicateSnapshotException {
        TransactionLogSnapshotMetadata snapshotMetadata = TransactionLogSnapshotMetadata.forFiles(
                basePath, snapshot.getTransactionNumber());

        snapshotSerDe.saveFiles(snapshotMetadata, snapshot.getState());
        try {
            metadataSaver.save(snapshotMetadata);
        } catch (DuplicateSnapshotException | RuntimeException e) {
            LOGGER.info("Failed to save snapshot to Dynamo DB. Deleting snapshot file.");
            Path path = new Path(snapshotMetadata.getPath());
            FileSystem fs = path.getFileSystem(configuration);
            fs.delete(path, false);
            throw e;
        }
    }

    /**
     * Saves a snapshot of partitions to S3 and to the index. Deletes the file if the metadata fails to save in the
     * index.
     *
     * @param  snapshot                   the snapshot
     * @throws IOException                if the snapshot fails to save to S3
     * @throws DuplicateSnapshotException if there is already a snapshot for the given transaction number
     */
    public void savePartitionsSnapshot(TransactionLogSnapshot snapshot) throws IOException, DuplicateSnapshotException {
        TransactionLogSnapshotMetadata snapshotMetadata = TransactionLogSnapshotMetadata.forPartitions(
                basePath, snapshot.getTransactionNumber());
        snapshotSerDe.savePartitions(snapshotMetadata, snapshot.getState());
        try {
            metadataSaver.save(snapshotMetadata);
        } catch (DuplicateSnapshotException | RuntimeException e) {
            LOGGER.info("Failed to save snapshot to Dynamo DB. Deleting snapshot file.");
            Path path = new Path(snapshotMetadata.getPath());
            FileSystem fs = path.getFileSystem(configuration);
            fs.delete(path, false);
            throw e;
        }
    }

    /**
     * Constructs the base path to a table data bucket.
     *
     * @param  instanceProperties the instance properties
     * @param  tableProperties    the table properties
     * @return                    the full path to the table data bucket (including the file system)
     */
    public static String getBasePath(InstanceProperties instanceProperties, TableProperties tableProperties) {
        return instanceProperties.get(FILE_SYSTEM)
                + instanceProperties.get(DATA_BUCKET) + "/"
                + tableProperties.get(TableProperty.TABLE_ID);
    }

    /**
     * Loads the metadata of the latest snapshots from the index.
     */
    public interface LatestSnapshotsMetadataLoader {

        /**
         * Loads the latest snapshots metadata.
         *
         * @return the metadata
         */
        LatestSnapshots load();
    }

    /**
     * Saves the metadata of a snapshot in the index.
     */
    public interface SnapshotMetadataSaver {

        /**
         * Saves the snapshot metadata.
         *
         * @param  snapshot                   the metadata
         * @throws DuplicateSnapshotException if there is already a snapshot for the given transaction number
         */
        void save(TransactionLogSnapshotMetadata snapshot) throws DuplicateSnapshotException;
    }
}
