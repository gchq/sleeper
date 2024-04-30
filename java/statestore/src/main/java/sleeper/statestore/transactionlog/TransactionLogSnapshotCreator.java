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
import com.amazonaws.services.s3.AmazonS3;
import org.apache.hadoop.conf.Configuration;
import org.apache.hadoop.fs.FileSystem;
import org.apache.hadoop.fs.Path;
import org.slf4j.Logger;
import org.slf4j.LoggerFactory;

import sleeper.configuration.properties.instance.InstanceProperties;
import sleeper.configuration.properties.table.TableProperties;
import sleeper.configuration.properties.table.TableProperty;
import sleeper.core.statestore.StateStoreException;
import sleeper.core.statestore.transactionlog.StateStoreFiles;
import sleeper.core.statestore.transactionlog.StateStorePartitions;
import sleeper.core.statestore.transactionlog.TransactionLogSnapshotUtils;
import sleeper.core.statestore.transactionlog.TransactionLogStore;
import sleeper.statestore.transactionlog.DynamoDBTransactionLogSnapshotStore.LatestSnapshots;

import java.io.IOException;

import static sleeper.configuration.properties.instance.CdkDefinedInstanceProperty.DATA_BUCKET;
import static sleeper.configuration.properties.instance.CdkDefinedInstanceProperty.TRANSACTION_LOG_FILES_TABLENAME;
import static sleeper.configuration.properties.instance.CdkDefinedInstanceProperty.TRANSACTION_LOG_PARTITIONS_TABLENAME;
import static sleeper.configuration.properties.instance.CommonProperty.FILE_SYSTEM;

public class TransactionLogSnapshotCreator {
    public static final Logger LOGGER = LoggerFactory.getLogger(TransactionLogSnapshotCreator.class);
    private final InstanceProperties instanceProperties;
    private final TableProperties tableProperties;
    private final TransactionLogStore filesLogStore;
    private final TransactionLogStore partitionsLogStore;
    private final Configuration configuration;
    private final TransactionLogSnapshotSerDe snapshotSerDe;
    private final LatestSnapshotsLoader latestSnapshotsLoader;
    private final SnapshotSaver snapshotSaver;

    public static TransactionLogSnapshotCreator from(
            InstanceProperties instanceProperties, TableProperties tableProperties,
            AmazonS3 s3Client, AmazonDynamoDB dynamoDBClient, Configuration configuration) {
        TransactionLogStore fileTransactionStore = new DynamoDBTransactionLogStore(
                instanceProperties.get(TRANSACTION_LOG_FILES_TABLENAME),
                instanceProperties, tableProperties, dynamoDBClient, s3Client);
        TransactionLogStore partitionTransactionStore = new DynamoDBTransactionLogStore(
                instanceProperties.get(TRANSACTION_LOG_PARTITIONS_TABLENAME),
                instanceProperties, tableProperties, dynamoDBClient, s3Client);
        DynamoDBTransactionLogSnapshotStore snapshotStore = new DynamoDBTransactionLogSnapshotStore(
                instanceProperties, tableProperties, dynamoDBClient);
        return new TransactionLogSnapshotCreator(instanceProperties, tableProperties,
                fileTransactionStore, partitionTransactionStore, configuration,
                snapshotStore::getLatestSnapshots, snapshotStore::saveSnapshot);
    }

    public TransactionLogSnapshotCreator(
            InstanceProperties instanceProperties, TableProperties tableProperties,
            TransactionLogStore filesLogStore, TransactionLogStore partitionsLogStore,
            Configuration configuration, LatestSnapshotsLoader latestSnapshotsLoader, SnapshotSaver snapshotSaver) {
        this.instanceProperties = instanceProperties;
        this.tableProperties = tableProperties;
        this.filesLogStore = filesLogStore;
        this.partitionsLogStore = partitionsLogStore;
        this.snapshotSerDe = new TransactionLogSnapshotSerDe(tableProperties.getSchema(), configuration);
        this.latestSnapshotsLoader = latestSnapshotsLoader;
        this.snapshotSaver = snapshotSaver;
        this.configuration = configuration;
    }

    public void createSnapshot() {
        LOGGER.info("Creating snapshot for table {}", tableProperties.getStatus());
        LatestSnapshots latestSnapshots = latestSnapshotsLoader.load();
        LOGGER.info("Found latest snapshots: {}", latestSnapshots);
        StateStoreFiles filesState = latestSnapshots.getFilesSnapshot()
                .map(snapshot -> {
                    try {
                        return snapshotSerDe.loadFiles(snapshot);
                    } catch (IOException e) {
                        return null;
                    }
                })
                .orElseGet(StateStoreFiles::new);
        long filesTransactionNumberBefore = latestSnapshots.getFilesSnapshot()
                .map(TransactionLogSnapshot::getTransactionNumber)
                .orElse(0L);
        StateStorePartitions partitionsState = latestSnapshots.getPartitionsSnapshot()
                .map(snapshot -> {
                    try {
                        return snapshotSerDe.loadPartitions(snapshot);
                    } catch (IOException e) {
                        return null;
                    }
                })
                .orElseGet(StateStorePartitions::new);
        long partitionsTransactionNumberBefore = latestSnapshots.getPartitionsSnapshot()
                .map(TransactionLogSnapshot::getTransactionNumber)
                .orElse(0L);
        try {
            saveFilesSnapshot(filesState, filesTransactionNumberBefore);
            savePartitionsSnapshot(partitionsState, partitionsTransactionNumberBefore);
        } catch (DuplicateSnapshotException | StateStoreException | IOException e) {
            LOGGER.error("Failed to create snapshot for table {}", tableProperties.getStatus());
            throw new RuntimeException(e);
        }
    }

    private void saveFilesSnapshot(StateStoreFiles filesState, long transactionNumberBefore) throws IOException, StateStoreException, DuplicateSnapshotException {
        long transactionNumberAfter = TransactionLogSnapshotUtils.updateFilesState(
                tableProperties.getStatus(), filesState, filesLogStore, transactionNumberBefore);
        if (transactionNumberBefore >= transactionNumberAfter) {
            LOGGER.info("No new file transactions found after transaction number {}, skipping snapshot creation.",
                    transactionNumberBefore);
            return;
        }
        LOGGER.info("Transaction found with transaction number {} is newer than latest files snapshot transaction number {}.",
                transactionNumberAfter, transactionNumberBefore);
        LOGGER.info("Creating a new files snapshot from latest transaction.");
        TransactionLogSnapshot snapshot = TransactionLogSnapshot.forFiles(getBasePath(), transactionNumberAfter);
        snapshotSerDe.saveFiles(snapshot, filesState);
        try {
            snapshotSaver.save(snapshot);
        } catch (DuplicateSnapshotException e) {
            LOGGER.info("Failed to save snapshot to Dynamo DB. Deleting snapshot file.");
            Path path = new Path(snapshot.getPath());
            FileSystem fs = path.getFileSystem(configuration);
            fs.delete(path, false);
            throw e;
        }
    }

    private void savePartitionsSnapshot(StateStorePartitions partitionsState, long transactionNumberBefore) throws IOException, StateStoreException, DuplicateSnapshotException {
        long transactionNumberAfter = TransactionLogSnapshotUtils.updatePartitionsState(
                tableProperties.getStatus(), partitionsState, partitionsLogStore, transactionNumberBefore);
        if (transactionNumberBefore >= transactionNumberAfter) {
            LOGGER.info("No new partition transactions found after transaction number {}, skipping snapshot creation.",
                    transactionNumberBefore);
            return;
        }

        LOGGER.info("Transaction found with transaction number {} is newer than latest partitions snapshot transaction number {}.",
                transactionNumberAfter, transactionNumberBefore);
        LOGGER.info("Creating a new partitions snapshot from latest transaction.");
        TransactionLogSnapshot snapshot = TransactionLogSnapshot.forPartitions(getBasePath(), transactionNumberAfter);
        snapshotSerDe.savePartitions(snapshot, partitionsState);
        try {
            snapshotSaver.save(snapshot);
        } catch (DuplicateSnapshotException e) {
            LOGGER.info("Failed to save snapshot to Dynamo DB. Deleting snapshot file.");
            Path path = new Path(snapshot.getPath());
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

    interface LatestSnapshotsLoader {
        LatestSnapshots load();
    }

    public interface SnapshotSaver {
        void save(TransactionLogSnapshot snapshot) throws DuplicateSnapshotException;
    }
}
