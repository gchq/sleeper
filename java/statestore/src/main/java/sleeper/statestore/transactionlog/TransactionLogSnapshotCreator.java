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
import org.slf4j.Logger;
import org.slf4j.LoggerFactory;

import sleeper.configuration.properties.instance.InstanceProperties;
import sleeper.configuration.properties.table.TableProperties;
import sleeper.core.statestore.StateStoreException;
import sleeper.core.statestore.transactionlog.StateStoreFiles;
import sleeper.core.statestore.transactionlog.StateStorePartitions;
import sleeper.core.statestore.transactionlog.TransactionLogSnapshot;
import sleeper.core.statestore.transactionlog.TransactionLogSnapshotUtils;
import sleeper.core.statestore.transactionlog.TransactionLogStore;
import sleeper.core.table.TableStatus;
import sleeper.statestore.transactionlog.DynamoDBTransactionLogSnapshotMetadataStore.LatestSnapshots;
import sleeper.statestore.transactionlog.DynamoDBTransactionLogSnapshotStore.LatestSnapshotsMetadataLoader;
import sleeper.statestore.transactionlog.DynamoDBTransactionLogSnapshotStore.SnapshotMetadataSaver;

import java.io.IOException;
import java.io.UncheckedIOException;

import static sleeper.configuration.properties.instance.CdkDefinedInstanceProperty.TRANSACTION_LOG_FILES_TABLENAME;
import static sleeper.configuration.properties.instance.CdkDefinedInstanceProperty.TRANSACTION_LOG_PARTITIONS_TABLENAME;

public class TransactionLogSnapshotCreator {
    public static final Logger LOGGER = LoggerFactory.getLogger(TransactionLogSnapshotCreator.class);
    private final TableStatus tableStatus;
    private final TransactionLogStore filesLogStore;
    private final TransactionLogStore partitionsLogStore;
    private final TransactionLogSnapshotSerDe snapshotSerDe;
    private final LatestSnapshotsMetadataLoader latestMetadataLoader;
    private final DynamoDBTransactionLogSnapshotStore snapshotStore;

    public static TransactionLogSnapshotCreator from(
            InstanceProperties instanceProperties, TableProperties tableProperties,
            AmazonS3 s3Client, AmazonDynamoDB dynamoDBClient, Configuration configuration) {
        TransactionLogStore fileTransactionStore = new DynamoDBTransactionLogStore(
                instanceProperties.get(TRANSACTION_LOG_FILES_TABLENAME),
                instanceProperties, tableProperties, dynamoDBClient, s3Client);
        TransactionLogStore partitionTransactionStore = new DynamoDBTransactionLogStore(
                instanceProperties.get(TRANSACTION_LOG_PARTITIONS_TABLENAME),
                instanceProperties, tableProperties, dynamoDBClient, s3Client);
        DynamoDBTransactionLogSnapshotMetadataStore snapshotStore = new DynamoDBTransactionLogSnapshotMetadataStore(
                instanceProperties, tableProperties, dynamoDBClient);
        return new TransactionLogSnapshotCreator(instanceProperties, tableProperties,
                fileTransactionStore, partitionTransactionStore, configuration,
                snapshotStore::getLatestSnapshots, snapshotStore::saveSnapshot);
    }

    public TransactionLogSnapshotCreator(
            InstanceProperties instanceProperties, TableProperties tableProperties,
            TransactionLogStore filesLogStore, TransactionLogStore partitionsLogStore,
            Configuration configuration, LatestSnapshotsMetadataLoader latestMetadataLoader, SnapshotMetadataSaver snapshotSaver) {
        this.tableStatus = tableProperties.getStatus();
        this.filesLogStore = filesLogStore;
        this.partitionsLogStore = partitionsLogStore;
        this.snapshotSerDe = new TransactionLogSnapshotSerDe(tableProperties.getSchema(), configuration);
        this.latestMetadataLoader = latestMetadataLoader;
        this.snapshotStore = new DynamoDBTransactionLogSnapshotStore(
                latestMetadataLoader, snapshotSaver, snapshotSerDe,
                instanceProperties, tableProperties, configuration);
    }

    public void createSnapshot() {
        LOGGER.info("Creating snapshot for table {}", tableStatus);
        LatestSnapshots latestSnapshots = latestMetadataLoader.load();
        LOGGER.info("Found latest snapshots: {}", latestSnapshots);
        StateStoreFiles filesState = latestSnapshots.getFilesSnapshot()
                .map(snapshot -> {
                    try {
                        return snapshotSerDe.loadFiles(snapshot);
                    } catch (IOException e) {
                        throw new UncheckedIOException(e);
                    }
                })
                .orElseGet(StateStoreFiles::new);
        long filesTransactionNumberBefore = latestSnapshots.getFilesSnapshot()
                .map(TransactionLogSnapshotMetadata::getTransactionNumber)
                .orElse(0L);
        StateStorePartitions partitionsState = latestSnapshots.getPartitionsSnapshot()
                .map(snapshot -> {
                    try {
                        return snapshotSerDe.loadPartitions(snapshot);
                    } catch (IOException e) {
                        throw new UncheckedIOException(e);
                    }
                })
                .orElseGet(StateStorePartitions::new);
        long partitionsTransactionNumberBefore = latestSnapshots.getPartitionsSnapshot()
                .map(TransactionLogSnapshotMetadata::getTransactionNumber)
                .orElse(0L);
        try {
            saveFilesSnapshot(filesState, filesTransactionNumberBefore);
            savePartitionsSnapshot(partitionsState, partitionsTransactionNumberBefore);
        } catch (DuplicateSnapshotException | StateStoreException | IOException e) {
            LOGGER.error("Failed to create snapshot for table {}", tableStatus);
            throw new RuntimeException(e);
        }
    }

    private void saveFilesSnapshot(StateStoreFiles filesState, long transactionNumberBefore) throws IOException, StateStoreException, DuplicateSnapshotException {
        long transactionNumberAfter = TransactionLogSnapshotUtils.updateFilesState(
                tableStatus, filesState, filesLogStore, transactionNumberBefore);
        if (transactionNumberBefore >= transactionNumberAfter) {
            LOGGER.info("No new file transactions found after transaction number {}, skipping snapshot creation.",
                    transactionNumberBefore);
            return;
        }
        LOGGER.info("Transaction found with transaction number {} is newer than latest files snapshot transaction number {}.",
                transactionNumberAfter, transactionNumberBefore);
        LOGGER.info("Creating a new files snapshot from latest transaction.");
        TransactionLogSnapshot snapshot = new TransactionLogSnapshot(filesState, transactionNumberAfter);
        snapshotStore.saveFilesSnapshot(snapshot);
    }

    private void savePartitionsSnapshot(StateStorePartitions partitionsState, long transactionNumberBefore) throws IOException, StateStoreException, DuplicateSnapshotException {
        long transactionNumberAfter = TransactionLogSnapshotUtils.updatePartitionsState(
                tableStatus, partitionsState, partitionsLogStore, transactionNumberBefore);
        if (transactionNumberBefore >= transactionNumberAfter) {
            LOGGER.info("No new partition transactions found after transaction number {}, skipping snapshot creation.",
                    transactionNumberBefore);
            return;
        }

        LOGGER.info("Transaction found with transaction number {} is newer than latest partitions snapshot transaction number {}.",
                transactionNumberAfter, transactionNumberBefore);
        LOGGER.info("Creating a new partitions snapshot from latest transaction.");
        TransactionLogSnapshot snapshot = new TransactionLogSnapshot(partitionsState, transactionNumberAfter);
        snapshotStore.savePartitionsSnapshot(snapshot);
    }
}
