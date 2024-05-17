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
import sleeper.core.statestore.transactionlog.FileReferenceTransaction;
import sleeper.core.statestore.transactionlog.PartitionTransaction;
import sleeper.core.statestore.transactionlog.StateStoreTransaction;
import sleeper.core.statestore.transactionlog.TransactionLogSnapshot;
import sleeper.core.statestore.transactionlog.TransactionLogSnapshotUtils;
import sleeper.core.statestore.transactionlog.TransactionLogStore;
import sleeper.core.table.TableStatus;
import sleeper.statestore.transactionlog.DynamoDBTransactionLogSnapshotMetadataStore.LatestSnapshots;
import sleeper.statestore.transactionlog.DynamoDBTransactionLogSnapshotStore.LatestSnapshotsMetadataLoader;
import sleeper.statestore.transactionlog.DynamoDBTransactionLogSnapshotStore.SnapshotMetadataSaver;

import java.io.IOException;
import java.util.Optional;

import static sleeper.configuration.properties.instance.CdkDefinedInstanceProperty.TRANSACTION_LOG_FILES_TABLENAME;
import static sleeper.configuration.properties.instance.CdkDefinedInstanceProperty.TRANSACTION_LOG_PARTITIONS_TABLENAME;

public class TransactionLogSnapshotCreator {
    public static final Logger LOGGER = LoggerFactory.getLogger(TransactionLogSnapshotCreator.class);
    private final TableStatus tableStatus;
    private final TransactionLogStore filesLogStore;
    private final TransactionLogStore partitionsLogStore;
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
        TransactionLogSnapshotSerDe snapshotSerDe = new TransactionLogSnapshotSerDe(tableProperties.getSchema(), configuration);
        this.latestMetadataLoader = latestMetadataLoader;
        this.snapshotStore = new DynamoDBTransactionLogSnapshotStore(
                latestMetadataLoader, snapshotSaver, snapshotSerDe,
                instanceProperties, tableProperties, configuration);
    }

    public void createSnapshot() {
        LOGGER.info("Creating snapshot for table {}", tableStatus);
        LatestSnapshots latestSnapshots = latestMetadataLoader.load();
        LOGGER.info("Found latest snapshots: {}", latestSnapshots);
        TransactionLogSnapshot filesSnapshot = snapshotStore.loadFilesSnapshot(latestSnapshots);
        TransactionLogSnapshot partitionsSnapshot = snapshotStore.loadPartitionsSnapshot(latestSnapshots);
        try {
            Optional<TransactionLogSnapshot> createForFiles = createSnapshot(filesSnapshot, filesLogStore, FileReferenceTransaction.class);
            Optional<TransactionLogSnapshot> createForPartitions = createSnapshot(partitionsSnapshot, partitionsLogStore, PartitionTransaction.class);
            if (createForFiles.isPresent()) {
                snapshotStore.saveFilesSnapshot(createForFiles.get());
            }
            if (createForPartitions.isPresent()) {
                snapshotStore.savePartitionsSnapshot(createForPartitions.get());
            }
        } catch (DuplicateSnapshotException | StateStoreException | IOException e) {
            LOGGER.error("Failed to create snapshot for table {}", tableStatus);
            throw new RuntimeException(e);
        }
    }

    private <T> Optional<TransactionLogSnapshot> createSnapshot(
            TransactionLogSnapshot lastSnapshot,
            TransactionLogStore logStore,
            Class<? extends StateStoreTransaction<T>> transactionType) throws StateStoreException {
        TransactionLogSnapshot newSnapshot = TransactionLogSnapshotUtils.updateState(
                lastSnapshot, transactionType, logStore, tableStatus);
        if (lastSnapshot.getTransactionNumber() >= newSnapshot.getTransactionNumber()) {
            LOGGER.info("No new {}s found after transaction number {}, skipping snapshot creation.",
                    transactionType.getSimpleName(),
                    lastSnapshot.getTransactionNumber());
            return Optional.empty();
        }
        LOGGER.info("Transaction found with number {} is newer than latest {} number {}.",
                newSnapshot.getTransactionNumber(), transactionType.getSimpleName(), lastSnapshot.getTransactionNumber());
        LOGGER.info("Creating a new snapshot from latest {}.", transactionType.getSimpleName());
        return Optional.of(newSnapshot);
    }
}
