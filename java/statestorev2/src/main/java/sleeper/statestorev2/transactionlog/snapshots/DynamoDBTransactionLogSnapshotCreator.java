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

import com.amazonaws.services.dynamodbv2.AmazonDynamoDB;
import com.amazonaws.services.s3.AmazonS3;
import org.apache.hadoop.conf.Configuration;
import org.slf4j.Logger;
import org.slf4j.LoggerFactory;

import sleeper.core.properties.instance.InstanceProperties;
import sleeper.core.properties.table.TableProperties;
import sleeper.core.statestore.transactionlog.log.TransactionBodyStore;
import sleeper.core.statestore.transactionlog.log.TransactionLogStore;
import sleeper.core.statestore.transactionlog.snapshot.TransactionLogSnapshot;
import sleeper.core.statestore.transactionlog.snapshot.TransactionLogSnapshotCreator;
import sleeper.core.statestore.transactionlog.transaction.FileReferenceTransaction;
import sleeper.core.statestore.transactionlog.transaction.PartitionTransaction;
import sleeper.core.statestore.transactionlog.transaction.TransactionSerDeProvider;
import sleeper.core.table.TableStatus;
import sleeper.statestorev2.StateStoreArrowFileStore;
import sleeper.statestorev2.transactionlog.DuplicateSnapshotException;
import sleeper.statestorev2.transactionlog.DynamoDBTransactionLogStore;
import sleeper.statestorev2.transactionlog.S3TransactionBodyStore;
import sleeper.statestorev2.transactionlog.snapshots.DynamoDBTransactionLogSnapshotSaver.SnapshotMetadataSaver;

import java.io.IOException;
import java.util.Optional;

/**
 * Creates a snapshot of the current state of a state store if it has changed since the previous snapshot.
 */
public class DynamoDBTransactionLogSnapshotCreator {
    public static final Logger LOGGER = LoggerFactory.getLogger(DynamoDBTransactionLogSnapshotCreator.class);
    private final TableStatus tableStatus;
    private final TransactionLogStore filesLogStore;
    private final TransactionLogStore partitionsLogStore;
    private final TransactionBodyStore transactionBodyStore;
    private final LatestSnapshotsMetadataLoader latestMetadataLoader;
    private final DynamoDBTransactionLogSnapshotSaver snapshotSaver;
    private final StateStoreArrowFileStore fileStore;

    /**
     * Builds a snapshot creator for a given Sleeper table.
     *
     * @param  instanceProperties the Sleeper instance properties
     * @param  tableProperties    the Sleeper table properties
     * @param  s3Client           the client for interacting with S3
     * @param  dynamoDBClient     the client for interacting with DynamoDB
     * @param  configuration      the Hadoop configuration for interacting with Parquet
     * @return                    the snapshot creator
     */
    public static DynamoDBTransactionLogSnapshotCreator from(
            InstanceProperties instanceProperties, TableProperties tableProperties,
            AmazonS3 s3Client, AmazonDynamoDB dynamoDBClient, Configuration configuration) {
        TransactionLogStore fileTransactionStore = DynamoDBTransactionLogStore.forFiles(
                instanceProperties, tableProperties, dynamoDBClient, s3Client);
        TransactionLogStore partitionTransactionStore = DynamoDBTransactionLogStore.forPartitions(
                instanceProperties, tableProperties, dynamoDBClient, s3Client);
        TransactionBodyStore transactionBodyStore = new S3TransactionBodyStore(instanceProperties, s3Client, TransactionSerDeProvider.forOneTable(tableProperties));
        DynamoDBTransactionLogSnapshotMetadataStore snapshotStore = new DynamoDBTransactionLogSnapshotMetadataStore(
                instanceProperties, tableProperties, dynamoDBClient);
        return new DynamoDBTransactionLogSnapshotCreator(instanceProperties, tableProperties,
                fileTransactionStore, partitionTransactionStore, transactionBodyStore, configuration,
                snapshotStore::getLatestSnapshots, snapshotStore::saveSnapshot);
    }

    public DynamoDBTransactionLogSnapshotCreator(
            InstanceProperties instanceProperties, TableProperties tableProperties,
            TransactionLogStore filesLogStore, TransactionLogStore partitionsLogStore, TransactionBodyStore transactionBodyStore,
            Configuration configuration, LatestSnapshotsMetadataLoader latestMetadataLoader, SnapshotMetadataSaver metadataSaver) {
        this.tableStatus = tableProperties.getStatus();
        this.filesLogStore = filesLogStore;
        this.partitionsLogStore = partitionsLogStore;
        this.transactionBodyStore = transactionBodyStore;
        this.latestMetadataLoader = latestMetadataLoader;
        this.snapshotSaver = new DynamoDBTransactionLogSnapshotSaver(
                latestMetadataLoader, metadataSaver, instanceProperties, tableProperties, configuration);
        this.fileStore = new StateStoreArrowFileStore(tableProperties, configuration);
    }

    /**
     * Creates a snapshot by reading the latest snapshot for the table and writing a new one if necessary.
     */
    public void createSnapshot() {
        LOGGER.info("Creating snapshot for table {}", tableStatus);
        LatestSnapshots latestSnapshots = latestMetadataLoader.load();
        LOGGER.info("Found latest snapshots: {}", latestSnapshots);
        updateFilesSnapshot(latestSnapshots);
        updatePartitionsSnapshot(latestSnapshots);
    }

    private void updateFilesSnapshot(LatestSnapshots latestSnapshots) {
        TransactionLogSnapshot oldSnapshot = latestSnapshots.getFilesSnapshot()
                .map(fileStore::loadSnapshot)
                .orElseGet(TransactionLogSnapshot::filesInitialState);
        try {
            Optional<TransactionLogSnapshot> newSnapshot = TransactionLogSnapshotCreator.createSnapshotIfChanged(
                    oldSnapshot, filesLogStore, transactionBodyStore, FileReferenceTransaction.class, tableStatus);
            if (newSnapshot.isPresent()) {
                snapshotSaver.saveFilesSnapshot(newSnapshot.get());
                LOGGER.info("Saved new files snapshot");
            }
        } catch (DuplicateSnapshotException | IOException e) {
            LOGGER.error("Failed to create files snapshot for table {}", tableStatus);
            throw new RuntimeException(e);
        }
    }

    private void updatePartitionsSnapshot(LatestSnapshots latestSnapshots) {
        TransactionLogSnapshot oldSnapshot = latestSnapshots.getPartitionsSnapshot()
                .map(fileStore::loadSnapshot)
                .orElseGet(TransactionLogSnapshot::partitionsInitialState);
        try {
            Optional<TransactionLogSnapshot> newSnapshot = TransactionLogSnapshotCreator.createSnapshotIfChanged(
                    oldSnapshot, partitionsLogStore, transactionBodyStore, PartitionTransaction.class, tableStatus);
            if (newSnapshot.isPresent()) {
                snapshotSaver.savePartitionsSnapshot(newSnapshot.get());
                LOGGER.info("Saved new partitions snapshot");
            }
        } catch (DuplicateSnapshotException | IOException e) {
            LOGGER.error("Failed to create partitions snapshot for table {}", tableStatus);
            throw new RuntimeException(e);
        }
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
}
