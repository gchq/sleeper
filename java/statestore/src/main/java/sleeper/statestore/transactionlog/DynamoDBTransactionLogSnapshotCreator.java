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
import sleeper.core.statestore.transactionlog.TransactionLogSnapshot;
import sleeper.core.statestore.transactionlog.TransactionLogSnapshotCreator;
import sleeper.core.statestore.transactionlog.TransactionLogStore;
import sleeper.core.table.TableStatus;
import sleeper.statestore.transactionlog.DynamoDBTransactionLogSnapshotStore.LatestSnapshotsMetadataLoader;
import sleeper.statestore.transactionlog.DynamoDBTransactionLogSnapshotStore.SnapshotMetadataSaver;

import java.io.IOException;
import java.util.Optional;

import static sleeper.configuration.properties.instance.CdkDefinedInstanceProperty.TRANSACTION_LOG_FILES_TABLENAME;
import static sleeper.configuration.properties.instance.CdkDefinedInstanceProperty.TRANSACTION_LOG_PARTITIONS_TABLENAME;

/**
 * Creates a snapshot of the current state of a state store if it has changed since the previous snapshot.
 */
public class DynamoDBTransactionLogSnapshotCreator {
    public static final Logger LOGGER = LoggerFactory.getLogger(DynamoDBTransactionLogSnapshotCreator.class);
    private final TableStatus tableStatus;
    private final TransactionLogStore filesLogStore;
    private final TransactionLogStore partitionsLogStore;
    private final LatestSnapshotsMetadataLoader latestMetadataLoader;
    private final DynamoDBTransactionLogSnapshotStore snapshotStore;

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
        TransactionLogStore fileTransactionStore = new DynamoDBTransactionLogStore(
                instanceProperties.get(TRANSACTION_LOG_FILES_TABLENAME),
                instanceProperties, tableProperties, dynamoDBClient, s3Client);
        TransactionLogStore partitionTransactionStore = new DynamoDBTransactionLogStore(
                instanceProperties.get(TRANSACTION_LOG_PARTITIONS_TABLENAME),
                instanceProperties, tableProperties, dynamoDBClient, s3Client);
        DynamoDBTransactionLogSnapshotMetadataStore snapshotStore = new DynamoDBTransactionLogSnapshotMetadataStore(
                instanceProperties, tableProperties, dynamoDBClient);
        return new DynamoDBTransactionLogSnapshotCreator(instanceProperties, tableProperties,
                fileTransactionStore, partitionTransactionStore, configuration,
                snapshotStore::getLatestSnapshots, snapshotStore::saveSnapshot);
    }

    public DynamoDBTransactionLogSnapshotCreator(
            InstanceProperties instanceProperties, TableProperties tableProperties,
            TransactionLogStore filesLogStore, TransactionLogStore partitionsLogStore,
            Configuration configuration, LatestSnapshotsMetadataLoader latestMetadataLoader, SnapshotMetadataSaver snapshotSaver) {
        this.tableStatus = tableProperties.getStatus();
        this.filesLogStore = filesLogStore;
        this.partitionsLogStore = partitionsLogStore;
        this.latestMetadataLoader = latestMetadataLoader;
        this.snapshotStore = new DynamoDBTransactionLogSnapshotStore(
                latestMetadataLoader, snapshotSaver, instanceProperties, tableProperties, configuration);
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
        TransactionLogSnapshot oldSnapshot = snapshotStore.loadFilesSnapshot(latestSnapshots);
        try {
            Optional<TransactionLogSnapshot> newSnapshot = TransactionLogSnapshotCreator.createSnapshotIfChanged(
                    oldSnapshot, filesLogStore, FileReferenceTransaction.class, tableStatus);
            if (newSnapshot.isPresent()) {
                snapshotStore.saveFilesSnapshot(newSnapshot.get());
                LOGGER.info("Saved new files snapshot");
            }
        } catch (DuplicateSnapshotException | StateStoreException | IOException e) {
            LOGGER.error("Failed to create files snapshot for table {}", tableStatus);
            throw new RuntimeException(e);
        }
    }

    private void updatePartitionsSnapshot(LatestSnapshots latestSnapshots) {
        TransactionLogSnapshot oldSnapshot = snapshotStore.loadPartitionsSnapshot(latestSnapshots);
        try {
            Optional<TransactionLogSnapshot> newSnapshot = TransactionLogSnapshotCreator.createSnapshotIfChanged(
                    oldSnapshot, partitionsLogStore, PartitionTransaction.class, tableStatus);
            if (newSnapshot.isPresent()) {
                snapshotStore.savePartitionsSnapshot(newSnapshot.get());
                LOGGER.info("Saved new partitions snapshot");
            }
        } catch (DuplicateSnapshotException | StateStoreException | IOException e) {
            LOGGER.error("Failed to create partitions snapshot for table {}", tableStatus);
            throw new RuntimeException(e);
        }
    }
}
