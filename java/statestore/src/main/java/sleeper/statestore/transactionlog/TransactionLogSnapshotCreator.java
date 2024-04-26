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
import sleeper.configuration.properties.table.TableProperty;
import sleeper.core.statestore.StateStoreException;
import sleeper.core.statestore.transactionlog.StateStoreFiles;
import sleeper.core.statestore.transactionlog.StateStorePartitions;
import sleeper.core.statestore.transactionlog.TransactionLogSnapshotUtils;
import sleeper.core.statestore.transactionlog.TransactionLogStore;
import sleeper.statestore.transactionlog.DynamoDBTransactionLogSnapshotStore.LatestSnapshots;

import java.io.IOException;
import java.io.UncheckedIOException;
import java.util.Optional;

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
    private final TransactionLogSnapshotSerDe snapshotSerDe;
    private final DynamoDBTransactionLogSnapshotStore snapshotStore;

    public TransactionLogSnapshotCreator(
            InstanceProperties instanceProperties, TableProperties tableProperties,
            AmazonS3 s3Client, AmazonDynamoDB dynamoDB, Configuration configuration) {
        this(instanceProperties, tableProperties,
                new DynamoDBTransactionLogStore(instanceProperties.get(TRANSACTION_LOG_FILES_TABLENAME),
                        instanceProperties, tableProperties, dynamoDB, s3Client),
                new DynamoDBTransactionLogStore(instanceProperties.get(TRANSACTION_LOG_PARTITIONS_TABLENAME),
                        instanceProperties, tableProperties, dynamoDB, s3Client),
                dynamoDB, configuration);

    }

    public TransactionLogSnapshotCreator(
            InstanceProperties instanceProperties, TableProperties tableProperties,
            TransactionLogStore filesLogStore, TransactionLogStore partitionsLogStore,
            AmazonDynamoDB dynamoDB, Configuration configuration) {
        this.instanceProperties = instanceProperties;
        this.tableProperties = tableProperties;
        this.filesLogStore = filesLogStore;
        this.partitionsLogStore = partitionsLogStore;
        this.snapshotSerDe = new TransactionLogSnapshotSerDe(tableProperties.getSchema(), configuration);
        this.snapshotStore = new DynamoDBTransactionLogSnapshotStore(instanceProperties, tableProperties, dynamoDB);
    }

    public void createSnapshot() {
        LOGGER.info("Creating snapshot for table {}", tableProperties.getStatus());
        Optional<LatestSnapshots> latestSnapshotsOpt = snapshotStore.getLatestSnapshots();
        StateStoreFiles filesState = latestSnapshotsOpt
                .filter(latestSnapshot -> latestSnapshot.getFilesSnapshot() != null)
                .map(latestSnapshot -> {
                    try {
                        return snapshotSerDe.loadFiles(latestSnapshot.getFilesSnapshot());
                    } catch (IOException e) {
                        throw new UncheckedIOException(e);
                    }
                })
                .orElseGet(StateStoreFiles::new);
        long filesTransactionNumberBefore = latestSnapshotsOpt
                .filter(latestSnapshot -> latestSnapshot.getFilesSnapshot() != null)
                .map(latestSnapshot -> latestSnapshot.getFilesSnapshot().getTransactionNumber())
                .orElse(0L);

        StateStorePartitions partitionsState = latestSnapshotsOpt
                .filter(latestSnapshot -> latestSnapshot.getPartitionsSnapshot() != null)
                .map(latestSnapshot -> {
                    try {
                        return snapshotSerDe.loadPartitions(latestSnapshot.getPartitionsSnapshot());
                    } catch (IOException e) {
                        throw new UncheckedIOException(e);
                    }
                })
                .orElseGet(StateStorePartitions::new);
        long partitionsTransactionNumberBefore = latestSnapshotsOpt
                .filter(latestSnapshot -> latestSnapshot.getPartitionsSnapshot() != null)
                .map(latestSnapshot -> latestSnapshot.getPartitionsSnapshot().getTransactionNumber())
                .orElse(0L);
        try {
            saveFilesSnapshot(filesState, filesTransactionNumberBefore, snapshotSerDe, snapshotStore);
            savePartitionsSnapshot(partitionsState, partitionsTransactionNumberBefore, snapshotSerDe, snapshotStore);
        } catch (DuplicateSnapshotException | StateStoreException | IOException e) {
            LOGGER.error("Failed to create snapshot for table {}", tableProperties.getStatus());
            throw new RuntimeException(e);
        }
    }

    private void saveFilesSnapshot(StateStoreFiles filesState, long transactionNumberBefore,
            TransactionLogSnapshotSerDe snapshotSerDe, DynamoDBTransactionLogSnapshotStore snapshotStore) throws IOException, StateStoreException, DuplicateSnapshotException {
        long transactionNumberAfter = TransactionLogSnapshotUtils.updateFilesState(
                tableProperties.getStatus(), filesState, filesLogStore, transactionNumberBefore);
        if (transactionNumberBefore == transactionNumberAfter) {
            // Log message
            return;
        }
        TransactionLogSnapshot snapshot = TransactionLogSnapshot.forFiles(getBasePath(), transactionNumberAfter);
        snapshotSerDe.saveFiles(snapshot, filesState);
        snapshotStore.saveSnapshot(snapshot);
    }

    private void savePartitionsSnapshot(StateStorePartitions partitionsState, long transactionNumberBefore,
            TransactionLogSnapshotSerDe snapshotSerDe, DynamoDBTransactionLogSnapshotStore snapshotStore) throws IOException, StateStoreException, DuplicateSnapshotException {
        long transactionNumberAfter = TransactionLogSnapshotUtils.updatePartitionsState(
                tableProperties.getStatus(), partitionsState, partitionsLogStore, transactionNumberBefore);
        if (transactionNumberBefore == transactionNumberAfter) {
            return;
        }
        TransactionLogSnapshot snapshot = TransactionLogSnapshot.forPartitions(getBasePath(), transactionNumberAfter);
        snapshotSerDe.savePartitions(snapshot, partitionsState);
        snapshotStore.saveSnapshot(snapshot);
    }

    private String getBasePath() {
        return instanceProperties.get(FILE_SYSTEM)
                + instanceProperties.get(DATA_BUCKET) + "/"
                + tableProperties.get(TableProperty.TABLE_ID);
    }
}
