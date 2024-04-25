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
import sleeper.configuration.properties.table.TablePropertiesProvider;
import sleeper.configuration.properties.table.TableProperty;
import sleeper.core.statestore.StateStoreException;
import sleeper.core.statestore.transactionlog.StateStoreFiles;
import sleeper.core.statestore.transactionlog.StateStorePartitions;
import sleeper.core.statestore.transactionlog.TransactionLogSnapshotUtils;
import sleeper.core.table.InvokeForTableRequest;
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
    private final TablePropertiesProvider tablePropertiesProvider;
    private final AmazonS3 s3Client;
    private final AmazonDynamoDB dynamoDB;
    private final Configuration configuration;

    public TransactionLogSnapshotCreator(
            InstanceProperties instanceProperties, TablePropertiesProvider tablePropertiesProvider,
            AmazonS3 s3Client, AmazonDynamoDB dynamoDB, Configuration configuration) {
        this.instanceProperties = instanceProperties;
        this.tablePropertiesProvider = tablePropertiesProvider;
        this.s3Client = s3Client;
        this.dynamoDB = dynamoDB;
        this.configuration = configuration;
    }

    public void run(InvokeForTableRequest tableRequest) throws StateStoreException {
        tableRequest.getTableIds().stream()
                .map(tablePropertiesProvider::getById)
                .forEach(this::createSnapshot);
    }

    public void createSnapshot(TableProperties table) {
        LOGGER.info("Creating snapshot for table {}", table.getStatus());
        DynamoDBTransactionLogSnapshotStore snapshotStore = new DynamoDBTransactionLogSnapshotStore(instanceProperties, table, dynamoDB);
        Optional<LatestSnapshots> latestSnapshotsOpt = snapshotStore.getLatestSnapshots();
        TransactionLogSnapshotSerDe snapshotSerDe = new TransactionLogSnapshotSerDe(table.getSchema(), configuration);
        StateStoreFiles filesState = latestSnapshotsOpt
                .map(latestSnapshot -> {
                    try {
                        return snapshotSerDe.loadFiles(latestSnapshot.getFilesSnapshot());
                    } catch (IOException e) {
                        throw new UncheckedIOException(e);
                    }
                })
                .orElseGet(StateStoreFiles::new);
        long filesTransactionNumberBefore = latestSnapshotsOpt
                .map(latestSnapshot -> latestSnapshot.getFilesSnapshot().getTransactionNumber())
                .orElse(0L);

        StateStorePartitions partitionsState = latestSnapshotsOpt
                .map(latestSnapshot -> {
                    try {
                        return snapshotSerDe.loadPartitions(latestSnapshot.getPartitionsSnapshot());
                    } catch (IOException e) {
                        throw new UncheckedIOException(e);
                    }
                })
                .orElseGet(StateStorePartitions::new);
        long partitionsTransactionNumberBefore = latestSnapshotsOpt
                .map(latestSnapshot -> latestSnapshot.getPartitionsSnapshot().getTransactionNumber())
                .orElse(0L);
        try {
            saveFilesSnapshot(table, filesState, filesTransactionNumberBefore, snapshotSerDe, snapshotStore);
            savePartitionsSnapshot(table, partitionsState, partitionsTransactionNumberBefore, snapshotSerDe, snapshotStore);
        } catch (DuplicateSnapshotException | StateStoreException | IOException e) {
            LOGGER.error("Failed to create snapshot for table {}", table.getStatus());
            throw new RuntimeException(e);
        }
    }

    private void saveFilesSnapshot(TableProperties table, StateStoreFiles filesState, long transactionNumberBefore,
            TransactionLogSnapshotSerDe snapshotSerDe, DynamoDBTransactionLogSnapshotStore snapshotStore) throws IOException, StateStoreException, DuplicateSnapshotException {
        long transactionNumberAfter = TransactionLogSnapshotUtils.updateFilesState(
                table.getStatus(), filesState,
                new DynamoDBTransactionLogStore(instanceProperties.get(TRANSACTION_LOG_FILES_TABLENAME),
                        instanceProperties, table, dynamoDB, s3Client),
                transactionNumberBefore);
        if (transactionNumberBefore == transactionNumberAfter) {
            return;
        }
        TransactionLogSnapshot snapshot = TransactionLogSnapshot.forFiles(getBasePath(instanceProperties, table), transactionNumberAfter);
        snapshotSerDe.saveFiles(snapshot, filesState);
        snapshotStore.saveSnapshot(snapshot);
    }

    private void savePartitionsSnapshot(TableProperties table, StateStorePartitions partitionsState, long transactionNumberBefore,
            TransactionLogSnapshotSerDe snapshotSerDe, DynamoDBTransactionLogSnapshotStore snapshotStore) throws IOException, StateStoreException, DuplicateSnapshotException {
        long transactionNumberAfter = TransactionLogSnapshotUtils.updatePartitionsState(
                table.getStatus(), partitionsState,
                new DynamoDBTransactionLogStore(instanceProperties.get(TRANSACTION_LOG_PARTITIONS_TABLENAME),
                        instanceProperties, table, dynamoDB, s3Client),
                transactionNumberBefore);
        if (transactionNumberBefore == transactionNumberAfter) {
            return;
        }
        TransactionLogSnapshot snapshot = TransactionLogSnapshot.forPartitions(getBasePath(instanceProperties, table), transactionNumberAfter);
        snapshotSerDe.savePartitions(snapshot, partitionsState);
        snapshotStore.saveSnapshot(snapshot);
    }

    private static String getBasePath(InstanceProperties instanceProperties, TableProperties tableProperties) {
        return instanceProperties.get(FILE_SYSTEM)
                + instanceProperties.get(DATA_BUCKET) + "/"
                + tableProperties.get(TableProperty.TABLE_ID);
    }
}
