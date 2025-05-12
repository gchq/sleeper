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

import org.slf4j.Logger;
import org.slf4j.LoggerFactory;
import software.amazon.awssdk.services.dynamodb.DynamoDbClient;
import software.amazon.awssdk.services.s3.S3Client;
import software.amazon.awssdk.transfer.s3.S3TransferManager;

import sleeper.core.properties.instance.InstanceProperties;
import sleeper.core.properties.table.TableProperties;
import sleeper.core.statestore.transactionlog.snapshot.TransactionLogSnapshot;
import sleeper.core.statestore.transactionlog.state.StateStoreFiles;
import sleeper.core.statestore.transactionlog.state.StateStorePartitions;
import sleeper.statestorev2.StateStoreArrowFileStore;
import sleeper.statestorev2.transactionlog.DuplicateSnapshotException;
import sleeper.statestorev2.transactionlog.snapshots.DynamoDBTransactionLogSnapshotCreator.LatestSnapshotsMetadataLoader;

import java.io.IOException;

/**
 * Stores snapshots derived from a transaction log. Holds an index of snapshots in DynamoDB, and stores snapshot data in
 * S3.
 */
public class DynamoDBTransactionLogSnapshotSaver {
    public static final Logger LOGGER = LoggerFactory.getLogger(DynamoDBTransactionLogSnapshotSaver.class);

    private final SnapshotMetadataSaver metadataSaver;
    private final StateStoreArrowFileStore fileStore;
    private final String basePath;

    public DynamoDBTransactionLogSnapshotSaver(
            InstanceProperties instanceProperties, TableProperties tableProperties, DynamoDbClient dynamo,
            S3Client s3Client, S3TransferManager s3TransferManager) {
        this(new DynamoDBTransactionLogSnapshotMetadataStore(instanceProperties, tableProperties, dynamo),
                instanceProperties, tableProperties, s3Client, s3TransferManager);
    }

    private DynamoDBTransactionLogSnapshotSaver(
            DynamoDBTransactionLogSnapshotMetadataStore metadataStore,
            InstanceProperties instanceProperties, TableProperties tableProperties, S3Client s3Client, S3TransferManager s3TransferManager) {
        this(metadataStore::getLatestSnapshots, metadataStore::saveSnapshot, instanceProperties, tableProperties, s3Client, s3TransferManager);
    }

    DynamoDBTransactionLogSnapshotSaver(
            LatestSnapshotsMetadataLoader latestMetadataLoader, SnapshotMetadataSaver metadataSaver,
            InstanceProperties instanceProperties, TableProperties tableProperties,
            S3Client s3Client, S3TransferManager s3TransferManager) {
        this.metadataSaver = metadataSaver;
        this.fileStore = new StateStoreArrowFileStore(instanceProperties, tableProperties, s3Client, s3TransferManager);
        this.basePath = TransactionLogSnapshotMetadata.getBasePath(instanceProperties, tableProperties);
    }

    /**
     * Saves a snapshot of files to S3 and to the index. Deletes the file if the metadata fails to save in the index.
     *
     * @param  snapshot                   the snapshot
     * @throws IOException                if the snapshot fails to save to S3
     * @throws DuplicateSnapshotException if there is already a snapshot for the given transaction number
     */
    public void saveFilesSnapshot(TransactionLogSnapshot snapshot) throws IOException, DuplicateSnapshotException {
        TransactionLogSnapshotMetadata metadata = TransactionLogSnapshotMetadata.forFiles(
                basePath, snapshot.getTransactionNumber());
        StateStoreFiles state = snapshot.getState();
        fileStore.saveFiles(metadata.getObjectKey(), state);
        saveMetadata(metadata);
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
        TransactionLogSnapshotMetadata metadata = TransactionLogSnapshotMetadata.forPartitions(
                basePath, snapshot.getTransactionNumber());
        StateStorePartitions state = snapshot.getState();
        fileStore.savePartitions(metadata.getObjectKey(), state.all());
        saveMetadata(metadata);
    }

    private void saveMetadata(TransactionLogSnapshotMetadata metadata) throws IOException, DuplicateSnapshotException {
        try {
            metadataSaver.save(metadata);
        } catch (DuplicateSnapshotException | RuntimeException e) {
            LOGGER.info("Failed to save snapshot metadata to DynamoDB. Deleting snapshot file.");
            fileStore.deleteSnapshotFile(metadata);
            throw e;
        }
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
