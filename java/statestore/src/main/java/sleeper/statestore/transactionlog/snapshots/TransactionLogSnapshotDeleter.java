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
package sleeper.statestore.transactionlog.snapshots;

import org.slf4j.Logger;
import org.slf4j.LoggerFactory;
import software.amazon.awssdk.services.dynamodb.DynamoDbClient;
import software.amazon.awssdk.services.s3.S3Client;
import software.amazon.awssdk.services.s3.model.DeleteObjectRequest;

import sleeper.core.properties.instance.InstanceProperties;
import sleeper.core.properties.table.TableProperties;

import java.io.IOException;
import java.io.UncheckedIOException;
import java.time.Duration;
import java.time.Instant;

import static sleeper.core.properties.instance.CdkDefinedInstanceProperty.DATA_BUCKET;
import static sleeper.core.properties.table.TableProperty.TRANSACTION_LOG_SNAPSHOT_EXPIRY_IN_DAYS;

/**
 * Searches for snapshots older than a certain time, then deletes them.
 */
public class TransactionLogSnapshotDeleter {
    public static final Logger LOGGER = LoggerFactory.getLogger(TransactionLogSnapshotDeleter.class);
    private final DynamoDBTransactionLogSnapshotMetadataStore metadataStore;
    private final Duration expiryInDays;
    private final SnapshotFileDeleter snapshotFileDeleter;

    public TransactionLogSnapshotDeleter(
            InstanceProperties instanceProperties, TableProperties tableProperties,
            DynamoDbClient dynamoDB, S3Client s3Client) {
        this(instanceProperties, tableProperties, dynamoDB, fileDeleter(instanceProperties, s3Client));
    }

    public TransactionLogSnapshotDeleter(
            InstanceProperties instanceProperties, TableProperties tableProperties,
            DynamoDbClient dynamoDB, SnapshotFileDeleter fileDeleter) {
        this.metadataStore = new DynamoDBTransactionLogSnapshotMetadataStore(instanceProperties, tableProperties, dynamoDB);
        this.expiryInDays = Duration.ofDays(tableProperties.getInt(TRANSACTION_LOG_SNAPSHOT_EXPIRY_IN_DAYS));
        this.snapshotFileDeleter = fileDeleter;
    }

    /**
     * Searches for snapshots that have expired based on the current time, then deletes the snapshot file in addition to
     * the snapshot metadata in the metadata store.
     *
     * @param  currentTime the current time
     * @return             the total number of snapshots deleted
     */
    public SnapshotDeletionTracker deleteSnapshots(Instant currentTime) {
        Instant expiryDate = currentTime.minus(expiryInDays);
        SnapshotDeletionTracker deletedSnapshotTracker = new SnapshotDeletionTracker();
        metadataStore.getExpiredSnapshots(expiryDate)
                .forEach(snapshot -> {
                    LOGGER.info("Deleting snapshot {}", snapshot);
                    try {
                        snapshotFileDeleter.delete(snapshot.getObjectKey());
                        deletedSnapshotTracker.deleteSuccess(snapshot.getTransactionNumber());
                    } catch (IOException e) {
                        LOGGER.error("Failed to delete snapshot file: {}", snapshot.getObjectKey(), e);
                        throw new UncheckedIOException(e);
                    }
                    metadataStore.deleteSnapshot(snapshot);
                });

        return deletedSnapshotTracker;
    }

    private static SnapshotFileDeleter fileDeleter(InstanceProperties instanceProperties, S3Client s3Client) {
        return file -> {
            s3Client.deleteObject(DeleteObjectRequest.builder()
                    .bucket(instanceProperties.get(DATA_BUCKET))
                    .key(file).build());
        };
    }

    /**
     * Deletes a snapshot file.
     */
    public interface SnapshotFileDeleter {
        /**
         * Deletes a snapshot file.
         *
         * @param  objectKey   the object key for the snapshot file
         * @throws IOException if the file fails to delete
         */
        void delete(String objectKey) throws IOException;
    }
}
