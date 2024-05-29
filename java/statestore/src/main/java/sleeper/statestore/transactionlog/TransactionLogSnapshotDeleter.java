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

import java.io.IOException;
import java.time.Duration;
import java.time.Instant;

import static sleeper.configuration.properties.table.TableProperty.TRANSACTION_LOG_SNAPSHOT_EXPIRY_IN_DAYS;

/**
 * Searches for snapshots older than a certain time, then deletes them.
 */
public class TransactionLogSnapshotDeleter {
    public static final Logger LOGGER = LoggerFactory.getLogger(TransactionLogSnapshotDeleter.class);
    private final Configuration configuration;
    private final DynamoDBTransactionLogSnapshotMetadataStore metadataStore;
    private final Duration expiryInDays;

    public TransactionLogSnapshotDeleter(
            InstanceProperties instanceProperties, TableProperties tableProperties,
            AmazonDynamoDB dynamoDB, Configuration configuration) {
        this.configuration = configuration;
        this.metadataStore = new DynamoDBTransactionLogSnapshotMetadataStore(instanceProperties, tableProperties, dynamoDB);
        this.expiryInDays = Duration.ofDays(tableProperties.getInt(TRANSACTION_LOG_SNAPSHOT_EXPIRY_IN_DAYS));
    }

    /**
     * Searches for snapshots that have expired based on the current time, then deletes the snapshot file in addition to
     * the snapshot metadata in the metadata store.
     *
     * @param currentTime the current time
     */
    public void deleteSnapshots(Instant currentTime) {
        Instant expiryDate = currentTime.minus(expiryInDays);
        metadataStore.getSnapshotsBefore(expiryDate)
                .forEach(snapshot -> {
                    LOGGER.info("Deleting snapshot {}", snapshot);
                    try {
                        Path path = new Path(snapshot.getPath());
                        FileSystem fs = path.getFileSystem(configuration);
                        boolean deleted = fs.delete(path, false);
                        if (!deleted) {
                            LOGGER.warn("Failed to delete file. File has already been deleted: {}", snapshot.getPath());
                        }
                    } catch (IOException e) {
                        LOGGER.error("Failed to delete file: {}", snapshot.getPath(), e);
                    }
                    metadataStore.deleteSnapshot(snapshot);
                });
    }
}
