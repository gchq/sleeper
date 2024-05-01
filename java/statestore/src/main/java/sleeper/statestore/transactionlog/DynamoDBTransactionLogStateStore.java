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
import sleeper.core.statestore.transactionlog.TransactionLogStateStore;
import sleeper.statestore.transactionlog.DynamoDBTransactionLogSnapshotStore.LatestSnapshots;

import java.io.IOException;
import java.io.UncheckedIOException;

public class DynamoDBTransactionLogStateStore {
    public static final Logger LOGGER = LoggerFactory.getLogger(DynamoDBTransactionLogStateStore.class);
    public static final String TABLE_ID = "TABLE_ID";
    public static final String TRANSACTION_NUMBER = "TRANSACTION_NUMBER";

    private DynamoDBTransactionLogStateStore() {
    }

    public static TransactionLogStateStore create(
            InstanceProperties instanceProperties, TableProperties tableProperties, AmazonDynamoDB dynamoDB, AmazonS3 s3, Configuration configuration) {
        return builderFrom(instanceProperties, tableProperties, dynamoDB, s3, configuration).build();
    }

    public static TransactionLogStateStore.Builder builderFrom(
            InstanceProperties instanceProperties, TableProperties tableProperties, AmazonDynamoDB dynamoDB, AmazonS3 s3, Configuration configuration) {
        TransactionLogStateStore.Builder builder = DynamoDBTransactionLogStateStoreNoSnapshots.builderFrom(instanceProperties, tableProperties, dynamoDB, s3);
        loadLatestSnapshots(builder, instanceProperties, tableProperties, dynamoDB, configuration);
        return builder;
    }

    private static void loadLatestSnapshots(
            TransactionLogStateStore.Builder builder, InstanceProperties instanceProperties, TableProperties tableProperties,
            AmazonDynamoDB dynamoDB, Configuration configuration) {
        LatestSnapshots latestSnapshots = new DynamoDBTransactionLogSnapshotStore(instanceProperties, tableProperties, dynamoDB).getLatestSnapshots();
        TransactionLogSnapshotSerDe snapshotSerDe = new TransactionLogSnapshotSerDe(tableProperties.getSchema(), configuration);
        try {
            if (latestSnapshots.getFilesSnapshot().isPresent()) {
                TransactionLogSnapshot filesSnapshot = latestSnapshots.getFilesSnapshot().get();
                LOGGER.info("Found latest files snapshot with last transaction number {}. Creating file reference store using this snapshot.",
                        filesSnapshot.getTransactionNumber());
                try {
                    builder.filesState(snapshotSerDe.loadFiles(filesSnapshot))
                            .filesTransactionNumber(filesSnapshot.getTransactionNumber());
                } catch (IOException e) {
                    throw new UncheckedIOException(e);
                }
            } else {
                LOGGER.info("Could not find latest files snapshot. Creating empty file reference store.");
            }
            if (latestSnapshots.getPartitionsSnapshot().isPresent()) {
                TransactionLogSnapshot partitionsSnapshot = latestSnapshots.getPartitionsSnapshot().get();
                LOGGER.info("Found latest partitions snapshot with last transaction number {}. Creating partitions store using this snapshot.",
                        partitionsSnapshot.getTransactionNumber());
                try {
                    builder.partitionsState(snapshotSerDe.loadPartitions(partitionsSnapshot))
                            .partitionsTransactionNumber(partitionsSnapshot.getTransactionNumber());
                } catch (IOException e) {
                    throw new UncheckedIOException(e);
                }
            } else {
                LOGGER.info("Could not find latest partitions snapshot. Creating empty partitions store.");
            }
        } catch (UncheckedIOException e) {
            throw new RuntimeException("Failed to load latest snapshots", e);
        }
    }
}
