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
package sleeper.statestorev2.transactionlog;

import org.slf4j.Logger;
import org.slf4j.LoggerFactory;
import software.amazon.awssdk.services.dynamodb.DynamoDbClient;
import software.amazon.awssdk.services.s3.S3Client;
import software.amazon.awssdk.transfer.s3.S3TransferManager;

import sleeper.core.properties.instance.InstanceProperties;
import sleeper.core.properties.table.TableProperties;
import sleeper.core.statestore.transactionlog.TransactionLogStateStore;
import sleeper.statestorev2.StateStoreArrowFileStore;
import sleeper.statestorev2.transactionlog.snapshots.DynamoDBTransactionLogSnapshotLoader;
import sleeper.statestorev2.transactionlog.snapshots.DynamoDBTransactionLogSnapshotMetadataStore;
import sleeper.statestorev2.transactionlog.snapshots.SnapshotType;

/**
 * An implementation of the state store backed by a transaction log held in DynamoDB and S3.
 */
public class DynamoDBTransactionLogStateStore {
    public static final Logger LOGGER = LoggerFactory.getLogger(DynamoDBTransactionLogStateStore.class);
    public static final String TABLE_ID = "TABLE_ID";
    public static final String TRANSACTION_NUMBER = "TRANSACTION_NUMBER";

    private DynamoDBTransactionLogStateStore() {
    }

    /**
     * Creates a builder for the state store for the given Sleeper table.
     *
     * @param  instanceProperties the Sleeper instance properties
     * @param  tableProperties    the Sleeper table properties
     * @param  dynamoDB           the client for interacting with DynamoDB
     * @param  s3                 the client for interacting with S3
     * @param  s3TransferManager  the transfer manager for interacting with async s3 actions
     * @return                    the builder
     */
    public static TransactionLogStateStore.Builder builderFrom(
            InstanceProperties instanceProperties, TableProperties tableProperties, DynamoDbClient dynamoDB, S3Client s3, S3TransferManager s3TransferManager) {
        DynamoDBTransactionLogSnapshotMetadataStore metadataStore = new DynamoDBTransactionLogSnapshotMetadataStore(instanceProperties, tableProperties, dynamoDB);
        StateStoreArrowFileStore fileStore = new StateStoreArrowFileStore(instanceProperties, s3);
        return DynamoDBTransactionLogStateStoreNoSnapshots.builderFrom(instanceProperties, tableProperties, dynamoDB, s3)
                .filesSnapshotLoader(new DynamoDBTransactionLogSnapshotLoader(metadataStore, fileStore, SnapshotType.FILES))
                .partitionsSnapshotLoader(new DynamoDBTransactionLogSnapshotLoader(metadataStore, fileStore, SnapshotType.PARTITIONS));
    }
}
