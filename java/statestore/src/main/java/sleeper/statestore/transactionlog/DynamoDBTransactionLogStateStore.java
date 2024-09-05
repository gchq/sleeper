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
     * @param  configuration      the Hadoop configuration for interacting with Parquet
     * @return                    the builder
     */
    public static TransactionLogStateStore.Builder builderFrom(
            InstanceProperties instanceProperties, TableProperties tableProperties, AmazonDynamoDB dynamoDB, AmazonS3 s3, Configuration configuration) {
        DynamoDBTransactionLogSnapshotStore snapshotStore = new DynamoDBTransactionLogSnapshotStore(instanceProperties, tableProperties, dynamoDB, configuration);
        return DynamoDBTransactionLogStateStoreNoSnapshots.builderFrom(instanceProperties, tableProperties, dynamoDB, s3)
                .filesSnapshotLoader(snapshotStore::loadLatestFilesSnapshotIfAtMinimumTransaction)
                .partitionsSnapshotLoader(snapshotStore::loadLatestPartitionsSnapshotIfAtMinimumTransaction);
    }
}
