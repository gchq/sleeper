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

import sleeper.core.properties.instance.InstanceProperties;
import sleeper.core.properties.table.TableProperties;
import sleeper.core.statestore.transactionlog.TransactionLogStateStore;
import sleeper.core.util.ExponentialBackoffWithJitter;
import sleeper.core.util.ExponentialBackoffWithJitter.WaitRange;

import java.time.Duration;

import static sleeper.core.properties.table.TableProperty.ADD_TRANSACTION_FIRST_RETRY_WAIT_CEILING_MS;
import static sleeper.core.properties.table.TableProperty.ADD_TRANSACTION_MAX_ATTEMPTS;
import static sleeper.core.properties.table.TableProperty.ADD_TRANSACTION_MAX_RETRY_WAIT_CEILING_MS;
import static sleeper.core.properties.table.TableProperty.MIN_TRANSACTIONS_AHEAD_TO_LOAD_SNAPSHOT;
import static sleeper.core.properties.table.TableProperty.TIME_BETWEEN_SNAPSHOT_CHECKS_SECS;
import static sleeper.core.properties.table.TableProperty.TIME_BETWEEN_TRANSACTION_CHECKS_MS;

/**
 * An implementation of the state store backed by a transaction log held in DynamoDB and S3, with snapshots disabled.
 */
public class DynamoDBTransactionLogStateStoreNoSnapshots {

    private DynamoDBTransactionLogStateStoreNoSnapshots() {
    }

    /**
     * Creates a builder for the state store for the given Sleeper table.
     *
     * @param  instanceProperties the Sleeper instance properties
     * @param  tableProperties    the Sleeper table properties
     * @param  dynamoDB           the client for interacting with DynamoDB
     * @param  s3                 the client for interacting with S3
     * @return                    the builder
     */
    public static TransactionLogStateStore.Builder builderFrom(
            InstanceProperties instanceProperties, TableProperties tableProperties,
            AmazonDynamoDB dynamoDB, AmazonS3 s3) {
        return TransactionLogStateStore.builder()
                .sleeperTable(tableProperties.getStatus())
                .schema(tableProperties.getSchema())
                .timeBetweenSnapshotChecks(Duration.ofSeconds(tableProperties.getLong(TIME_BETWEEN_SNAPSHOT_CHECKS_SECS)))
                .timeBetweenTransactionChecks(Duration.ofMillis(tableProperties.getLong(TIME_BETWEEN_TRANSACTION_CHECKS_MS)))
                .minTransactionsAheadToLoadSnapshot(tableProperties.getLong(MIN_TRANSACTIONS_AHEAD_TO_LOAD_SNAPSHOT))
                .maxAddTransactionAttempts(tableProperties.getInt(ADD_TRANSACTION_MAX_ATTEMPTS))
                .retryBackoff(new ExponentialBackoffWithJitter(WaitRange.firstAndMaxWaitCeilingSecs(
                        tableProperties.getLong(ADD_TRANSACTION_FIRST_RETRY_WAIT_CEILING_MS) / 1000.0,
                        tableProperties.getLong(ADD_TRANSACTION_MAX_RETRY_WAIT_CEILING_MS) / 1000.0)))
                .filesLogStore(DynamoDBTransactionLogStore.forFiles(instanceProperties, tableProperties, dynamoDB, s3))
                .partitionsLogStore(DynamoDBTransactionLogStore.forPartitions(instanceProperties, tableProperties, dynamoDB, s3))
                .transactionBodyStore(new S3TransactionBodyStore(tableProperties, s3));
    }

}
