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
package sleeper.statestore.transactionlog;

import software.amazon.awssdk.services.dynamodb.DynamoDbClient;
import software.amazon.awssdk.services.s3.S3Client;

import sleeper.core.properties.instance.InstanceProperties;
import sleeper.core.properties.table.TableProperties;
import sleeper.core.statestore.transactionlog.TransactionLogStateStore;
import sleeper.core.statestore.transactionlog.transaction.TransactionSerDeProvider;

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
            DynamoDbClient dynamoDB, S3Client s3) {
        return TransactionLogStateStore.builder()
                .tableProperties(tableProperties)
                .filesLogStore(DynamoDBTransactionLogStore.forFiles(instanceProperties, tableProperties, dynamoDB, s3))
                .partitionsLogStore(DynamoDBTransactionLogStore.forPartitions(instanceProperties, tableProperties, dynamoDB, s3))
                .transactionBodyStore(new S3TransactionBodyStore(instanceProperties, s3, TransactionSerDeProvider.forOneTable(tableProperties)));
    }

}
