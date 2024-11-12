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
package sleeper.statestore;

import com.amazonaws.services.dynamodbv2.AmazonDynamoDB;
import com.amazonaws.services.s3.AmazonS3;
import org.apache.hadoop.conf.Configuration;

import sleeper.core.properties.instance.InstanceProperties;
import sleeper.core.properties.table.TableProperties;
import sleeper.core.statestore.StateStore;
import sleeper.core.statestore.StateStoreProvider;
import sleeper.core.statestore.transactionlog.TransactionLogStateStore;
import sleeper.statestore.dynamodb.DynamoDBStateStore;
import sleeper.statestore.s3.S3StateStore;
import sleeper.statestore.transactionlog.DynamoDBTransactionLogStateStore;
import sleeper.statestore.transactionlog.DynamoDBTransactionLogStateStoreNoSnapshots;

import static sleeper.core.properties.table.TableProperty.STATESTORE_CLASSNAME;
import static sleeper.core.properties.table.TableProperty.STATESTORE_COMMITTER_UPDATE_ON_EVERY_COMMIT;

/**
 * Creates a client to access the state store for a Sleeper table. The client may not be thread safe, as it may cache
 * the state.
 */
public class StateStoreFactory implements StateStoreProvider.Factory {
    private final InstanceProperties instanceProperties;
    private final AmazonS3 s3;
    private final AmazonDynamoDB dynamoDB;
    private final Configuration configuration;
    private final boolean committerProcess;

    public StateStoreFactory(InstanceProperties instanceProperties, AmazonS3 s3, AmazonDynamoDB dynamoDB, Configuration configuration) {
        this(instanceProperties, s3, dynamoDB, configuration, false);
    }

    private StateStoreFactory(InstanceProperties instanceProperties, AmazonS3 s3, AmazonDynamoDB dynamoDB, Configuration configuration, boolean committerProcess) {
        this.instanceProperties = instanceProperties;
        this.s3 = s3;
        this.dynamoDB = dynamoDB;
        this.configuration = configuration;
        this.committerProcess = committerProcess;
    }

    /**
     * Creates a factory for state stores which will be updated by a single process. Used in the state store committer,
     * to avoid the need to check constantly for new transactions in the transaction log implementation.
     * <p>
     * This will detect depending on the Sleeper table, whether that table's state store is updated by a single process
     * asynchronously, or whether updates are made directly by multiple processes. This will configure the state store
     * client appropriately if only a single process updates that state store, and will assume that this is that
     * process. The current process should handle almost all updates to that state store across the whole Sleeper
     * instance.
     *
     * @param  instanceProperties the Sleeper instance properties
     * @param  s3                 the S3 client
     * @param  dynamoDB           the DynamoDB client
     * @param  configuration      the Hadoop configuration
     * @return                    the factory
     */
    public static StateStoreFactory forCommitterProcess(InstanceProperties instanceProperties, AmazonS3 s3, AmazonDynamoDB dynamoDB, Configuration configuration) {
        return new StateStoreFactory(instanceProperties, s3, dynamoDB, configuration, true);
    }

    /**
     * Creates a state store provider backed by an instance of this class to populate its cache.
     *
     * @param  instanceProperties the Sleeper instance properties
     * @param  s3Client           the S3 client
     * @param  dynamoDBClient     the DynamoDB client
     * @param  configuration      the Hadoop configuration
     * @return                    the state store provider
     */
    public static StateStoreProvider createProvider(InstanceProperties instanceProperties, AmazonS3 s3Client, AmazonDynamoDB dynamoDBClient, Configuration configuration) {
        return new StateStoreProvider(instanceProperties,
                new StateStoreFactory(instanceProperties, s3Client, dynamoDBClient, configuration));
    }

    /**
     * Creates a client to access a state store.
     *
     * @param  tableProperties the Sleeper table properties
     * @return                 the state store
     */
    @Override
    public StateStore getStateStore(TableProperties tableProperties) {
        String stateStoreClassName = tableProperties.get(STATESTORE_CLASSNAME);
        if (stateStoreClassName.equals(DynamoDBStateStore.class.getName())) {
            return new DynamoDBStateStore(instanceProperties, tableProperties, dynamoDB);
        }
        if (stateStoreClassName.equals(S3StateStore.class.getName())) {
            return new S3StateStore(instanceProperties, tableProperties, dynamoDB, configuration);
        }
        if (stateStoreClassName.equals(DynamoDBTransactionLogStateStore.class.getName())) {
            return forCommitterProcess(committerProcess, tableProperties,
                    DynamoDBTransactionLogStateStore.builderFrom(instanceProperties, tableProperties, dynamoDB, s3, configuration)).build();
        }
        if (stateStoreClassName.equals(DynamoDBTransactionLogStateStoreNoSnapshots.class.getName())) {
            return forCommitterProcess(committerProcess, tableProperties,
                    DynamoDBTransactionLogStateStoreNoSnapshots.builderFrom(instanceProperties, tableProperties, dynamoDB, s3)).build();
        }
        throw new RuntimeException("Unknown StateStore class: " + stateStoreClassName);
    }

    /**
     * Applies configuration to transaction log state store based on whether or not this is a committer process.
     *
     * @param  committerProcess true if the current process is the single process responsible for applying updates
     * @param  tableProperties  the table properties
     * @param  builder          the builder
     * @return                  the builder with configuration applied
     */
    public static TransactionLogStateStore.Builder forCommitterProcess(boolean committerProcess, TableProperties tableProperties, TransactionLogStateStore.Builder builder) {
        return builder.updateLogBeforeAddTransaction(isUpdateLogBeforeAddTransaction(committerProcess, tableProperties));
    }

    private static boolean isUpdateLogBeforeAddTransaction(boolean committerProcess, TableProperties tableProperties) {
        if (committerProcess) {
            return tableProperties.getBoolean(STATESTORE_COMMITTER_UPDATE_ON_EVERY_COMMIT);
        } else {
            return true;
        }
    }
}
