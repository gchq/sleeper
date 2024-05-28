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

import sleeper.configuration.properties.instance.InstanceProperties;
import sleeper.configuration.properties.table.TableProperties;
import sleeper.core.statestore.StateStore;
import sleeper.statestore.dynamodb.DynamoDBStateStore;
import sleeper.statestore.s3.S3StateStore;
import sleeper.statestore.transactionlog.DynamoDBTransactionLogStateStore;
import sleeper.statestore.transactionlog.DynamoDBTransactionLogStateStoreNoSnapshots;

import static sleeper.configuration.properties.table.TableProperty.STATESTORE_CLASSNAME;

/**
 * Creates a client to access the state store for a Sleeper table. The client may not be thread safe, as it may cache
 * the state.
 */
public class StateStoreFactory {
    private final InstanceProperties instanceProperties;
    private final AmazonS3 s3;
    private final AmazonDynamoDB dynamoDB;
    private final Configuration configuration;

    public StateStoreFactory(InstanceProperties instanceProperties, AmazonS3 s3, AmazonDynamoDB dynamoDB, Configuration configuration) {
        this.instanceProperties = instanceProperties;
        this.s3 = s3;
        this.dynamoDB = dynamoDB;
        this.configuration = configuration;
    }

    /**
     * Creates a client to access a state store.
     *
     * @param  tableProperties the Sleeper table properties
     * @return                 the state store
     */
    public StateStore getStateStore(TableProperties tableProperties) {
        String stateStoreClassName = tableProperties.get(STATESTORE_CLASSNAME);
        if (stateStoreClassName.equals(DynamoDBStateStore.class.getName())) {
            return new DynamoDBStateStore(instanceProperties, tableProperties, dynamoDB);
        }
        if (stateStoreClassName.equals(S3StateStore.class.getName())) {
            return new S3StateStore(instanceProperties, tableProperties, dynamoDB, configuration);
        }
        if (stateStoreClassName.equals(DynamoDBTransactionLogStateStore.class.getName())) {
            return DynamoDBTransactionLogStateStore.create(instanceProperties, tableProperties, dynamoDB, s3, configuration);
        }
        if (stateStoreClassName.equals(DynamoDBTransactionLogStateStoreNoSnapshots.class.getName())) {
            return DynamoDBTransactionLogStateStoreNoSnapshots.create(instanceProperties, tableProperties, dynamoDB, s3);
        }
        throw new RuntimeException("Unknown StateStore class: " + stateStoreClassName);
    }
}
