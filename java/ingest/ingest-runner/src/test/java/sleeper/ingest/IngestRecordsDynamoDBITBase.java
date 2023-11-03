/*
 * Copyright 2022-2023 Crown Copyright
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

package sleeper.ingest;

import com.amazonaws.services.dynamodbv2.AmazonDynamoDB;
import com.amazonaws.services.dynamodbv2.AmazonDynamoDBClientBuilder;
import org.junit.jupiter.api.BeforeEach;
import org.testcontainers.junit.jupiter.Container;
import org.testcontainers.junit.jupiter.Testcontainers;

import sleeper.core.statestore.StateStoreException;
import sleeper.dynamodb.tools.DynamoDBContainer;
import sleeper.statestore.dynamodb.DynamoDBStateStore;
import sleeper.statestore.dynamodb.DynamoDBStateStoreCreator;

import static sleeper.configuration.properties.table.TableProperty.STATESTORE_CLASSNAME;
import static sleeper.dynamodb.tools.GenericContainerAwsV1ClientHelper.buildAwsV1Client;

@Testcontainers
public class IngestRecordsDynamoDBITBase extends IngestRecordsTestBase {

    @Container
    public static DynamoDBContainer dynamoDb = new DynamoDBContainer();

    private final AmazonDynamoDB dynamoDBClient = buildAwsV1Client(dynamoDb, dynamoDb.getDynamoPort(), AmazonDynamoDBClientBuilder.standard());

    @BeforeEach
    void setUp() {
        tableProperties.set(STATESTORE_CLASSNAME, DynamoDBStateStore.class.getName());
        new DynamoDBStateStoreCreator(instanceProperties, dynamoDBClient).create();
    }

    protected DynamoDBStateStore initialiseStateStore() throws StateStoreException {
        DynamoDBStateStore stateStore = new DynamoDBStateStore(instanceProperties, tableProperties, dynamoDBClient);
        stateStore.initialise();
        return stateStore;
    }
}
