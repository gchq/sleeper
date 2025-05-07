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

package sleeper.ingest.runner;

import org.junit.jupiter.api.BeforeEach;

import sleeper.core.statestore.StateStore;
import sleeper.localstack.test.SleeperLocalStackClients;
import sleeper.statestore.transactionlog.DynamoDBTransactionLogStateStore;
import sleeper.statestore.transactionlog.TransactionLogStateStoreCreator;

import static sleeper.core.statestore.testutils.StateStoreUpdatesWrapper.update;

public class IngestRecordsLocalStackITBase extends IngestRecordsTestBase {

    @BeforeEach
    void setUp() {
        new TransactionLogStateStoreCreator(instanceProperties, SleeperLocalStackClients.DYNAMO_CLIENT).create();
    }

    protected StateStore initialiseStateStore() {
        StateStore stateStore = DynamoDBTransactionLogStateStore.builderFrom(instanceProperties, tableProperties,
                SleeperLocalStackClients.DYNAMO_CLIENT, SleeperLocalStackClients.S3_CLIENT, SleeperLocalStackClients.HADOOP_CONF).build();
        update(stateStore).initialise(tableProperties.getSchema());
        return stateStore;
    }
}
