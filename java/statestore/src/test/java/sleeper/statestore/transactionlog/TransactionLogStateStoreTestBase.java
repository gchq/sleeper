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

import org.junit.jupiter.api.BeforeEach;

import sleeper.core.properties.instance.InstanceProperties;
import sleeper.core.properties.table.TableProperties;
import sleeper.core.statestore.StateStore;
import sleeper.core.statestore.transactionlog.TransactionLogStateStore;
import sleeper.localstack.test.LocalStackTestBase;

import static sleeper.core.properties.instance.CdkDefinedInstanceProperty.DATA_BUCKET;
import static sleeper.core.properties.testutils.InstancePropertiesTestHelper.createTestInstanceProperties;
import static sleeper.core.statestore.FileReferenceTestData.DEFAULT_UPDATE_TIME;

public class TransactionLogStateStoreTestBase extends LocalStackTestBase {

    protected final InstanceProperties instanceProperties = createTestInstanceProperties();

    @BeforeEach
    void setUpBase() {
        new TransactionLogStateStoreCreator(instanceProperties, DYNAMO_CLIENT).create();
        s3Client.createBucket(instanceProperties.get(DATA_BUCKET));
    }

    public StateStore createStateStore(TableProperties tableProperties) {
        return stateStore(stateStoreBuilder(tableProperties));
    }

    protected StateStore stateStore(TransactionLogStateStore.Builder builder) {
        StateStore stateStore = builder.build();
        stateStore.fixFileUpdateTime(DEFAULT_UPDATE_TIME);
        stateStore.fixPartitionUpdateTime(DEFAULT_UPDATE_TIME);
        return stateStore;
    }

    protected TransactionLogStateStore.Builder stateStoreBuilder(TableProperties tableProperties) {
        return DynamoDBTransactionLogStateStore.builderFrom(instanceProperties, tableProperties, DYNAMO_CLIENT, s3Client, HADOOP_CONF);
    }
}
