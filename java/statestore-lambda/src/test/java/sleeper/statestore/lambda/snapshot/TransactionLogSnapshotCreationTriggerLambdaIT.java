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
package sleeper.statestore.lambda.snapshot;

import com.amazonaws.services.lambda.runtime.events.ScheduledEvent;
import org.joda.time.DateTime;
import org.junit.jupiter.api.BeforeEach;
import org.junit.jupiter.api.Disabled;
import org.junit.jupiter.api.Test;

import sleeper.configuration.properties.S3TableProperties;
import sleeper.configuration.table.index.DynamoDBTableIndexCreator;
import sleeper.core.properties.instance.InstanceProperties;
import sleeper.core.properties.table.TableProperties;
import sleeper.core.properties.table.TablePropertiesStore;
import sleeper.localstack.test.LocalStackTestBase;

import static org.assertj.core.api.Assertions.assertThat;
import static sleeper.core.properties.instance.CdkDefinedInstanceProperty.CONFIG_BUCKET;
import static sleeper.core.properties.instance.CdkDefinedInstanceProperty.TRANSACTION_LOG_SNAPSHOT_CREATION_QUEUE_URL;
import static sleeper.core.properties.table.TableProperty.TABLE_NAME;
import static sleeper.core.properties.testutils.InstancePropertiesTestHelper.createTestInstanceProperties;
import static sleeper.core.properties.testutils.TablePropertiesTestHelper.createTestTableProperties;
import static sleeper.core.schema.SchemaTestHelper.createSchemaWithKey;

public class TransactionLogSnapshotCreationTriggerLambdaIT extends LocalStackTestBase {

    private final InstanceProperties instanceProperties = createTestInstanceProperties();
    private final TablePropertiesStore tablePropertiesStore = S3TableProperties.createStore(instanceProperties, s3Client, dynamoClient);

    @BeforeEach
    void setUp() {
        createBucket(instanceProperties.get(CONFIG_BUCKET));
        DynamoDBTableIndexCreator.create(dynamoClient, instanceProperties);
        instanceProperties.set(TRANSACTION_LOG_SNAPSHOT_CREATION_QUEUE_URL, createFifoQueueGetUrl());
    }

    @Test
    @Disabled("TODO")
    void shouldTriggerSnapshotCreationForTableWithTransactionLogStateStore() {
        // Given
        TableProperties tableProperties = createTestTableProperties(instanceProperties, createSchemaWithKey("key"));
        tableProperties.set(TABLE_NAME, "test-table");
        tablePropertiesStore.createTable(tableProperties);

        // When
        invokeLambda();

        // Then
        assertThat(receiveMessagesAndMessageGroupId(instanceProperties.get(TRANSACTION_LOG_SNAPSHOT_CREATION_QUEUE_URL)))
                .isEmpty();
    }

    private void invokeLambda() {
        lambda().handleRequest(new ScheduledEvent().withTime(DateTime.now()), null);
    }

    private TransactionLogSnapshotCreationTriggerLambda lambda() {
        return new TransactionLogSnapshotCreationTriggerLambda(s3Client, dynamoClient, sqsClient, instanceProperties.get(CONFIG_BUCKET));
    }
}
