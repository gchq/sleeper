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
package sleeper.compaction.status.store.task;

import com.amazonaws.services.dynamodbv2.model.DescribeTableResult;
import com.amazonaws.services.dynamodbv2.model.ResourceNotFoundException;
import org.junit.jupiter.api.AfterEach;
import org.junit.jupiter.api.Test;

import sleeper.compaction.task.CompactionTaskStatusStore;
import sleeper.configuration.properties.instance.InstanceProperties;
import sleeper.dynamodb.test.DynamoDBTestBase;

import static org.assertj.core.api.Assertions.assertThat;
import static org.assertj.core.api.Assertions.assertThatThrownBy;
import static sleeper.configuration.properties.InstancePropertiesTestHelper.createTestInstanceProperties;
import static sleeper.configuration.properties.instance.CommonProperty.ID;
import static sleeper.configuration.properties.instance.CompactionProperty.COMPACTION_STATUS_STORE_ENABLED;

public class DynamoDBCompactionTaskStatusStoreCreatorIT extends DynamoDBTestBase {

    private final InstanceProperties instanceProperties = createTestInstanceProperties();
    private final String tableName = DynamoDBCompactionTaskStatusStore.taskStatusTableName(instanceProperties.get(ID));

    @Test
    public void shouldCreateStore() {
        // When
        DynamoDBCompactionTaskStatusStoreCreator.create(instanceProperties, dynamoDBClient);
        CompactionTaskStatusStore store = CompactionTaskStatusStoreFactory.getStatusStore(dynamoDBClient, instanceProperties);

        // Then
        assertThat(dynamoDBClient.describeTable(tableName))
                .extracting(DescribeTableResult::getTable).isNotNull();
        assertThat(store).isInstanceOf(DynamoDBCompactionTaskStatusStore.class);
    }

    @Test
    public void shouldNotCreateStoreIfDisabled() {
        // Given
        instanceProperties.set(COMPACTION_STATUS_STORE_ENABLED, "false");

        // When
        DynamoDBCompactionTaskStatusStoreCreator.create(instanceProperties, dynamoDBClient);
        CompactionTaskStatusStore store = CompactionTaskStatusStoreFactory.getStatusStore(dynamoDBClient, instanceProperties);

        // Then
        assertThatThrownBy(() -> dynamoDBClient.describeTable(tableName))
                .isInstanceOf(ResourceNotFoundException.class);
        assertThat(store).isSameAs(CompactionTaskStatusStore.NONE);
        assertThatThrownBy(store::getAllTasks).isInstanceOf(UnsupportedOperationException.class);
        assertThatThrownBy(store::getTasksInProgress).isInstanceOf(UnsupportedOperationException.class);
        assertThatThrownBy(() -> store.getTask("some-task")).isInstanceOf(UnsupportedOperationException.class);
    }

    @AfterEach
    public void tearDown() {
        DynamoDBCompactionTaskStatusStoreCreator.tearDown(instanceProperties, dynamoDBClient);
    }
}
