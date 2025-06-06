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
package sleeper.compaction.tracker.task;

import org.junit.jupiter.api.AfterEach;
import org.junit.jupiter.api.Test;
import software.amazon.awssdk.services.dynamodb.model.DescribeTableRequest;
import software.amazon.awssdk.services.dynamodb.model.DescribeTableResponse;
import software.amazon.awssdk.services.dynamodb.model.ResourceNotFoundException;

import sleeper.core.properties.instance.InstanceProperties;
import sleeper.core.tracker.compaction.task.CompactionTaskTracker;
import sleeper.localstack.test.LocalStackTestBase;

import static org.assertj.core.api.Assertions.assertThat;
import static org.assertj.core.api.Assertions.assertThatThrownBy;
import static sleeper.core.properties.instance.CommonProperty.ID;
import static sleeper.core.properties.instance.CompactionProperty.COMPACTION_TRACKER_ENABLED;
import static sleeper.core.properties.testutils.InstancePropertiesTestHelper.createTestInstanceProperties;

public class DynamoDBCompactionTaskTrackerCreatorIT extends LocalStackTestBase {

    private final InstanceProperties instanceProperties = createTestInstanceProperties();
    private final String tableName = DynamoDBCompactionTaskTracker.taskStatusTableName(instanceProperties.get(ID));

    @Test
    public void shouldCreateStore() {
        // When
        DynamoDBCompactionTaskTrackerCreator.create(instanceProperties, dynamoClientV2);
        CompactionTaskTracker tracker = CompactionTaskTrackerFactory.getTracker(dynamoClientV2, instanceProperties);

        // Then
        assertThat(dynamoClientV2.describeTable(DescribeTableRequest.builder()
                .tableName(tableName)
                .build()))
                .extracting(DescribeTableResponse::table).isNotNull();
        assertThat(tracker).isInstanceOf(DynamoDBCompactionTaskTracker.class);
    }

    @Test
    public void shouldNotCreateStoreIfDisabled() {
        // Given
        instanceProperties.set(COMPACTION_TRACKER_ENABLED, "false");

        // When
        DynamoDBCompactionTaskTrackerCreator.create(instanceProperties, dynamoClientV2);
        CompactionTaskTracker tracker = CompactionTaskTrackerFactory.getTracker(dynamoClientV2, instanceProperties);

        // Then
        assertThatThrownBy(() -> dynamoClientV2.describeTable(DescribeTableRequest.builder()
                .tableName(tableName)
                .build()))
                .isInstanceOf(ResourceNotFoundException.class);
        assertThat(tracker).isSameAs(CompactionTaskTracker.NONE);
        assertThatThrownBy(tracker::getAllTasks).isInstanceOf(UnsupportedOperationException.class);
        assertThatThrownBy(tracker::getTasksInProgress).isInstanceOf(UnsupportedOperationException.class);
        assertThatThrownBy(() -> tracker.getTask("some-task")).isInstanceOf(UnsupportedOperationException.class);
    }

    @AfterEach
    public void tearDown() {
        DynamoDBCompactionTaskTrackerCreator.tearDown(instanceProperties, dynamoClientV2);
    }
}
