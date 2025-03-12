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
package sleeper.compaction.tracker.job;

import com.amazonaws.services.dynamodbv2.model.DescribeTableResult;
import com.amazonaws.services.dynamodbv2.model.ResourceNotFoundException;
import org.junit.jupiter.api.AfterEach;
import org.junit.jupiter.api.Test;

import sleeper.core.properties.instance.InstanceProperties;
import sleeper.core.tracker.compaction.job.CompactionJobTracker;
import sleeper.localstack.test.LocalStackTestBase;

import static org.assertj.core.api.Assertions.assertThat;
import static org.assertj.core.api.Assertions.assertThatThrownBy;
import static sleeper.core.properties.instance.CommonProperty.ID;
import static sleeper.core.properties.instance.CompactionProperty.COMPACTION_TRACKER_ENABLED;
import static sleeper.core.properties.testutils.InstancePropertiesTestHelper.createTestInstanceProperties;

public class DynamoDBCompactionJobTrackerCreatorIT extends LocalStackTestBase {

    private final InstanceProperties instanceProperties = createTestInstanceProperties();
    private final String updatesTableName = DynamoDBCompactionJobTracker.jobUpdatesTableName(instanceProperties.get(ID));
    private final String jobsTableName = DynamoDBCompactionJobTracker.jobLookupTableName(instanceProperties.get(ID));

    @Test
    public void shouldCreateStore() {
        // When
        DynamoDBCompactionJobTrackerCreator.create(instanceProperties, dynamoClient);
        CompactionJobTracker store = CompactionJobTrackerFactory.getTracker(dynamoClient, instanceProperties);

        // Then
        assertThat(dynamoClient.describeTable(updatesTableName))
                .extracting(DescribeTableResult::getTable).isNotNull();
        assertThat(dynamoClient.describeTable(jobsTableName))
                .extracting(DescribeTableResult::getTable).isNotNull();
        assertThat(store).isInstanceOf(DynamoDBCompactionJobTracker.class);
    }

    @Test
    public void shouldNotCreateStoreIfDisabled() {
        // Given
        instanceProperties.set(COMPACTION_TRACKER_ENABLED, "false");

        // When
        DynamoDBCompactionJobTrackerCreator.create(instanceProperties, dynamoClient);
        CompactionJobTracker store = CompactionJobTrackerFactory.getTracker(dynamoClient, instanceProperties);

        // Then
        assertThatThrownBy(() -> dynamoClient.describeTable(updatesTableName))
                .isInstanceOf(ResourceNotFoundException.class);
        assertThatThrownBy(() -> dynamoClient.describeTable(jobsTableName))
                .isInstanceOf(ResourceNotFoundException.class);
        assertThat(store).isSameAs(CompactionJobTracker.NONE);
        assertThatThrownBy(() -> store.getAllJobs("some-table"))
                .isInstanceOf(UnsupportedOperationException.class);
        assertThatThrownBy(() -> store.getUnfinishedJobs("some-table"))
                .isInstanceOf(UnsupportedOperationException.class);
        assertThatThrownBy(() -> store.getJob("some-job"))
                .isInstanceOf(UnsupportedOperationException.class);
    }

    @AfterEach
    public void tearDown() {
        DynamoDBCompactionJobTrackerCreator.tearDown(instanceProperties, dynamoClient);
    }
}
