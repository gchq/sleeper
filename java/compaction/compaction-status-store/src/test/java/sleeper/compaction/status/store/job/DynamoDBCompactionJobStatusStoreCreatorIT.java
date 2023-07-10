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
package sleeper.compaction.status.store.job;

import com.amazonaws.services.dynamodbv2.model.DescribeTableResult;
import com.amazonaws.services.dynamodbv2.model.ResourceNotFoundException;
import org.junit.jupiter.api.AfterEach;
import org.junit.jupiter.api.Test;

import sleeper.compaction.job.CompactionJobStatusStore;
import sleeper.compaction.status.store.testutils.CompactionStatusStoreTestUtils;
import sleeper.configuration.properties.InstanceProperties;
import sleeper.dynamodb.tools.DynamoDBTestBase;

import static org.assertj.core.api.Assertions.assertThat;
import static org.assertj.core.api.Assertions.assertThatThrownBy;
import static sleeper.configuration.properties.CompactionProperties.COMPACTION_STATUS_STORE_ENABLED;
import static sleeper.configuration.properties.CommonProperties.ID;

public class DynamoDBCompactionJobStatusStoreCreatorIT extends DynamoDBTestBase {

    private final InstanceProperties instanceProperties = CompactionStatusStoreTestUtils.createInstanceProperties();
    private final String tableName = DynamoDBCompactionJobStatusStore.jobStatusTableName(instanceProperties.get(ID));

    @Test
    public void shouldCreateStore() {
        // When
        DynamoDBCompactionJobStatusStoreCreator.create(instanceProperties, dynamoDBClient);
        CompactionJobStatusStore store = CompactionJobStatusStoreFactory.getStatusStore(dynamoDBClient, instanceProperties);

        // Then
        assertThat(dynamoDBClient.describeTable(tableName))
                .extracting(DescribeTableResult::getTable).isNotNull();
        assertThat(store).isInstanceOf(DynamoDBCompactionJobStatusStore.class);
    }

    @Test
    public void shouldNotCreateStoreIfDisabled() {
        // Given
        instanceProperties.set(COMPACTION_STATUS_STORE_ENABLED, "false");

        // When
        DynamoDBCompactionJobStatusStoreCreator.create(instanceProperties, dynamoDBClient);
        CompactionJobStatusStore store = CompactionJobStatusStoreFactory.getStatusStore(dynamoDBClient, instanceProperties);

        // Then
        assertThatThrownBy(() -> dynamoDBClient.describeTable(tableName))
                .isInstanceOf(ResourceNotFoundException.class);
        assertThat(store).isSameAs(CompactionJobStatusStore.NONE);
        assertThatThrownBy(() -> store.getAllJobs("some-table")).isInstanceOf(UnsupportedOperationException.class);
        assertThatThrownBy(() -> store.getUnfinishedJobs("some-table")).isInstanceOf(UnsupportedOperationException.class);
        assertThatThrownBy(() -> store.getJob("some-job")).isInstanceOf(UnsupportedOperationException.class);
    }

    @AfterEach
    public void tearDown() {
        DynamoDBCompactionJobStatusStoreCreator.tearDown(instanceProperties, dynamoDBClient);
    }
}
