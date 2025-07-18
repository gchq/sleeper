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

package sleeper.ingest.tracker.task;

import org.junit.jupiter.api.AfterEach;
import org.junit.jupiter.api.Test;
import software.amazon.awssdk.services.dynamodb.model.DescribeTableRequest;
import software.amazon.awssdk.services.dynamodb.model.DescribeTableResponse;

import sleeper.core.properties.instance.InstanceProperties;
import sleeper.localstack.test.LocalStackTestBase;

import static org.assertj.core.api.Assertions.assertThat;
import static sleeper.core.properties.instance.CommonProperty.ID;
import static sleeper.ingest.tracker.testutils.IngestTrackerTestUtils.createInstanceProperties;

public class DynamoDBIngestTaskTrackerCreatorIT extends LocalStackTestBase {
    private final InstanceProperties instanceProperties = createInstanceProperties();
    private final String tableName = DynamoDBIngestTaskTracker.taskStatusTableName(instanceProperties.get(ID));

    @Test
    public void shouldCreateStore() {
        // When
        DynamoDBIngestTaskTrackerCreator.create(instanceProperties, dynamoClient);

        // Then
        assertThat(dynamoClient.describeTable(DescribeTableRequest.builder().tableName(tableName).build()))
                .extracting(DescribeTableResponse::table).isNotNull();
    }

    @AfterEach
    public void tearDown() {
        DynamoDBIngestTaskTrackerCreator.tearDown(instanceProperties, dynamoClient);
    }
}
