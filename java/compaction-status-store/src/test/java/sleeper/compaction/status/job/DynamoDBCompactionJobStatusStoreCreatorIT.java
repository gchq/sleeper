/*
 * Copyright 2022 Crown Copyright
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
package sleeper.compaction.status.job;

import com.amazonaws.services.dynamodbv2.model.DescribeTableResult;
import org.junit.After;
import org.junit.Test;
import sleeper.configuration.properties.InstanceProperties;
import sleeper.dynamodb.tools.DynamoDBTestBase;

import static org.assertj.core.api.Assertions.assertThat;
import static sleeper.compaction.status.job.DynamoDBCompactionJobStatusStore.jobStatusTableName;
import static sleeper.compaction.status.testutils.CompactionStatusStoreTestUtils.createInstanceProperties;
import static sleeper.configuration.properties.UserDefinedInstanceProperty.ID;

public class DynamoDBCompactionJobStatusStoreCreatorIT extends DynamoDBTestBase {

    private final InstanceProperties instanceProperties = createInstanceProperties();
    private final String tableName = jobStatusTableName(instanceProperties.get(ID));

    @Test
    public void shouldCreateStore() {
        // When
        DynamoDBCompactionJobStatusStoreCreator.create(instanceProperties, dynamoDBClient);

        // Then
        assertThat(dynamoDBClient.describeTable(tableName))
                .extracting(DescribeTableResult::getTable).isNotNull();
    }

    @After
    public void tearDown() {
        dynamoDBClient.deleteTable(tableName);
    }
}
