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
package sleeper.compaction.dynamodb;

import com.amazonaws.services.dynamodbv2.model.DescribeTableResult;
import org.junit.Test;
import sleeper.compaction.dynamodb.job.DynamoDBCompactionJobStatusStoreCreator;

import static org.assertj.core.api.Assertions.assertThat;
import static sleeper.compaction.dynamodb.job.DynamoDBCompactionJobStatusStore.jobStatusTableName;

public class DynamoDBCompactionJobStatusStoreCreatorIT extends DynamoDBTestBase {

    @Test
    public void shouldCreateStore() {
        String instanceId = "test-instance";
        DynamoDBCompactionJobStatusStoreCreator creator = new DynamoDBCompactionJobStatusStoreCreator(instanceId, dynamoDBClient);

        creator.create();

        assertThat(dynamoDBClient.describeTable(jobStatusTableName(instanceId)))
                .extracting(DescribeTableResult::getTable).isNotNull();
    }
}
