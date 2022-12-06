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

package sleeper.ingest.job;

import org.junit.Test;
import sleeper.configuration.jars.ObjectFactory;
import sleeper.configuration.properties.InstanceProperties;
import sleeper.ingest.task.IngestTaskStatus;
import sleeper.ingest.task.IngestTaskStatusStore;
import sleeper.ingest.task.status.DynamoDBIngestTaskStatusStore;
import sleeper.ingest.task.status.DynamoDBIngestTaskStatusStoreCreator;

import static org.assertj.core.api.Assertions.assertThat;

public class IngestJobQueueConsumerRunnerIT extends IngestJobQueueConsumerTestBase {
    @Test
    public void shouldRecordIngestTaskFinishedWhenIngestCompleteWithNoJobRuns() throws Exception {
        // Given
        InstanceProperties instanceProperties = getInstanceProperties();
        DynamoDBIngestTaskStatusStoreCreator.create(instanceProperties, AWS_EXTERNAL_RESOURCE.getDynamoDBClient());
        IngestTaskStatusStore taskStatusStore = DynamoDBIngestTaskStatusStore.from(AWS_EXTERNAL_RESOURCE.getDynamoDBClient(),
                instanceProperties);

        // When
        IngestJobQueueConsumerRunner jobRunner = new IngestJobQueueConsumerRunner(
                ObjectFactory.noUserJars(),
                instanceProperties,
                "/mnt/scratch",
                AWS_EXTERNAL_RESOURCE.getSqsClient(),
                AWS_EXTERNAL_RESOURCE.getCloudWatchClient(),
                AWS_EXTERNAL_RESOURCE.getS3Client(),
                AWS_EXTERNAL_RESOURCE.getDynamoDBClient());
        String testTaskId = "test-task";
        jobRunner.run(testTaskId);

        // Then
        assertThat(taskStatusStore.getTask(testTaskId))
                .extracting(IngestTaskStatus::isFinished, IngestTaskStatus::getJobRuns)
                .containsExactly(true, 0);
    }
}
