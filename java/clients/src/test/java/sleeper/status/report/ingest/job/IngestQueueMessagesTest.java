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

package sleeper.status.report.ingest.job;

import org.junit.jupiter.api.Test;

import sleeper.configuration.properties.InstanceProperties;
import sleeper.job.common.QueueMessageCount;
import sleeper.job.common.QueueMessageCountsInMemory;

import java.util.Map;

import static org.assertj.core.api.Assertions.assertThat;
import static sleeper.configuration.properties.InstancePropertiesTestHelper.createTestInstanceProperties;
import static sleeper.configuration.properties.SystemDefinedInstanceProperty.INGEST_JOB_QUEUE_URL;
import static sleeper.job.common.QueueMessageCount.approximateNumberVisibleAndNotVisible;

public class IngestQueueMessagesTest {
    @Test
    void shouldCountMessagesOnIngestQueue() {
        InstanceProperties instanceProperties = createTestInstanceProperties();
        instanceProperties.set(INGEST_JOB_QUEUE_URL, "ingest-queue");


        QueueMessageCount.Client client = QueueMessageCountsInMemory.from(
                Map.of("ingest-queue", approximateNumberVisibleAndNotVisible(1, 2)));

        IngestQueueMessages ingestQueueMessages = IngestQueueMessages.from(instanceProperties, client);
        assertThat(ingestQueueMessages.getIngestJobMessageCount())
                .isEqualTo(1);
    }
}
