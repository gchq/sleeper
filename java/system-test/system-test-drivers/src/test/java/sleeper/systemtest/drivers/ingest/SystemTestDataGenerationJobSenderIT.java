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
package sleeper.systemtest.drivers.ingest;

import org.junit.jupiter.api.BeforeEach;
import org.junit.jupiter.api.Test;
import software.amazon.awssdk.services.sqs.model.Message;
import software.amazon.awssdk.services.sqs.model.ReceiveMessageResponse;

import sleeper.core.properties.instance.InstanceProperties;
import sleeper.localstack.test.LocalStackTestBase;
import sleeper.systemtest.configuration.SystemTestDataGenerationJob;
import sleeper.systemtest.configuration.SystemTestDataGenerationJobSerDe;
import sleeper.systemtest.configuration.SystemTestStandaloneProperties;

import java.util.List;

import static org.assertj.core.api.Assertions.assertThat;
import static sleeper.core.properties.testutils.InstancePropertiesTestHelper.createTestInstanceProperties;
import static sleeper.systemtest.configuration.SystemTestProperty.SYSTEM_TEST_JOBS_QUEUE_URL;

public class SystemTestDataGenerationJobSenderIT extends LocalStackTestBase {

    InstanceProperties instanceProperties = createTestInstanceProperties();
    SystemTestStandaloneProperties testProperties = new SystemTestStandaloneProperties();

    @BeforeEach
    void setUp() {
        testProperties.set(SYSTEM_TEST_JOBS_QUEUE_URL, createSqsQueueGetUrl());
    }

    @Test
    void shouldSendDataGenerationJob() {
        // Given
        SystemTestDataGenerationJob job = SystemTestDataGenerationJob.builder()
                .instanceProperties(instanceProperties)
                .testProperties(testProperties)
                .tableName("test-table")
                .build();

        // When
        sender().sendJobsToQueue(List.of(job));

        // Then
        assertThat(receiveJobs()).containsExactly(job);
    }

    private SystemTestDataGenerationJobSender sender() {
        return new SystemTestDataGenerationJobSender(testProperties, sqsClientV2);
    }

    private List<SystemTestDataGenerationJob> receiveJobs() {
        SystemTestDataGenerationJobSerDe serDe = new SystemTestDataGenerationJobSerDe();
        ReceiveMessageResponse response = sqsClientV2.receiveMessage(request -> request
                .queueUrl(testProperties.get(SYSTEM_TEST_JOBS_QUEUE_URL))
                .waitTimeSeconds(1));
        return response.messages().stream()
                .map(Message::body)
                .map(serDe::fromJson)
                .toList();
    }

}
