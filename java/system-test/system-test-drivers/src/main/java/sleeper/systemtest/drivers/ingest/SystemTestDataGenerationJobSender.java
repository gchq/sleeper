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

import software.amazon.awssdk.services.sqs.SqsClient;
import software.amazon.awssdk.services.sqs.model.SendMessageBatchRequestEntry;

import sleeper.core.util.SplitIntoBatches;
import sleeper.systemtest.configuration.SystemTestDataGenerationJob;
import sleeper.systemtest.configuration.SystemTestDataGenerationJobSerDe;
import sleeper.systemtest.configuration.SystemTestPropertyValues;

import java.util.List;

import static sleeper.systemtest.configuration.SystemTestProperty.SYSTEM_TEST_JOBS_QUEUE_URL;

public class SystemTestDataGenerationJobSender {
    private final SystemTestPropertyValues properties;
    private final SqsClient sqsClient;

    public SystemTestDataGenerationJobSender(SystemTestPropertyValues properties, SqsClient sqsClient) {
        this.properties = properties;
        this.sqsClient = sqsClient;
    }

    public void sendJobsToQueue(List<SystemTestDataGenerationJob> jobs) {
        SystemTestDataGenerationJobSerDe serDe = new SystemTestDataGenerationJobSerDe();
        for (List<SystemTestDataGenerationJob> batch : SplitIntoBatches.splitListIntoBatchesOf(10, jobs)) {
            sqsClient.sendMessageBatch(builder -> builder
                    .queueUrl(properties.get(SYSTEM_TEST_JOBS_QUEUE_URL))
                    .entries(batch.stream()
                            .map(job -> SendMessageBatchRequestEntry.builder()
                                    .id(job.getJobId())
                                    .messageBody(serDe.toJson(job))
                                    .build())
                            .toList()));
        }
    }

}
