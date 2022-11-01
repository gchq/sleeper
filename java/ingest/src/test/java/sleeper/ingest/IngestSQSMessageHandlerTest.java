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

package sleeper.ingest;

import org.apache.commons.lang3.tuple.Pair;
import org.junit.ClassRule;
import org.junit.Test;
import org.testcontainers.containers.localstack.LocalStackContainer;
import sleeper.ingest.job.IngestJob;
import sleeper.ingest.job.IngestJobSerDe;
import sleeper.ingest.job.sqs.AWSSQSMessageHandler;
import sleeper.ingest.job.sqs.InMemorySQSMessageHandler;
import sleeper.ingest.job.sqs.SQSMessageHandler;
import sleeper.ingest.testutils.AwsExternalResource;

import java.util.ArrayList;
import java.util.Collections;
import java.util.List;
import java.util.Optional;

import static org.assertj.core.api.Assertions.assertThat;

public class IngestSQSMessageHandlerTest {
    @ClassRule
    public static final AwsExternalResource AWS_EXTERNAL_RESOURCE = new AwsExternalResource(
            LocalStackContainer.Service.SQS);

    private final IngestJobSerDe ingestJobSerDe = new IngestJobSerDe();
    private static final String TEST_QUEUE_NAME = "test-queue-for-message-receiver";

    @Test
    public void shouldReturnIngestJobsFromAWSSQSMessageHandler() {
        // Given we have a list of ingest jobs
        List<IngestJob> ingestJobList = createIngestJobs();
        // and an AWSSQSMessageHandler defined
        String queueUrl = setupTestQueue();
        SQSMessageHandler messageHandler = AWSSQSMessageHandler.builder()
                .ingestJobSerDe(ingestJobSerDe)
                .queueUrl(queueUrl)
                .sqsClient(AWS_EXTERNAL_RESOURCE.getSqsClient()).build();

        // When we send all jobs using an AWSSQSMessageHandler
        ingestJobList.forEach(messageHandler::send);

        // Then all ingest jobs should be retrieved from the SQSMessageReceiver
        List<IngestJob> deserialisedIngestJobs = receiveAllJobs(messageHandler);
        try {
            assertThat(deserialisedIngestJobs)
                    .containsAll(ingestJobList);
        } finally {
            tearDownTestQueue();
        }
    }

    @Test
    public void shouldReturnIngestJobsFromFixedSQSMessageHandler() {
        // Given we have a list of ingest jobs
        List<IngestJob> ingestJobList = createIngestJobs();
        // and a FixedSQSMessageHandler defined
        SQSMessageHandler messageHandler = InMemorySQSMessageHandler.builder()
                .ingestJobSerDe(ingestJobSerDe)
                .build();
        // When we send all jobs using a FixedSQSMessageHandler
        ingestJobList.forEach(messageHandler::send);

        // Then all ingest jobs should be retrieved from the FixedSQSMessageHandler
        List<IngestJob> deserialisedIngestJobs = receiveAllJobs(messageHandler);
        assertThat(deserialisedIngestJobs)
                .containsAll(ingestJobList);
    }

    @Test
    public void shouldNotReturnIngestJobsWhenNoMessagesFromAWSSQSMessageHandler() {
        // Given we have no jobs to send
        // and an AWSSQSMessageHandler defined
        String queueUrl = setupTestQueue();
        SQSMessageHandler messageHandler = AWSSQSMessageHandler.builder()
                .ingestJobSerDe(ingestJobSerDe)
                .queueUrl(queueUrl)
                .sqsClient(AWS_EXTERNAL_RESOURCE.getSqsClient()).build();

        // When we send no jobs to the SQS queue
        // Then no job should be retrieved from the SQSMessageReceiver
        List<IngestJob> deserialisedIngestJobs = receiveAllJobs(messageHandler);
        try {
            assertThat(deserialisedIngestJobs).isEmpty();
        } finally {
            tearDownTestQueue();
        }
    }

    @Test
    public void shouldNotReturnIngestJobsWhenNoMessagesFromFixedSQSMessageHandler() {
        // Given we have an empty list of jobs
        // and an AWSSQSMessageHandler defined
        SQSMessageHandler messageHandler = InMemorySQSMessageHandler.builder()
                .ingestJobSerDe(ingestJobSerDe).build();
        // When we send no jobs to the SQS queue
        // Then no job should be retrieved from the SQSMessageReceiver
        List<IngestJob> deserialisedIngestJobs = receiveAllJobs(messageHandler);
        assertThat(deserialisedIngestJobs).isEmpty();
    }

    private List<IngestJob> createIngestJobs() {
        List<IngestJob> ingestJobs = new ArrayList<>();
        for (int i = 0; i < 10; i++) {
            ingestJobs.add(new IngestJob("test-table-name", "id" + i, Collections.emptyList()));
        }
        return ingestJobs;
    }

    private List<IngestJob> receiveAllJobs(SQSMessageHandler messageHandler) {
        List<IngestJob> deserialisedIngestJobs = new ArrayList<>();
        Optional<Pair<IngestJob, String>> job = messageHandler.receive();
        while (job.isPresent()) {
            deserialisedIngestJobs.add(job.get().getLeft());
            job = messageHandler.receive();
        }
        return deserialisedIngestJobs;
    }

    private String setupTestQueue() {
        AWS_EXTERNAL_RESOURCE.getSqsClient().createQueue(TEST_QUEUE_NAME);
        return AWS_EXTERNAL_RESOURCE.getSqsClient().getQueueUrl(TEST_QUEUE_NAME).getQueueUrl();
    }

    private void tearDownTestQueue() {
        AWS_EXTERNAL_RESOURCE.getSqsClient().deleteQueue(AWS_EXTERNAL_RESOURCE.getSqsClient().getQueueUrl(TEST_QUEUE_NAME).getQueueUrl());
    }
}
