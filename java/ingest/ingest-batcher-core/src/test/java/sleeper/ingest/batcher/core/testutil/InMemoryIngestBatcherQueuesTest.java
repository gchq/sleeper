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
package sleeper.ingest.batcher.core.testutil;

import org.junit.jupiter.api.Test;

import sleeper.ingest.core.job.IngestJob;

import java.util.List;
import java.util.Map;

import static org.assertj.core.api.Assertions.assertThat;

class InMemoryIngestBatcherQueuesTest {

    private final InMemoryIngestBatcherQueues queueClient = new InMemoryIngestBatcherQueues();

    @Test
    void shouldSendJobToNamedQueue() {
        IngestJob job = IngestJob.builder()
                .id("test-ingest-job")
                .files(List.of("test-bucket/test-file.parquet"))
                .tableName("test-table")
                .build();
        queueClient.send("test-queue-url", job);

        assertThat(queueClient.getMessagesByQueueUrl()).isEqualTo(
                Map.of("test-queue-url", List.of(job)));
    }

    @Test
    void shouldSendJobsToMultipleQueues() {
        IngestJob job1 = IngestJob.builder()
                .id("test-ingest-job-1")
                .files(List.of("test-bucket/test-file-1.parquet"))
                .tableName("test-table")
                .build();
        IngestJob job2 = IngestJob.builder()
                .id("test-ingest-job-2")
                .files(List.of("test-bucket/test-file-2.parquet"))
                .tableName("test-table")
                .build();
        queueClient.send("test-queue-url-1", job1);
        queueClient.send("test-queue-url-2", job2);

        assertThat(queueClient.getMessagesByQueueUrl()).isEqualTo(
                Map.of("test-queue-url-1", List.of(job1),
                        "test-queue-url-2", List.of(job2)));
    }

    @Test
    void shouldSendMultipleJobsToOneQueue() {
        IngestJob job1 = IngestJob.builder()
                .id("test-ingest-job-1")
                .files(List.of("test-bucket/test-file-1.parquet"))
                .tableName("test-table")
                .build();
        IngestJob job2 = IngestJob.builder()
                .id("test-ingest-job-2")
                .files(List.of("test-bucket/test-file-2.parquet"))
                .tableName("test-table")
                .build();
        queueClient.send("test-queue-url", job1);
        queueClient.send("test-queue-url", job2);

        assertThat(queueClient.getMessagesByQueueUrl()).isEqualTo(
                Map.of("test-queue-url", List.of(job1, job2)));
    }

    @Test
    void shouldSendSameJobMultipleTimes() {
        IngestJob job = IngestJob.builder()
                .id("test-ingest-job")
                .files(List.of("test-bucket/test-file.parquet"))
                .tableName("test-table")
                .build();
        queueClient.send("test-queue-url", job);
        queueClient.send("test-queue-url", job);

        assertThat(queueClient.getMessagesByQueueUrl()).isEqualTo(
                Map.of("test-queue-url", List.of(job, job)));
    }
}
