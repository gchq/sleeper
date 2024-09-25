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

package sleeper.clients.status.report.ingest.job;

import org.junit.jupiter.api.DisplayName;
import org.junit.jupiter.api.Nested;
import org.junit.jupiter.api.Test;

import sleeper.clients.testutil.ToStringConsoleOutput;
import sleeper.core.properties.instance.InstanceProperties;
import sleeper.task.common.InMemoryQueueMessageCounts;
import sleeper.task.common.QueueMessageCount;

import java.util.Map;

import static org.assertj.core.api.Assertions.assertThat;
import static sleeper.clients.status.report.ingest.job.IngestJobStatusReporterTestData.ingestMessageCount;
import static sleeper.configuration.properties.InstancePropertiesTestHelper.createTestInstanceProperties;
import static sleeper.core.properties.instance.CdkDefinedInstanceProperty.BULK_IMPORT_EKS_JOB_QUEUE_URL;
import static sleeper.core.properties.instance.CdkDefinedInstanceProperty.BULK_IMPORT_EMR_JOB_QUEUE_URL;
import static sleeper.core.properties.instance.CdkDefinedInstanceProperty.BULK_IMPORT_PERSISTENT_EMR_JOB_QUEUE_URL;
import static sleeper.core.properties.instance.CdkDefinedInstanceProperty.INGEST_JOB_QUEUE_URL;
import static sleeper.task.common.QueueMessageCount.approximateNumberVisibleAndNotVisible;

class IngestQueueMessagesTest {
    @Test
    void shouldCountMessagesOnIngestQueue() {
        // Given
        InstanceProperties instanceProperties = createTestInstanceProperties();
        instanceProperties.set(INGEST_JOB_QUEUE_URL, "ingest-queue");
        QueueMessageCount.Client client = InMemoryQueueMessageCounts.from(
                Map.of("ingest-queue", approximateNumberVisibleAndNotVisible(1, 2)));

        // When / Then
        assertThat(IngestQueueMessages.from(instanceProperties, client))
                .isEqualTo(IngestQueueMessages.builder()
                        .ingestMessages(1)
                        .build());
    }

    @Test
    void shouldCountMessagesOnAllQueues() {
        // Given
        InstanceProperties instanceProperties = createTestInstanceProperties();
        instanceProperties.set(INGEST_JOB_QUEUE_URL, "ingest-queue");
        instanceProperties.set(BULK_IMPORT_EMR_JOB_QUEUE_URL, "emr-queue");
        instanceProperties.set(BULK_IMPORT_PERSISTENT_EMR_JOB_QUEUE_URL, "persistent-emr-queue");
        instanceProperties.set(BULK_IMPORT_EKS_JOB_QUEUE_URL, "eks-queue");
        QueueMessageCount.Client client = InMemoryQueueMessageCounts.from(Map.of(
                "ingest-queue", approximateNumberVisibleAndNotVisible(1, 2),
                "emr-queue", approximateNumberVisibleAndNotVisible(3, 4),
                "persistent-emr-queue", approximateNumberVisibleAndNotVisible(5, 6),
                "eks-queue", approximateNumberVisibleAndNotVisible(7, 8)));

        // When / Then
        assertThat(IngestQueueMessages.from(instanceProperties, client))
                .isEqualTo(IngestQueueMessages.builder()
                        .ingestMessages(1)
                        .emrMessages(3)
                        .persistentEmrMessages(5)
                        .eksMessages(7)
                        .build());
    }

    @Test
    void shouldGetTotalMessagesWhenAllQueuesAreEnabled() {
        // Given
        IngestQueueMessages messages = IngestQueueMessages.builder().ingestMessages(1)
                .emrMessages(2)
                .persistentEmrMessages(3)
                .eksMessages(4)
                .build();

        // When / Then
        assertThat(messages.getTotalMessages())
                .isEqualTo(10);
    }

    @Test
    void shouldGetTotalMessagesWhenSomeQueuesAreNotEnabled() {
        // Given
        IngestQueueMessages messages = IngestQueueMessages.builder().ingestMessages(1).emrMessages(2).build();

        // When / Then
        assertThat(messages.getTotalMessages())
                .isEqualTo(3);
    }

    @Test
    void shouldGetTotalMessagesWhenNoQueuesAreEnabled() {
        // Given
        IngestQueueMessages messages = IngestQueueMessages.builder().build();

        // When / Then
        assertThat(messages.getTotalMessages())
                .isZero();
    }

    @Nested
    @DisplayName("Report message count")
    class ReportMessageCount {
        @Test
        void shouldReportMessagesWhenOnlyIngestQueueIsDeployed() {
            // Given
            IngestQueueMessages messages = ingestMessageCount(10);

            // When
            ToStringConsoleOutput out = new ToStringConsoleOutput();
            messages.print(out.getPrintStream());

            // Then
            assertThat(out).hasToString("" +
                    "Jobs waiting in ingest queue (excluded from report): 10\n" +
                    "Total jobs waiting across all queues: 10\n");
        }

        @Test
        void shouldReportMessagesWhenOnlyBulkImportEmrQueueIsDeployed() {
            // Given
            IngestQueueMessages messages = IngestQueueMessages.builder().emrMessages(10).build();

            // When
            ToStringConsoleOutput out = new ToStringConsoleOutput();
            messages.print(out.getPrintStream());

            // Then
            assertThat(out).hasToString("" +
                    "Jobs waiting in EMR queue (excluded from report): 10\n" +
                    "Total jobs waiting across all queues: 10\n");
        }

        @Test
        void shouldReportMessagesWhenOnlyBulkImportPersistentEmrQueueIsDeployed() {
            // Given
            IngestQueueMessages messages = IngestQueueMessages.builder().persistentEmrMessages(10).build();

            // When
            ToStringConsoleOutput out = new ToStringConsoleOutput();
            messages.print(out.getPrintStream());

            // Then
            assertThat(out).hasToString("" +
                    "Jobs waiting in persistent EMR queue (excluded from report): 10\n" +
                    "Total jobs waiting across all queues: 10\n");
        }

        @Test
        void shouldReportMessagesWhenOnlyBulkImportEksQueueIsDeployed() {
            // Given
            IngestQueueMessages messages = IngestQueueMessages.builder().eksMessages(10).build();

            // When
            ToStringConsoleOutput out = new ToStringConsoleOutput();
            messages.print(out.getPrintStream());

            // Then
            assertThat(out).hasToString("" +
                    "Jobs waiting in EKS queue (excluded from report): 10\n" +
                    "Total jobs waiting across all queues: 10\n");
        }

        @Test
        void shouldReportMessagesWhenAllQueuesAreDeployed() {
            // Given
            IngestQueueMessages messages = IngestQueueMessages.builder()
                    .ingestMessages(1)
                    .emrMessages(2)
                    .persistentEmrMessages(3)
                    .eksMessages(4)
                    .build();

            // When
            ToStringConsoleOutput out = new ToStringConsoleOutput();
            messages.print(out.getPrintStream());

            // Then
            assertThat(out).hasToString("" +
                    "Jobs waiting in ingest queue (excluded from report): 1\n" +
                    "Jobs waiting in EMR queue (excluded from report): 2\n" +
                    "Jobs waiting in persistent EMR queue (excluded from report): 3\n" +
                    "Jobs waiting in EKS queue (excluded from report): 4\n" +
                    "Total jobs waiting across all queues: 10\n");
        }

        @Test
        void shouldReportZeroMessagesWhenNoQueuesAreDeployed() {
            // Given
            IngestQueueMessages messages = IngestQueueMessages.builder().build();

            // When
            ToStringConsoleOutput out = new ToStringConsoleOutput();
            messages.print(out.getPrintStream());

            // Then
            assertThat(out).hasToString("Total jobs waiting across all queues: 0\n");
        }
    }
}
