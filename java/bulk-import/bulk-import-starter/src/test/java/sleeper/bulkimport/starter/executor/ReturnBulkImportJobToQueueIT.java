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
package sleeper.bulkimport.starter.executor;

import org.junit.jupiter.api.BeforeEach;
import org.junit.jupiter.api.Test;
import software.amazon.awssdk.services.sqs.model.Message;
import software.amazon.awssdk.services.sqs.model.ReceiveMessageRequest;

import sleeper.bulkimport.core.job.BulkImportJob;
import sleeper.bulkimport.core.job.BulkImportJobSerDe;
import sleeper.bulkimport.starter.executor.persistent.ReturnBulkImportJobToQueue;
import sleeper.core.properties.instance.InstanceProperties;
import sleeper.localstack.test.LocalStackTestBase;

import java.util.List;

import static org.assertj.core.api.Assertions.assertThat;
import static sleeper.core.properties.instance.CdkDefinedInstanceProperty.BULK_IMPORT_PERSISTENT_EMR_JOB_QUEUE_URL;
import static sleeper.core.properties.instance.PersistentEMRProperty.BULK_IMPORT_PERSISTENT_EMR_REQUEUE_DELAY_SECONDS;
import static sleeper.core.properties.testutils.InstancePropertiesTestHelper.createTestInstanceProperties;

public class ReturnBulkImportJobToQueueIT extends LocalStackTestBase {

    InstanceProperties instanceProperties = createTestInstanceProperties();

    @BeforeEach
    void setUp() {
        instanceProperties.set(BULK_IMPORT_PERSISTENT_EMR_JOB_QUEUE_URL, createSqsQueueGetUrl());
        instanceProperties.set(BULK_IMPORT_PERSISTENT_EMR_REQUEUE_DELAY_SECONDS, "1");
    }

    @Test
    void shouldSendJobWithDelay() {
        // Given
        BulkImportJob job = BulkImportJob.builder()
                .tableName("test-table")
                .id("test-job")
                .files(List.of("file.parquet"))
                .build();

        // When
        ReturnBulkImportJobToQueue.forFullPersistentEmrCluster(instanceProperties, sqsClientV2)
                .send(job);

        // Then
        assertThat(receiveBulkImportJobsWithWaitTimeSeconds(0))
                .isEmpty();
        assertThat(receiveBulkImportJobsWithWaitTimeSeconds(2))
                .containsExactly(job);
    }

    private List<BulkImportJob> receiveBulkImportJobsWithWaitTimeSeconds(int waitTimeSeconds) {
        return sqsClientV2.receiveMessage(
                ReceiveMessageRequest.builder()
                        .queueUrl(instanceProperties.get(BULK_IMPORT_PERSISTENT_EMR_JOB_QUEUE_URL))
                        .waitTimeSeconds(waitTimeSeconds)
                        .build())
                .messages().stream()
                .map(Message::body)
                .map(new BulkImportJobSerDe()::fromJson)
                .toList();
    }

}
