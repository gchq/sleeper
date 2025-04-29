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
package sleeper.systemtest.drivers.compaction;

import org.junit.jupiter.api.BeforeEach;
import org.junit.jupiter.api.Test;
import org.slf4j.Logger;
import org.slf4j.LoggerFactory;
import software.amazon.awssdk.services.s3.S3Client;
import software.amazon.awssdk.services.sqs.SqsClient;
import software.amazon.awssdk.services.sqs.model.Message;

import sleeper.compaction.core.job.CompactionJob;
import sleeper.compaction.core.job.CompactionJobFactory;
import sleeper.compaction.core.job.CompactionJobSerDe;
import sleeper.compaction.core.job.dispatch.CompactionJobDispatchRequest;
import sleeper.compaction.core.job.dispatch.CompactionJobDispatchRequestSerDe;
import sleeper.core.partition.PartitionTree;
import sleeper.core.statestore.FileReference;
import sleeper.core.statestore.FileReferenceFactory;
import sleeper.systemtest.drivers.testutil.AwsSendCompactionJobsTestHelper;
import sleeper.systemtest.drivers.testutil.LocalStackDslTest;
import sleeper.systemtest.drivers.testutil.LocalStackSystemTestDrivers;
import sleeper.systemtest.drivers.util.sqs.AwsDrainSqsQueue;
import sleeper.systemtest.dsl.SleeperSystemTest;
import sleeper.systemtest.dsl.SystemTestContext;
import sleeper.systemtest.dsl.compaction.CompactionDriver;
import sleeper.systemtest.dsl.instance.SystemTestInstanceContext;

import java.util.HashSet;
import java.util.List;

import static org.assertj.core.api.Assertions.assertThat;
import static sleeper.core.properties.instance.CdkDefinedInstanceProperty.COMPACTION_PENDING_DLQ_URL;
import static sleeper.core.properties.instance.CdkDefinedInstanceProperty.COMPACTION_PENDING_QUEUE_URL;
import static sleeper.core.properties.instance.CdkDefinedInstanceProperty.DATA_BUCKET;
import static sleeper.localstack.test.LocalStackTestBase.createSqsQueueGetUrl;
import static sleeper.systemtest.drivers.testutil.LocalStackTestInstance.DRAIN_COMPACTIONS;

@LocalStackDslTest
public class AwsCompactionDriverIT {
    public static final Logger LOGGER = LoggerFactory.getLogger(AwsCompactionDriverIT.class);

    S3Client s3;
    SqsClient sqs;
    CompactionDriver driver;
    SystemTestInstanceContext instance;

    @BeforeEach
    void setUp(SleeperSystemTest sleeper, SystemTestContext context, LocalStackSystemTestDrivers drivers) {
        sleeper.connectToInstanceAddOfflineTable(DRAIN_COMPACTIONS);
        s3 = drivers.clients().getS3V2();
        sqs = drivers.clients().getSqsV2();
        driver = drivers.compaction(context);
        instance = context.instance();
    }

    @Test
    void shouldDrainCompactionJobsFromQueue() {
        // Given
        List<CompactionJob> jobs = AwsSendCompactionJobsTestHelper.sendNCompactionJobs(20,
                instance.getInstanceProperties(), instance.getTableProperties(), instance.getStateStore(), sqs);

        // When / Then
        assertThat(new HashSet<>(driver.drainJobsQueueForWholeInstance(20)))
                .isEqualTo(new HashSet<>(jobs));
    }

    @Test
    void shouldSendCompactionBatch() {
        // Given
        instance.getInstanceProperties().set(COMPACTION_PENDING_QUEUE_URL, createSqsQueueGetUrl());
        FileReference inputFile = fileReferenceFactory().rootFile("test", 100);
        CompactionJob job = createCompactionJob(List.of(inputFile));

        // When
        String batchKey = driver.sendCompactionBatchGetKey(List.of(job));

        // Then
        assertThat(drainPendingCompactionQueue())
                .extracting(CompactionJobDispatchRequest::getBatchKey)
                .containsExactly(batchKey);
        assertThat(readCompactionBatch(batchKey)).containsExactly(job);
    }

    @Test
    void shouldDrainCompactionBatchFromDLQ() {
        // Given
        String queueUrl = createSqsQueueGetUrl();
        instance.getInstanceProperties().set(COMPACTION_PENDING_QUEUE_URL, queueUrl);
        instance.getInstanceProperties().set(COMPACTION_PENDING_DLQ_URL, queueUrl);
        FileReference inputFile = fileReferenceFactory().rootFile("test", 100);
        CompactionJob job = createCompactionJob(List.of(inputFile));
        String batchKey = driver.sendCompactionBatchGetKey(List.of(job));

        // When
        List<CompactionJobDispatchRequest> requests = driver.drainPendingDeadLetterQueueForWholeInstance();

        // Then
        assertThat(requests)
                .extracting(CompactionJobDispatchRequest::getBatchKey)
                .containsExactly(batchKey);
    }

    private FileReferenceFactory fileReferenceFactory() {
        return FileReferenceFactory.from(instance.getInstanceProperties(), instance.getTableProperties(), new PartitionTree(instance.getStateStore().getAllPartitions()));
    }

    private CompactionJob createCompactionJob(List<FileReference> files) {
        CompactionJobFactory jobFactory = new CompactionJobFactory(instance.getInstanceProperties(), instance.getTableProperties());
        return jobFactory.createCompactionJob(files, files.get(0).getPartitionId());
    }

    private List<CompactionJobDispatchRequest> drainPendingCompactionQueue() {
        CompactionJobDispatchRequestSerDe serDe = new CompactionJobDispatchRequestSerDe();
        return AwsDrainSqsQueue.forLocalStackTests(sqs)
                .drain(instance.getInstanceProperties().get(COMPACTION_PENDING_QUEUE_URL))
                .map(Message::body)
                .map(serDe::fromJson)
                .toList();
    }

    private List<CompactionJob> readCompactionBatch(String batchKey) {
        String batchJson = s3.getObjectAsBytes(builder -> builder
                .bucket(instance.getInstanceProperties().get(DATA_BUCKET))
                .key(batchKey))
                .asUtf8String();
        CompactionJobSerDe serDe = new CompactionJobSerDe();
        return serDe.batchFromJson(batchJson);
    }

}
