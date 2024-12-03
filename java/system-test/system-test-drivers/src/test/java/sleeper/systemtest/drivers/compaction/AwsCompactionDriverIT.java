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
package sleeper.systemtest.drivers.compaction;

import org.junit.jupiter.api.BeforeEach;
import org.junit.jupiter.api.Test;
import software.amazon.awssdk.services.sqs.SqsClient;
import software.amazon.awssdk.services.sqs.model.SendMessageBatchRequestEntry;

import sleeper.compaction.core.job.CompactionJob;
import sleeper.compaction.core.job.CompactionJobFactory;
import sleeper.compaction.core.job.CompactionJobSerDe;
import sleeper.core.properties.instance.InstanceProperties;
import sleeper.core.statestore.FileReferenceFactory;
import sleeper.core.util.SplitIntoBatches;
import sleeper.systemtest.drivers.testutil.LocalStackDslTest;
import sleeper.systemtest.drivers.testutil.LocalStackSystemTestDrivers;
import sleeper.systemtest.dsl.SleeperSystemTest;
import sleeper.systemtest.dsl.SystemTestContext;
import sleeper.systemtest.dsl.compaction.CompactionDriver;
import sleeper.systemtest.dsl.instance.SystemTestInstanceContext;

import java.util.List;
import java.util.stream.IntStream;

import static org.assertj.core.api.Assertions.assertThat;
import static sleeper.core.properties.instance.CdkDefinedInstanceProperty.COMPACTION_JOB_QUEUE_URL;
import static sleeper.core.properties.instance.CommonProperty.ID;
import static sleeper.systemtest.drivers.testutil.LocalStackTestInstance.MAIN;

@LocalStackDslTest
public class AwsCompactionDriverIT {

    SqsClient sqs;
    CompactionDriver driver;
    SystemTestInstanceContext instance;

    @BeforeEach
    void setUp(SleeperSystemTest sleeper, SystemTestContext context, LocalStackSystemTestDrivers drivers) {
        sleeper.connectToInstance(MAIN);
        sqs = drivers.clients().getSqsV2();
        driver = drivers.compaction(context);
        instance = context.instance();
        createCompactionQueue();
    }

    @Test
    void shouldDrainCompactionJobsFromQueue(SleeperSystemTest sleeper) {
        // Given
        FileReferenceFactory fileFactory = FileReferenceFactory.from(instance.getStateStore());
        CompactionJobFactory jobFactory = new CompactionJobFactory(instance.getInstanceProperties(), instance.getTableProperties());
        List<CompactionJob> jobs = IntStream.rangeClosed(1, 20)
                .mapToObj(i -> jobFactory.createCompactionJob(
                        List.of(fileFactory.rootFile("file" + i + ".parquet", i)), "root"))
                .toList();
        send(jobs);

        // When / Then
        assertThat(driver.drainJobsQueueForWholeInstance())
                .containsExactlyElementsOf(jobs);
    }

    private void send(List<CompactionJob> jobs) {
        InstanceProperties instanceProperties = instance.getInstanceProperties();
        CompactionJobSerDe serDe = new CompactionJobSerDe();
        for (List<CompactionJob> batch : SplitIntoBatches.splitListIntoBatchesOf(10, jobs)) {
            sqs.sendMessageBatch(request -> request
                    .queueUrl(instanceProperties.get(COMPACTION_JOB_QUEUE_URL))
                    .entries(batch.stream()
                            .map(job -> SendMessageBatchRequestEntry.builder()
                                    .messageBody(serDe.toJson(job))
                                    .id(job.getId())
                                    .build())
                            .toList()));
        }
    }

    private void createCompactionQueue() {
        InstanceProperties instanceProperties = instance.getInstanceProperties();
        String queueName = String.join("-", "sleeper", instanceProperties.get(ID), "CompactionJobQ");
        String queueUrl = sqs.createQueue(request -> request.queueName(queueName)).queueUrl();
        instanceProperties.set(COMPACTION_JOB_QUEUE_URL, queueUrl);
    }

}
