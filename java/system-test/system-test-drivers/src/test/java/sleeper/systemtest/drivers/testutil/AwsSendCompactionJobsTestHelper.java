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
package sleeper.systemtest.drivers.testutil;

import org.slf4j.Logger;
import org.slf4j.LoggerFactory;
import software.amazon.awssdk.services.sqs.SqsClient;
import software.amazon.awssdk.services.sqs.model.SendMessageBatchRequestEntry;

import sleeper.compaction.core.job.CompactionJob;
import sleeper.compaction.core.job.CompactionJobFactory;
import sleeper.compaction.core.job.CompactionJobSerDe;
import sleeper.core.properties.instance.InstanceProperties;
import sleeper.core.properties.table.TableProperties;
import sleeper.core.statestore.FileReferenceFactory;
import sleeper.core.statestore.StateStore;
import sleeper.core.util.SplitIntoBatches;

import java.util.List;
import java.util.stream.IntStream;

import static sleeper.core.properties.instance.CdkDefinedInstanceProperty.COMPACTION_JOB_QUEUE_URL;

public class AwsSendCompactionJobsTestHelper {
    public static final Logger LOGGER = LoggerFactory.getLogger(AwsSendCompactionJobsTestHelper.class);

    private AwsSendCompactionJobsTestHelper() {
    }

    public static List<CompactionJob> sendNCompactionJobs(int n, InstanceProperties instanceProperties, TableProperties tableProperties, StateStore stateStore, SqsClient sqs) {

        FileReferenceFactory fileFactory = FileReferenceFactory.from(stateStore);
        CompactionJobFactory jobFactory = new CompactionJobFactory(instanceProperties, tableProperties);
        List<CompactionJob> jobs = IntStream.rangeClosed(1, n)
                .mapToObj(i -> jobFactory.createCompactionJob(
                        List.of(fileFactory.rootFile("file" + i + ".parquet", i)), "root"))
                .toList();
        send(instanceProperties, sqs, jobs);
        return jobs;
    }

    private static void send(InstanceProperties instanceProperties, SqsClient sqs, List<CompactionJob> jobs) {
        String queueUrl = instanceProperties.get(COMPACTION_JOB_QUEUE_URL);
        LOGGER.info("Sending to compaction jobs queue: {}", queueUrl);
        CompactionJobSerDe serDe = new CompactionJobSerDe();
        for (List<CompactionJob> batch : SplitIntoBatches.splitListIntoBatchesOf(10, jobs)) {
            sqs.sendMessageBatch(request -> request
                    .queueUrl(queueUrl)
                    .entries(batch.stream()
                            .map(job -> SendMessageBatchRequestEntry.builder()
                                    .messageBody(serDe.toJson(job))
                                    .id(job.getId())
                                    .build())
                            .toList()));
        }
    }

}
