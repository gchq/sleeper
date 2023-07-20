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

package sleeper.systemtest.drivers.ingest;

import com.amazonaws.services.sqs.AmazonSQS;
import org.slf4j.Logger;
import org.slf4j.LoggerFactory;
import software.amazon.awssdk.services.lambda.LambdaClient;

import sleeper.clients.deploy.InvokeLambda;
import sleeper.configuration.properties.instance.InstanceProperties;
import sleeper.configuration.properties.table.TableProperties;
import sleeper.core.util.PollWithRetries;
import sleeper.ingest.batcher.FileIngestRequest;
import sleeper.ingest.batcher.IngestBatcherStore;
import sleeper.ingest.batcher.submitter.FileIngestRequestSerDe;
import sleeper.systemtest.drivers.ingest.batcher.SendFilesToIngestBatcher;

import java.util.List;

import static sleeper.configuration.properties.instance.SystemDefinedInstanceProperty.INGEST_BATCHER_JOB_CREATION_FUNCTION;
import static sleeper.configuration.properties.instance.SystemDefinedInstanceProperty.INGEST_BATCHER_SUBMIT_QUEUE_URL;
import static sleeper.configuration.properties.table.TableProperty.TABLE_NAME;

public class IngestBatcherDriver {
    private static final Logger LOGGER = LoggerFactory.getLogger(SendFilesToIngestBatcher.class);

    private final IngestBatcherStore batcherStore;
    private final AmazonSQS sqs;
    private final LambdaClient lambda;
    private final PollWithRetries pollBatcherStore = PollWithRetries
            .intervalAndPollingTimeout(5000, 1000L * 60L * 2L);

    public IngestBatcherDriver(IngestBatcherStore batcherStore, AmazonSQS sqs, LambdaClient lambda) {
        this.batcherStore = batcherStore;
        this.sqs = sqs;
        this.lambda = lambda;
    }

    public void sendFiles(
            InstanceProperties properties, TableProperties tableProperties,
            String bucketName, List<String> files) throws InterruptedException {
        LOGGER.info("Sending {} files to ingest batcher queue", files.size());
        int filesBefore = batcherStore.getPendingFilesOldestFirst().size();
        int filesAfter = filesBefore + files.size();
        sqs.sendMessage(properties.get(INGEST_BATCHER_SUBMIT_QUEUE_URL),
                FileIngestRequestSerDe.toJson(bucketName, files, tableProperties.get(TABLE_NAME)));
        pollBatcherStore.pollUntil("files appear in batcher store", () -> {
            List<FileIngestRequest> pending = batcherStore.getPendingFilesOldestFirst();
            LOGGER.info("Found pending files in batcher store: {}", pending);
            return pending.size() == filesAfter;
        });
    }

    public void invoke(InstanceProperties properties) {
        LOGGER.info("Triggering ingest batcher job creation lambda");
        InvokeLambda.invokeWith(lambda, properties.get(INGEST_BATCHER_JOB_CREATION_FUNCTION));
    }
}
