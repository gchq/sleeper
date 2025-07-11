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

import org.slf4j.Logger;
import org.slf4j.LoggerFactory;
import software.amazon.awssdk.services.sqs.SqsClient;
import software.amazon.awssdk.services.sqs.model.SendMessageRequest;

import sleeper.ingest.core.job.IngestJob;
import sleeper.ingest.core.job.IngestJobSerDe;
import sleeper.systemtest.drivers.util.SystemTestClients;
import sleeper.systemtest.dsl.ingest.IngestByQueueDriver;

import java.util.List;
import java.util.UUID;

public class AwsIngestByQueueDriver implements IngestByQueueDriver {
    private static final Logger LOGGER = LoggerFactory.getLogger(AwsIngestByQueueDriver.class);

    private final SqsClient sqsClient;

    public AwsIngestByQueueDriver(SystemTestClients clients) {
        this.sqsClient = clients.getSqs();
    }

    public String sendJobGetId(String queueUrl, String tableName, List<String> files) {
        String jobId = UUID.randomUUID().toString();
        LOGGER.info("Sending ingest job {} with {} files to queue: {}", jobId, files.size(), queueUrl);
        sqsClient.sendMessage(SendMessageRequest.builder()
                .queueUrl(queueUrl)
                .messageBody(new IngestJobSerDe().toJson(IngestJob.builder()
                        .id(jobId)
                        .tableName(tableName)
                        .files(files)
                        .build()))
                .build());
        return jobId;
    }
}
