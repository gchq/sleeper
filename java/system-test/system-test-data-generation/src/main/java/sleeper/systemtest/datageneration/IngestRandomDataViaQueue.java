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
package sleeper.systemtest.datageneration;

import com.amazonaws.services.sqs.AmazonSQS;
import com.amazonaws.services.sqs.AmazonSQSClientBuilder;
import org.slf4j.Logger;
import org.slf4j.LoggerFactory;

import sleeper.configuration.properties.instance.InstanceProperties;
import sleeper.configuration.properties.table.TableProperties;
import sleeper.configuration.properties.validation.IngestQueue;
import sleeper.ingest.job.IngestJob;
import sleeper.ingest.job.IngestJobSerDe;
import sleeper.systemtest.configuration.SystemTestPropertyValues;

import java.util.Collections;

import static sleeper.configuration.properties.table.TableProperty.TABLE_NAME;
import static sleeper.systemtest.configuration.SystemTestProperty.INGEST_QUEUE;

public class IngestRandomDataViaQueue {
    private static final Logger LOGGER = LoggerFactory.getLogger(IngestRandomDataViaQueue.class);

    private IngestRandomDataViaQueue() {
    }

    public static void sendJob(
            String jobId, String dir, InstanceProperties instanceProperties, TableProperties tableProperties,
            SystemTestPropertyValues systemTestProperties) {

        AmazonSQS sqsClient = AmazonSQSClientBuilder.defaultClient();

        IngestJob ingestJob = IngestJob.builder()
                .tableName(tableProperties.get(TABLE_NAME))
                .id(jobId)
                .files(Collections.singletonList(dir))
                .build();
        String jsonJob = new IngestJobSerDe().toJson(ingestJob);
        IngestQueue ingestQueue = systemTestProperties.getEnumValue(INGEST_QUEUE, IngestQueue.class);
        String queueUrl = ingestQueue.getJobQueueUrl(instanceProperties);
        LOGGER.debug("Sending message to ingest queue {}: {}", queueUrl, jsonJob);
        sqsClient.sendMessage(queueUrl, jsonJob);
        sqsClient.shutdown();
    }
}
