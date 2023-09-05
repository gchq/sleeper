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
package sleeper.systemtest.datageneration;

import com.amazonaws.services.sqs.AmazonSQS;
import com.amazonaws.services.sqs.AmazonSQSClientBuilder;
import com.amazonaws.services.sqs.model.SendMessageRequest;
import org.slf4j.Logger;
import org.slf4j.LoggerFactory;

import sleeper.bulkimport.job.BulkImportJob;
import sleeper.bulkimport.job.BulkImportJobSerDe;
import sleeper.configuration.properties.instance.InstanceProperties;
import sleeper.configuration.properties.table.TableProperties;
import sleeper.core.record.Record;
import sleeper.ingest.job.IngestJob;
import sleeper.ingest.job.IngestJobSerDe;
import sleeper.systemtest.configuration.IngestMode;
import sleeper.systemtest.configuration.SystemTestPropertyValues;

import java.io.IOException;
import java.util.Collections;
import java.util.Iterator;

import static sleeper.configuration.properties.instance.SystemDefinedInstanceProperty.BULK_IMPORT_EMR_JOB_QUEUE_URL;
import static sleeper.configuration.properties.instance.SystemDefinedInstanceProperty.INGEST_JOB_QUEUE_URL;
import static sleeper.configuration.properties.table.TableProperty.TABLE_NAME;

public class WriteRandomDataViaQueue {
    private static final Logger LOGGER = LoggerFactory.getLogger(WriteRandomDataViaQueue.class);

    private WriteRandomDataViaQueue() {
    }

    public static void writeAndSendToQueue(
            String ingestMode, InstanceProperties instanceProperties, TableProperties tableProperties,
            SystemTestPropertyValues systemTestProperties) throws IOException {
        Iterator<Record> recordIterator = WriteRandomData.createRecordIterator(systemTestProperties, tableProperties);
        String dir = WriteRandomDataFiles.writeToS3GetDirectory(instanceProperties, tableProperties, recordIterator);
        int jobDirIndex = dir.lastIndexOf('/');
        String jobId = dir.substring(jobDirIndex + 1);

        AmazonSQS sqsClient = AmazonSQSClientBuilder.defaultClient();

        SendMessageRequest sendMessageRequest;
        if (ingestMode.equalsIgnoreCase(IngestMode.QUEUE.name())) {
            IngestJob ingestJob = IngestJob.builder()
                    .tableName(tableProperties.get(TABLE_NAME))
                    .id(jobId)
                    .files(Collections.singletonList(dir))
                    .build();
            String jsonJob = new IngestJobSerDe().toJson(ingestJob);
            LOGGER.debug("Sending message to ingest queue ({})", jsonJob);
            sendMessageRequest = new SendMessageRequest()
                    .withQueueUrl(instanceProperties.get(INGEST_JOB_QUEUE_URL))
                    .withMessageBody(jsonJob);
        } else if (ingestMode.equalsIgnoreCase(IngestMode.BULK_IMPORT_QUEUE.name())) {
            BulkImportJob bulkImportJob = new BulkImportJob.Builder()
                    .tableName(tableProperties.get(TABLE_NAME))
                    .id(jobId)
                    .files(Collections.singletonList(dir))
                    .build();
            String jsonJob = new BulkImportJobSerDe().toJson(bulkImportJob);
            LOGGER.debug("Sending message to ingest queue ({})", jsonJob);
            sendMessageRequest = new SendMessageRequest()
                    .withQueueUrl(instanceProperties.get(BULK_IMPORT_EMR_JOB_QUEUE_URL))
                    .withMessageBody(jsonJob);
        } else {
            throw new IllegalArgumentException("Unknown ingest mode of " + ingestMode);
        }

        sqsClient.sendMessage(sendMessageRequest);
        sqsClient.shutdown();
    }
}
