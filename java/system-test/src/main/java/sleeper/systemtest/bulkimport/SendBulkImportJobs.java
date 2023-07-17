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
package sleeper.systemtest.bulkimport;

import com.amazonaws.services.s3.AmazonS3;
import com.amazonaws.services.s3.AmazonS3ClientBuilder;
import com.amazonaws.services.s3.iterable.S3Objects;
import com.amazonaws.services.s3.model.S3ObjectSummary;
import com.amazonaws.services.sqs.AmazonSQS;
import com.amazonaws.services.sqs.AmazonSQSClientBuilder;
import com.amazonaws.services.sqs.model.SendMessageRequest;
import org.slf4j.Logger;
import org.slf4j.LoggerFactory;

import sleeper.bulkimport.job.BulkImportJob;
import sleeper.bulkimport.job.BulkImportJobSerDe;
import sleeper.configuration.properties.InstanceProperty;
import sleeper.systemtest.SystemTestProperties;

import java.io.IOException;
import java.util.ArrayList;
import java.util.List;
import java.util.UUID;

import static sleeper.configuration.properties.UserDefinedInstanceProperty.INGEST_SOURCE_BUCKET;
import static sleeper.systemtest.SystemTestProperty.BULK_IMPORT_QUEUE_PROPERTY;
import static sleeper.systemtest.SystemTestProperty.NUMBER_OF_BULK_IMPORT_JOBS;

public class SendBulkImportJobs {
    private static final Logger LOGGER = LoggerFactory.getLogger(SendBulkImportJobs.class);

    private SendBulkImportJobs() {
    }

    public static void main(String[] args) throws IOException {

        AmazonS3 s3Client = AmazonS3ClientBuilder.defaultClient();
        SystemTestProperties systemTestProperties = new SystemTestProperties();
        systemTestProperties.loadFromS3GivenInstanceId(s3Client, args[0]);
        String tableName = args[1];

        List<String> files = new ArrayList<>();
        for (S3ObjectSummary object : S3Objects.withPrefix(
                s3Client, systemTestProperties.getList(INGEST_SOURCE_BUCKET).get(0), "ingest/")) {
            files.add(object.getBucketName() + "/" + object.getKey());
        }

        int sendCopies = systemTestProperties.getInt(NUMBER_OF_BULK_IMPORT_JOBS);
        LOGGER.info("Sending {} copies", sendCopies);

        AmazonSQS sqsClient = AmazonSQSClientBuilder.defaultClient();
        for (int i = 0; i < sendCopies; i++) {
            BulkImportJob bulkImportJob = new BulkImportJob.Builder()
                    .tableName(tableName)
                    .id(UUID.randomUUID().toString())
                    .files(files)
                    .build();
            String jsonJob = new BulkImportJobSerDe().toJson(bulkImportJob);
            String queueUrl = systemTestProperties.get(InstanceProperty.getByName(
                    systemTestProperties.get(BULK_IMPORT_QUEUE_PROPERTY)).orElseThrow());
            LOGGER.info("Sending message to bulk import queue {} ({})", queueUrl, jsonJob);
            sqsClient.sendMessage(new SendMessageRequest()
                    .withQueueUrl(queueUrl)
                    .withMessageBody(jsonJob));
        }
        sqsClient.shutdown();
    }
}
