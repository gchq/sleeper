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
package sleeper.systemtest.drivers.ingest.batcher;

import com.amazonaws.services.sqs.AmazonSQS;
import org.apache.hadoop.conf.Configuration;
import org.apache.parquet.hadoop.ParquetWriter;
import org.slf4j.Logger;
import org.slf4j.LoggerFactory;
import software.amazon.awssdk.services.lambda.LambdaClient;

import sleeper.clients.deploy.InvokeLambda;
import sleeper.configuration.properties.InstanceProperties;
import sleeper.configuration.properties.table.TableProperties;
import sleeper.core.record.Record;
import sleeper.core.util.PollWithRetries;
import sleeper.ingest.batcher.FileIngestRequest;
import sleeper.ingest.batcher.IngestBatcherStore;
import sleeper.ingest.batcher.submitter.FileIngestRequestSerDe;
import sleeper.io.parquet.record.ParquetRecordWriterFactory;
import sleeper.systemtest.datageneration.RandomRecordSupplier;

import java.io.IOException;
import java.util.List;

import static sleeper.configuration.properties.SystemDefinedInstanceProperty.INGEST_BATCHER_JOB_CREATION_FUNCTION;
import static sleeper.configuration.properties.SystemDefinedInstanceProperty.INGEST_BATCHER_SUBMIT_QUEUE_URL;

public class SendFilesToIngestBatcher {

    private static final Logger LOGGER = LoggerFactory.getLogger(SendFilesToIngestBatcher.class);


    private final InstanceProperties instanceProperties;
    private final TableProperties tableProperties;
    private final String sourceBucketName;
    private final IngestBatcherStore batcherStore;
    private final AmazonSQS sqs;
    private final LambdaClient lambda;

    public SendFilesToIngestBatcher(
            InstanceProperties instanceProperties, TableProperties tableProperties, String sourceBucketName,
            IngestBatcherStore batcherStore, AmazonSQS sqs, LambdaClient lambda) {
        this.instanceProperties = instanceProperties;
        this.tableProperties = tableProperties;
        this.sourceBucketName = sourceBucketName;
        this.batcherStore = batcherStore;
        this.sqs = sqs;
        this.lambda = lambda;
    }

    public void writeFilesAndSendToBatcher(List<String> files)
            throws IOException, InterruptedException {
        LOGGER.info("Writing test ingest files to {}", sourceBucketName);
        for (String file : files) {
            writeFileWithRecords(tableProperties, sourceBucketName + "/" + file, 100);
        }
        sendFilesAndTriggerJobCreation(instanceProperties, sourceBucketName, files);
    }

    private void writeFileWithRecords(TableProperties tableProperties, String filePath, int numRecords) throws IOException {
        try (ParquetWriter<Record> writer = ParquetRecordWriterFactory.createParquetRecordWriter(
                new org.apache.hadoop.fs.Path("s3a://" + filePath), tableProperties, new Configuration())) {
            RandomRecordSupplier supplier = new RandomRecordSupplier(tableProperties.getSchema());
            for (int i = 0; i < numRecords; i++) {
                writer.write(supplier.get());
            }
        }
    }

    private void sendFilesAndTriggerJobCreation(
            InstanceProperties properties, String sourceBucketName, List<String> files) throws InterruptedException {
        LOGGER.info("Sending {} files to ingest batcher queue", files.size());
        sqs.sendMessage(properties.get(INGEST_BATCHER_SUBMIT_QUEUE_URL),
                FileIngestRequestSerDe.toJson(sourceBucketName, files, "system-test"));
        PollWithRetries.intervalAndPollingTimeout(5000, 1000L * 60L * 2L)
                .pollUntil("files appear in batcher store", () -> {
                    List<FileIngestRequest> pending = batcherStore.getPendingFilesOldestFirst();
                    LOGGER.info("Found pending files in batcher store: {}", pending);
                    return pending.size() == files.size();
                });
        LOGGER.info("Triggering ingest batcher job creation lambda");
        InvokeLambda.invokeWith(lambda, properties.get(INGEST_BATCHER_JOB_CREATION_FUNCTION));
    }

}
