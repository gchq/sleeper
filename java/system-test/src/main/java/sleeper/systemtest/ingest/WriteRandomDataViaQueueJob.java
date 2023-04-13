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
package sleeper.systemtest.ingest;

import com.amazonaws.services.sqs.AmazonSQS;
import com.amazonaws.services.sqs.AmazonSQSClientBuilder;
import com.amazonaws.services.sqs.model.SendMessageRequest;
import org.apache.hadoop.conf.Configuration;
import org.apache.hadoop.fs.Path;
import org.apache.parquet.hadoop.ParquetWriter;
import org.slf4j.Logger;
import org.slf4j.LoggerFactory;

import sleeper.bulkimport.job.BulkImportJob;
import sleeper.bulkimport.job.BulkImportJobSerDe;
import sleeper.configuration.jars.ObjectFactory;
import sleeper.configuration.properties.table.TableProperties;
import sleeper.core.record.Record;
import sleeper.ingest.job.IngestJob;
import sleeper.ingest.job.IngestJobSerDe;
import sleeper.io.parquet.record.ParquetRecordWriterFactory;
import sleeper.statestore.StateStoreProvider;
import sleeper.systemtest.SystemTestProperties;

import java.io.IOException;
import java.util.Arrays;
import java.util.Collections;
import java.util.Iterator;
import java.util.UUID;

import static sleeper.configuration.properties.SystemDefinedInstanceProperty.BULK_IMPORT_EMR_JOB_QUEUE_URL;
import static sleeper.configuration.properties.SystemDefinedInstanceProperty.INGEST_JOB_QUEUE_URL;
import static sleeper.configuration.properties.table.TableProperty.DATA_BUCKET;
import static sleeper.configuration.properties.table.TableProperty.TABLE_NAME;

public class WriteRandomDataViaQueueJob extends WriteRandomDataJob {
    private static final Logger LOGGER = LoggerFactory.getLogger(WriteRandomDataViaQueueJob.class);

    private final String ingestMode;

    public WriteRandomDataViaQueueJob(
            String ingestMode,
            ObjectFactory objectFactory,
            SystemTestProperties properties,
            TableProperties tableProperties,
            StateStoreProvider stateStoreProvider) {
        super(objectFactory, properties, tableProperties, stateStoreProvider);
        this.ingestMode = ingestMode;
    }

    @Override
    public void run() throws IOException {
        Iterator<Record> recordIterator = createRecordIterator(getTableProperties().getSchema());

        int fileNumber = 0;
        String dir = getTableProperties().get(DATA_BUCKET) + "/ingest/" + UUID.randomUUID() + "/";
        String filename = dir + fileNumber + ".parquet";
        String path = "s3a://" + filename;

        Configuration conf = new Configuration();
        conf.set("fs.s3a.aws.credentials.provider", "com.amazonaws.auth.EC2ContainerCredentialsProviderWrapper");
        conf.set("fs.s3a.fast.upload", "true");

        ParquetWriter<Record> writer = ParquetRecordWriterFactory.createParquetRecordWriter(new Path(path), getTableProperties(), conf);
        long count = 0L;
        LOGGER.info("Created writer to path {}", path);
        while (recordIterator.hasNext()) {
            writer.write(recordIterator.next());
            count++;
            if (0 == count % 1_000_000L) {
                LOGGER.info("Wrote {} records", count);
                if (0 == count % 100_000_000L) {
                    writer.close();
                    LOGGER.info("Closed writer to path {}", path);
                    fileNumber++;
                    filename = dir + fileNumber + ".parquet";
                    path = "s3a://" + filename;
                    writer = ParquetRecordWriterFactory.createParquetRecordWriter(new Path(path), getTableProperties(), conf);
                }
            }
        }
        LOGGER.info("Closed writer to path {}", path);
        writer.close();
        LOGGER.info("Wrote {} records", count);

        AmazonSQS sqsClient = AmazonSQSClientBuilder.defaultClient();

        SendMessageRequest sendMessageRequest;
        if (ingestMode.equalsIgnoreCase(IngestMode.QUEUE.name())) {
            IngestJob ingestJob = IngestJob.builder()
                    .tableName(getTableProperties().get(TABLE_NAME))
                    .id(UUID.randomUUID().toString())
                    .files(Collections.singletonList(filename))
                    .build();
            String jsonJob = new IngestJobSerDe().toJson(ingestJob);
            LOGGER.debug("Sending message to ingest queue ({})", jsonJob);
            sendMessageRequest = new SendMessageRequest()
                    .withQueueUrl(getSystemTestProperties().get(INGEST_JOB_QUEUE_URL))
                    .withMessageBody(jsonJob);
        } else if (ingestMode.equalsIgnoreCase(IngestMode.BULK_IMPORT_QUEUE.name())) {
            BulkImportJob bulkImportJob = new BulkImportJob.Builder()
                    .tableName(getTableProperties().get(TABLE_NAME))
                    .id(UUID.randomUUID().toString())
                    .files(Arrays.asList(dir))
                    .build();
            String jsonJob = new BulkImportJobSerDe().toJson(bulkImportJob);
            LOGGER.debug("Sending message to ingest queue ({})", jsonJob);
            sendMessageRequest = new SendMessageRequest()
                    .withQueueUrl(getSystemTestProperties().get(BULK_IMPORT_EMR_JOB_QUEUE_URL))
                    .withMessageBody(jsonJob);
        } else {
            throw new IllegalArgumentException("Unknown ingest mode of " + ingestMode);
        }

        sqsClient.sendMessage(sendMessageRequest);
        sqsClient.shutdown();
    }
}
