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

package sleeper.systemtest.drivers.ingest;

import com.amazonaws.services.dynamodbv2.AmazonDynamoDB;
import com.amazonaws.services.sqs.AmazonSQS;
import org.slf4j.Logger;
import org.slf4j.LoggerFactory;

import sleeper.ingest.batcher.core.IngestBatcherStore;
import sleeper.ingest.batcher.store.DynamoDBIngestBatcherStore;
import sleeper.ingest.batcher.submitter.FileIngestRequestSerDe;
import sleeper.systemtest.drivers.util.SystemTestClients;
import sleeper.systemtest.dsl.ingest.IngestBatcherDriver;
import sleeper.systemtest.dsl.instance.SystemTestInstanceContext;
import sleeper.systemtest.dsl.sourcedata.IngestSourceFilesContext;

import java.util.List;

import static sleeper.core.properties.instance.CdkDefinedInstanceProperty.INGEST_BATCHER_SUBMIT_QUEUE_URL;
import static sleeper.core.properties.table.TableProperty.TABLE_NAME;

public class AwsIngestBatcherDriver implements IngestBatcherDriver {
    private static final Logger LOGGER = LoggerFactory.getLogger(AwsIngestBatcherDriver.class);

    private final SystemTestInstanceContext instance;
    private final AmazonDynamoDB dynamoDBClient;
    private final AmazonSQS sqsClient;

    public AwsIngestBatcherDriver(
            SystemTestInstanceContext instance,
            IngestSourceFilesContext sourceFiles,
            SystemTestClients clients) {
        this.instance = instance;
        this.dynamoDBClient = clients.getDynamoDB();
        this.sqsClient = clients.getSqs();
    }

    @Override
    public void sendFiles(List<String> files) {
        LOGGER.info("Sending {} files to ingest batcher queue", files.size());
        sqsClient.sendMessage(instance.getInstanceProperties().get(INGEST_BATCHER_SUBMIT_QUEUE_URL),
                FileIngestRequestSerDe.toJson(files,
                        instance.getTableProperties().get(TABLE_NAME)));
    }

    @Override
    public IngestBatcherStore batcherStore() {
        return new DynamoDBIngestBatcherStore(dynamoDBClient,
                instance.getInstanceProperties(), instance.getTablePropertiesProvider());
    }
}
