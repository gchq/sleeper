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
import software.amazon.awssdk.services.dynamodb.DynamoDbClient;
import software.amazon.awssdk.services.sqs.SqsClient;
import software.amazon.awssdk.services.sqs.model.SendMessageRequest;

import sleeper.ingest.batcher.core.IngestBatcherStore;
import sleeper.ingest.batcher.core.IngestBatcherSubmitRequest;
import sleeper.ingest.batcher.core.IngestBatcherSubmitRequestSerDe;
import sleeper.ingest.batcher.storev2.DynamoDBIngestBatcherStore;
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
    private final DynamoDbClient dynamoClient;
    private final SqsClient sqsClient;
    private final IngestBatcherSubmitRequestSerDe serDe = new IngestBatcherSubmitRequestSerDe();

    public AwsIngestBatcherDriver(
            SystemTestInstanceContext instance,
            IngestSourceFilesContext sourceFiles,
            SystemTestClients clients) {
        this.instance = instance;
        this.dynamoClient = clients.getDynamoV2();
        this.sqsClient = clients.getSqsV2();
    }

    @Override
    public void sendFiles(List<String> files) {
        LOGGER.info("Sending {} files to ingest batcher queue", files.size());
        sqsClient.sendMessage(SendMessageRequest.builder()
                .queueUrl(instance.getInstanceProperties().get(INGEST_BATCHER_SUBMIT_QUEUE_URL))
                .messageBody(serDe.toJson(new IngestBatcherSubmitRequest(
                        instance.getTableProperties().get(TABLE_NAME),
                        files)))
                .build());
    }

    @Override
    public IngestBatcherStore batcherStore() {
        return new DynamoDBIngestBatcherStore(dynamoClient,
                instance.getInstanceProperties(), instance.getTablePropertiesProvider());
    }
}
