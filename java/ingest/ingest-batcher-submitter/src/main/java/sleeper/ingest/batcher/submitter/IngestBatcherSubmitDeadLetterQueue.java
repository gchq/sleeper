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
package sleeper.ingest.batcher.submitter;

import com.amazonaws.services.sqs.AmazonSQS;
import com.amazonaws.services.sqs.model.SendMessageRequest;

import sleeper.core.properties.instance.InstanceProperties;
import sleeper.ingest.batcher.core.IngestBatcherSubmitRequest;
import sleeper.ingest.batcher.core.IngestBatcherSubmitRequestSerDe;

import static sleeper.core.properties.instance.CdkDefinedInstanceProperty.INGEST_BATCHER_SUBMIT_DLQ_URL;

public class IngestBatcherSubmitDeadLetterQueue {

    InstanceProperties instanceProperties;
    AmazonSQS sqsClient;

    public IngestBatcherSubmitDeadLetterQueue(InstanceProperties instanceProperties, AmazonSQS sqsClient) {
        this.instanceProperties = instanceProperties;
        this.sqsClient = sqsClient;
    }

    void submit(IngestBatcherSubmitRequest request) {
        IngestBatcherSubmitRequestSerDe serDe = new IngestBatcherSubmitRequestSerDe();
        SendMessageRequest sendMessageRequest = new SendMessageRequest()
                .withQueueUrl(instanceProperties.get(INGEST_BATCHER_SUBMIT_DLQ_URL))
                .withMessageBody(serDe.toJson(request));
        sqsClient.sendMessage(sendMessageRequest);

    }

    void submit(String json) {
        SendMessageRequest sendMessageRequest = new SendMessageRequest()
                .withQueueUrl(instanceProperties.get(INGEST_BATCHER_SUBMIT_DLQ_URL))
                .withMessageBody(json);
        sqsClient.sendMessage(sendMessageRequest);
    }

}
