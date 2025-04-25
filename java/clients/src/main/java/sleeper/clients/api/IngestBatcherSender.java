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
package sleeper.clients.api;

import com.amazonaws.services.sqs.AmazonSQS;

import sleeper.core.properties.instance.InstanceProperties;
import sleeper.ingest.batcher.core.IngestBatcherSubmitRequest;
import sleeper.ingest.batcher.core.IngestBatcherSubmitRequestSerDe;

import static sleeper.core.properties.instance.CdkDefinedInstanceProperty.INGEST_BATCHER_SUBMIT_QUEUE_URL;

public interface IngestBatcherSender {

    void submit(IngestBatcherSubmitRequest request);

    static IngestBatcherSender toSqs(InstanceProperties instanceProperties, AmazonSQS sqsClient) {
        IngestBatcherSubmitRequestSerDe serDe = new IngestBatcherSubmitRequestSerDe();
        return request -> sqsClient.sendMessage(
                instanceProperties.get(INGEST_BATCHER_SUBMIT_QUEUE_URL),
                serDe.toJson(request));
    }

}
