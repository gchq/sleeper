/*
 * Copyright 2022 Crown Copyright
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

package sleeper.ingest.job.sqs;

import com.amazonaws.services.sqs.AmazonSQS;
import com.amazonaws.services.sqs.model.Message;
import com.amazonaws.services.sqs.model.ReceiveMessageRequest;
import com.amazonaws.services.sqs.model.ReceiveMessageResult;
import org.apache.commons.lang3.tuple.Pair;
import org.slf4j.Logger;
import org.slf4j.LoggerFactory;
import sleeper.ingest.job.IngestJob;
import sleeper.ingest.job.IngestJobSerDe;

import java.util.List;
import java.util.Optional;

public class AWSSQSMessageReciever implements SQSMessageReceiver {
    private static final Logger LOGGER = LoggerFactory.getLogger(AWSSQSMessageReciever.class);
    private AmazonSQS sqsClient;
    private String sqsJobQueueUrl;
    private IngestJobSerDe ingestJobSerDe;

    private AWSSQSMessageReciever(Builder builder) {
        sqsClient = builder.sqsClient;
        sqsJobQueueUrl = builder.sqsJobQueueUrl;
        ingestJobSerDe = builder.ingestJobSerDe;
    }

    public static Builder builder() {
        return new Builder();
    }

    @Override
    public Optional<Pair<IngestJob, String>> receive() {
        ReceiveMessageRequest receiveMessageRequest = new ReceiveMessageRequest(sqsJobQueueUrl)
                .withMaxNumberOfMessages(1)
                .withWaitTimeSeconds(20); // Must be >= 0 and <= 20
        ReceiveMessageResult receiveMessageResult = sqsClient.receiveMessage(receiveMessageRequest);
        List<Message> messages = receiveMessageResult.getMessages();
        if (messages.isEmpty()) {
            LOGGER.info("Finishing as no jobs have been received");
            return Optional.empty();
        }
        LOGGER.info("Received message {}", messages.get(0).getBody());
        IngestJob ingestJob = ingestJobSerDe.fromJson(messages.get(0).getBody());
        LOGGER.info("Deserialised message to ingest job {}", ingestJob);
        return Optional.of(Pair.of(ingestJob, messages.get(0).getReceiptHandle()));
    }

    public static final class Builder {
        private AmazonSQS sqsClient;
        private String sqsJobQueueUrl;
        private IngestJobSerDe ingestJobSerDe;

        private Builder() {
        }

        public Builder sqsClient(AmazonSQS sqsClient) {
            this.sqsClient = sqsClient;
            return this;
        }

        public Builder sqsJobQueueUrl(String sqsJobQueueUrl) {
            this.sqsJobQueueUrl = sqsJobQueueUrl;
            return this;
        }

        public Builder ingestJobSerDe(IngestJobSerDe ingestJobSerDe) {
            this.ingestJobSerDe = ingestJobSerDe;
            return this;
        }

        public AWSSQSMessageReciever build() {
            return new AWSSQSMessageReciever(this);
        }
    }
}
