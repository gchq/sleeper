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
import org.apache.commons.lang3.tuple.Pair;
import sleeper.ingest.job.IngestJob;
import sleeper.ingest.job.IngestJobSerDe;

import java.util.Optional;

public class AWSSQSMessageHandler extends SQSMessageHandler {
    private AmazonSQS sqsClient;

    private AWSSQSMessageHandler(Builder builder) {
        messageReceiver = builder.messageReceiverBuilder.build();
        messageSender = builder.messageSenderBuilder.build();
        this.sqsClient = builder.sqsClient;
    }

    public void send(IngestJob ingestJob) {
        messageSender.send(ingestJob);
    }

    public Optional<Pair<IngestJob, String>> receive() {
        return messageReceiver.receive();
    }

    public AmazonSQS getSqsClient() {
        return sqsClient;
    }

    public static Builder builder() {
        return new Builder();
    }

    public static final class Builder {
        private final AWSSQSMessageReciever.Builder messageReceiverBuilder;
        private final AWSSQSMessageSender.Builder messageSenderBuilder;
        private AmazonSQS sqsClient;

        private Builder() {
            messageSenderBuilder = AWSSQSMessageSender.builder();
            messageReceiverBuilder = AWSSQSMessageReciever.builder();
        }

        public Builder queueUrl(String queueUrl) {
            messageSenderBuilder.queueUrl(queueUrl);
            messageReceiverBuilder.sqsJobQueueUrl(queueUrl);
            return this;
        }

        public Builder sqsClient(AmazonSQS sqsClient) {
            messageSenderBuilder.sqsClient(sqsClient);
            messageReceiverBuilder.sqsClient(sqsClient);
            this.sqsClient = sqsClient;
            return this;
        }

        public Builder ingestJobSerDe(IngestJobSerDe ingestJobSerDe) {
            messageSenderBuilder.ingestJobSerDe(ingestJobSerDe);
            messageReceiverBuilder.ingestJobSerDe(ingestJobSerDe);
            return this;
        }

        public AWSSQSMessageHandler build() {
            return new AWSSQSMessageHandler(this);
        }
    }
}
