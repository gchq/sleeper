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
import com.amazonaws.services.sqs.model.SendMessageRequest;
import sleeper.ingest.job.IngestJob;
import sleeper.ingest.job.IngestJobSerDe;

public class AWSSQSMessageSender implements SQSMessageSender {
    private final AmazonSQS sqsClient;
    private final String queueUrl;
    private final IngestJobSerDe ingestJobSerDe;

    private AWSSQSMessageSender(Builder builder) {
        sqsClient = builder.sqsClient;
        queueUrl = builder.queueUrl;
        ingestJobSerDe = builder.ingestJobSerDe;
    }

    public static Builder builder() {
        return new Builder();
    }

    @Override
    public void send(IngestJob ingestJob) {
        sqsClient.sendMessage(new SendMessageRequest()
                .withQueueUrl(queueUrl)
                .withMessageBody(ingestJobSerDe.toJson(ingestJob)));
    }

    public static final class Builder {
        private AmazonSQS sqsClient;
        private String queueUrl;
        private IngestJobSerDe ingestJobSerDe;

        private Builder() {
        }

        public Builder sqsClient(AmazonSQS sqsClient) {
            this.sqsClient = sqsClient;
            return this;
        }

        public Builder queueUrl(String queueUrl) {
            this.queueUrl = queueUrl;
            return this;
        }

        public Builder ingestJobSerDe(IngestJobSerDe ingestJobSerDe) {
            this.ingestJobSerDe = ingestJobSerDe;
            return this;
        }

        public AWSSQSMessageSender build() {
            return new AWSSQSMessageSender(this);
        }
    }
}
