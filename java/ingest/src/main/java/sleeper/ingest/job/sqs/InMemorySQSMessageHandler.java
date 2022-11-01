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

import java.util.LinkedList;
import java.util.Optional;
import java.util.Queue;

public class InMemorySQSMessageHandler extends SQSMessageHandler {
    private static final Queue<String> QUEUE = new LinkedList<>();

    private InMemorySQSMessageHandler(Builder builder) {
        this.messageReceiver = builder.messageReceiverBuilder.build();
        this.messageSender = builder.messageSenderBuilder.build();
    }

    public static Builder builder() {
        return new Builder();
    }

    @Override
    public void send(IngestJob ingestJob) {
        messageSender.send(ingestJob);
    }

    @Override
    public Optional<Pair<IngestJob, String>> receive() {
        return messageReceiver.receive();
    }

    public AmazonSQS getSqsClient() {
        return null;
    }

    public static class Builder {
        private InMemorySQSMessageSender.Builder messageSenderBuilder = InMemorySQSMessageSender.builder();
        private InMemorySQSMessageReceiver.Builder messageReceiverBuilder = InMemorySQSMessageReceiver.builder();

        private Builder() {
            messageSenderBuilder.queue(QUEUE);
            messageReceiverBuilder.queue(QUEUE);
        }

        public Builder ingestJobSerDe(IngestJobSerDe ingestJobSerDe) {
            messageSenderBuilder.ingestJobSerDe(ingestJobSerDe);
            messageReceiverBuilder.ingestJobSerDe(ingestJobSerDe);
            return this;
        }

        public InMemorySQSMessageHandler build() {
            return new InMemorySQSMessageHandler(this);
        }
    }
}
