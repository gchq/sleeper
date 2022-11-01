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

import sleeper.ingest.job.IngestJob;
import sleeper.ingest.job.IngestJobSerDe;

import java.util.Queue;

public class InMemorySQSMessageSender implements SQSMessageSender {
    private final Queue<String> queue;
    private final IngestJobSerDe ingestJobSerDe;

    private InMemorySQSMessageSender(Builder builder) {
        queue = builder.queue;
        ingestJobSerDe = builder.ingestJobSerDe;
    }

    public static Builder builder() {
        return new Builder();
    }

    @Override
    public void send(IngestJob ingestJob) {
        queue.add(ingestJobSerDe.toJson(ingestJob));
    }

    public static final class Builder {
        private Queue<String> queue;
        private IngestJobSerDe ingestJobSerDe;

        private Builder() {
        }

        public Builder queue(Queue<String> queue) {
            this.queue = queue;
            return this;
        }

        public Builder ingestJobSerDe(IngestJobSerDe ingestJobSerDe) {
            this.ingestJobSerDe = ingestJobSerDe;
            return this;
        }

        public InMemorySQSMessageSender build() {
            return new InMemorySQSMessageSender(this);
        }
    }
}
