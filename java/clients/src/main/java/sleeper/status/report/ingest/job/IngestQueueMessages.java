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

package sleeper.status.report.ingest.job;

import sleeper.configuration.properties.InstanceProperties;
import sleeper.job.common.QueueMessageCount;

import java.util.Objects;

import static sleeper.configuration.properties.SystemDefinedInstanceProperty.INGEST_JOB_QUEUE_URL;

public class IngestQueueMessages {
    private final int ingestMessages;

    private IngestQueueMessages(Builder builder) {
        ingestMessages = builder.ingestMessages;
    }

    public static IngestQueueMessages from(InstanceProperties properties, QueueMessageCount.Client client) {
        int ingestJobMessageCount = client.getQueueMessageCount(properties.get(INGEST_JOB_QUEUE_URL))
                .getApproximateNumberOfMessages();
        return builder().ingestMessages(ingestJobMessageCount).build();
    }

    public static Builder builder() {
        return new Builder();
    }

    @Override
    public boolean equals(Object o) {
        if (this == o) {
            return true;
        }
        if (o == null || getClass() != o.getClass()) {
            return false;
        }
        IngestQueueMessages that = (IngestQueueMessages) o;
        return ingestMessages == that.ingestMessages;
    }

    @Override
    public int hashCode() {
        return Objects.hash(ingestMessages);
    }

    @Override
    public String toString() {
        return "IngestQueueMessages{" +
                "ingestMessages=" + ingestMessages +
                '}';
    }

    public static final class Builder {
        private int ingestMessages;

        private Builder() {
        }

        public Builder ingestMessages(int ingestMessages) {
            this.ingestMessages = ingestMessages;
            return this;
        }

        public IngestQueueMessages build() {
            return new IngestQueueMessages(this);
        }
    }
}
