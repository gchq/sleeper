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
import sleeper.configuration.properties.InstanceProperty;
import sleeper.job.common.QueueMessageCount;

import java.io.PrintStream;
import java.util.Objects;

import static sleeper.configuration.properties.SystemDefinedInstanceProperty.BULK_IMPORT_EKS_JOB_QUEUE_URL;
import static sleeper.configuration.properties.SystemDefinedInstanceProperty.BULK_IMPORT_EMR_JOB_QUEUE_URL;
import static sleeper.configuration.properties.SystemDefinedInstanceProperty.BULK_IMPORT_PERSISTENT_EMR_JOB_QUEUE_URL;
import static sleeper.configuration.properties.SystemDefinedInstanceProperty.INGEST_JOB_QUEUE_URL;

public class IngestQueueMessages {
    private final int ingestMessages;
    private final int emrMessages;
    private final int persistentEmrMessages;
    private final int eksMessages;

    private IngestQueueMessages(Builder builder) {
        ingestMessages = builder.ingestMessages;
        emrMessages = builder.emrMessages;
        persistentEmrMessages = builder.persistentEmrMessages;
        eksMessages = builder.eksMessages;
    }

    public static IngestQueueMessages from(InstanceProperties properties, QueueMessageCount.Client client) {
        return builder()
                .ingestMessages(getMessages(properties, client, INGEST_JOB_QUEUE_URL))
                .emrMessages(getMessages(properties, client, BULK_IMPORT_EMR_JOB_QUEUE_URL))
                .persistentEmrMessages(getMessages(properties, client, BULK_IMPORT_PERSISTENT_EMR_JOB_QUEUE_URL))
                .eksMessages(getMessages(properties, client, BULK_IMPORT_EKS_JOB_QUEUE_URL))
                .build();
    }

    private static int getMessages(
            InstanceProperties properties, QueueMessageCount.Client client, InstanceProperty property) {
        String queueUrl = properties.get(property);
        if (queueUrl == null) {
            return 0;
        } else {
            return client.getQueueMessageCount(queueUrl).getApproximateNumberOfMessages();
        }
    }

    public static Builder builder() {
        return new Builder();
    }

    public void print(PrintStream out) {
        out.printf("Total jobs waiting in queue (excluded from report): %s%n", ingestMessages);
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
        return ingestMessages == that.ingestMessages && emrMessages == that.emrMessages && persistentEmrMessages == that.persistentEmrMessages && eksMessages == that.eksMessages;
    }

    @Override
    public int hashCode() {
        return Objects.hash(ingestMessages, emrMessages, persistentEmrMessages, eksMessages);
    }

    @Override
    public String toString() {
        return "IngestQueueMessages{" +
                "ingestMessages=" + ingestMessages +
                ", emrMessages=" + emrMessages +
                ", persistentEmrMessages=" + persistentEmrMessages +
                ", eksMessages=" + eksMessages +
                '}';
    }

    public static final class Builder {
        private int ingestMessages;
        private int emrMessages;
        private int persistentEmrMessages;
        private int eksMessages;

        private Builder() {
        }

        public Builder ingestMessages(int ingestMessages) {
            this.ingestMessages = ingestMessages;
            return this;
        }

        public Builder emrMessages(int emrMessages) {
            this.emrMessages = emrMessages;
            return this;
        }

        public Builder persistentEmrMessages(int persistentEmrMessages) {
            this.persistentEmrMessages = persistentEmrMessages;
            return this;
        }

        public Builder eksMessages(int eksMessages) {
            this.eksMessages = eksMessages;
            return this;
        }

        public IngestQueueMessages build() {
            return new IngestQueueMessages(this);
        }
    }
}
