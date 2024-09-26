/*
 * Copyright 2022-2024 Crown Copyright
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

package sleeper.clients.status.report.ingest.job;

import sleeper.core.properties.instance.InstanceProperties;
import sleeper.core.properties.instance.InstanceProperty;
import sleeper.task.common.QueueMessageCount;

import java.io.PrintStream;
import java.util.Objects;
import java.util.Optional;

import static sleeper.core.properties.instance.CdkDefinedInstanceProperty.BULK_IMPORT_EKS_JOB_QUEUE_URL;
import static sleeper.core.properties.instance.CdkDefinedInstanceProperty.BULK_IMPORT_EMR_JOB_QUEUE_URL;
import static sleeper.core.properties.instance.CdkDefinedInstanceProperty.BULK_IMPORT_PERSISTENT_EMR_JOB_QUEUE_URL;
import static sleeper.core.properties.instance.CdkDefinedInstanceProperty.INGEST_JOB_QUEUE_URL;

public class IngestQueueMessages {
    private final Integer ingestMessages;
    private final Integer emrMessages;
    private final Integer persistentEmrMessages;
    private final Integer eksMessages;

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

    private static Integer getMessages(
            InstanceProperties properties, QueueMessageCount.Client client, InstanceProperty property) {
        String queueUrl = properties.get(property);
        if (queueUrl == null) {
            return null;
        } else {
            return client.getQueueMessageCount(queueUrl).getApproximateNumberOfMessages();
        }
    }

    public static Builder builder() {
        return new Builder();
    }

    public void print(PrintStream out) {
        if (ingestMessages != null) {
            out.printf("Jobs waiting in ingest queue (excluded from report): %s%n", ingestMessages);
        }
        if (emrMessages != null) {
            out.printf("Jobs waiting in EMR queue (excluded from report): %s%n", emrMessages);
        }
        if (persistentEmrMessages != null) {
            out.printf("Jobs waiting in persistent EMR queue (excluded from report): %s%n", persistentEmrMessages);
        }
        if (eksMessages != null) {
            out.printf("Jobs waiting in EKS queue (excluded from report): %s%n", eksMessages);
        }
        out.printf("Total jobs waiting across all queues: %s%n", getTotalMessages());
    }

    public int getTotalMessages() {
        return Optional.ofNullable(ingestMessages).orElse(0) +
                Optional.ofNullable(emrMessages).orElse(0) +
                Optional.ofNullable(persistentEmrMessages).orElse(0) +
                Optional.ofNullable(eksMessages).orElse(0);
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
        return Objects.equals(ingestMessages, that.ingestMessages)
                && Objects.equals(emrMessages, that.emrMessages)
                && Objects.equals(persistentEmrMessages, that.persistentEmrMessages)
                && Objects.equals(eksMessages, that.eksMessages);
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
        private Integer ingestMessages;
        private Integer emrMessages;
        private Integer persistentEmrMessages;
        private Integer eksMessages;

        private Builder() {
        }

        public Builder ingestMessages(Integer ingestMessages) {
            this.ingestMessages = ingestMessages;
            return this;
        }

        public Builder emrMessages(Integer emrMessages) {
            this.emrMessages = emrMessages;
            return this;
        }

        public Builder persistentEmrMessages(Integer persistentEmrMessages) {
            this.persistentEmrMessages = persistentEmrMessages;
            return this;
        }

        public Builder eksMessages(Integer eksMessages) {
            this.eksMessages = eksMessages;
            return this;
        }

        public IngestQueueMessages build() {
            return new IngestQueueMessages(this);
        }
    }
}
