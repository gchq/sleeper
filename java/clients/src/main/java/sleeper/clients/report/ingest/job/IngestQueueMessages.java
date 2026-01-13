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

package sleeper.clients.report.ingest.job;

import sleeper.common.task.QueueMessageCount;
import sleeper.core.properties.instance.InstanceProperties;
import sleeper.core.properties.instance.InstanceProperty;

import java.io.PrintStream;
import java.util.Objects;
import java.util.Optional;

import static sleeper.core.properties.instance.CdkDefinedInstanceProperty.BULK_IMPORT_EKS_JOB_QUEUE_URL;
import static sleeper.core.properties.instance.CdkDefinedInstanceProperty.BULK_IMPORT_EMR_JOB_QUEUE_URL;
import static sleeper.core.properties.instance.CdkDefinedInstanceProperty.BULK_IMPORT_EMR_SERVERLESS_JOB_QUEUE_URL;
import static sleeper.core.properties.instance.CdkDefinedInstanceProperty.BULK_IMPORT_PERSISTENT_EMR_JOB_QUEUE_URL;
import static sleeper.core.properties.instance.CdkDefinedInstanceProperty.INGEST_JOB_QUEUE_URL;

/**
 * Counts of messages on the ingest and bulk import job queues.
 */
public class IngestQueueMessages {
    private final Integer ingestMessages;
    private final Integer emrMessages;
    private final Integer persistentEmrMessages;
    private final Integer eksMessages;
    private final Integer emrServerlessMessages;

    private IngestQueueMessages(Builder builder) {
        ingestMessages = builder.ingestMessages;
        emrMessages = builder.emrMessages;
        persistentEmrMessages = builder.persistentEmrMessages;
        eksMessages = builder.eksMessages;
        emrServerlessMessages = builder.emrServerlessMessages;
    }

    /**
     * Creates estimates of the number of messages on each ingest and bulk import job queue.
     *
     * @param  properties the instance properties
     * @param  client     a client to find the approximate number of messages in a queue
     * @return            the message counts
     */
    public static IngestQueueMessages from(InstanceProperties properties, QueueMessageCount.Client client) {
        return builder()
                .ingestMessages(getMessages(properties, client, INGEST_JOB_QUEUE_URL))
                .emrMessages(getMessages(properties, client, BULK_IMPORT_EMR_JOB_QUEUE_URL))
                .persistentEmrMessages(getMessages(properties, client, BULK_IMPORT_PERSISTENT_EMR_JOB_QUEUE_URL))
                .eksMessages(getMessages(properties, client, BULK_IMPORT_EKS_JOB_QUEUE_URL))
                .emrServerlessMessages(getMessages(properties, client, BULK_IMPORT_EMR_SERVERLESS_JOB_QUEUE_URL))
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

    /**
     * Prints the number of messages in each ingest and bulk import queue in a report.
     *
     * @param out the output to print to
     */
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
        if (emrServerlessMessages != null) {
            out.printf("Jobs waiting in EMR serverless queue (excluded from report): %s%n", emrServerlessMessages);
        }
        out.printf("Total jobs waiting across all queues: %s%n", getTotalMessages());
    }

    public int getTotalMessages() {
        return Optional.ofNullable(ingestMessages).orElse(0) +
                Optional.ofNullable(emrMessages).orElse(0) +
                Optional.ofNullable(persistentEmrMessages).orElse(0) +
                Optional.ofNullable(eksMessages).orElse(0) +
                Optional.ofNullable(emrServerlessMessages).orElse(0);
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
                && Objects.equals(eksMessages, that.eksMessages)
                && Objects.equals(emrServerlessMessages, that.emrServerlessMessages);
    }

    @Override
    public int hashCode() {
        return Objects.hash(ingestMessages, emrMessages, persistentEmrMessages, eksMessages, emrServerlessMessages);
    }

    @Override
    public String toString() {
        return "IngestQueueMessages{" +
                "ingestMessages=" + ingestMessages +
                ", emrMessages=" + emrMessages +
                ", persistentEmrMessages=" + persistentEmrMessages +
                ", eksMessages=" + eksMessages +
                ", emrServerlessMessages=" + emrServerlessMessages +
                '}';
    }

    /**
     * A builder for the counts of messages on ingest and bulk import queues.
     */
    public static final class Builder {
        private Integer ingestMessages;
        private Integer emrMessages;
        private Integer persistentEmrMessages;
        private Integer eksMessages;
        private Integer emrServerlessMessages;

        private Builder() {
        }

        /**
         * Sets the number of messages on the ingest job queue.
         *
         * @param  ingestMessages the number of messages, or null if the queue is not present
         * @return                this builder
         */
        public Builder ingestMessages(Integer ingestMessages) {
            this.ingestMessages = ingestMessages;
            return this;
        }

        /**
         * Sets the number of messages on the EMR bulk import job queue.
         *
         * @param  emrMessages the number of messages, or null if the queue is not present
         * @return             this builder
         */
        public Builder emrMessages(Integer emrMessages) {
            this.emrMessages = emrMessages;
            return this;
        }

        /**
         * Sets the number of messages on the persistent EMR bulk import job queue.
         *
         * @param  persistentEmrMessages the number of messages, or null if the queue is not present
         * @return                       this builder
         */
        public Builder persistentEmrMessages(Integer persistentEmrMessages) {
            this.persistentEmrMessages = persistentEmrMessages;
            return this;
        }

        /**
         * Sets the number of messages on the EKS bulk import job queue.
         *
         * @param  eksMessages the number of messages, or null if the queue is not present
         * @return             this builder
         */
        public Builder eksMessages(Integer eksMessages) {
            this.eksMessages = eksMessages;
            return this;
        }

        /**
         * Sets the number of messages on the EMR serverless bulk import job queue.
         *
         * @param  emrServerlessMessages the number of messages, or null if the queue is not present
         * @return                       this builder
         */
        public Builder emrServerlessMessages(Integer emrServerlessMessages) {
            this.emrServerlessMessages = emrServerlessMessages;
            return this;
        }

        public IngestQueueMessages build() {
            return new IngestQueueMessages(this);
        }
    }
}
