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
package sleeper.ingest.impl;

import sleeper.configuration.jars.ObjectFactory;
import sleeper.core.record.Record;
import sleeper.core.schema.Schema;
import sleeper.ingest.impl.partitionfilewriter.PartitionFileWriterFactory;
import sleeper.ingest.impl.recordbatch.RecordBatchFactory;
import sleeper.statestore.StateStore;

public class StandardIngestCoordinator {
    private StandardIngestCoordinator() {
    }

    public static Builder builder() {
        return new Builder();
    }

    public static class Builder {
        private ObjectFactory objectFactory;
        private StateStore stateStore;
        private Schema schema;
        private String iteratorClassName;
        private String iteratorConfig;
        private int ingestPartitionRefreshFrequencyInSeconds;
        private RecordBatchFactory<Record> recordBatchFactory;
        private PartitionFileWriterFactory partitionFileWriterFactory;

        private Builder() {
        }

        public Builder objectFactory(ObjectFactory objectFactory) {
            this.objectFactory = objectFactory;
            return this;
        }

        public Builder stateStore(StateStore stateStore) {
            this.stateStore = stateStore;
            return this;
        }

        public Builder schema(Schema schema) {
            this.schema = schema;
            return this;
        }

        public Builder iteratorClassName(String iteratorClassName) {
            this.iteratorClassName = iteratorClassName;
            return this;
        }

        public Builder iteratorConfig(String iteratorConfig) {
            this.iteratorConfig = iteratorConfig;
            return this;
        }

        public Builder ingestPartitionRefreshFrequencyInSeconds(int ingestPartitionRefreshFrequencyInSeconds) {
            this.ingestPartitionRefreshFrequencyInSeconds = ingestPartitionRefreshFrequencyInSeconds;
            return this;
        }

        public Builder recordBatchFactory(RecordBatchFactory<Record> recordBatchFactory) {
            this.recordBatchFactory = recordBatchFactory;
            return this;
        }

        public Builder partitionFileWriterFactory(PartitionFileWriterFactory partitionFileWriterFactory) {
            this.partitionFileWriterFactory = partitionFileWriterFactory;
            return this;
        }

        public IngestCoordinator<Record> build() {
            return new IngestCoordinator<>(
                    objectFactory,
                    stateStore,
                    schema,
                    iteratorClassName,
                    iteratorConfig,
                    ingestPartitionRefreshFrequencyInSeconds,
                    recordBatchFactory,
                    partitionFileWriterFactory);
        }

    }
}
