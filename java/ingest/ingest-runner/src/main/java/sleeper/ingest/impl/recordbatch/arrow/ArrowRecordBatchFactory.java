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

package sleeper.ingest.impl.recordbatch.arrow;

import org.apache.arrow.memory.BufferAllocator;
import org.apache.arrow.memory.RootAllocator;
import org.slf4j.Logger;
import org.slf4j.LoggerFactory;
import sleeper.configuration.properties.InstanceProperties;
import sleeper.core.record.Record;
import sleeper.core.schema.Schema;
import sleeper.ingest.impl.recordbatch.RecordBatch;
import sleeper.ingest.impl.recordbatch.RecordBatchFactory;

import java.util.Objects;
import java.util.function.Function;

import static sleeper.configuration.properties.UserDefinedInstanceProperty.ARROW_INGEST_BATCH_BUFFER_BYTES;
import static sleeper.configuration.properties.UserDefinedInstanceProperty.ARROW_INGEST_MAX_LOCAL_STORE_BYTES;
import static sleeper.configuration.properties.UserDefinedInstanceProperty.ARROW_INGEST_MAX_SINGLE_WRITE_TO_FILE_RECORDS;
import static sleeper.configuration.properties.UserDefinedInstanceProperty.ARROW_INGEST_WORKING_BUFFER_BYTES;

public class ArrowRecordBatchFactory<INCOMINGDATATYPE> implements RecordBatchFactory<INCOMINGDATATYPE> {
    private static final Logger LOGGER = LoggerFactory.getLogger(ArrowRecordBatchFactory.class);

    private final Schema schema;
    private final String localWorkingDirectory;
    private final long workingBufferAllocatorBytes;
    private final long minBatchBufferAllocatorBytes;
    private final long maxBatchBufferAllocatorBytes;
    private final long maxNoOfBytesToWriteLocally;
    private final int maxNoOfRecordsToWriteToArrowFileAtOnce;
    private final Function<ArrowRecordBatchFactory<?>, RecordBatch<INCOMINGDATATYPE>> createBatchFn;
    private final BufferAllocator bufferAllocator;
    private final boolean closeBufferAllocator;

    private ArrowRecordBatchFactory(
            Builder builder,
            Function<ArrowRecordBatchFactory<?>, RecordBatch<INCOMINGDATATYPE>> createBatchFn,
            String createBatchFnName) {
        this.schema = Objects.requireNonNull(builder.schema, "schema must not be null");
        localWorkingDirectory = Objects.requireNonNull(builder.localWorkingDirectory, "localWorkingDirectory must not be null");
        if (builder.workingBufferAllocatorBytes < 1) {
            throw new IllegalArgumentException("workingBufferAllocatorBytes must be positive");
        }
        if (builder.maxBatchBufferAllocatorBytes < 1) {
            throw new IllegalArgumentException("maxBatchBufferAllocatorBytes must be positive");
        }
        if (builder.maxNoOfBytesToWriteLocally < 1) {
            throw new IllegalArgumentException("maxNoOfBytesToWriteLocally must be positive");
        }
        if (builder.maxNoOfRecordsToWriteToArrowFileAtOnce < 1) {
            throw new IllegalArgumentException("maxNoOfRecordsToWriteToArrowFileAtOnce must be positive");
        }
        this.workingBufferAllocatorBytes = builder.workingBufferAllocatorBytes;
        this.minBatchBufferAllocatorBytes = builder.minBatchBufferAllocatorBytes;
        this.maxBatchBufferAllocatorBytes = builder.maxBatchBufferAllocatorBytes;
        this.maxNoOfBytesToWriteLocally = builder.maxNoOfBytesToWriteLocally;
        this.maxNoOfRecordsToWriteToArrowFileAtOnce = builder.maxNoOfRecordsToWriteToArrowFileAtOnce;
        this.createBatchFn = Objects.requireNonNull(createBatchFn, "createBatchFn must not be null");
        if (builder.bufferAllocator == null) {
            this.closeBufferAllocator = true;
            this.bufferAllocator = new RootAllocator(workingBufferAllocatorBytes + maxBatchBufferAllocatorBytes);
        } else {
            this.closeBufferAllocator = false;
            this.bufferAllocator = builder.bufferAllocator;
        }
        LOGGER.info("Created ArrowRecordBatchFactory with:\n" +
                        "\tschema of {}\n" +
                        "\tlocalWorkingDirectory of {}\n" +
                        "\tworkingBufferAllocatorBytes of {}\n" +
                        "\tmaxBatchBufferAllocatorBytes of {}\n" +
                        "\tmaxNoOfBytesToWriteLocally of {}\n" +
                        "\tmaxNoOfRecordsToWriteToArrowFileAtOnce of {}\n" +
                        "\tcreateBatchFn of {}",
                this.schema, this.localWorkingDirectory, this.workingBufferAllocatorBytes,
                this.maxBatchBufferAllocatorBytes, this.maxNoOfBytesToWriteLocally,
                this.maxNoOfRecordsToWriteToArrowFileAtOnce, createBatchFnName);
    }

    public static Builder builder() {
        return new Builder();
    }

    public static Builder builderWith(InstanceProperties instanceProperties) {
        return builder().instanceProperties(instanceProperties);
    }

    @Override
    public RecordBatch<INCOMINGDATATYPE> createRecordBatch() {
        return createBatchFn.apply(this);
    }

    public RecordBatch<Record> createRecordBatchAcceptingRecords() {
        return new ArrowRecordBatchAcceptingRecords(
                bufferAllocator,
                schema,
                localWorkingDirectory,
                workingBufferAllocatorBytes,
                minBatchBufferAllocatorBytes,
                maxBatchBufferAllocatorBytes,
                maxNoOfBytesToWriteLocally,
                maxNoOfRecordsToWriteToArrowFileAtOnce);
    }

    @Override
    public void close() {
        if (closeBufferAllocator) {
            bufferAllocator.close();
        }
    }

    public static final class Builder {

        private Schema schema;
        private String localWorkingDirectory;
        private long workingBufferAllocatorBytes;
        private long minBatchBufferAllocatorBytes;
        private long maxBatchBufferAllocatorBytes;
        private long maxNoOfBytesToWriteLocally;
        private int maxNoOfRecordsToWriteToArrowFileAtOnce;
        private BufferAllocator bufferAllocator;

        private Builder() {
        }

        public Builder schema(Schema schema) {
            this.schema = schema;
            return this;
        }

        public Builder localWorkingDirectory(String localWorkingDirectory) {
            this.localWorkingDirectory = localWorkingDirectory;
            return this;
        }

        public Builder workingBufferAllocatorBytes(long workingBufferAllocatorBytes) {
            this.workingBufferAllocatorBytes = workingBufferAllocatorBytes;
            return this;
        }

        public Builder minBatchBufferAllocatorBytes(long minBatchBufferAllocatorBytes) {
            this.minBatchBufferAllocatorBytes = minBatchBufferAllocatorBytes;
            return this;
        }

        public Builder maxBatchBufferAllocatorBytes(long maxBatchBufferAllocatorBytes) {
            this.maxBatchBufferAllocatorBytes = maxBatchBufferAllocatorBytes;
            return this;
        }

        public Builder maxNoOfBytesToWriteLocally(long maxNoOfBytesToWriteLocally) {
            this.maxNoOfBytesToWriteLocally = maxNoOfBytesToWriteLocally;
            return this;
        }

        public Builder maxNoOfRecordsToWriteToArrowFileAtOnce(int maxNoOfRecordsToWriteToArrowFileAtOnce) {
            this.maxNoOfRecordsToWriteToArrowFileAtOnce = maxNoOfRecordsToWriteToArrowFileAtOnce;
            return this;
        }

        public Builder bufferAllocator(BufferAllocator bufferAllocator) {
            this.bufferAllocator = bufferAllocator;
            return this;
        }

        public Builder instanceProperties(InstanceProperties instanceProperties) {
            return maxNoOfRecordsToWriteToArrowFileAtOnce(instanceProperties.getInt(ARROW_INGEST_MAX_SINGLE_WRITE_TO_FILE_RECORDS))
                    .workingBufferAllocatorBytes(instanceProperties.getLong(ARROW_INGEST_WORKING_BUFFER_BYTES))
                    .minBatchBufferAllocatorBytes(instanceProperties.getLong(ARROW_INGEST_BATCH_BUFFER_BYTES))
                    .maxBatchBufferAllocatorBytes(instanceProperties.getLong(ARROW_INGEST_BATCH_BUFFER_BYTES))
                    .maxNoOfBytesToWriteLocally(instanceProperties.getLong(ARROW_INGEST_MAX_LOCAL_STORE_BYTES));
        }

        public ArrowRecordBatchFactory<Record> buildAcceptingRecords() {
            return new ArrowRecordBatchFactory<>(this,
                    ArrowRecordBatchFactory::createRecordBatchAcceptingRecords,
                    "createRecordBatchAcceptingRecords");
        }
    }
}
