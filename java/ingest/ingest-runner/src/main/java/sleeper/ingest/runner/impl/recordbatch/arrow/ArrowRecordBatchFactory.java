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

package sleeper.ingest.runner.impl.recordbatch.arrow;

import org.apache.arrow.memory.BufferAllocator;
import org.apache.arrow.memory.RootAllocator;
import org.slf4j.Logger;
import org.slf4j.LoggerFactory;

import sleeper.core.properties.instance.InstanceProperties;
import sleeper.core.record.Record;
import sleeper.core.schema.Schema;
import sleeper.ingest.runner.impl.recordbatch.RecordBatch;
import sleeper.ingest.runner.impl.recordbatch.RecordBatchFactory;

import java.util.Objects;

import static sleeper.core.properties.instance.ArrowIngestProperty.ARROW_INGEST_BATCH_BUFFER_BYTES;
import static sleeper.core.properties.instance.ArrowIngestProperty.ARROW_INGEST_MAX_LOCAL_STORE_BYTES;
import static sleeper.core.properties.instance.ArrowIngestProperty.ARROW_INGEST_MAX_SINGLE_WRITE_TO_FILE_RECORDS;
import static sleeper.core.properties.instance.ArrowIngestProperty.ARROW_INGEST_WORKING_BUFFER_BYTES;

public class ArrowRecordBatchFactory<INCOMINGDATATYPE> implements RecordBatchFactory<INCOMINGDATATYPE> {
    private static final Logger LOGGER = LoggerFactory.getLogger(ArrowRecordBatchFactory.class);

    private final Schema schema;
    private final String localWorkingDirectory;
    private final long workingBufferAllocatorBytes;
    private final long minBatchBufferAllocatorBytes;
    private final long maxBatchBufferAllocatorBytes;
    private final long maxNoOfBytesToWriteLocally;
    private final int maxNoOfRecordsToWriteToArrowFileAtOnce;
    private final ArrowRecordWriter<INCOMINGDATATYPE> recordWriter;
    private final BufferAllocator bufferAllocator;
    private final boolean closeBufferAllocator;

    private ArrowRecordBatchFactory(
            Builder<INCOMINGDATATYPE> builder) {
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
        this.recordWriter = Objects.requireNonNull(builder.recordWriter, "recordWriter must not be null");
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
                "\trecordWriter of type {}",
                this.schema, this.localWorkingDirectory, this.workingBufferAllocatorBytes,
                this.maxBatchBufferAllocatorBytes, this.maxNoOfBytesToWriteLocally,
                this.maxNoOfRecordsToWriteToArrowFileAtOnce, recordWriter.getClass().getSimpleName());
    }

    public static Builder<?> builder() {
        return new Builder<>();
    }

    public static Builder<?> builderWith(InstanceProperties instanceProperties) {
        return builder().instanceProperties(instanceProperties);
    }

    @Override
    public RecordBatch<INCOMINGDATATYPE> createRecordBatch() {
        return new ArrowRecordBatch<>(
                bufferAllocator,
                schema,
                recordWriter,
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

    public static final class Builder<T> {
        private Schema schema;
        private String localWorkingDirectory;
        private long workingBufferAllocatorBytes;
        private long minBatchBufferAllocatorBytes;
        private long maxBatchBufferAllocatorBytes;
        private long maxNoOfBytesToWriteLocally;
        private int maxNoOfRecordsToWriteToArrowFileAtOnce;
        private BufferAllocator bufferAllocator;
        private ArrowRecordWriter<T> recordWriter;

        private Builder() {
        }

        public Builder<T> schema(Schema schema) {
            this.schema = schema;
            return this;
        }

        public Builder<T> localWorkingDirectory(String localWorkingDirectory) {
            this.localWorkingDirectory = localWorkingDirectory;
            return this;
        }

        public Builder<T> workingBufferAllocatorBytes(long workingBufferAllocatorBytes) {
            this.workingBufferAllocatorBytes = workingBufferAllocatorBytes;
            return this;
        }

        public Builder<T> minBatchBufferAllocatorBytes(long minBatchBufferAllocatorBytes) {
            this.minBatchBufferAllocatorBytes = minBatchBufferAllocatorBytes;
            return this;
        }

        public Builder<T> maxBatchBufferAllocatorBytes(long maxBatchBufferAllocatorBytes) {
            this.maxBatchBufferAllocatorBytes = maxBatchBufferAllocatorBytes;
            return this;
        }

        public Builder<T> batchBufferAllocatorBytes(long batchBufferAllocatorBytes) {
            return minBatchBufferAllocatorBytes(batchBufferAllocatorBytes)
                    .maxBatchBufferAllocatorBytes(batchBufferAllocatorBytes);
        }

        public Builder<T> maxNoOfBytesToWriteLocally(long maxNoOfBytesToWriteLocally) {
            this.maxNoOfBytesToWriteLocally = maxNoOfBytesToWriteLocally;
            return this;
        }

        public Builder<T> maxNoOfRecordsToWriteToArrowFileAtOnce(int maxNoOfRecordsToWriteToArrowFileAtOnce) {
            this.maxNoOfRecordsToWriteToArrowFileAtOnce = maxNoOfRecordsToWriteToArrowFileAtOnce;
            return this;
        }

        public Builder<T> bufferAllocator(BufferAllocator bufferAllocator) {
            this.bufferAllocator = bufferAllocator;
            return this;
        }

        public Builder<T> instanceProperties(InstanceProperties instanceProperties) {
            return maxNoOfRecordsToWriteToArrowFileAtOnce(instanceProperties.getInt(ARROW_INGEST_MAX_SINGLE_WRITE_TO_FILE_RECORDS))
                    .workingBufferAllocatorBytes(instanceProperties.getLong(ARROW_INGEST_WORKING_BUFFER_BYTES))
                    .minBatchBufferAllocatorBytes(instanceProperties.getLong(ARROW_INGEST_BATCH_BUFFER_BYTES))
                    .maxBatchBufferAllocatorBytes(instanceProperties.getLong(ARROW_INGEST_BATCH_BUFFER_BYTES))
                    .maxNoOfBytesToWriteLocally(instanceProperties.getLong(ARROW_INGEST_MAX_LOCAL_STORE_BYTES));
        }

        public <INCOMINGDATATYPE> Builder<INCOMINGDATATYPE> recordWriter(ArrowRecordWriter<INCOMINGDATATYPE> recordWriter) {
            this.recordWriter = (ArrowRecordWriter<T>) recordWriter;
            return (Builder<INCOMINGDATATYPE>) this;
        }

        public ArrowRecordBatchFactory<Record> buildAcceptingRecords() {
            return recordWriter(new ArrowRecordWriterAcceptingRecords()).build();
        }

        public ArrowRecordBatchFactory<T> build() {
            return new ArrowRecordBatchFactory<>(this);
        }
    }
}
