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

import org.apache.arrow.memory.BufferAllocator;
import org.apache.hadoop.conf.Configuration;
import sleeper.configuration.jars.ObjectFactory;
import sleeper.core.partition.Partition;
import sleeper.core.record.Record;
import sleeper.core.schema.Schema;
import sleeper.ingest.IngestProperties;
import sleeper.ingest.impl.partitionfilewriter.AsyncS3PartitionFileWriter;
import sleeper.ingest.impl.partitionfilewriter.DirectPartitionFileWriter;
import sleeper.ingest.impl.partitionfilewriter.PartitionFileWriter;
import sleeper.ingest.impl.recordbatch.RecordBatch;
import sleeper.ingest.impl.recordbatch.arraylist.ArrayListRecordBatchAcceptingRecords;
import sleeper.ingest.impl.recordbatch.arrow.ArrowRecordBatchAcceptingRecords;
import sleeper.statestore.StateStore;
import software.amazon.awssdk.services.s3.S3AsyncClient;

import java.io.IOException;
import java.util.function.Function;
import java.util.function.Supplier;

public class StandardIngestCoordinator {
    private StandardIngestCoordinator() {
    }

    public static Builder builder() {
        return new Builder();
    }

    public static IngestCoordinator<Record> directWriteBackedByArrayList(IngestProperties ingestProperties) {
        return builder().fromProperties(ingestProperties)
                .backedByArrayList()
                .maxNoOfRecordsInMemory((int) ingestProperties.getMaxInMemoryBatchSize())
                .maxNoOfRecordsInLocalStore(ingestProperties.getMaxRecordsToWriteLocally())
                .buildDirectWrite(ingestProperties.getFilePrefix() + ingestProperties.getBucketName());
    }

    private static IngestCoordinator<Record> directWriteBackedByArrayList(
            Builder builder, BackedByArrayBuilder arrayBuilder, String filePathPrefix) {
        Supplier<RecordBatch<Record>> recordBatchFactoryFn = () -> new ArrayListRecordBatchAcceptingRecords(
                builder.schema,
                builder.localWorkingDirectory,
                arrayBuilder.maxNoOfRecordsInMemory,
                arrayBuilder.maxNoOfRecordsInLocalStore,
                builder.parquetRowGroupSize,
                builder.parquetPageSize,
                builder.parquetCompressionCodec,
                builder.hadoopConfiguration);
        Function<Partition, PartitionFileWriter> partitionFileFactoryFn = partition -> {
            try {
                return new DirectPartitionFileWriter(
                        builder.schema,
                        partition,
                        builder.parquetRowGroupSize,
                        builder.parquetPageSize,
                        builder.parquetCompressionCodec,
                        builder.hadoopConfiguration,
                        filePathPrefix);
            } catch (Exception e) {
                throw new RuntimeException(e);
            }
        };
        return new IngestCoordinator<>(
                builder.objectFactory,
                builder.stateStore,
                builder.schema,
                builder.iteratorClassName,
                builder.iteratorConfig,
                builder.ingestPartitionRefreshFrequencyInSeconds,
                recordBatchFactoryFn,
                partitionFileFactoryFn);
    }

    public static IngestCoordinator<Record> directWriteBackedByArrow(IngestProperties ingestProperties,
                                                                     BufferAllocator arrowBufferAllocator,
                                                                     int maxNoOfRecordsToWriteToArrowFileAtOnce,
                                                                     long workingArrowBufferAllocatorBytes,
                                                                     long minBatchArrowBufferAllocatorBytes,
                                                                     long maxBatchArrowBufferAllocatorBytes) {
        return builder()
                .fromProperties(ingestProperties)
                .backedByArrow()
                .arrowBufferAllocator(arrowBufferAllocator)
                .maxNoOfRecordsToWriteToArrowFileAtOnce(maxNoOfRecordsToWriteToArrowFileAtOnce)
                .workingArrowBufferAllocatorBytes(workingArrowBufferAllocatorBytes)
                .minBatchArrowBufferAllocatorBytes(minBatchArrowBufferAllocatorBytes)
                .maxBatchArrowBufferAllocatorBytes(maxBatchArrowBufferAllocatorBytes)
                .maxNoOfBytesToWriteLocally(ingestProperties.getMaxRecordsToWriteLocally())
                .buildDirectWrite(ingestProperties.getFilePrefix() + ingestProperties.getBucketName());
    }

    private static IngestCoordinator<Record> directWriteBackedByArrow(
            Builder builder, BackedByArrowBuilder arrowBuilder, String filePathPrefix) {
        Supplier<RecordBatch<Record>> recordBatchFactoryFn = () ->
                new ArrowRecordBatchAcceptingRecords(
                        arrowBuilder.arrowBufferAllocator,
                        builder.schema,
                        builder.localWorkingDirectory,
                        arrowBuilder.workingArrowBufferAllocatorBytes,
                        arrowBuilder.minBatchArrowBufferAllocatorBytes,
                        arrowBuilder.maxBatchArrowBufferAllocatorBytes,
                        arrowBuilder.maxNoOfBytesToWriteLocally,
                        arrowBuilder.maxNoOfRecordsToWriteToArrowFileAtOnce);
        Function<Partition, PartitionFileWriter> partitionFileFactoryFn = partition -> {
            try {
                return new DirectPartitionFileWriter(
                        builder.schema,
                        partition,
                        builder.parquetRowGroupSize,
                        builder.parquetPageSize,
                        builder.parquetCompressionCodec,
                        builder.hadoopConfiguration,
                        filePathPrefix);
            } catch (Exception e) {
                throw new RuntimeException(e);
            }
        };
        return new IngestCoordinator<>(
                builder.objectFactory,
                builder.stateStore,
                builder.schema,
                builder.iteratorClassName,
                builder.iteratorConfig,
                builder.ingestPartitionRefreshFrequencyInSeconds,
                recordBatchFactoryFn,
                partitionFileFactoryFn);
    }

    public static IngestCoordinator<Record> asyncS3WriteBackedByArrow(IngestProperties ingestProperties,
                                                                      String s3BucketName,
                                                                      S3AsyncClient s3AsyncClient,
                                                                      BufferAllocator arrowBufferAllocator,
                                                                      int maxNoOfRecordsToWriteToArrowFileAtOnce,
                                                                      long workingArrowBufferAllocatorBytes,
                                                                      long minBatchArrowBufferAllocatorBytes,
                                                                      long maxBatchArrowBufferAllocatorBytes) {
        return builder()
                .fromProperties(ingestProperties)
                .backedByArrow()
                .arrowBufferAllocator(arrowBufferAllocator)
                .maxNoOfRecordsToWriteToArrowFileAtOnce(maxNoOfRecordsToWriteToArrowFileAtOnce)
                .workingArrowBufferAllocatorBytes(workingArrowBufferAllocatorBytes)
                .minBatchArrowBufferAllocatorBytes(minBatchArrowBufferAllocatorBytes)
                .maxBatchArrowBufferAllocatorBytes(maxBatchArrowBufferAllocatorBytes)
                .maxNoOfBytesToWriteLocally(ingestProperties.getMaxRecordsToWriteLocally())
                .buildAsyncS3Write(s3BucketName, s3AsyncClient);
    }

    private static IngestCoordinator<Record> asyncS3WriteBackedByArrow(
            Builder builder, BackedByArrowBuilder arrowBuilder, String s3BucketName, S3AsyncClient s3AsyncClient) {
        Supplier<RecordBatch<Record>> recordBatchFactoryFn = () ->
                new ArrowRecordBatchAcceptingRecords(
                        arrowBuilder.arrowBufferAllocator,
                        builder.schema,
                        builder.localWorkingDirectory,
                        arrowBuilder.workingArrowBufferAllocatorBytes,
                        arrowBuilder.minBatchArrowBufferAllocatorBytes,
                        arrowBuilder.maxBatchArrowBufferAllocatorBytes,
                        arrowBuilder.maxNoOfBytesToWriteLocally,
                        arrowBuilder.maxNoOfRecordsToWriteToArrowFileAtOnce);
        Function<Partition, PartitionFileWriter> partitionFileFactoryFn = partition -> {
            try {
                return new AsyncS3PartitionFileWriter(
                        builder.schema,
                        partition,
                        builder.parquetRowGroupSize,
                        builder.parquetPageSize,
                        builder.parquetCompressionCodec,
                        builder.hadoopConfiguration,
                        s3BucketName,
                        s3AsyncClient,
                        builder.localWorkingDirectory);
            } catch (IOException e) {
                throw new RuntimeException(e);
            }
        };
        return new IngestCoordinator<>(
                builder.objectFactory,
                builder.stateStore,
                builder.schema,
                builder.iteratorClassName,
                builder.iteratorConfig,
                builder.ingestPartitionRefreshFrequencyInSeconds,
                recordBatchFactoryFn,
                partitionFileFactoryFn);
    }

    public static class BackedByArrayBuilder {
        private final Builder builder;
        private int maxNoOfRecordsInMemory;
        private long maxNoOfRecordsInLocalStore;

        private BackedByArrayBuilder(Builder builder) {
            this.builder = builder;
        }

        public BackedByArrayBuilder maxNoOfRecordsInMemory(int maxNoOfRecordsInMemory) {
            this.maxNoOfRecordsInMemory = maxNoOfRecordsInMemory;
            return this;
        }

        public BackedByArrayBuilder maxNoOfRecordsInLocalStore(long maxNoOfRecordsInLocalStore) {
            this.maxNoOfRecordsInLocalStore = maxNoOfRecordsInLocalStore;
            return this;
        }

        public IngestCoordinator<Record> buildDirectWrite(String filePathPrefix) {
            return directWriteBackedByArrayList(builder, this, filePathPrefix);
        }
    }

    public static class BackedByArrowBuilder {
        private final Builder builder;
        private BufferAllocator arrowBufferAllocator;
        private int maxNoOfRecordsToWriteToArrowFileAtOnce;
        private long workingArrowBufferAllocatorBytes;
        private long minBatchArrowBufferAllocatorBytes;
        private long maxBatchArrowBufferAllocatorBytes;
        private long maxNoOfBytesToWriteLocally;

        private BackedByArrowBuilder(Builder builder) {
            this.builder = builder;
        }

        public BackedByArrowBuilder arrowBufferAllocator(BufferAllocator arrowBufferAllocator) {
            this.arrowBufferAllocator = arrowBufferAllocator;
            return this;
        }

        public BackedByArrowBuilder maxNoOfRecordsToWriteToArrowFileAtOnce(int maxNoOfRecordsToWriteToArrowFileAtOnce) {
            this.maxNoOfRecordsToWriteToArrowFileAtOnce = maxNoOfRecordsToWriteToArrowFileAtOnce;
            return this;
        }

        public BackedByArrowBuilder workingArrowBufferAllocatorBytes(long workingArrowBufferAllocatorBytes) {
            this.workingArrowBufferAllocatorBytes = workingArrowBufferAllocatorBytes;
            return this;
        }

        public BackedByArrowBuilder minBatchArrowBufferAllocatorBytes(long minBatchArrowBufferAllocatorBytes) {
            this.minBatchArrowBufferAllocatorBytes = minBatchArrowBufferAllocatorBytes;
            return this;
        }

        public BackedByArrowBuilder maxBatchArrowBufferAllocatorBytes(long maxBatchArrowBufferAllocatorBytes) {
            this.maxBatchArrowBufferAllocatorBytes = maxBatchArrowBufferAllocatorBytes;
            return this;
        }

        public BackedByArrowBuilder maxNoOfBytesToWriteLocally(long maxNoOfBytesToWriteLocally) {
            this.maxNoOfBytesToWriteLocally = maxNoOfBytesToWriteLocally;
            return this;
        }

        public IngestCoordinator<Record> buildDirectWrite(String filePathPrefix) {
            return directWriteBackedByArrow(builder, this, filePathPrefix);
        }

        public IngestCoordinator<Record> buildAsyncS3Write(String s3BucketName, S3AsyncClient s3AsyncClient) {
            return asyncS3WriteBackedByArrow(builder, this, s3BucketName, s3AsyncClient);
        }
    }

    public static class Builder {
        private ObjectFactory objectFactory;
        private String localWorkingDirectory;
        private int parquetRowGroupSize;
        private int parquetPageSize;
        private String parquetCompressionCodec;
        private StateStore stateStore;
        private Schema schema;
        private String iteratorClassName;
        private String iteratorConfig;
        private int ingestPartitionRefreshFrequencyInSeconds;
        private Configuration hadoopConfiguration;

        private Builder() {
        }

        public Builder fromProperties(IngestProperties ingestProperties) {
            return this.objectFactory(ingestProperties.getObjectFactory())
                    .localWorkingDirectory(ingestProperties.getLocalDir())
                    .parquetRowGroupSize(ingestProperties.getRowGroupSize())
                    .parquetPageSize(ingestProperties.getPageSize())
                    .stateStore(ingestProperties.getStateStore())
                    .schema(ingestProperties.getSchema())
                    .parquetCompressionCodec(ingestProperties.getCompressionCodec())
                    .iteratorClassName(ingestProperties.getIteratorClassName())
                    .iteratorConfig(ingestProperties.getIteratorConfig())
                    .ingestPartitionRefreshFrequencyInSeconds(ingestProperties.getIngestPartitionRefreshFrequencyInSecond())
                    .hadoopConfiguration(ingestProperties.getHadoopConfiguration());
        }

        public Builder objectFactory(ObjectFactory objectFactory) {
            this.objectFactory = objectFactory;
            return this;
        }

        public Builder localWorkingDirectory(String localWorkingDirectory) {
            this.localWorkingDirectory = localWorkingDirectory;
            return this;
        }

        public Builder parquetRowGroupSize(int parquetRowGroupSize) {
            this.parquetRowGroupSize = parquetRowGroupSize;
            return this;
        }

        public Builder parquetPageSize(int parquetPageSize) {
            this.parquetPageSize = parquetPageSize;
            return this;
        }

        public Builder parquetCompressionCodec(String parquetCompressionCodec) {
            this.parquetCompressionCodec = parquetCompressionCodec;
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

        public Builder hadoopConfiguration(Configuration hadoopConfiguration) {
            this.hadoopConfiguration = hadoopConfiguration;
            return this;
        }

        public BackedByArrayBuilder backedByArrayList() {
            return new BackedByArrayBuilder(this);
        }

        public BackedByArrowBuilder backedByArrow() {
            return new BackedByArrowBuilder(this);
        }

    }
}
