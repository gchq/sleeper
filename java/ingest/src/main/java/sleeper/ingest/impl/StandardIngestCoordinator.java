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
import sleeper.configuration.jars.ObjectFactory;
import sleeper.core.record.Record;
import sleeper.core.schema.Schema;
import sleeper.ingest.IngestProperties;
import sleeper.ingest.impl.partitionfilewriter.AsyncS3PartitionFileWriterFactory;
import sleeper.ingest.impl.partitionfilewriter.DirectPartitionFileWriterFactory;
import sleeper.ingest.impl.partitionfilewriter.PartitionFileWriterFactory;
import sleeper.ingest.impl.recordbatch.RecordBatchFactory;
import sleeper.ingest.impl.recordbatch.arraylist.ArrayListRecordBatchFactory;
import sleeper.ingest.impl.recordbatch.arrow.ArrowRecordBatchFactory;
import sleeper.statestore.StateStore;
import software.amazon.awssdk.services.s3.S3AsyncClient;

import java.util.function.Consumer;

public class StandardIngestCoordinator {
    private StandardIngestCoordinator() {
    }

    public static Builder builder() {
        return new Builder();
    }

    public static IngestCoordinator<Record> directWriteBackedByArrayList(IngestProperties ingestProperties) {
        return builder().fromProperties(ingestProperties)
                .arrayListRecordBatchFactory(
                        (int) ingestProperties.getMaxInMemoryBatchSize(),
                        ingestProperties.getMaxRecordsToWriteLocally())
                .directPartitionFileWriterFactory(
                        ingestProperties.getFilePrefix() + ingestProperties.getBucketName())
                .build();
    }

    public static IngestCoordinator<Record> directWriteBackedByArrow(IngestProperties ingestProperties,
                                                                     BufferAllocator arrowBufferAllocator,
                                                                     int maxNoOfRecordsToWriteToArrowFileAtOnce,
                                                                     long workingArrowBufferAllocatorBytes,
                                                                     long minBatchArrowBufferAllocatorBytes,
                                                                     long maxBatchArrowBufferAllocatorBytes) {
        return builder()
                .fromProperties(ingestProperties)
                .arrowRecordBatchFactory(builder -> builder
                        .bufferAllocator(arrowBufferAllocator)
                        .maxNoOfRecordsToWriteToArrowFileAtOnce(maxNoOfRecordsToWriteToArrowFileAtOnce)
                        .workingBufferAllocatorBytes(workingArrowBufferAllocatorBytes)
                        .minBatchBufferAllocatorBytes(minBatchArrowBufferAllocatorBytes)
                        .maxBatchBufferAllocatorBytes(maxBatchArrowBufferAllocatorBytes)
                        .maxNoOfBytesToWriteLocally(ingestProperties.getMaxRecordsToWriteLocally()))
                .directPartitionFileWriterFactory(
                        ingestProperties.getFilePrefix() + ingestProperties.getBucketName())
                .build();
    }

    public static IngestCoordinator<Record> asyncS3WriteBackedByArrayList(IngestProperties ingestProperties,
                                                                          String s3BucketName,
                                                                          S3AsyncClient s3AsyncClient) {
        return builder()
                .fromProperties(ingestProperties)
                .arrayListRecordBatchFactory(
                        (int) ingestProperties.getMaxInMemoryBatchSize(),
                        ingestProperties.getMaxRecordsToWriteLocally())
                .asyncS3PartitionFileWriterFactory(s3BucketName, s3AsyncClient)
                .build();
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
                .arrowRecordBatchFactory(builder -> builder
                        .bufferAllocator(arrowBufferAllocator)
                        .maxNoOfRecordsToWriteToArrowFileAtOnce(maxNoOfRecordsToWriteToArrowFileAtOnce)
                        .workingBufferAllocatorBytes(workingArrowBufferAllocatorBytes)
                        .minBatchBufferAllocatorBytes(minBatchArrowBufferAllocatorBytes)
                        .maxBatchBufferAllocatorBytes(maxBatchArrowBufferAllocatorBytes)
                        .maxNoOfBytesToWriteLocally(ingestProperties.getMaxRecordsToWriteLocally()))
                .asyncS3PartitionFileWriterFactory(s3BucketName, s3AsyncClient)
                .build();
    }

    public static class Builder {
        private ObjectFactory objectFactory;
        private String localWorkingDirectory;
        private ParquetConfiguration parquetConfiguration;
        private StateStore stateStore;
        private Schema schema;
        private String iteratorClassName;
        private String iteratorConfig;
        private int ingestPartitionRefreshFrequencyInSeconds;
        private RecordBatchFactory<Record> recordBatchFactory;
        private PartitionFileWriterFactory partitionFileWriterFactory;

        private Builder() {
        }

        public Builder fromProperties(IngestProperties ingestProperties) {
            return this.objectFactory(ingestProperties.getObjectFactory())
                    .localWorkingDirectory(ingestProperties.getLocalDir())
                    .parquetConfiguration(ParquetConfiguration.builder()
                            .sleeperSchema(ingestProperties.getSchema())
                            .parquetCompressionCodec(ingestProperties.getCompressionCodec())
                            .parquetRowGroupSize(ingestProperties.getRowGroupSize())
                            .parquetPageSize(ingestProperties.getPageSize())
                            .hadoopConfiguration(ingestProperties.getHadoopConfiguration())
                            .build())
                    .stateStore(ingestProperties.getStateStore())
                    .schema(ingestProperties.getSchema())
                    .iteratorClassName(ingestProperties.getIteratorClassName())
                    .iteratorConfig(ingestProperties.getIteratorConfig())
                    .ingestPartitionRefreshFrequencyInSeconds(ingestProperties.getIngestPartitionRefreshFrequencyInSecond());
        }

        public Builder objectFactory(ObjectFactory objectFactory) {
            this.objectFactory = objectFactory;
            return this;
        }

        public Builder localWorkingDirectory(String localWorkingDirectory) {
            this.localWorkingDirectory = localWorkingDirectory;
            return this;
        }

        public Builder parquetConfiguration(ParquetConfiguration parquetConfiguration) {
            this.parquetConfiguration = parquetConfiguration;
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

        public Builder arrayListRecordBatchFactory(
                int maxNoOfRecordsInMemory,
                long maxNoOfRecordsInLocalStore) {
            return recordBatchFactory(ArrayListRecordBatchFactory.builder()
                    .parquetConfiguration(parquetConfiguration)
                    .localWorkingDirectory(localWorkingDirectory)
                    .maxNoOfRecordsInMemory(maxNoOfRecordsInMemory)
                    .maxNoOfRecordsInLocalStore(maxNoOfRecordsInLocalStore)
                    .buildAcceptingRecords());
        }

        public Builder arrowRecordBatchFactory(Consumer<ArrowRecordBatchFactory.Builder> config) {
            ArrowRecordBatchFactory.Builder builder = ArrowRecordBatchFactory.builder()
                    .schema(schema)
                    .localWorkingDirectory(localWorkingDirectory);
            config.accept(builder);
            return recordBatchFactory(builder.buildAcceptingRecords());
        }

        public Builder partitionFileWriterFactory(PartitionFileWriterFactory partitionFileWriterFactory) {
            this.partitionFileWriterFactory = partitionFileWriterFactory;
            return this;
        }

        public Builder directPartitionFileWriterFactory(String filePathPrefix) {
            return partitionFileWriterFactory(
                    new DirectPartitionFileWriterFactory(parquetConfiguration, filePathPrefix));
        }

        public Builder asyncS3PartitionFileWriterFactory(
                String s3BucketName, S3AsyncClient s3AsyncClient) {
            return partitionFileWriterFactory(AsyncS3PartitionFileWriterFactory.builder()
                    .parquetConfiguration(parquetConfiguration)
                    .localWorkingDirectory(localWorkingDirectory)
                    .s3BucketName(s3BucketName).s3AsyncClient(s3AsyncClient)
                    .build());
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
