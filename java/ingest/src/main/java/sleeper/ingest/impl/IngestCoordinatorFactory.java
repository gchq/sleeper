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
import sleeper.configuration.properties.InstanceProperties;
import sleeper.configuration.properties.table.TableProperties;
import sleeper.configuration.properties.table.TablePropertiesProvider;
import sleeper.core.record.Record;
import sleeper.ingest.IngestProperties;
import sleeper.ingest.IngestRecordsFromIterator;
import sleeper.statestore.StateStoreProvider;
import software.amazon.awssdk.services.s3.S3AsyncClient;

import java.util.Iterator;
import java.util.Locale;
import java.util.Objects;

import static sleeper.configuration.properties.UserDefinedInstanceProperty.ARROW_INGEST_BATCH_BUFFER_BYTES;
import static sleeper.configuration.properties.UserDefinedInstanceProperty.ARROW_INGEST_MAX_SINGLE_WRITE_TO_FILE_RECORDS;
import static sleeper.configuration.properties.UserDefinedInstanceProperty.ARROW_INGEST_WORKING_BUFFER_BYTES;
import static sleeper.configuration.properties.UserDefinedInstanceProperty.FILE_SYSTEM;
import static sleeper.configuration.properties.UserDefinedInstanceProperty.INGEST_PARTITION_FILE_WRITER_TYPE;
import static sleeper.configuration.properties.UserDefinedInstanceProperty.INGEST_PARTITION_REFRESH_PERIOD_IN_SECONDS;
import static sleeper.configuration.properties.UserDefinedInstanceProperty.INGEST_RECORD_BATCH_TYPE;
import static sleeper.configuration.properties.UserDefinedInstanceProperty.MAX_IN_MEMORY_BATCH_SIZE;
import static sleeper.configuration.properties.UserDefinedInstanceProperty.MAX_RECORDS_TO_WRITE_LOCALLY;
import static sleeper.configuration.properties.table.TableProperty.COMPRESSION_CODEC;
import static sleeper.configuration.properties.table.TableProperty.DATA_BUCKET;
import static sleeper.configuration.properties.table.TableProperty.ITERATOR_CLASS_NAME;
import static sleeper.configuration.properties.table.TableProperty.ITERATOR_CONFIG;
import static sleeper.configuration.properties.table.TableProperty.PAGE_SIZE;
import static sleeper.configuration.properties.table.TableProperty.ROW_GROUP_SIZE;

public class IngestCoordinatorFactory {

    private final ObjectFactory objectFactory;
    private final String localDir;
    private final StateStoreProvider stateStoreProvider;
    private final Configuration hadoopConfiguration;
    private final BufferAllocator bufferAllocator;
    private final S3AsyncClient s3AsyncClient;
    private final InstanceProperties instanceProperties;

    private IngestCoordinatorFactory(Builder builder) {
        this.objectFactory = Objects.requireNonNull(builder.objectFactory);
        this.localDir = Objects.requireNonNull(builder.localDir);
        this.stateStoreProvider = Objects.requireNonNull(builder.stateStoreProvider);
        this.hadoopConfiguration = Objects.requireNonNull(builder.hadoopConfiguration);
        this.bufferAllocator = builder.bufferAllocator;
        this.s3AsyncClient = builder.s3AsyncClient;
        this.instanceProperties = Objects.requireNonNull(builder.instanceProperties);
    }

    public static Builder builder() {
        return new Builder();
    }

    public AllTablesIngestCoordinatorFactory withTablePropertiesProvider(
            TablePropertiesProvider tablePropertiesProvider) {
        return new AllTablesIngestCoordinatorFactory(this, tablePropertiesProvider);
    }

    public IngestCoordinator<Record> createIngestCoordinator(TableProperties tableProperties) {
        S3AsyncClient internalS3AsyncClient =
                instanceProperties.get(INGEST_PARTITION_FILE_WRITER_TYPE).toLowerCase(Locale.ROOT).equals("async") ?
                        ((s3AsyncClient == null) ? S3AsyncClient.create() : s3AsyncClient) :
                        null;
        IngestProperties ingestProperties = createIngestProperties(instanceProperties, tableProperties);
        String recordBatchType = instanceProperties.get(INGEST_RECORD_BATCH_TYPE).toLowerCase(Locale.ROOT);
        String fileWriterType = instanceProperties.get(INGEST_PARTITION_FILE_WRITER_TYPE).toLowerCase(Locale.ROOT);
        StandardIngestCoordinator.BackedBuilder ingestCoordinatorBuilder;
        try {
            if (recordBatchType.equals("arraylist")) {
                ingestCoordinatorBuilder = StandardIngestCoordinator.builder().fromProperties(ingestProperties)
                        .backedByArrayList()
                        .maxNoOfRecordsInMemory((int) ingestProperties.getMaxInMemoryBatchSize())
                        .maxNoOfRecordsInLocalStore(ingestProperties.getMaxRecordsToWriteLocally());
            } else if (recordBatchType.equals("arrow")) {
                ingestCoordinatorBuilder = StandardIngestCoordinator.builder().fromProperties(ingestProperties)
                        .backedByArrow()
                        .arrowBufferAllocator(bufferAllocator)
                        .maxNoOfRecordsToWriteToArrowFileAtOnce(instanceProperties.getInt(ARROW_INGEST_MAX_SINGLE_WRITE_TO_FILE_RECORDS))
                        .workingArrowBufferAllocatorBytes(instanceProperties.getLong(ARROW_INGEST_WORKING_BUFFER_BYTES))
                        .minBatchArrowBufferAllocatorBytes(instanceProperties.getLong(ARROW_INGEST_BATCH_BUFFER_BYTES))
                        .maxBatchArrowBufferAllocatorBytes(instanceProperties.getLong(ARROW_INGEST_BATCH_BUFFER_BYTES))
                        .maxNoOfBytesToWriteLocally(ingestProperties.getMaxRecordsToWriteLocally());
            } else {
                throw new UnsupportedOperationException(String.format("Record batch type %s not supported", recordBatchType));
            }
            if (fileWriterType.equals("direct")) {
                return ingestCoordinatorBuilder.buildDirectWrite(ingestProperties.getFilePrefix() + ingestProperties.getBucketName());
            } else if (fileWriterType.equals("async")) {
                if (!instanceProperties.get(FILE_SYSTEM).toLowerCase(Locale.ROOT).equals("s3a://")) {
                    throw new UnsupportedOperationException("Attempting an asynchronous write to a file system that is not s3a://");
                } else {
                    return ingestCoordinatorBuilder.buildAsyncS3Write(ingestProperties.getBucketName(), internalS3AsyncClient);
                }
            } else {
                throw new UnsupportedOperationException(String.format("Record batch type %s not supported", recordBatchType));
            }
        } finally {
            if (s3AsyncClient == null && internalS3AsyncClient != null) {
                internalS3AsyncClient.close();
            }
        }
    }

    public IngestRecordsFromIterator createIngestRecordsFromIterator(TableProperties tableProperties, Iterator<Record> recordIterator) {
        return new IngestRecordsFromIterator(createIngestCoordinator(tableProperties), recordIterator);
    }

    public IngestProperties createIngestProperties(InstanceProperties instanceProperties, TableProperties tableProperties) {
        return IngestProperties.builder()
                .objectFactory(objectFactory)
                .localDir(localDir)
                .rowGroupSize(tableProperties.getInt(ROW_GROUP_SIZE))
                .pageSize(tableProperties.getInt(PAGE_SIZE))
                .stateStore(stateStoreProvider.getStateStore(tableProperties))
                .schema(tableProperties.getSchema())
                .iteratorClassName(tableProperties.get(ITERATOR_CLASS_NAME))
                .iteratorConfig(tableProperties.get(ITERATOR_CONFIG))
                .compressionCodec(tableProperties.get(COMPRESSION_CODEC))
                .filePathPrefix(instanceProperties.get(FILE_SYSTEM))
                .bucketName(tableProperties.get(DATA_BUCKET))
                .hadoopConfiguration(hadoopConfiguration)
                .maxInMemoryBatchSize(instanceProperties.getInt(MAX_IN_MEMORY_BATCH_SIZE))
                .maxRecordsToWriteLocally(instanceProperties.getLong(MAX_RECORDS_TO_WRITE_LOCALLY))
                .ingestPartitionRefreshFrequencyInSecond(instanceProperties.getInt(INGEST_PARTITION_REFRESH_PERIOD_IN_SECONDS)).build();
    }

    public static final class Builder {
        private ObjectFactory objectFactory;
        private String localDir;
        private StateStoreProvider stateStoreProvider;
        private Configuration hadoopConfiguration;
        private BufferAllocator bufferAllocator;
        private S3AsyncClient s3AsyncClient;
        private InstanceProperties instanceProperties;

        private Builder() {
        }

        public Builder objectFactory(ObjectFactory objectFactory) {
            this.objectFactory = objectFactory;
            return this;
        }

        public Builder localDir(String localDir) {
            this.localDir = localDir;
            return this;
        }

        public Builder stateStoreProvider(StateStoreProvider stateStore) {
            this.stateStoreProvider = stateStore;
            return this;
        }

        public Builder hadoopConfiguration(Configuration hadoopConfiguration) {
            this.hadoopConfiguration = hadoopConfiguration;
            return this;
        }

        public Builder bufferAllocator(BufferAllocator bufferAllocator) {
            this.bufferAllocator = bufferAllocator;
            return this;
        }

        public Builder s3AsyncClient(S3AsyncClient s3AsyncClient) {
            this.s3AsyncClient = s3AsyncClient;
            return this;
        }

        public Builder instanceProperties(InstanceProperties instanceProperties) {
            this.instanceProperties = instanceProperties;
            return this;
        }

        public IngestCoordinatorFactory build() {
            return new IngestCoordinatorFactory(this);
        }
    }
}
