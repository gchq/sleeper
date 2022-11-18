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
package sleeper.ingest;

import org.apache.hadoop.conf.Configuration;
import sleeper.configuration.jars.ObjectFactory;
import sleeper.configuration.properties.InstanceProperties;
import sleeper.configuration.properties.table.TableProperties;
import sleeper.core.iterator.CloseableIterator;
import sleeper.core.iterator.IteratorException;
import sleeper.core.record.Record;
import sleeper.ingest.impl.IngestCoordinator;
import sleeper.ingest.impl.ParquetConfiguration;
import sleeper.ingest.impl.StandardIngestCoordinator;
import sleeper.ingest.impl.partitionfilewriter.AsyncS3PartitionFileWriterFactory;
import sleeper.ingest.impl.partitionfilewriter.DirectPartitionFileWriterFactory;
import sleeper.ingest.impl.partitionfilewriter.PartitionFileWriterFactory;
import sleeper.ingest.impl.recordbatch.RecordBatchFactory;
import sleeper.ingest.impl.recordbatch.arraylist.ArrayListRecordBatchFactory;
import sleeper.ingest.impl.recordbatch.arrow.ArrowRecordBatchFactory;
import sleeper.statestore.StateStoreException;
import sleeper.statestore.StateStoreProvider;
import software.amazon.awssdk.services.s3.S3AsyncClient;

import java.io.IOException;
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
import static sleeper.configuration.properties.table.TableProperty.DATA_BUCKET;
import static sleeper.configuration.properties.table.TableProperty.ITERATOR_CLASS_NAME;
import static sleeper.configuration.properties.table.TableProperty.ITERATOR_CONFIG;

public class IngestFactory {

    private final ObjectFactory objectFactory;
    private final String localDir;
    private final StateStoreProvider stateStoreProvider;
    private final InstanceProperties instanceProperties;
    private final Configuration hadoopConfiguration;
    private final S3AsyncClient s3AsyncClient;

    private IngestFactory(Builder builder) {
        objectFactory = Objects.requireNonNull(builder.objectFactory, "objectFactory must not be null");
        localDir = Objects.requireNonNull(builder.localDir, "localDir must not be null");
        stateStoreProvider = Objects.requireNonNull(builder.stateStoreProvider, "stateStoreProvider must not be null");
        instanceProperties = Objects.requireNonNull(builder.instanceProperties, "instanceProperties must not be null");
        if (builder.hadoopConfiguration == null) {
            hadoopConfiguration = defaultHadoopConfiguration();
        } else {
            hadoopConfiguration = builder.hadoopConfiguration;
        }
        // If S3AsyncClient is not set, a default client will be created if it is needed.
        s3AsyncClient = builder.s3AsyncClient;
    }

    public static Builder builder() {
        return new Builder();
    }

    public IngestResult ingestFromRecordIteratorAndClose(TableProperties tableProperties, CloseableIterator<Record> recordIterator)
            throws StateStoreException, IteratorException, IOException {
        try {
            return ingestFromRecordIterator(tableProperties, recordIterator);
        } finally {
            recordIterator.close();
        }
    }

    public IngestResult ingestFromRecordIterator(TableProperties tableProperties, Iterator<Record> recordIterator)
            throws StateStoreException, IteratorException, IOException {
        try (IngestCoordinator<Record> ingestCoordinator = createIngestCoordinator(tableProperties)) {
            return new IngestRecordsFromIterator(ingestCoordinator, recordIterator).write();
        }
    }

    public IngestRecords createIngestRecords(TableProperties tableProperties) {
        return new IngestRecords(createIngestCoordinator(tableProperties));
    }

    public IngestCoordinator<Record> createIngestCoordinator(TableProperties tableProperties) {
        ParquetConfiguration parquetConfiguration = ParquetConfiguration.from(tableProperties, hadoopConfiguration);
        return StandardIngestCoordinator.builder()
                .objectFactory(objectFactory)
                .stateStore(stateStoreProvider.getStateStore(tableProperties))
                .schema(tableProperties.getSchema())
                .iteratorClassName(tableProperties.get(ITERATOR_CLASS_NAME))
                .iteratorConfig(tableProperties.get(ITERATOR_CONFIG))
                .ingestPartitionRefreshFrequencyInSeconds(instanceProperties.getInt(INGEST_PARTITION_REFRESH_PERIOD_IN_SECONDS))
                .recordBatchFactory(standardRecordBatchFactory(parquetConfiguration))
                .partitionFileWriterFactory(standardPartitionFileWriterFactory(tableProperties, parquetConfiguration))
                .build();

    }

    private RecordBatchFactory<Record> standardRecordBatchFactory(ParquetConfiguration parquetConfiguration) {
        String recordBatchType = instanceProperties.get(INGEST_RECORD_BATCH_TYPE).toLowerCase(Locale.ROOT);
        if (recordBatchType.equals("arraylist")) {
            return ArrayListRecordBatchFactory.builder()
                    .parquetConfiguration(parquetConfiguration)
                    .localWorkingDirectory(localDir)
                    .maxNoOfRecordsInMemory(instanceProperties.getInt(MAX_IN_MEMORY_BATCH_SIZE))
                    .maxNoOfRecordsInLocalStore(instanceProperties.getLong(MAX_RECORDS_TO_WRITE_LOCALLY))
                    .buildAcceptingRecords();
        } else if (recordBatchType.equals("arrow")) {
            return ArrowRecordBatchFactory.builder()
                    .schema(parquetConfiguration.getSleeperSchema())
                    .localWorkingDirectory(localDir)
                    .maxNoOfRecordsToWriteToArrowFileAtOnce(
                            instanceProperties.getInt(ARROW_INGEST_MAX_SINGLE_WRITE_TO_FILE_RECORDS))
                    .workingBufferAllocatorBytes(instanceProperties.getLong(ARROW_INGEST_WORKING_BUFFER_BYTES))
                    .minBatchBufferAllocatorBytes(instanceProperties.getLong(ARROW_INGEST_BATCH_BUFFER_BYTES))
                    .maxBatchBufferAllocatorBytes(instanceProperties.getLong(ARROW_INGEST_BATCH_BUFFER_BYTES))
                    .maxNoOfBytesToWriteLocally(instanceProperties.getLong(MAX_RECORDS_TO_WRITE_LOCALLY))
                    .buildAcceptingRecords();
        } else {
            throw new UnsupportedOperationException(String.format("Record batch type %s not supported", recordBatchType));
        }
    }

    private PartitionFileWriterFactory standardPartitionFileWriterFactory(
            TableProperties tableProperties, ParquetConfiguration parquetConfiguration) {
        String fileWriterType = instanceProperties.get(INGEST_PARTITION_FILE_WRITER_TYPE).toLowerCase(Locale.ROOT);
        if (fileWriterType.equals("direct")) {
            return new DirectPartitionFileWriterFactory(parquetConfiguration,
                    instanceProperties.get(FILE_SYSTEM) + tableProperties.get(DATA_BUCKET));
        } else if (fileWriterType.equals("async")) {
            if (!instanceProperties.get(FILE_SYSTEM).toLowerCase(Locale.ROOT).equals("s3a://")) {
                throw new UnsupportedOperationException("Attempting an asynchronous write to a file system that is not s3a://");
            }
            return AsyncS3PartitionFileWriterFactory.builder()
                    .parquetConfiguration(parquetConfiguration)
                    .localWorkingDirectory(localDir)
                    .s3BucketName(tableProperties.get(DATA_BUCKET)).s3AsyncClient(s3AsyncClient)
                    .build();
        } else {
            throw new UnsupportedOperationException(String.format("File writer type %s not supported", fileWriterType));
        }
    }

    /**
     * Create a simple default Hadoop configuration which may be used if no other configuration is provided.
     *
     * @return The Hadoop configuration
     */
    private static Configuration defaultHadoopConfiguration() {
        Configuration conf = new Configuration();
        conf.set("fs.s3a.aws.credentials.provider", "com.amazonaws.auth.EC2ContainerCredentialsProviderWrapper");
        conf.set("fs.s3a.fast.upload", "true");
        return conf;
    }

    public static final class Builder {
        private ObjectFactory objectFactory;
        private String localDir;
        private StateStoreProvider stateStoreProvider;
        private InstanceProperties instanceProperties;
        private Configuration hadoopConfiguration;
        private S3AsyncClient s3AsyncClient;

        private Builder() {
        }

        public static Builder builder() {
            return new Builder();
        }

        public Builder objectFactory(ObjectFactory objectFactory) {
            this.objectFactory = objectFactory;
            return this;
        }

        public Builder localDir(String localDir) {
            this.localDir = localDir;
            return this;
        }

        public Builder stateStoreProvider(StateStoreProvider stateStoreProvider) {
            this.stateStoreProvider = stateStoreProvider;
            return this;
        }

        public Builder instanceProperties(InstanceProperties instanceProperties) {
            this.instanceProperties = instanceProperties;
            return this;
        }

        /**
         * The configuration to use for interacting with files through a Hadoop file system,
         * and any other needed operations.
         * <p>
         * This is not required. If it is not set, a default configuration will be created.
         *
         * @param hadoopConfiguration The configuration to use
         * @return The builder for chaining calls
         */
        public Builder hadoopConfiguration(Configuration hadoopConfiguration) {
            this.hadoopConfiguration = hadoopConfiguration;
            return this;
        }

        /**
         * The client to use for asynchronous S3 operations.
         * This may or may not be used depending on the settings for an ingest.
         * <p>
         * This is not required. If it is not set, a default client will be created if it is needed.
         *
         * @param s3AsyncClient The client to use
         * @return The builder for chaining calls
         */
        public Builder s3AsyncClient(S3AsyncClient s3AsyncClient) {
            this.s3AsyncClient = s3AsyncClient;
            return this;
        }

        public IngestFactory build() {
            return new IngestFactory(this);
        }
    }
}
