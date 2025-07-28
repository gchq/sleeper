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
package sleeper.ingest.runner;

import org.apache.hadoop.conf.Configuration;
import software.amazon.awssdk.services.s3.S3AsyncClient;

import sleeper.core.iterator.CloseableIterator;
import sleeper.core.iterator.IteratorCreationException;
import sleeper.core.properties.instance.InstanceProperties;
import sleeper.core.properties.table.TableProperties;
import sleeper.core.row.Row;
import sleeper.core.statestore.StateStoreException;
import sleeper.core.statestore.StateStoreProvider;
import sleeper.core.util.ObjectFactory;
import sleeper.ingest.core.IngestResult;
import sleeper.ingest.runner.impl.IngestCoordinator;
import sleeper.ingest.runner.impl.ParquetConfiguration;
import sleeper.ingest.runner.impl.partitionfilewriter.AsyncS3PartitionFileWriterFactory;
import sleeper.ingest.runner.impl.partitionfilewriter.DirectPartitionFileWriterFactory;
import sleeper.ingest.runner.impl.partitionfilewriter.PartitionFileWriterFactory;
import sleeper.ingest.runner.impl.rowbatch.RowBatchFactory;
import sleeper.ingest.runner.impl.rowbatch.arraylist.ArrayListRowBatchFactory;
import sleeper.ingest.runner.impl.rowbatch.arrow.ArrowRowBatchFactory;
import sleeper.parquet.utils.HadoopConfigurationProvider;

import java.io.IOException;
import java.util.Iterator;
import java.util.Locale;
import java.util.Objects;
import java.util.UUID;
import java.util.function.Supplier;

import static sleeper.core.properties.instance.CommonProperty.FILE_SYSTEM;
import static sleeper.core.properties.table.TableProperty.INGEST_PARTITION_FILE_WRITER_TYPE;
import static sleeper.core.properties.table.TableProperty.INGEST_ROW_BATCH_TYPE;

public class IngestFactory {

    private final ObjectFactory objectFactory;
    private final String localDir;
    private final StateStoreProvider stateStoreProvider;
    private final InstanceProperties instanceProperties;
    private final Configuration hadoopConfiguration;
    private final S3AsyncClient s3AsyncClient;
    private final Supplier<String> fileNameGenerator;

    private IngestFactory(Builder builder) {
        objectFactory = Objects.requireNonNull(builder.objectFactory, "objectFactory must not be null");
        localDir = Objects.requireNonNull(builder.localDir, "localDir must not be null");
        stateStoreProvider = Objects.requireNonNull(builder.stateStoreProvider, "stateStoreProvider must not be null");
        instanceProperties = Objects.requireNonNull(builder.instanceProperties, "instanceProperties must not be null");
        hadoopConfiguration = Objects.requireNonNullElseGet(builder.hadoopConfiguration,
                () -> HadoopConfigurationProvider.getConfigurationForECS(instanceProperties));
        // If S3AsyncClient is not set, a default client will be created if it is needed.
        s3AsyncClient = builder.s3AsyncClient;
        fileNameGenerator = Objects.requireNonNull(builder.fileNameGenerator, "fileNameGenerator must not be null");
    }

    public static Builder builder() {
        return new Builder();
    }

    public IngestResult ingestFromRowIteratorAndClose(TableProperties tableProperties, CloseableIterator<Row> rowIterator) throws StateStoreException, IteratorCreationException, IOException {
        try (rowIterator) {
            return ingestFromRowIterator(tableProperties, rowIterator);
        }
    }

    public IngestResult ingestFromRowIterator(TableProperties tableProperties, Iterator<Row> rowIterator) throws StateStoreException, IteratorCreationException, IOException {
        try (IngestCoordinator<Row> ingestCoordinator = createIngestCoordinator(tableProperties)) {
            return new IngestRowsFromIterator(ingestCoordinator, rowIterator).write();
        }
    }

    public IngestRows createIngestRows(TableProperties tableProperties) {
        return new IngestRows(createIngestCoordinator(tableProperties));
    }

    public IngestCoordinator.Builder<Row> ingestCoordinatorBuilder(TableProperties tableProperties) {
        ParquetConfiguration parquetConfiguration = ParquetConfiguration.from(tableProperties, hadoopConfiguration);
        return IngestCoordinator.builderWith(instanceProperties, tableProperties)
                .objectFactory(objectFactory)
                .stateStore(stateStoreProvider.getStateStore(tableProperties))
                .rowBatchFactory(standardRowBatchFactory(tableProperties, parquetConfiguration))
                .partitionFileWriterFactory(standardPartitionFileWriterFactory(tableProperties, parquetConfiguration));
    }

    public IngestCoordinator<Row> createIngestCoordinator(TableProperties tableProperties) {
        return ingestCoordinatorBuilder(tableProperties).build();
    }

    private RowBatchFactory<Row> standardRowBatchFactory(
            TableProperties tableProperties, ParquetConfiguration parquetConfiguration) {
        String rowBatchType = tableProperties.get(INGEST_ROW_BATCH_TYPE).toLowerCase(Locale.ROOT);
        if ("arraylist".equals(rowBatchType)) {
            return ArrayListRowBatchFactory.builderWith(instanceProperties)
                    .parquetConfiguration(parquetConfiguration)
                    .localWorkingDirectory(localDir)
                    .buildAcceptingRows();
        } else if ("arrow".equals(rowBatchType)) {
            return ArrowRowBatchFactory.builderWith(instanceProperties)
                    .schema(parquetConfiguration.getTableProperties().getSchema())
                    .localWorkingDirectory(localDir)
                    .buildAcceptingRows();
        } else {
            throw new UnsupportedOperationException(String.format("Row batch type %s not supported", rowBatchType));
        }
    }

    private PartitionFileWriterFactory standardPartitionFileWriterFactory(
            TableProperties tableProperties, ParquetConfiguration parquetConfiguration) {
        String fileWriterType = tableProperties.get(INGEST_PARTITION_FILE_WRITER_TYPE).toLowerCase(Locale.ROOT);
        if ("direct".equals(fileWriterType)) {
            return DirectPartitionFileWriterFactory.builder()
                    .parquetConfiguration(parquetConfiguration)
                    .filePathsAndSketchesStoreFromPropertiesAndClientOrDefault(instanceProperties, tableProperties, s3AsyncClient)
                    .fileNameGenerator(fileNameGenerator)
                    .build();
        } else if ("async".equals(fileWriterType)) {
            if (!instanceProperties.get(FILE_SYSTEM).toLowerCase(Locale.ROOT).equals("s3a://")) {
                throw new UnsupportedOperationException("Attempting an asynchronous write to a file system that is not s3a://");
            }
            return AsyncS3PartitionFileWriterFactory.builderWith(instanceProperties, tableProperties)
                    .parquetConfiguration(parquetConfiguration)
                    .localWorkingDirectory(localDir)
                    .s3AsyncClientOrDefaultFromProperties(s3AsyncClient, instanceProperties)
                    .fileNameGenerator(fileNameGenerator)
                    .build();
        } else {
            throw new UnsupportedOperationException(String.format("File writer type %s not supported", fileWriterType));
        }
    }

    public static final class Builder {
        private ObjectFactory objectFactory;
        private String localDir;
        private StateStoreProvider stateStoreProvider;
        private InstanceProperties instanceProperties;
        private Configuration hadoopConfiguration;
        private S3AsyncClient s3AsyncClient;
        private Supplier<String> fileNameGenerator = () -> UUID.randomUUID().toString();

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
         * Configuration for Hadoop integrations. Used for interacting with files through a Hadoop file system,
         * and any other needed operations.
         * <p>
         * This is not required. If it is not set, a default configuration will be created.
         *
         * @param  hadoopConfiguration The configuration to use
         * @return                     The builder for chaining calls
         */
        public Builder hadoopConfiguration(Configuration hadoopConfiguration) {
            this.hadoopConfiguration = hadoopConfiguration;
            return this;
        }

        /**
         * Client for asynchronous S3 operations. This may or may not be used depending on the settings for ingest.
         * <p>
         * This is not required. If it is not set, a default client will be created if it is needed.
         *
         * @param  s3AsyncClient The client to use
         * @return               The builder for chaining calls
         */
        public Builder s3AsyncClient(S3AsyncClient s3AsyncClient) {
            this.s3AsyncClient = s3AsyncClient;
            return this;
        }

        public Builder fileNameGenerator(Supplier<String> fileNameGenerator) {
            this.fileNameGenerator = fileNameGenerator;
            return this;
        }

        public IngestFactory build() {
            return new IngestFactory(this);
        }
    }
}
