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

package sleeper.ingest.testutils;

import org.apache.hadoop.conf.Configuration;
import software.amazon.awssdk.services.s3.S3AsyncClient;

import sleeper.configuration.jars.ObjectFactory;
import sleeper.configuration.properties.instance.InstanceProperties;
import sleeper.configuration.properties.table.TableProperties;
import sleeper.configuration.properties.validation.IngestFileWritingStrategy;
import sleeper.configuration.statestore.FixedStateStoreProvider;
import sleeper.core.record.Record;
import sleeper.core.schema.Schema;
import sleeper.core.statestore.StateStore;
import sleeper.ingest.IngestFactory;
import sleeper.ingest.impl.IngestCoordinator;
import sleeper.ingest.impl.recordbatch.arrow.ArrowRecordBatchFactory;
import sleeper.ingest.impl.recordbatch.arrow.ArrowRecordWriter;

import java.io.IOException;
import java.io.UncheckedIOException;
import java.nio.file.Path;
import java.util.List;
import java.util.Objects;
import java.util.UUID;
import java.util.function.Consumer;

import static java.nio.file.Files.createTempDirectory;
import static sleeper.configuration.properties.InstancePropertiesTestHelper.createTestInstanceProperties;
import static sleeper.configuration.properties.instance.CdkDefinedInstanceProperty.DATA_BUCKET;
import static sleeper.configuration.properties.instance.CommonProperty.FILE_SYSTEM;
import static sleeper.configuration.properties.instance.DefaultProperty.DEFAULT_INGEST_PARTITION_FILE_WRITER_TYPE;
import static sleeper.configuration.properties.instance.DefaultProperty.DEFAULT_INGEST_RECORD_BATCH_TYPE;
import static sleeper.configuration.properties.table.TablePropertiesTestHelper.createTestTableProperties;
import static sleeper.configuration.properties.table.TableProperty.INGEST_FILE_WRITING_STRATEGY;
import static sleeper.configuration.properties.table.TableProperty.ITERATOR_CLASS_NAME;
import static sleeper.configuration.properties.table.TableProperty.TABLE_ID;

public class IngestCoordinatorTestParameters {

    private final StateStore stateStore;
    private final Schema schema;
    private final String workingDir;
    private final String dataBucketName;
    private final String localDataPath;
    private final Configuration hadoopConfiguration;
    private final S3AsyncClient s3AsyncClient;
    private final List<String> fileNames;
    private final String tableId;
    private final SetProperties setProperties;

    private IngestCoordinatorTestParameters(Builder builder) {
        stateStore = Objects.requireNonNull(builder.stateStore, "stateStore must not be null");
        schema = Objects.requireNonNull(builder.schema, "schema must not be null");
        workingDir = Objects.requireNonNull(builder.workingDir, "workingDir must not be null");
        dataBucketName = builder.dataBucketName;
        localDataPath = builder.localDataPath;
        hadoopConfiguration = Objects.requireNonNull(builder.hadoopConfiguration, "hadoopConfiguration must not be null");
        s3AsyncClient = builder.s3AsyncClient;
        fileNames = Objects.requireNonNull(builder.fileNames, "fileNames must not be null");
        tableId = Objects.requireNonNull(builder.tableId, "tableId must not be null");
        setProperties = Objects.requireNonNull(builder.setProperties, "setProperties must not be null");
    }

    public static Builder builder() {
        return new Builder();
    }

    public String getLocalFilePrefix() {
        return "file://" + Objects.requireNonNull(localDataPath, "localDataPath must not be null") + "/" + tableId;
    }

    public String getS3Prefix() {
        return "s3a://" + Objects.requireNonNull(dataBucketName, "dataBucketName must not be null") + "/" + tableId;
    }

    public IngestCoordinator<Record> buildCoordinator() {
        InstanceProperties instanceProperties = createTestInstanceProperties();
        TableProperties tableProperties = createTestTableProperties(instanceProperties, schema);
        setProperties.setProperties(instanceProperties, tableProperties, this);
        return coordinatorBuilder(instanceProperties, tableProperties).build();
    }

    public <T extends ArrowRecordWriter<U>, U> IngestCoordinator<U> buildCoordinatorWithArrowWriter(T recordWriter) {
        InstanceProperties instanceProperties = createTestInstanceProperties();
        TableProperties tableProperties = createTestTableProperties(instanceProperties, schema);
        setProperties.setProperties(instanceProperties, tableProperties, this);
        ArrowRecordBatchFactory.Builder<U> arrowConfigBuilder = ArrowRecordBatchFactory.builderWith(instanceProperties)
                .schema(schema)
                .localWorkingDirectory(workingDir)
                .recordWriter(recordWriter);
        return coordinatorBuilder(instanceProperties, tableProperties)
                .recordBatchFactory(arrowConfigBuilder.build())
                .build();
    }

    private IngestCoordinator.Builder<Record> coordinatorBuilder(
            InstanceProperties instanceProperties, TableProperties tableProperties) {
        return IngestFactory.builder()
                .instanceProperties(instanceProperties)
                .hadoopConfiguration(hadoopConfiguration)
                .localDir(workingDir)
                .objectFactory(ObjectFactory.noUserJars())
                .s3AsyncClient(s3AsyncClient)
                .stateStoreProvider(new FixedStateStoreProvider(tableProperties, stateStore))
                .fileNameGenerator(fileNames.iterator()::next)
                .build().ingestCoordinatorBuilder(tableProperties);
    }

    public Builder toBuilder() {
        return new Builder(this);
    }

    public static final class Builder {
        private StateStore stateStore;
        private Schema schema;
        private String workingDir;
        private String dataBucketName;
        private String localDataPath;
        private Configuration hadoopConfiguration;
        private S3AsyncClient s3AsyncClient;
        private List<String> fileNames;
        private String tableId = UUID.randomUUID().toString();
        private SetProperties setProperties = (instanceProperties, tableProperties, parameters) -> tableProperties.set(TABLE_ID, parameters.tableId);

        private Builder() {
        }

        private Builder(IngestCoordinatorTestParameters parameters) {
            this.stateStore = parameters.stateStore;
            this.schema = parameters.schema;
            this.workingDir = parameters.workingDir;
            this.dataBucketName = parameters.dataBucketName;
            this.localDataPath = parameters.localDataPath;
            this.hadoopConfiguration = parameters.hadoopConfiguration;
            this.s3AsyncClient = parameters.s3AsyncClient;
            this.fileNames = parameters.fileNames;
            this.tableId = parameters.tableId;
            this.setProperties = parameters.setProperties;
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
            return setTableProperties(properties -> properties.set(ITERATOR_CLASS_NAME, iteratorClassName));
        }

        public Builder workingDir(String workingDir) {
            this.workingDir = workingDir;
            return this;
        }

        public Builder dataBucketName(String dataBucketName) {
            this.dataBucketName = dataBucketName;
            return this;
        }

        public Builder localDataPath(String localDataPath) {
            this.localDataPath = localDataPath;
            return this;
        }

        public Builder temporaryFolder(Path temporaryFolder) {
            try {
                return localDataPath(createTempDirectory(temporaryFolder, null).toString());
            } catch (IOException e) {
                throw new UncheckedIOException(e);
            }
        }

        public Builder hadoopConfiguration(Configuration hadoopConfiguration) {
            this.hadoopConfiguration = hadoopConfiguration;
            return this;
        }

        public Builder s3AsyncClient(S3AsyncClient s3AsyncClient) {
            this.s3AsyncClient = s3AsyncClient;
            return this;
        }

        public Builder fileNames(List<String> fileNames) {
            this.fileNames = fileNames;
            return this;
        }

        public Builder tableId(String tableId) {
            this.tableId = tableId;
            return this;
        }

        public Builder ingestFileWritingStrategy(IngestFileWritingStrategy ingestFileWritingStrategy) {
            return setTableProperties(properties -> properties.set(INGEST_FILE_WRITING_STRATEGY, ingestFileWritingStrategy.toString()));
        }

        public Builder setInstanceProperties(Consumer<InstanceProperties> config) {
            setProperties = setProperties.andThen((instanceProperties, tableProperties, parameters) -> {
                config.accept(instanceProperties);
            });
            return this;
        }

        public Builder setTableProperties(Consumer<TableProperties> config) {
            setProperties = setProperties.andThen((instanceProperties, tableProperties, parameters) -> {
                config.accept(tableProperties);
            });
            return this;
        }

        public Builder backedByArrow() {
            return setInstanceProperties(properties -> properties.set(DEFAULT_INGEST_RECORD_BATCH_TYPE, "arrow"));
        }

        public Builder backedByArrayList() {
            return setInstanceProperties(properties -> properties.set(DEFAULT_INGEST_RECORD_BATCH_TYPE, "arraylist"));
        }

        public Builder localDirectWrite() {
            setProperties = setProperties.andThen((instanceProperties, tableProperties, parameters) -> {
                instanceProperties.set(FILE_SYSTEM, "file://");
                instanceProperties.set(DATA_BUCKET, parameters.localDataPath);
                instanceProperties.set(DEFAULT_INGEST_PARTITION_FILE_WRITER_TYPE, "direct");
            });
            return this;
        }

        public Builder s3DirectWrite() {
            setProperties = setProperties.andThen((instanceProperties, tableProperties, parameters) -> {
                instanceProperties.set(FILE_SYSTEM, "s3a://");
                instanceProperties.set(DATA_BUCKET, parameters.dataBucketName);
                instanceProperties.set(DEFAULT_INGEST_PARTITION_FILE_WRITER_TYPE, "direct");
            });
            return this;
        }

        public Builder s3AsyncWrite() {
            setProperties = setProperties.andThen((instanceProperties, tableProperties, parameters) -> {
                instanceProperties.set(FILE_SYSTEM, "s3a://");
                instanceProperties.set(DATA_BUCKET, parameters.dataBucketName);
                instanceProperties.set(DEFAULT_INGEST_PARTITION_FILE_WRITER_TYPE, "async");
                Objects.requireNonNull(parameters.s3AsyncClient, "s3AsyncClient must not be null");
            });
            return this;
        }

        public IngestCoordinatorTestParameters build() {
            return new IngestCoordinatorTestParameters(this);
        }

        public IngestCoordinator<Record> buildCoordinator() {
            return build().buildCoordinator();
        }
    }

    /**
     * Sets values of properties based on test parameters.
     */
    public interface SetProperties {
        void setProperties(InstanceProperties instanceProperties, TableProperties tableProperties, IngestCoordinatorTestParameters parameters);

        default SetProperties andThen(SetProperties next) {
            return (instanceProperties, tableProperties, parameters) -> {
                setProperties(instanceProperties, tableProperties, parameters);
                next.setProperties(instanceProperties, tableProperties, parameters);
            };
        }
    }
}
