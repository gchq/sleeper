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
import sleeper.core.record.Record;
import sleeper.core.schema.Schema;
import sleeper.core.statestore.StateStore;
import sleeper.ingest.IngestFactory;
import sleeper.ingest.impl.IngestCoordinator;
import sleeper.statestore.FixedStateStoreProvider;

import java.io.IOException;
import java.io.UncheckedIOException;
import java.nio.file.Path;
import java.util.List;
import java.util.UUID;
import java.util.function.Consumer;
import java.util.function.Supplier;

import static java.nio.file.Files.createTempDirectory;
import static sleeper.configuration.properties.InstancePropertiesTestHelper.createTestInstanceProperties;
import static sleeper.configuration.properties.instance.CdkDefinedInstanceProperty.DATA_BUCKET;
import static sleeper.configuration.properties.instance.CommonProperty.FILE_SYSTEM;
import static sleeper.configuration.properties.instance.DefaultProperty.DEFAULT_INGEST_PARTITION_FILE_WRITER_TYPE;
import static sleeper.configuration.properties.instance.DefaultProperty.DEFAULT_INGEST_RECORD_BATCH_TYPE;
import static sleeper.configuration.properties.table.TablePropertiesTestHelper.createTestTablePropertiesWithNoSchema;
import static sleeper.configuration.properties.table.TableProperty.INGEST_FILE_WRITING_STRATEGY;
import static sleeper.configuration.properties.table.TableProperty.ITERATOR_CLASS_NAME;
import static sleeper.configuration.properties.table.TableProperty.TABLE_ID;

public class IngestCoordinatorTestParameters {

    private final StateStore stateStore;
    private final Schema schema;
    private final String iteratorClassName;
    private final String workingDir;
    private final String dataBucketName;
    private final String localDataPath;
    private final Configuration hadoopConfiguration;
    private final S3AsyncClient s3AsyncClient;
    private final List<String> fileNames;
    private final String tableId;
    private final IngestFileWritingStrategy ingestFileWritingStrategy;

    private IngestCoordinatorTestParameters(Builder builder) {
        stateStore = builder.stateStore;
        schema = builder.schema;
        iteratorClassName = builder.iteratorClassName;
        workingDir = builder.workingDir;
        dataBucketName = builder.dataBucketName;
        localDataPath = builder.localDataPath;
        hadoopConfiguration = builder.hadoopConfiguration;
        s3AsyncClient = builder.s3AsyncClient;
        fileNames = builder.fileNames;
        tableId = builder.tableId;
        ingestFileWritingStrategy = builder.ingestFileWritingStrategy;
    }

    public static Builder builder() {
        return new Builder();
    }

    public String getLocalFilePrefix() {
        return "file://" + localDataPath + "/" + tableId;
    }

    public String getS3Prefix() {
        return "s3a://" + dataBucketName + "/" + tableId;
    }

    public String getDataBucketName() {
        return dataBucketName;
    }

    public Configuration getHadoopConfiguration() {
        return hadoopConfiguration;
    }

    public StateStore getStateStore() {
        return stateStore;
    }

    public Schema getSchema() {
        return schema;
    }

    public String getIteratorClassName() {
        return iteratorClassName;
    }

    public String getWorkingDir() {
        return workingDir;
    }

    public S3AsyncClient getS3AsyncClient() {
        return s3AsyncClient;
    }

    public Supplier<String> getFileNameGenerator() {
        return fileNames.iterator()::next;
    }

    public String getTableId() {
        return tableId;
    }

    public IngestFileWritingStrategy getIngestFileWritingStrategy() {
        return ingestFileWritingStrategy;
    }

    public CoordinatorConfig ingestCoordinatorConfig() {
        return new CoordinatorConfig(createTestInstanceProperties());
    }

    public IngestCoordinator.Builder<Record> ingestCoordinatorBuilder(InstanceProperties instanceProperties, TableProperties tableProperties) {
        tableProperties.set(TABLE_ID, tableId);
        tableProperties.set(ITERATOR_CLASS_NAME, iteratorClassName);
        tableProperties.set(INGEST_FILE_WRITING_STRATEGY, ingestFileWritingStrategy.toString());
        tableProperties.setSchema(schema);
        return IngestFactory.builder()
                .instanceProperties(instanceProperties)
                .hadoopConfiguration(hadoopConfiguration)
                .localDir(workingDir)
                .objectFactory(ObjectFactory.noUserJars())
                .s3AsyncClient(s3AsyncClient)
                .stateStoreProvider(new FixedStateStoreProvider(tableProperties, stateStore))
                .fileNameGenerator(getFileNameGenerator())
                .build().ingestCoordinatorBuilder(tableProperties);
    }

    public class CoordinatorConfig {

        private final InstanceProperties instanceProperties;

        private CoordinatorConfig(InstanceProperties instanceProperties) {
            this.instanceProperties = instanceProperties;
        }

        public CoordinatorConfig backedByArrow() {
            instanceProperties.set(DEFAULT_INGEST_RECORD_BATCH_TYPE, "arrow");
            return this;
        }

        public CoordinatorConfig backedByArrayList() {
            instanceProperties.set(DEFAULT_INGEST_RECORD_BATCH_TYPE, "arraylist");
            return this;
        }

        public CoordinatorConfig localDirectWrite() {
            instanceProperties.set(FILE_SYSTEM, "file://");
            instanceProperties.set(DATA_BUCKET, localDataPath);
            instanceProperties.set(DEFAULT_INGEST_PARTITION_FILE_WRITER_TYPE, "direct");
            return this;
        }

        public CoordinatorConfig s3DirectWrite() {
            instanceProperties.set(FILE_SYSTEM, "s3a://");
            instanceProperties.set(DATA_BUCKET, getDataBucketName());
            instanceProperties.set(DEFAULT_INGEST_PARTITION_FILE_WRITER_TYPE, "direct");
            return this;
        }

        public CoordinatorConfig s3AsyncWrite() {
            instanceProperties.set(FILE_SYSTEM, "s3a://");
            instanceProperties.set(DATA_BUCKET, getDataBucketName());
            instanceProperties.set(DEFAULT_INGEST_PARTITION_FILE_WRITER_TYPE, "async");
            return this;
        }

        public CoordinatorConfig setInstanceProperties(Consumer<InstanceProperties> config) {
            config.accept(instanceProperties);
            return this;
        }

        public IngestCoordinator<Record> buildCoordinator() {
            TableProperties tableProperties = createTestTablePropertiesWithNoSchema(instanceProperties);
            return ingestCoordinatorBuilder(instanceProperties, tableProperties).build();
        }

    }

    public static final class Builder {
        private StateStore stateStore;
        private Schema schema;
        private String iteratorClassName;
        private String workingDir;
        private String dataBucketName;
        private String localDataPath;
        private Configuration hadoopConfiguration;
        private S3AsyncClient s3AsyncClient;
        private List<String> fileNames;
        private String tableId = UUID.randomUUID().toString();
        private IngestFileWritingStrategy ingestFileWritingStrategy;

        private Builder() {
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
            this.ingestFileWritingStrategy = ingestFileWritingStrategy;
            return this;
        }

        public IngestCoordinatorTestParameters build() {
            return new IngestCoordinatorTestParameters(this);
        }
    }
}
