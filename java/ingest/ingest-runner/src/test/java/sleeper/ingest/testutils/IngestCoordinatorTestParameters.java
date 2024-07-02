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
import java.util.function.Supplier;

import static java.nio.file.Files.createTempDirectory;
import static sleeper.configuration.properties.InstancePropertiesTestHelper.createTestInstanceProperties;
import static sleeper.configuration.properties.table.TablePropertiesTestHelper.createTestTableProperties;
import static sleeper.configuration.properties.table.TableProperty.INGEST_FILE_WRITING_STRATEGY;
import static sleeper.configuration.properties.table.TableProperty.ITERATOR_CLASS_NAME;

public class IngestCoordinatorTestParameters {

    private final StateStore stateStore;
    private final Schema schema;
    private final String iteratorClassName;
    private final String workingDir;
    private final String dataBucketName;
    private final String localFilePrefix;
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
        localFilePrefix = builder.localFilePrefix;
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
        return localFilePrefix;
    }

    public String getAsyncS3Prefix() {
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

    public IngestCoordinator.Builder<Record> ingestCoordinatorBuilder() {
        InstanceProperties instanceProperties = createTestInstanceProperties();
        TableProperties tableProperties = createTestTableProperties(instanceProperties, schema);
        tableProperties.set(ITERATOR_CLASS_NAME, iteratorClassName);
        tableProperties.set(INGEST_FILE_WRITING_STRATEGY, ingestFileWritingStrategy.toString());
        return IngestFactory.builder()
                .instanceProperties(instanceProperties)
                .hadoopConfiguration(hadoopConfiguration)
                .localDir(workingDir)
                .objectFactory(ObjectFactory.noUserJars())
                .s3AsyncClient(s3AsyncClient)
                .stateStoreProvider(new FixedStateStoreProvider(tableProperties, stateStore))
                .build().ingestCoordinatorBuilder(tableProperties);
    }

    public static final class Builder {
        private StateStore stateStore;
        private Schema schema;
        private String iteratorClassName;
        private String workingDir;
        private String dataBucketName;
        private String localFilePrefix;
        private Configuration hadoopConfiguration;
        private S3AsyncClient s3AsyncClient;
        private List<String> fileNames;
        private String tableId;
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

        public Builder localFilePrefix(String localFilePrefix) {
            this.localFilePrefix = localFilePrefix;
            return this;
        }

        public Builder temporaryFolder(Path temporaryFolder) {
            try {
                return localFilePrefix(createTempDirectory(temporaryFolder, null).toString());
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
