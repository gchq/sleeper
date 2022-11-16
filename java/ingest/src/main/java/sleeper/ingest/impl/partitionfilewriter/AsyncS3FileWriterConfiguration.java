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
package sleeper.ingest.impl.partitionfilewriter;

import org.apache.hadoop.conf.Configuration;
import sleeper.configuration.properties.table.TableProperties;
import sleeper.core.partition.Partition;
import software.amazon.awssdk.services.s3.S3AsyncClient;

import java.io.IOException;
import java.util.Objects;

import static sleeper.configuration.properties.table.TableProperty.COMPRESSION_CODEC;
import static sleeper.configuration.properties.table.TableProperty.PAGE_SIZE;
import static sleeper.configuration.properties.table.TableProperty.ROW_GROUP_SIZE;

public class AsyncS3FileWriterConfiguration implements FileWriterConfiguration {

    private final TableProperties tableProperties;
    private final Configuration hadoopConfiguration;
    private final String localWorkingDirectory;
    private final S3AsyncClient s3AsyncClient;
    private final String s3BucketName;

    private AsyncS3FileWriterConfiguration(Builder builder) {
        tableProperties = Objects.requireNonNull(builder.tableProperties, "tableProperties must not be null");
        hadoopConfiguration = Objects.requireNonNull(builder.hadoopConfiguration, "hadoopConfiguration must not be null");
        localWorkingDirectory = Objects.requireNonNull(builder.localWorkingDirectory, "localWorkingDirectory must not be null");
        s3AsyncClient = Objects.requireNonNull(builder.s3AsyncClient, "s3AsyncClient must not be null");
        s3BucketName = Objects.requireNonNull(builder.s3BucketName, "s3BucketName must not be null");
    }

    @Override
    public PartitionFileWriter createPartitionFileWriter(Partition partition) {
        try {
            return new AsyncS3PartitionFileWriter(
                    tableProperties.getSchema(),
                    partition,
                    tableProperties.getInt(ROW_GROUP_SIZE),
                    tableProperties.getInt(PAGE_SIZE),
                    tableProperties.get(COMPRESSION_CODEC),
                    hadoopConfiguration,
                    s3BucketName,
                    s3AsyncClient,
                    localWorkingDirectory);
        } catch (IOException e) {
            throw new RuntimeException(e);
        }
    }

    public static final class Builder {
        private TableProperties tableProperties;
        private Configuration hadoopConfiguration;
        private String localWorkingDirectory;
        private S3AsyncClient s3AsyncClient;
        private String s3BucketName;

        private Builder() {
        }

        public static Builder builder() {
            return new Builder();
        }

        public Builder tableProperties(TableProperties tableProperties) {
            this.tableProperties = tableProperties;
            return this;
        }

        public Builder hadoopConfiguration(Configuration hadoopConfiguration) {
            this.hadoopConfiguration = hadoopConfiguration;
            return this;
        }

        public Builder localWorkingDirectory(String localWorkingDirectory) {
            this.localWorkingDirectory = localWorkingDirectory;
            return this;
        }

        public Builder s3AsyncClient(S3AsyncClient s3AsyncClient) {
            this.s3AsyncClient = s3AsyncClient;
            return this;
        }

        public Builder s3BucketName(String s3BucketName) {
            this.s3BucketName = s3BucketName;
            return this;
        }

        public AsyncS3FileWriterConfiguration build() {
            return new AsyncS3FileWriterConfiguration(this);
        }
    }
}
