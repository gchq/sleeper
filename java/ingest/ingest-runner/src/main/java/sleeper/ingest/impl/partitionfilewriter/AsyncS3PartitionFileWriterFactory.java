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

import sleeper.configuration.properties.table.TableProperties;
import sleeper.core.partition.Partition;
import sleeper.ingest.impl.ParquetConfiguration;
import software.amazon.awssdk.services.s3.S3AsyncClient;

import java.io.IOException;
import java.util.Objects;

import static sleeper.configuration.properties.table.TableProperty.DATA_BUCKET;

public class AsyncS3PartitionFileWriterFactory implements PartitionFileWriterFactory {

    private final ParquetConfiguration parquetConfiguration;
    private final String s3BucketName;
    private final String localWorkingDirectory;
    private final S3AsyncClient s3AsyncClient;
    private final boolean closeS3AsyncClient;

    private AsyncS3PartitionFileWriterFactory(Builder builder) {
        parquetConfiguration = Objects.requireNonNull(builder.parquetConfiguration, "parquetWriterConfiguration must not be null");
        s3BucketName = Objects.requireNonNull(builder.s3BucketName, "s3BucketName must not be null");
        localWorkingDirectory = Objects.requireNonNull(builder.localWorkingDirectory, "localWorkingDirectory must not be null");
        if (builder.s3AsyncClient != null) {
            s3AsyncClient = builder.s3AsyncClient;
            closeS3AsyncClient = false;
        } else {
            s3AsyncClient = S3AsyncClient.create();
            closeS3AsyncClient = true;
        }
    }

    public static Builder builder() {
        return new Builder();
    }

    public static Builder builderWith(TableProperties tableProperties) {
        return builder().tableProperties(tableProperties);
    }

    @Override
    public PartitionFileWriter createPartitionFileWriter(Partition partition) {
        try {
            return new AsyncS3PartitionFileWriter(
                    partition,
                    parquetConfiguration,
                    s3BucketName,
                    s3AsyncClient,
                    localWorkingDirectory);
        } catch (IOException e) {
            throw new RuntimeException(e);
        }
    }

    @Override
    public void close() throws Exception {
        if (closeS3AsyncClient) {
            s3AsyncClient.close();
        }
    }

    public static final class Builder {
        private ParquetConfiguration parquetConfiguration;
        private S3AsyncClient s3AsyncClient;
        private String s3BucketName;
        private String localWorkingDirectory;

        private Builder() {
        }

        public Builder parquetConfiguration(ParquetConfiguration parquetConfiguration) {
            this.parquetConfiguration = parquetConfiguration;
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

        public Builder localWorkingDirectory(String localWorkingDirectory) {
            this.localWorkingDirectory = localWorkingDirectory;
            return this;
        }

        public Builder tableProperties(TableProperties tableProperties) {
            return s3BucketName(tableProperties.get(DATA_BUCKET));
        }

        public AsyncS3PartitionFileWriterFactory build() {
            return new AsyncS3PartitionFileWriterFactory(this);
        }
    }
}
