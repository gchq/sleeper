/*
 * Copyright 2022-2023 Crown Copyright
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

import sleeper.core.schema.Schema;
import sleeper.statestore.StateStore;

import java.io.IOException;
import java.io.UncheckedIOException;
import java.nio.file.Path;

import static java.nio.file.Files.createTempDirectory;

public class IngestCoordinatorTestParameters {

    private final StateStore stateStore;
    private final Schema schema;
    private final String iteratorClassName;
    private final String workingDir;
    private final String dataBucketName;
    private final Path temporaryFolder;
    private final AwsExternalResource awsResource;

    private IngestCoordinatorTestParameters(Builder builder) {
        stateStore = builder.stateStore;
        schema = builder.schema;
        iteratorClassName = builder.iteratorClassName;
        workingDir = builder.workingDir;
        dataBucketName = builder.dataBucketName;
        temporaryFolder = builder.temporaryFolder;
        awsResource = builder.awsResource;
    }

    public static Builder builder() {
        return new Builder();
    }

    public String getLocalFilePrefix() {
        try {
            return createTempDirectory(temporaryFolder, null).toString();
        } catch (IOException e) {
            throw new UncheckedIOException(e);
        }
    }

    public String getAsyncS3Prefix() {
        return "s3a://" + dataBucketName;
    }

    public String getDirectS3Prefix() {
        return dataBucketName;
    }

    public Configuration getHadoopConfiguration() {
        return awsResource.getHadoopConfiguration();
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
        return awsResource.getS3AsyncClient();
    }

    public static final class Builder {
        private StateStore stateStore;
        private Schema schema;
        private String iteratorClassName;
        private String workingDir;
        private String dataBucketName;
        private Path temporaryFolder;
        private AwsExternalResource awsResource;

        private Builder() {
        }

        public static Builder builder() {
            return new Builder();
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

        public Builder temporaryFolder(Path temporaryFolder) {
            this.temporaryFolder = temporaryFolder;
            return this;
        }

        public Builder awsResource(AwsExternalResource awsResource) {
            this.awsResource = awsResource;
            return this;
        }

        public IngestCoordinatorTestParameters build() {
            return new IngestCoordinatorTestParameters(this);
        }
    }
}
