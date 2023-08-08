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

import sleeper.core.record.Record;
import sleeper.core.schema.Schema;
import sleeper.ingest.impl.IngestCoordinator;
import sleeper.ingest.impl.ParquetConfiguration;
import sleeper.ingest.impl.partitionfilewriter.AsyncS3PartitionFileWriterFactory;
import sleeper.ingest.impl.partitionfilewriter.DirectPartitionFileWriterFactory;
import sleeper.ingest.impl.recordbatch.arraylist.ArrayListRecordBatchFactory;
import sleeper.ingest.impl.recordbatch.arrow.ArrowRecordBatchFactory;
import sleeper.statestore.StateStore;

import java.io.IOException;
import java.io.UncheckedIOException;
import java.nio.file.Path;
import java.util.function.Function;

import static java.nio.file.Files.createTempDirectory;
import static sleeper.ingest.testutils.IngestCoordinatorTestHelper.parquetConfiguration;
import static sleeper.ingest.testutils.IngestCoordinatorTestHelper.standardIngestCoordinatorBuilder;

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

    public static Function<IngestCoordinatorTestParameters, IngestCoordinator<Record>> createIngestCoordinatorDirectWriteBackedByArrowWriteToLocalFile() {
        return parameters -> parameters.ingestCoordinatorDirectWriteBackedByArrow(parameters.localFilePrefix());
    }

    public static Function<IngestCoordinatorTestParameters, IngestCoordinator<Record>> createIngestCoordinatorDirectWriteBackedByArrowWriteToS3() {
        return parameters -> parameters.ingestCoordinatorDirectWriteBackedByArrow(parameters.asyncS3Prefix());
    }

    public static Function<IngestCoordinatorTestParameters, IngestCoordinator<Record>> createIngestCoordinatorAsyncWriteBackedByArrow() {
        return IngestCoordinatorTestParameters::ingestCoordinatorAsyncWriteBackedByArrow;
    }

    public static Function<IngestCoordinatorTestParameters, IngestCoordinator<Record>> createIngestCoordinatorDirectWriteBackedByArrayListWriteToLocalFile() {
        return parameters -> parameters.ingestCoordinatorDirectWriteBackedByArrayList(parameters.localFilePrefix());
    }

    public static Function<IngestCoordinatorTestParameters, IngestCoordinator<Record>> createIngestCoordinatorDirectWriteBackedByArrayListWriteToS3() {
        return parameters -> parameters.ingestCoordinatorDirectWriteBackedByArrayList(parameters.asyncS3Prefix());
    }

    public String localFilePrefix() {
        try {
            return createTempDirectory(temporaryFolder, null).toString();
        } catch (IOException e) {
            throw new UncheckedIOException(e);
        }
    }

    public String asyncS3Prefix() {
        return "s3a://" + dataBucketName;
    }

    private IngestCoordinator<Record> ingestCoordinatorDirectWriteBackedByArrow(String filePathPrefix) {
        try {
            ParquetConfiguration parquetConfiguration = parquetConfiguration(
                    schema, awsResource.getHadoopConfiguration());
            return standardIngestCoordinatorBuilder(
                    stateStore, schema,
                    ArrowRecordBatchFactory.builder()
                            .schema(schema)
                            .maxNoOfRecordsToWriteToArrowFileAtOnce(128)
                            .workingBufferAllocatorBytes(16 * 1024 * 1024L)
                            .minBatchBufferAllocatorBytes(16 * 1024 * 1024L)
                            .maxBatchBufferAllocatorBytes(16 * 1024 * 1024L)
                            .maxNoOfBytesToWriteLocally(512 * 1024 * 1024L)
                            .localWorkingDirectory(workingDir)
                            .buildAcceptingRecords(),
                    DirectPartitionFileWriterFactory.from(
                            parquetConfiguration, filePathPrefix))
                    .iteratorClassName(iteratorClassName)
                    .build();
        } catch (Exception e) {
            throw new RuntimeException(e);
        }
    }


    private IngestCoordinator<Record> ingestCoordinatorAsyncWriteBackedByArrow() {
        try {
            ParquetConfiguration parquetConfiguration = parquetConfiguration(
                    schema, awsResource.getHadoopConfiguration());
            return standardIngestCoordinatorBuilder(
                    stateStore, schema,
                    ArrowRecordBatchFactory.builder()
                            .schema(schema)
                            .maxNoOfRecordsToWriteToArrowFileAtOnce(128)
                            .workingBufferAllocatorBytes(16 * 1024 * 1024L)
                            .minBatchBufferAllocatorBytes(16 * 1024 * 1024L)
                            .maxBatchBufferAllocatorBytes(16 * 1024 * 1024L)
                            .maxNoOfBytesToWriteLocally(16 * 1024 * 1024L)
                            .localWorkingDirectory(workingDir)
                            .buildAcceptingRecords(),
                    AsyncS3PartitionFileWriterFactory.builder()
                            .parquetConfiguration(parquetConfiguration)
                            .s3AsyncClient(awsResource.getS3AsyncClient())
                            .localWorkingDirectory(workingDir)
                            .s3BucketName(dataBucketName)
                            .build())
                    .iteratorClassName(iteratorClassName)
                    .build();
        } catch (Exception e) {
            throw new RuntimeException(e);
        }
    }

    private IngestCoordinator<Record> ingestCoordinatorDirectWriteBackedByArrayList(String filePathPrefix) {
        try {
            ParquetConfiguration parquetConfiguration = parquetConfiguration(
                    schema, awsResource.getHadoopConfiguration());
            return standardIngestCoordinatorBuilder(
                    stateStore, schema,
                    ArrayListRecordBatchFactory.builder()
                            .parquetConfiguration(parquetConfiguration)
                            .maxNoOfRecordsInLocalStore(1000)
                            .maxNoOfRecordsInMemory(100000)
                            .localWorkingDirectory(workingDir)
                            .buildAcceptingRecords(),
                    DirectPartitionFileWriterFactory.from(
                            parquetConfiguration, filePathPrefix))
                    .iteratorClassName(iteratorClassName)
                    .build();
        } catch (Exception e) {
            throw new RuntimeException(e);
        }
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
