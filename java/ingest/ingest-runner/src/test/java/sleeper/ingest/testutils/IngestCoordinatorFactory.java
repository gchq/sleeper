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
import sleeper.ingest.impl.IngestCoordinator;
import sleeper.ingest.impl.ParquetConfiguration;
import sleeper.ingest.impl.partitionfilewriter.AsyncS3PartitionFileWriterFactory;
import sleeper.ingest.impl.partitionfilewriter.DirectPartitionFileWriterFactory;
import sleeper.ingest.impl.recordbatch.arraylist.ArrayListRecordBatchFactory;
import sleeper.ingest.impl.recordbatch.arrow.ArrowRecordBatchFactory;

import static sleeper.ingest.testutils.IngestCoordinatorTestHelper.parquetConfiguration;
import static sleeper.ingest.testutils.IngestCoordinatorTestHelper.standardIngestCoordinatorBuilder;

public interface IngestCoordinatorFactory {


    IngestCoordinator<Record> createIngestCoordinator(IngestCoordinatorTestParameters parameters);

    static IngestCoordinatorFactory createIngestCoordinatorDirectWriteBackedByArrowWriteToLocalFile() {
        return parameters -> ingestCoordinatorDirectWriteBackedByArrow(parameters, parameters.getLocalFilePrefix());
    }

    static IngestCoordinatorFactory createIngestCoordinatorDirectWriteBackedByArrowWriteToS3() {
        return parameters -> ingestCoordinatorDirectWriteBackedByArrow(parameters, parameters.getAsyncS3Prefix());
    }

    static IngestCoordinatorFactory createIngestCoordinatorAsyncWriteBackedByArrow() {
        return IngestCoordinatorFactory::ingestCoordinatorAsyncWriteBackedByArrow;
    }

    static IngestCoordinatorFactory createIngestCoordinatorDirectWriteBackedByArrayListWriteToLocalFile() {
        return parameters -> ingestCoordinatorDirectWriteBackedByArrayList(parameters, parameters.getLocalFilePrefix());
    }

    static IngestCoordinatorFactory createIngestCoordinatorDirectWriteBackedByArrayListWriteToS3() {
        return parameters -> ingestCoordinatorDirectWriteBackedByArrayList(parameters, parameters.getAsyncS3Prefix());
    }

    private static IngestCoordinator<Record> ingestCoordinatorDirectWriteBackedByArrow(
            IngestCoordinatorTestParameters parameters, String filePathPrefix) {
        try {
            ParquetConfiguration parquetConfiguration = parquetConfiguration(parameters);
            return standardIngestCoordinatorBuilder(parameters,
                    ArrowRecordBatchFactory.builder()
                            .schema(parameters.getSchema())
                            .maxNoOfRecordsToWriteToArrowFileAtOnce(128)
                            .workingBufferAllocatorBytes(16 * 1024 * 1024L)
                            .minBatchBufferAllocatorBytes(16 * 1024 * 1024L)
                            .maxBatchBufferAllocatorBytes(16 * 1024 * 1024L)
                            .maxNoOfBytesToWriteLocally(512 * 1024 * 1024L)
                            .localWorkingDirectory(parameters.getWorkingDir())
                            .buildAcceptingRecords(),
                    DirectPartitionFileWriterFactory.from(
                            parquetConfiguration, filePathPrefix))
                    .build();
        } catch (Exception e) {
            throw new RuntimeException(e);
        }
    }

    private static IngestCoordinator<Record> ingestCoordinatorAsyncWriteBackedByArrow(IngestCoordinatorTestParameters parameters) {
        try {
            ParquetConfiguration parquetConfiguration = parquetConfiguration(parameters);
            return standardIngestCoordinatorBuilder(parameters,
                    ArrowRecordBatchFactory.builder()
                            .schema(parameters.getSchema())
                            .maxNoOfRecordsToWriteToArrowFileAtOnce(128)
                            .workingBufferAllocatorBytes(16 * 1024 * 1024L)
                            .minBatchBufferAllocatorBytes(16 * 1024 * 1024L)
                            .maxBatchBufferAllocatorBytes(16 * 1024 * 1024L)
                            .maxNoOfBytesToWriteLocally(16 * 1024 * 1024L)
                            .localWorkingDirectory(parameters.getWorkingDir())
                            .buildAcceptingRecords(),
                    AsyncS3PartitionFileWriterFactory.builder()
                            .parquetConfiguration(parquetConfiguration)
                            .s3AsyncClient(parameters.getS3AsyncClient())
                            .localWorkingDirectory(parameters.getWorkingDir())
                            .s3BucketName(parameters.getDirectS3Prefix())
                            .build())
                    .build();
        } catch (Exception e) {
            throw new RuntimeException(e);
        }
    }

    private static IngestCoordinator<Record> ingestCoordinatorDirectWriteBackedByArrayList(
            IngestCoordinatorTestParameters parameters, String filePathPrefix) {
        try {
            ParquetConfiguration parquetConfiguration = parquetConfiguration(parameters);
            return standardIngestCoordinatorBuilder(parameters,
                    ArrayListRecordBatchFactory.builder()
                            .parquetConfiguration(parquetConfiguration)
                            .maxNoOfRecordsInLocalStore(1000)
                            .maxNoOfRecordsInMemory(100000)
                            .localWorkingDirectory(parameters.getWorkingDir())
                            .buildAcceptingRecords(),
                    DirectPartitionFileWriterFactory.from(
                            parquetConfiguration, filePathPrefix, parameters.getFileNameGenerator(), parameters.getFileUpdatedTimeSupplier()))
                    .build();
        } catch (Exception e) {
            throw new RuntimeException(e);
        }
    }
}
