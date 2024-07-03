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

import sleeper.configuration.properties.instance.InstanceProperties;
import sleeper.configuration.properties.table.TableProperties;
import sleeper.core.record.Record;
import sleeper.ingest.impl.IngestCoordinator;
import sleeper.ingest.impl.ParquetConfiguration;
import sleeper.ingest.impl.partitionfilewriter.AsyncS3PartitionFileWriterFactory;
import sleeper.ingest.impl.partitionfilewriter.DirectPartitionFileWriterFactory;
import sleeper.ingest.impl.recordbatch.arraylist.ArrayListRecordBatchFactory;
import sleeper.ingest.impl.recordbatch.arrow.ArrowRecordBatchFactory;
import sleeper.ingest.impl.recordbatch.arrow.ArrowRecordWriter;
import sleeper.ingest.impl.recordbatch.arrow.ArrowRecordWriterAcceptingRecords;

import java.util.function.Consumer;

import static sleeper.configuration.properties.InstancePropertiesTestHelper.createTestInstanceProperties;
import static sleeper.configuration.properties.instance.DefaultProperty.DEFAULT_INGEST_PARTITION_FILE_WRITER_TYPE;
import static sleeper.configuration.properties.instance.DefaultProperty.DEFAULT_INGEST_RECORD_BATCH_TYPE;
import static sleeper.configuration.properties.table.TablePropertiesTestHelper.createTestTablePropertiesWithNoSchema;
import static sleeper.ingest.testutils.IngestCoordinatorTestHelper.parquetConfiguration;

public class IngestCoordinatorFactory {

    private IngestCoordinatorFactory() {
    }

    public static <T extends ArrowRecordWriter<U>, U> IngestCoordinator<U> ingestCoordinatorDirectWriteBackedByArrow(
            IngestCoordinatorTestParameters parameters, String filePathPrefix,
            Consumer<ArrowRecordBatchFactory.Builder<U>> arrowConfig,
            T recordWriter) {
        ArrowRecordBatchFactory.Builder<U> arrowConfigBuilder = ArrowRecordBatchFactory.builder()
                .schema(parameters.getSchema())
                .maxNoOfRecordsToWriteToArrowFileAtOnce(128)
                .workingBufferAllocatorBytes(16 * 1024 * 1024L)
                .minBatchBufferAllocatorBytes(16 * 1024 * 1024L)
                .maxBatchBufferAllocatorBytes(16 * 1024 * 1024L)
                .maxNoOfBytesToWriteLocally(512 * 1024 * 1024L)
                .recordWriter(recordWriter)
                .localWorkingDirectory(parameters.getWorkingDir());
        arrowConfig.accept(arrowConfigBuilder);
        InstanceProperties instanceProperties = createTestInstanceProperties();
        instanceProperties.set(DEFAULT_INGEST_RECORD_BATCH_TYPE, "arraylist");
        instanceProperties.set(DEFAULT_INGEST_PARTITION_FILE_WRITER_TYPE, "direct");
        TableProperties tableProperties = createTestTablePropertiesWithNoSchema(instanceProperties);
        return parameters.ingestCoordinatorBuilder(instanceProperties, tableProperties)
                .recordBatchFactory(arrowConfigBuilder.build())
                .partitionFileWriterFactory(DirectPartitionFileWriterFactory.from(
                        parquetConfiguration(parameters), filePathPrefix,
                        parameters.getFileNameGenerator()))
                .build();
    }

    public static IngestCoordinator<Record> ingestCoordinatorDirectWriteBackedByArrow(
            IngestCoordinatorTestParameters parameters, String filePathPrefix) {
        return ingestCoordinatorDirectWriteBackedByArrow(parameters, filePathPrefix, arrowConfig -> {
        }, new ArrowRecordWriterAcceptingRecords());
    }

    public static IngestCoordinator<Record> ingestCoordinatorAsyncWriteBackedByArrow(
            IngestCoordinatorTestParameters parameters) {
        try {
            InstanceProperties instanceProperties = createTestInstanceProperties();
            TableProperties tableProperties = createTestTablePropertiesWithNoSchema(instanceProperties);
            instanceProperties.set(DEFAULT_INGEST_RECORD_BATCH_TYPE, "arraylist");
            instanceProperties.set(DEFAULT_INGEST_PARTITION_FILE_WRITER_TYPE, "direct");
            return parameters.ingestCoordinatorBuilder(instanceProperties, tableProperties)
                    .recordBatchFactory(ArrowRecordBatchFactory.builder()
                            .schema(parameters.getSchema())
                            .maxNoOfRecordsToWriteToArrowFileAtOnce(128)
                            .workingBufferAllocatorBytes(16 * 1024 * 1024L)
                            .minBatchBufferAllocatorBytes(16 * 1024 * 1024L)
                            .maxBatchBufferAllocatorBytes(16 * 1024 * 1024L)
                            .maxNoOfBytesToWriteLocally(16 * 1024 * 1024L)
                            .localWorkingDirectory(parameters.getWorkingDir())
                            .buildAcceptingRecords())
                    .partitionFileWriterFactory(AsyncS3PartitionFileWriterFactory.builder()
                            .parquetConfiguration(parquetConfiguration(parameters))
                            .s3AsyncClient(parameters.getS3AsyncClient())
                            .localWorkingDirectory(parameters.getWorkingDir())
                            .s3BucketName(parameters.getDataBucketName())
                            .fileNameGenerator(parameters.getFileNameGenerator())
                            .filePathPrefix(parameters.getTableId())
                            .build())
                    .build();
        } catch (Exception e) {
            throw new RuntimeException(e);
        }
    }

    public static IngestCoordinator<Record> ingestCoordinatorDirectWriteBackedByArrayList(
            IngestCoordinatorTestParameters parameters, String filePathPrefix) {
        return ingestCoordinatorDirectWriteBackedByArrayList(parameters, filePathPrefix, 100000, 1000);
    }

    public static IngestCoordinator<Record> ingestCoordinatorDirectWriteBackedByArrayList(
            IngestCoordinatorTestParameters parameters, String filePathPrefix,
            int maxRecordsInMemory, long maxRecordsToWriteToLocalStore) {
        try {
            ParquetConfiguration parquetConfiguration = parquetConfiguration(parameters);
            ArrayListRecordBatchFactory.Builder<?> arrayListRecordBatch = ArrayListRecordBatchFactory
                    .builder()
                    .parquetConfiguration(parquetConfiguration)
                    .maxNoOfRecordsInLocalStore(maxRecordsToWriteToLocalStore)
                    .maxNoOfRecordsInMemory(maxRecordsInMemory)
                    .localWorkingDirectory(parameters.getWorkingDir());
            InstanceProperties instanceProperties = createTestInstanceProperties();
            TableProperties tableProperties = createTestTablePropertiesWithNoSchema(instanceProperties);
            instanceProperties.set(DEFAULT_INGEST_RECORD_BATCH_TYPE, "arraylist");
            instanceProperties.set(DEFAULT_INGEST_PARTITION_FILE_WRITER_TYPE, "direct");
            return parameters.ingestCoordinatorBuilder(instanceProperties, tableProperties)
                    .recordBatchFactory(arrayListRecordBatch.buildAcceptingRecords())
                    .partitionFileWriterFactory(
                            DirectPartitionFileWriterFactory.from(
                                    parquetConfiguration, filePathPrefix,
                                    parameters.getFileNameGenerator()))
                    .build();
        } catch (Exception e) {
            throw new RuntimeException(e);
        }
    }
}
